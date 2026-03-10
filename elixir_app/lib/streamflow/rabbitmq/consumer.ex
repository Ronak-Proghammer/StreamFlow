defmodule Streamflow.RabbitMQ.Consumer do
  @moduledoc """
  GenServer that maintains a persistent RabbitMQ connection
  and consumes events, writing them to PostgreSQL via Ecto.

  Fault tolerance strategy:
  - On connection failure: retry with exponential backoff
  - On message processing failure: NACK the message (goes to dead letter queue)
  - On process crash: supervisor restarts us automatically
  """

  use GenServer
  require Logger

  alias Streamflow.Events

  @exchange        "streamflow.events"
  @queue           "streamflow.events.queue"
  @dead_letter_q   "streamflow.events.dead_letter"
  @reconnect_base  1_000   # 1 second base
  @reconnect_max   30_000  # 30 seconds max
  @prefetch_count  10      # process up to 10 msgs at once

  # ============================================================
  # PUBLIC API
  # ============================================================

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Get consumer status and stats."
  def status do
    GenServer.call(__MODULE__, :status)
  end

  @doc "Manually trigger reconnection (useful for testing)."
  def reconnect do
    GenServer.cast(__MODULE__, :reconnect)
  end

  # ============================================================
  # GENSERVER CALLBACKS
  # ============================================================

  @impl true
  def init(_opts) do
    # Schedule connection attempt immediately after init
    # We don't connect IN init because if it fails,
    # the supervisor would restart us and we'd enter a fast
    # crash loop. Sending ourselves a message lets init succeed,
    # then we can retry with backoff.
    send(self(), {:connect, 0})

    state = %{
      connection:          nil,
      channel:             nil,
      status:              :disconnected,
      messages_received:   0,
      messages_processed:  0,
      messages_failed:     0,
      last_error:          nil,
      reconnect_attempts:  0
    }

    {:ok, state}
  end

  # ============================================================
  # HANDLE_INFO: Connection attempts
  # ============================================================

  @impl true
  def handle_info({:connect, attempt}, state) do
    rabbitmq_url = System.get_env("RABBITMQ_URL", "amqp://guest:guest@localhost/")

    Logger.info("Connecting to RabbitMQ (attempt #{attempt + 1})...")

    case connect(rabbitmq_url) do
      {:ok, connection, channel} ->
        Logger.info("✅ RabbitMQ connected successfully")

        # Monitor the connection process so we know if it goes down
        Process.monitor(connection.pid)

        new_state = %{state |
          connection:         connection,
          channel:            channel,
          status:             :connected,
          reconnect_attempts: 0,
          last_error:         nil
        }

        {:noreply, new_state}

      {:error, reason} ->
        Logger.error("❌ RabbitMQ connection failed: #{inspect(reason)}")

        # Exponential backoff: 1s, 2s, 4s, 8s... capped at 30s
        delay = min(@reconnect_base * :math.pow(2, attempt) |> round(), @reconnect_max)
        Logger.info("Retrying in #{delay}ms...")
        Process.send_after(self(), {:connect, attempt + 1}, delay)

        {:noreply, %{state |
          status:             :connecting,
          last_error:         inspect(reason),
          reconnect_attempts: attempt + 1
        }}
    end
  end

  # ============================================================
  # HANDLE_INFO: RabbitMQ message delivery
  # This is called for EVERY message RabbitMQ sends us
  # ============================================================

  @impl true
  def handle_info({:basic_deliver, payload, meta}, state) do
    new_received = state.messages_received + 1

    Logger.debug("📨 Message ##{new_received} received (#{byte_size(payload)} bytes)")

    {processed, failed} =
      case process_message(payload) do
        :ok ->
          # ACK = "I processed this, remove it from the queue"
          AMQP.Basic.ack(state.channel, meta.delivery_tag)
          {state.messages_processed + 1, state.messages_failed}

        {:error, reason} ->
          Logger.error("Failed to process message: #{inspect(reason)}")
          # NACK with requeue: false sends to dead letter queue
          # requeue: true would re-deliver — risky if the msg itself is bad
          AMQP.Basic.nack(state.channel, meta.delivery_tag, requeue: false)
          {state.messages_processed, state.messages_failed + 1}
      end

    {:noreply, %{state |
      messages_received:  new_received,
      messages_processed: processed,
      messages_failed:    failed
    }}
  end

  # ============================================================
  # HANDLE_INFO: Connection dropped
  # Erlang's Process.monitor sends this when the monitored
  # process dies. We use it to detect RabbitMQ disconnections.
  # ============================================================

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    Logger.error("💔 RabbitMQ connection lost: #{inspect(reason)}")
    # Schedule reconnection immediately
    send(self(), {:connect, 0})
    {:noreply, %{state | connection: nil, channel: nil, status: :disconnected}}
  end

  # Ignore RabbitMQ protocol messages we don't care about
  def handle_info({:basic_consume_ok, _meta}, state), do: {:noreply, state}
  def handle_info({:basic_cancel, _meta}, state),     do: {:noreply, state}
  def handle_info({:basic_cancel_ok, _meta}, state),  do: {:noreply, state}

  # ============================================================
  # HANDLE_CALL: Status
  # ============================================================

  @impl true
  def handle_call(:status, _from, state) do
    reply = Map.take(state, [
      :status, :messages_received, :messages_processed,
      :messages_failed, :last_error, :reconnect_attempts
    ])
    {:reply, reply, state}
  end

  # ============================================================
  # HANDLE_CAST: Manual reconnect
  # ============================================================

  @impl true
  def handle_cast(:reconnect, state) do
    if state.connection, do: AMQP.Connection.close(state.connection)
    send(self(), {:connect, 0})
    {:noreply, %{state | connection: nil, channel: nil, status: :disconnected}}
  end

  # ============================================================
  # TERMINATE: Called when the GenServer is shutting down
  # Clean up the connection gracefully
  # ============================================================

  @impl true
  def terminate(_reason, state) do
    if state.connection do
      Logger.info("Closing RabbitMQ connection...")
      AMQP.Connection.close(state.connection)
    end
    :ok
  end

  # ============================================================
  # PRIVATE: Connection setup
  # ============================================================

  defp connect(url) do
    with {:ok, connection} <- AMQP.Connection.open(url),
         {:ok, channel}    <- AMQP.Channel.open(connection),
         :ok               <- setup_topology(channel),
         {:ok, _tag}       <- AMQP.Basic.consume(channel, @queue) do
      {:ok, connection, channel}
    end
  end

  defp setup_topology(channel) do
    # Declare dead letter exchange first
    AMQP.Exchange.declare(channel, "#{@exchange}.dead_letter", :direct, durable: true)
    AMQP.Queue.declare(channel, @dead_letter_q, durable: true)
    AMQP.Queue.bind(channel, @dead_letter_q, "#{@exchange}.dead_letter",
      routing_key: "dead_letter"
    )

    # Declare main exchange
    AMQP.Exchange.declare(channel, @exchange, :direct, durable: true)

    # Declare main queue with dead letter routing
    AMQP.Queue.declare(channel, @queue,
      durable: true,
      arguments: [
        # Failed messages go to the dead letter exchange
        {"x-dead-letter-exchange", :longstr, "#{@exchange}.dead_letter"},
        {"x-dead-letter-routing-key", :longstr, "dead_letter"},
        # Max queue length — oldest messages dropped if exceeded
        {"x-max-length", :long, 100_000}
      ]
    )

    AMQP.Queue.bind(channel, @queue, @exchange, routing_key: "events")

    # Prefetch: don't send more than N unacknowledged messages
    AMQP.Basic.qos(channel, prefetch_count: @prefetch_count)

    :ok
  end

  # ============================================================
  # PRIVATE: Message processing
  # ============================================================

  defp process_message(payload) do
    with {:ok, raw}   <- Jason.decode(payload),
         {:ok, attrs} <- validate_and_normalize(raw),
         {:ok, _event} <- Events.create_event(attrs) do
      :ok
    else
      {:error, %Jason.DecodeError{} = e} ->
        {:error, {:json_decode, Exception.message(e)}}
      {:error, %Ecto.Changeset{} = cs} ->
        {:error, {:validation, cs.errors}}
      {:error, reason} ->
        {:error, reason}
    end
  end

  # Convert raw JSON map to the shape our Event schema expects
  defp validate_and_normalize(raw) do
    attrs = %{
      source_service: raw["source_service"],
      event_type:     raw["event_type"] && String.to_atom(raw["event_type"]),
      user_id:        raw["user_id"],
      session_id:     raw["session_id"],
      event_data:     raw["event_data"] || %{},
      client_ip:      raw["client_ip"],
      occurred_at:    parse_datetime(raw["occurred_at"])
    }

    if attrs.source_service && attrs.event_type && attrs.occurred_at do
      {:ok, attrs}
    else
      {:error, :missing_required_fields}
    end
  end

  defp parse_datetime(nil), do: DateTime.utc_now()
  defp parse_datetime(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _}   -> dt
      {:error, _}    -> DateTime.utc_now()
    end
  end
end
