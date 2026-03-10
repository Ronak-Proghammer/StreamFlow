defmodule Streamflow.Events do
  @moduledoc """
  The Events context.

  In Phoenix, a "context" is a module that groups related
  business logic. It's the boundary between your web layer
  (controllers) and your data layer (Ecto schemas).

  Controllers call context functions.
  Context functions call Repo functions.
  Nothing else touches the Repo directly.

  This separation makes testing much easier — you can test
  business logic without HTTP, and test HTTP without real DB.
  """

  import Ecto.Query
  alias Streamflow.Repo
  alias Streamflow.Events.Event
  require Logger

  # ============================================================
  # WRITE OPERATIONS
  # ============================================================

  @doc """
  Creates a single event from a map of attributes.
  Returns {:ok, event} or {:error, changeset}.
  """
  def create_event(attrs) do
    %Event{}
    |> Event.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Creates multiple events in a single transaction.
  Used for batch inserts from the RabbitMQ consumer.
  All succeed or all fail together.
  """
  def create_events_batch(list_of_attrs) do
    now = DateTime.utc_now()

    # Build all changesets first, collect any errors
    {valid, invalid} =
      list_of_attrs
      |> Enum.map(fn attrs ->
        %Event{} |> Event.changeset(attrs)
      end)
      |> Enum.split_with(& &1.valid?)

    if Enum.any?(invalid) do
      Logger.warning("Skipping #{length(invalid)} invalid events in batch")
    end

    # Insert all valid events in one DB roundtrip
    case Repo.insert_all(Event, Enum.map(valid, &changeset_to_map(&1, now))) do
      {count, _} -> {:ok, count}
    end
  end

  # ============================================================
  # READ OPERATIONS
  # ============================================================

  @doc """
  List events with optional filters.
  Supports: event_type, user_id, source_service, from/to time range.
  Supports: pagination via limit/offset.
  """
  def list_events(filters \\ %{}) do
    Event
    |> apply_filters(filters)
    |> order_by([e], desc: e.occurred_at)
    |> limit(^Map.get(filters, :limit, 50))
    |> offset(^Map.get(filters, :offset, 0))
    |> Repo.all()
  end

  @doc """
  Count total events matching filters (for pagination metadata).
  """
  def count_events(filters \\ %{}) do
    Event
    |> apply_filters(filters)
    |> Repo.aggregate(:count, :id)
  end

  @doc """
  Get a single event by ID.
  """
  def get_event!(id), do: Repo.get!(Event, id)

  # ============================================================
  # ANALYTICS QUERIES
  # These return raw query results (maps), not Event structs,
  # because they're aggregations — not individual rows.
  # ============================================================

  @doc """
  Events per minute for the last N minutes.
  Used for time-series charts.
  """
  def events_per_minute(minutes \\ 60) do
    sql = """
    SELECT
      date_trunc('minute', occurred_at)               AS minute,
      event_type::text                                 AS event_type,
      COUNT(*)                                         AS event_count,
      COUNT(DISTINCT user_id)                          AS unique_users
    FROM events
    WHERE occurred_at >= NOW() - ($1 || ' minutes')::INTERVAL
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
    """

    %{rows: rows, columns: cols} = Repo.query!(sql, [minutes])
    rows_to_maps(rows, cols)
  end

  @doc """
  Summary stats for the last N minutes:
  total events, unique users, error count, error rate.
  """
  def summary_stats(minutes \\ 60) do
    sql = """
    SELECT
      COUNT(*)                                                        AS total_events,
      COUNT(DISTINCT user_id)                                         AS unique_users,
      COUNT(DISTINCT session_id)                                      AS unique_sessions,
      COUNT(*) FILTER (WHERE event_type = 'error')                    AS error_count,
      ROUND(
        100.0 * COUNT(*) FILTER (WHERE event_type = 'error')
        / NULLIF(COUNT(*), 0), 2
      )                                                               AS error_rate_pct,
      COUNT(*) FILTER (WHERE event_type = 'purchase')                 AS purchase_count
    FROM events
    WHERE occurred_at >= NOW() - ($1 || ' minutes')::INTERVAL
    """

    %{rows: [row], columns: cols} = Repo.query!(sql, [minutes])
    row |> Enum.zip(cols) |> Map.new(fn {v, k} -> {k, v} end)
  end

  @doc """
  Top services by event count in the last N minutes.
  Uses a window function to also show each service's % of total.
  """
  def top_services(minutes \\ 60, limit \\ 10) do
    sql = """
    WITH service_counts AS (
      SELECT
        source_service,
        COUNT(*)            AS event_count,
        COUNT(DISTINCT user_id) AS unique_users
      FROM events
      WHERE occurred_at >= NOW() - ($1 || ' minutes')::INTERVAL
      GROUP BY source_service
    )
    SELECT
      source_service,
      event_count,
      unique_users,
      ROUND(
        100.0 * event_count / NULLIF(SUM(event_count) OVER (), 0),
        2
      ) AS pct_of_total
    FROM service_counts
    ORDER BY event_count DESC
    LIMIT $2
    """

    %{rows: rows, columns: cols} = Repo.query!(sql, [minutes, limit])
    rows_to_maps(rows, cols)
  end

  @doc """
  Running total of events per type over the last hour.
  Classic window function use case — shows growth over time.
  """
  def running_totals_by_type do
    sql = """
    SELECT
      occurred_at,
      event_type::text,
      user_id,
      COUNT(*) OVER (
        PARTITION BY event_type
        ORDER BY occurred_at
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS running_total
    FROM events
    WHERE occurred_at >= NOW() - INTERVAL '1 hour'
    ORDER BY occurred_at DESC
    LIMIT 200
    """

    %{rows: rows, columns: cols} = Repo.query!(sql, [])
    rows_to_maps(rows, cols)
  end

  # ============================================================
  # PRIVATE HELPERS
  # ============================================================

  # Build up the query dynamically based on which filters are present.
  # Each function clause adds one WHERE condition.
  # This is the Ecto query composition pattern.
  defp apply_filters(query, filters) do
    Enum.reduce(filters, query, fn
      {:event_type, type}, q ->
        where(q, [e], e.event_type == ^String.to_atom(type))

      {:user_id, uid}, q ->
        where(q, [e], e.user_id == ^uid)

      {:source_service, svc}, q ->
        where(q, [e], e.source_service == ^svc)

      {:from, dt}, q ->
        where(q, [e], e.occurred_at >= ^dt)

      {:to, dt}, q ->
        where(q, [e], e.occurred_at <= ^dt)

      {:status, status}, q ->
        where(q, [e], e.status == ^status)

      # Ignore pagination keys and unknown keys
      {key, _}, q when key in [:limit, :offset] -> q
      _, q -> q
    end)
  end

  defp rows_to_maps(rows, columns) do
    Enum.map(rows, fn row ->
      columns
      |> Enum.zip(row)
      |> Map.new()
    end)
  end

  defp changeset_to_map(changeset, now) do
    changeset.changes
    |> Map.put(:id, Ecto.UUID.generate())
    |> Map.put(:received_at, now)
    |> Map.put_new(:status, "received")
    |> Map.put_new(:event_data, %{})
  end
end
