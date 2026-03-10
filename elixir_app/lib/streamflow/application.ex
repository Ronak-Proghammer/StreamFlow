defmodule Streamflow.Application do
  # ============================================================
  # OTP Application & Supervision Tree
  # ============================================================
  # This is one of the most important files to understand for
  # your interview. Let me explain OTP and supervisors.
  #
  # WHAT IS OTP?
  # OTP (Open Telecom Platform) is a set of libraries and design
  # principles built into Erlang/Elixir. It gives you:
  #   - Supervisors: processes that watch other processes
  #   - GenServers: generic server processes (stateful workers)
  #   - Applications: self-contained systems with lifecycle management
  #
  # WHY DOES THIS MATTER?
  # In Elixir, instead of handling errors with try/catch everywhere,
  # you "let it crash". If a process crashes, its supervisor
  # automatically restarts it. This gives you fault tolerance
  # WITHOUT complex error handling code.
  #
  # THE SUPERVISION TREE:
  # Think of it like a org chart. The Application is the CEO.
  # Supervisors are managers. GenServers are workers.
  # If a worker crashes, the manager restarts just that worker.
  # If too many workers crash, the manager itself restarts.
  #
  # Streamflow.Application (root supervisor)
  # ├── Streamflow.Repo              (database connection pool)
  # ├── StreamflowWeb.Endpoint       (HTTP server)
  # ├── Streamflow.RabbitMQ.Consumer (event consumer GenServer)
  # └── Streamflow.Telemetry         (metrics collection)
  # ============================================================

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # --- Database Connection Pool ---
      # Ecto.Repo manages a pool of DB connections.
      # Instead of opening/closing a connection per query,
      # it keeps N connections open and reuses them.
      # Default pool size is 10 — configurable in config/
      Streamflow.Repo,

      # --- HTTP Server ---
      # StreamflowWeb.Endpoint is the Phoenix HTTP server.
      # It handles routing, plug pipeline, etc.
      StreamflowWeb.Endpoint,

      # --- RabbitMQ Consumer ---
      # Our GenServer that consumes events from RabbitMQ.
      # If this crashes (e.g., RabbitMQ connection drops),
      # the supervisor restarts it automatically.
      # We'll build this in Day 2!
      Streamflow.RabbitMQ.Consumer,

      # --- Telemetry Supervisor ---
      # Collects and reports metrics from Phoenix/Ecto
      Streamflow.Telemetry,
    ]

    # ============================================================
    # SUPERVISOR STRATEGY
    # ============================================================
    # :one_for_one = if one child crashes, restart ONLY that child
    #
    # Other strategies:
    # :one_for_all  = if one crashes, restart ALL children
    #                 (use when children are tightly coupled)
    # :rest_for_one = if one crashes, restart it AND everything
    #                 started after it
    #                 (use when children depend on startup order)
    #
    # For our use case, :one_for_one is correct — the DB pool
    # crashing shouldn't restart the HTTP server.
    # ============================================================
    opts = [
      strategy: :one_for_one,
      name: Streamflow.Supervisor,
      # max_restarts: how many restarts are allowed in max_seconds
      # If exceeded, the supervisor itself crashes (escalation)
      max_restarts: 3,
      max_seconds: 5
    ]

    Supervisor.start_link(children, opts)
  end

  # ============================================================
  # CONFIGURATION CALLBACK
  # ============================================================
  # Called when the application config changes at runtime.
  # Important for hot code reloading in development.
  # ============================================================
  @impl true
  def config_change(changed, _new, removed) do
    StreamflowWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
