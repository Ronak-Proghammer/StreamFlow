defmodule Streamflow.MixProject do
  use Mix.Project

  # ============================================================
  # mix.exs is the heart of every Elixir project.
  # It defines:
  #   - Project metadata (name, version, Elixir version)
  #   - Dependencies (like package.json in Node, requirements.txt in Python)
  #   - How to build a production release
  #
  # "mix" is Elixir's build tool (like pip + setuptools + make combined)
  # Common commands:
  #   mix deps.get       → install dependencies
  #   mix compile        → compile the project
  #   mix test           → run tests
  #   mix phx.server     → start the Phoenix web server
  #   mix ecto.migrate   → run database migrations
  # ============================================================

  def project do
    [
      app: :streamflow,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,  # Crash the VM if app fails to start in prod
      aliases: aliases(),
      deps: deps()
    ]
  end

  # ============================================================
  # OTP Application definition
  # ============================================================
  # This tells the Erlang/OTP runtime what to start when
  # the application boots. Our top-level supervisor goes here.
  #
  # "mod" = the module that starts our supervision tree
  # "env" = compile-time configuration
  # ============================================================
  def application do
    [
      mod: {Streamflow.Application, []},
      extra_applications: [:logger, :runtime_tools]
    ]
  end

  # Compile test helpers only in test environment
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # ============================================================
  # DEPENDENCIES
  # ============================================================
  # Format: {:package_name, "~> version"}
  # ~> means "compatible with" (e.g., ~> 1.2 means >= 1.2 and < 2.0)
  # ============================================================
  defp deps do
    [
      # --- Phoenix Web Framework ---
      # Phoenix is to Elixir what Django is to Python or Rails is to Ruby
      # It handles HTTP routing, controllers, views, etc.
      {:phoenix, "~> 1.7.0"},

      # Phoenix's HTML template engine (we won't use it much — REST API)
      {:phoenix_html, "~> 3.3"},

      # Live reload in development (auto-refresh browser on code changes)
      {:phoenix_live_reload, "~> 1.2", only: :dev},

      # --- Ecto (Database ORM/Query builder) ---
      # Ecto is to Elixir what SQLAlchemy is to Python
      # It handles: connection pooling, queries, migrations, validations
      {:ecto_sql, "~> 3.10"},

      # PostgreSQL adapter for Ecto
      {:postgrex, ">= 0.0.0"},

      # --- AMQP (RabbitMQ client) ---
      # AMQP = Advanced Message Queuing Protocol
      # This is how we connect to and consume from RabbitMQ
      {:amqp, "~> 3.3"},

      # --- JSON ---
      # Jason is the fastest JSON encoder/decoder for Elixir
      # Phoenix uses it automatically for API responses
      {:jason, "~> 1.2"},

      # --- HTTP Server ---
      # Bandit is a pure-Elixir HTTP server (alternative to Cowboy)
      {:bandit, "~> 1.2"},

      # --- Telemetry (Observability) ---
      # Telemetry is Elixir's built-in metrics/instrumentation system
      # Phoenix and Ecto emit telemetry events automatically
      {:telemetry_metrics, "~> 0.6"},
      {:telemetry_poller, "~> 1.0"},

      # --- Dev/Test Only ---
      {:phoenix_live_dashboard, "~> 0.8.3", only: [:dev]},  # Built-in monitoring UI
      {:floki, ">= 0.30.0", only: :test},  # HTML parsing for tests
      {:ex_machina, "~> 2.7", only: :test},  # Test data factories
      {:mox, "~> 1.0", only: :test},  # Mocking library
    ]
  end

  # ============================================================
  # ALIASES
  # ============================================================
  # Shortcuts for common tasks. "mix setup" will run all of these.
  # Very useful for onboarding new developers.
  # ============================================================
  defp aliases do
    [
      setup: ["deps.get", "ecto.setup"],
      "ecto.setup": ["ecto.create", "ecto.migrate", "run priv/repo/seeds.exs"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end
end