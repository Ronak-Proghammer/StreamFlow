defmodule StreamflowWeb.Router do
  use Phoenix.Router
  import Plug.Conn

  pipeline :api do
    plug :accepts, ["json"]
    plug :put_resp_content_type, "application/json"
  end

  scope "/api", StreamflowWeb do
    pipe_through :api

    # Health check
    get "/health", HealthController, :index

    # Consumer status
    get "/consumer/status", ConsumerController, :status

    # Events
    get  "/events",     EventController, :index
    get  "/events/:id", EventController, :show
    post "/events",     EventController, :create

    # Analytics
    get "/stats",           StatsController, :summary
    get "/stats/timeseries", StatsController, :timeseries
    get "/stats/services",   StatsController, :services
    get "/stats/running",    StatsController, :running_totals

    # Aggregates & Anomalies
    get "/aggregates",          AnalyticsController, :aggregates
    get "/anomalies",           AnalyticsController, :anomalies
    put "/anomalies/:id/resolve", AnalyticsController, :resolve
  end
end
