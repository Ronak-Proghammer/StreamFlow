defmodule StreamflowWeb.StatsController do
  use Phoenix.Controller, formats: [:json]

  alias Streamflow.Events

  @doc "GET /api/stats — summary numbers for last N minutes"
  def summary(conn, params) do
    minutes = parse_int(params["minutes"], 60)
    stats   = Events.summary_stats(minutes)
    json(conn, %{data: stats, meta: %{window_minutes: minutes}})
  end

  @doc "GET /api/stats/timeseries — events per minute"
  def timeseries(conn, params) do
    minutes = parse_int(params["minutes"], 60)
    series  = Events.events_per_minute(minutes)
    json(conn, %{data: series, meta: %{window_minutes: minutes}})
  end

  @doc "GET /api/stats/services — top services by event count"
  def services(conn, params) do
    minutes = parse_int(params["minutes"], 60)
    limit   = parse_int(params["limit"], 10)
    data    = Events.top_services(minutes, limit)
    json(conn, %{data: data})
  end

  @doc "GET /api/stats/running — running totals per event type"
  def running_totals(conn, _params) do
    data = Events.running_totals_by_type()
    json(conn, %{data: data})
  end

  defp parse_int(nil, default), do: default
  defp parse_int(str, default) do
    case Integer.parse(str) do
      {n, ""} -> n
      _       -> default
    end
  end
end
