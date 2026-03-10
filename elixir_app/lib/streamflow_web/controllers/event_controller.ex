defmodule StreamflowWeb.EventController do
  use Phoenix.Controller, formats: [:json]

  alias Streamflow.Events

  @doc """
  GET /api/events
  Supports query params: event_type, user_id, source_service,
                         from, to, limit, offset
  """
  def index(conn, params) do
    filters = build_filters(params)
    events  = Events.list_events(filters)
    total   = Events.count_events(filters)

    json(conn, %{
      data: events,
      meta: %{
        total:  total,
        limit:  Map.get(filters, :limit, 50),
        offset: Map.get(filters, :offset, 0)
      }
    })
  end

  @doc "GET /api/events/:id"
  def show(conn, %{"id" => id}) do
    event = Events.get_event!(id)
    json(conn, %{data: event})
  rescue
    Ecto.NoResultsError ->
      conn
      |> put_status(404)
      |> json(%{error: "Event not found"})
  end

  @doc "POST /api/events — inject an event directly (for testing)"
  def create(conn, params) do
    attrs = %{
      source_service: params["source_service"],
      event_type:     params["event_type"] && String.to_atom(params["event_type"]),
      user_id:        params["user_id"],
      session_id:     params["session_id"],
      event_data:     params["event_data"] || %{},
      occurred_at:    DateTime.utc_now()
    }

    case Events.create_event(attrs) do
      {:ok, event} ->
        conn
        |> put_status(201)
        |> json(%{data: event})

      {:error, changeset} ->
        conn
        |> put_status(422)
        |> json(%{errors: format_errors(changeset)})
    end
  end

  # ============================================================
  # PRIVATE
  # ============================================================

  defp build_filters(params) do
    %{}
    |> maybe_put(:event_type,     params["event_type"])
    |> maybe_put(:user_id,        params["user_id"])
    |> maybe_put(:source_service, params["source_service"])
    |> maybe_put(:status,         params["status"])
    |> maybe_put(:limit,          parse_int(params["limit"], 50))
    |> maybe_put(:offset,         parse_int(params["offset"], 0))
    |> maybe_put_datetime(:from,  params["from"])
    |> maybe_put_datetime(:to,    params["to"])
  end

  defp maybe_put(map, _key, nil),   do: map
  defp maybe_put(map, key, value),  do: Map.put(map, key, value)

  defp maybe_put_datetime(map, _key, nil), do: map
  defp maybe_put_datetime(map, key, str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _} -> Map.put(map, key, dt)
      _            -> map
    end
  end

  defp parse_int(nil, default), do: default
  defp parse_int(str, default) do
    case Integer.parse(str) do
      {n, ""} -> n
      _       -> default
    end
  end

  defp format_errors(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Enum.reduce(opts, msg, fn {key, val}, acc ->
        String.replace(acc, "%{#{key}}", to_string(val))
      end)
    end)
  end
end
