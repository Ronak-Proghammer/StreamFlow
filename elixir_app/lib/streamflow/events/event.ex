defmodule Streamflow.Events.Event do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}

  @valid_event_types ~w(
    user_login user_logout page_view button_click
    purchase error api_call data_export
  )a

  @valid_statuses ~w(received processed failed)

  schema "events" do
    field :source_service, :string
    field :event_type,     Ecto.Enum, values: @valid_event_types
    field :user_id,        :string
    field :session_id,     :string
    field :event_data,     :map,    default: %{}
    field :client_ip,      :string
    field :status,         :string, default: "received"
    field :occurred_at,    :utc_datetime_usec
    field :received_at,    :utc_datetime_usec
  end

  @required ~w(source_service event_type occurred_at)a
  @optional  ~w(user_id session_id event_data client_ip status received_at)a

  def changeset(event, attrs) do
    event
    |> cast(attrs, @required ++ @optional)
    |> validate_required(@required)
    |> validate_inclusion(:status, @valid_statuses)
    |> validate_length(:source_service, max: 100)
    |> put_received_at()
  end

  defp put_received_at(changeset) do
    case get_field(changeset, :received_at) do
      nil -> put_change(changeset, :received_at, DateTime.utc_now())
      _   -> changeset
    end
  end
end
