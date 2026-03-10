defmodule Streamflow.Repo do
  # ============================================================
  # Ecto Repository
  # ============================================================
  # The Repo is our interface to the database. Every DB
  # operation goes through here.
  #
  # Usage examples (you'll use these constantly):
  #
  #   # Insert a new event
  #   Streamflow.Repo.insert(%Event{event_type: "page_view"})
  #
  #   # Query events
  #   Streamflow.Repo.all(Event)
  #
  #   # Query with conditions (using Ecto.Query)
  #   import Ecto.Query
  #   from(e in Event, where: e.event_type == "error") |> Repo.all()
  #
  #   # Transactions
  #   Repo.transaction(fn ->
  #     Repo.insert!(event)
  #     Repo.insert!(aggregate)
  #   end)
  #
  # INTERVIEW TIP: Ecto separates queries (Ecto.Query) from
  # changesets (validation) from the repo (execution).
  # This is unlike ActiveRecord/Django ORM where the model
  # does everything. Elixir's approach is more composable.
  # ============================================================

  use Ecto.Repo,
    otp_app: :streamflow,
    adapter: Ecto.Adapters.Postgres
end
