defmodule StreamflowWeb.ConsumerController do
  use Phoenix.Controller, formats: [:json]

  def status(conn, _params) do
    status = Streamflow.RabbitMQ.Consumer.status()
    json(conn, %{data: status})
  end
end
