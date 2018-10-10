defmodule DataWorker.Supervisor do
  @moduledoc """
  Supervisor for watching a Cachex and DataWorker pair
  """

  use Supervisor
  alias DataWorker.Config

  def start_link(mod, initial_opts) do
    Supervisor.start_link(__MODULE__, {mod, initial_opts})
  end

  @impl true
  def init({mod, initial_opts}) do
    config = Config.normalize!(mod, initial_opts)

    children = [
      worker(Cachex, [config.bucket, []]),
      %{id: mod, start: {DataWorker, :start_link, [config]}}
    ]

    opts = [strategy: :one_for_one, name: __MODULE__]
    Supervisor.init(children, opts)
  end
end
