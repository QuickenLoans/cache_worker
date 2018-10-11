defmodule DataWorker.Supervisor do
  @moduledoc """
  Supervisor for watching a DataWorker and Twiddler pair
  """

  use Supervisor
  alias DataWorker.{Config, Twiddler}

  def start_link(mod, opts) do
    Supervisor.start_link(__MODULE__, {mod, opts})
  end

  @impl true
  def init({mod, opts}) do
    config = Config.normalize!(mod, opts)

    children = [
      {Twiddler, [config]},
      {DataWorker, [config]}
    ]

    sup_opts = [strategy: :one_for_one, name: __MODULE__]
    Supervisor.init(children, sup_opts)
  end
end
