alias DataWorker, as: D
alias DataWorker.Bucket, as: B
alias DataWorker.Config, as: C
alias TestWorker, as: TW

defmodule T do
  def launch(mod, opts \\ []) do
    {m, f, a} = Map.get(mod.child_spec(opts), :start)
    apply(m, f, a)
  end

  def sup(opts \\ []) do
    {:ok, sup} = TSup.start_link(TestWorker, opts)
    sup
  end

  # eg: {:ok, pid} = T.tw(file: "foo")
  def tw(o \\ []), do: launch(TestWorker, o)

  def ag do
    {:ok, pid} = Agent.start_link(fn -> false end)
    Process.register(pid, :ta)
    pid
  end
end

defmodule TSup do
  use Supervisor

  def start_link(mod, initial_opts) do
    Supervisor.start_link(__MODULE__, {mod, initial_opts})
  end

  @impl true
  def init({mod, opts}) do
    children = [{mod, opts}]
    opts = [strategy: :one_for_one, name: __MODULE__]
    Supervisor.init(children, opts)
  end
end