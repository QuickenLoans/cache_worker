alias DataWorker, as: D
alias DataWorker.Bucket, as: B
alias DataWorker.Config, as: C

defmodule T do
  def launch(mod, opts \\ []) do
    {m, f, a} = Map.get(mod.child_spec(opts), :start)
    apply(m, f, a)
  end

  # eg: {:ok, pid} = T.tw(file: "foo")
  def tw(o \\ []), do: launch(TestWorker, o)
end