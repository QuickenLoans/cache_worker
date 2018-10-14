alias DataWorker, as: D
alias DataWorker.Bucket, as: B
alias DataWorker.Config, as: C

defmodule T do
  def launch(mod) do
    {m, f, a} = Map.get(mod.child_spec(:_), :start)
    apply(m, f, a)
  end
end

# defmodule Basic do
#   use DataWorker, bucket: :basic

#   def init(_) do
#     {:ok, %{seeded: "on init"}}
#   end

#   def load(input) do
#     {:ok, "The value for #{input}"}
#   end
# end