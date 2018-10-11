defmodule DataWorker.Twiddler do
  @moduledoc """
  The Twiddler's job is to own and make writes to a particular bucket's ETS
  tables.
  """

  use GenServer
  alias DataWorker.Bucket

  @doc false
  @impl true
  @spec child_spec(keyword) :: Supervisor.child_spec()
  def child_spec(%{mod: mod, bucket: bucket}) do
    %{id: "#{mod} Twiddler", start: {__MODULE__, :start_link, [bucket]}}
  end

  @doc false
  def start_link(bucket) do
    GenServer.start_link(__MODULE__, bucket, name: name(bucket))
  end

  # Internals...

  @doc false
  def init(bucket) do
    :ok = Bucket.new(bucket)
    {:ok, nil}
  end

  def handle_call()

  defp name(bucket), do: :"#{bucket}_twiddler"
end
