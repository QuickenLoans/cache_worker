defmodule TestWorker do
  @moduledoc """
  A simple data worker for testing with
  """
  use DataWorker, bucket: :test_worker

  def init(_) do
    {:ok, %{seeded: "on init"}}
  end

  def load(:crash) do
    raise "AAAAAAHHHHH"
  end

  def load(input) do
    {:ok, "The value for #{input}"}
  end
end
