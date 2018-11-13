defmodule TestWorker do
  @moduledoc """
  A simple cache worker for testing with
  """
  use CacheWorker, bucket: :test_worker

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
