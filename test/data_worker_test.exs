defmodule DataWorkerTest do
  @moduledoc false
  use ExUnit.Case
  doctest DataWorker
  import ExUnit.CaptureLog
  #assert capture_log(fn) =~ "txt"

  # test "greets the world" do
  #   assert DataWorker.hello() == :world
  # end
end
