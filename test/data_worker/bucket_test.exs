defmodule DataWorker.BucketTest do
  @moduledoc false
  use ExUnit.Case
  import DataWorker.Bucket

  test "New table works" do
    assert :ok == new(:a)
    assert :ok == new(:a)
    assert :ok == new(:a1)
  end

  test "Buckets generally function" do
    assert :ok == init_bucket(%{bucket: :b})
    assert :ok == set(:b, :key, "val!")
    assert "val!" == get(:b, :key)
    assert {:ok, "val!"} == fetch(:b, :key)
  end

  test "Negative cases" do
    assert :ok == init_bucket(%{bucket: :c})
    assert :no_bucket == set(:x, "bla", "a value")
    assert nil == get(:x, "never set")
    assert :thanks == get(:x, "never set", :thanks)
    assert :no_bucket == fetch(:x, "no bucket anyway")
    assert :undefined == fetch(:c, "never set")
  end

  test "keys" do
    assert :ok == init_bucket(%{bucket: :d})
    assert :ok == set(:d, "first", "the first one")
    assert :ok == set(:d, "second", "the second one")
    assert :ok == set(:d, "third", "the third one")
    assert ~w(first second third) == :d |> keys() |> Enum.sort()
  end

  test "dump file, and delete, and config functionality" do
    assert :ok == init_bucket(%{bucket: :e})
    assert :ok == set(:e, "one", "first one!")
    assert :ok == set(:e, "two", "second one!")
    assert :ok == set(:e, "three", "third one!")
    assert %{bucket: :e} == config(:e)
    assert :ok == dump(:e, "/tmp/dataworker-bucket-e")
    assert "first one!" == get(:e, "one")
    assert :ok == delete_bucket(:e)
    assert nil == get(:e, "one")
    assert nil == config(:e)
    assert :ok == init_bucket(%{bucket: :e})
    assert :ok == load(:e, "/tmp/dataworker-bucket-e")
    assert "first one!" == get(:e, "one")
    assert {:error, _} = load(:e, "/bad/filename")
    assert {:error, _} = load(:x, "/tmp/dataworker-bucket-e")
  end
end
