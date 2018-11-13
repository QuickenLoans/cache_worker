defmodule CacheWorker.BucketTest do
  @moduledoc false
  use ExUnit.Case
  import CacheWorker.Bucket

  test "New table works" do
    assert :ok == ensure_new(:a)
    assert :ok == ensure_new(:a)
    assert :ok == ensure_new(:a1)
  end

  test "Buckets generally function" do
    assert :ok == ensure_new(:b)
    assert :ok == set(:b, :key, "val!")
    assert "val!" == get(:b, :key)
    assert {:ok, "val!"} == fetch(:b, :key)
  end

  test "Negative cases" do
    assert :ok == ensure_new(:c)
    assert :no_bucket == set(:x, "bla", "a value")
    assert nil == get(:x, "never set")
    assert :thanks == get(:x, "never set", :thanks)
    assert :no_bucket == fetch(:x, "no bucket anyway")
    assert :undefined == fetch(:c, "never set")
  end

  test "keys" do
    assert :ok == ensure_new(:d)
    assert :ok == set(:d, "first", "the first one")
    assert :ok == set(:d, "second", "the second one")
    assert :ok == set(:d, "third", "the third one")
    assert {:ok, ~w(third second first)} == :d |> keys()
  end

  test "dump file and delete functionality" do
    file = "/tmp/cacheworker-bucket-e"
    File.rm(file)
    assert :ok == ensure_new(:e)
    assert :ok == set(:e, "one", "first one!")
    assert :ok == set(:e, "two", "second one!")
    assert :ok == set(:e, "three", "third one!")
    assert :ok == dump(:e, file)
    assert "first one!" == get(:e, "one")
    assert :ok == delete(:e)
    assert nil == get(:e, "one")
    assert :ok == ensure_new(:e)
    delete(:e)
    assert :ok == load(:e, file)
    assert "first one!" == get(:e, "one")
    assert {:error, _} = load(:e, "/bad/filename")
    assert {:error, _} = load(:x, file)
    File.rm(file)
  end

  test "Import a dump file for a different name" do
    file = "/tmp/cacheworker-bucket-f"
    File.rm(file)
    assert :ok == ensure_new(:f)
    assert :ok == set(:f, :lorem, "ipsum")
    assert :ok == dump(:f, file)
    assert :ok == delete(:f)
    assert nil == get(:f, :lorem)
    assert {:error, _} = load(:x, file)
    File.rm(file)
  end
end
