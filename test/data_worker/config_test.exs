defmodule MyWorker do
  @moduledoc false
end

defmodule DataWorker.ConfigTest do
  @moduledoc false
  use ExUnit.Case
  import DataWorker.Config
  alias DataWorker.Config

  @expected_defaults [
    mod: nil,
    cache_enabled: true,
    bucket: nil,
    file: nil,
    refresh_interval: 900,
    config_fn: nil
  ]

  test "config defaults" do
    assert struct(Config, @expected_defaults) == %Config{}
  end

  test "normalize!" do
    assert %DataWorker.Config{
             bucket: :demo,
             cache_enabled: true,
             config_fn: nil,
             file: nil,
             mod: MyWorker,
             refresh_interval: 900
           } == normalize!(MyWorker, bucket: :demo)
  end

  test "bad module" do
    assert_raise RuntimeError, fn ->
      normalize!(NotAModule, bucket: :demo)
    end
  end

  test "config fn" do
    fun = fn -> [file: "/yay"] end
    assert %{file: "/yay"} = normalize!(MyWorker, bucket: :demo, config_fn: fun)

    fun = fn -> [file: "/yep"] end
    conf = normalize!(MyWorker, bucket: :x, file: "overridden", config_fn: fun)
    assert %{file: "/yep"} = conf

    assert_raise RuntimeError, fn ->
      normalize!(MyWorker, bucket: :demo, config_fn: fn -> :what_the end)
    end

    assert_raise RuntimeError, fn ->
      normalize!(MyWorker, bucket: :demo, config_fn: :no_wait_seriously)
    end
  end

  test "bucket" do
    assert_raise RuntimeError, fn -> normalize!(MyWorker, []) end
    assert_raise RuntimeError, fn -> normalize!(MyWorker, bucket: "hardly") end
    assert %{bucket: :okay} = normalize!(MyWorker, bucket: :okay)
  end

  test "cache_enabled" do
    [{true, true}, {"on", false}, {"nil", false}, {false, false}]
    |> Enum.each(fn {input, expected} ->
      assert %{cache_enabled: ^expected} =
               normalize!(MyWorker, bucket: :demo, cache_enabled: input)
    end)
  end

  test "file" do
    assert %{} = normalize!(MyWorker, bucket: :demo, file: nil)
    assert %{} = normalize!(MyWorker, bucket: :demo, file: "/good")

    assert_raise RuntimeError, fn ->
      normalize!(MyWorker, bucket: :demo, file: 7)
    end
  end

  test "refresh interval" do
    assert %{} = normalize!(MyWorker, bucket: :demo, refresh_interval: nil)
    assert %{} = normalize!(MyWorker, bucket: :demo, refresh_interval: 4)

    assert_raise RuntimeError, fn ->
      normalize!(MyWorker, bucket: :demo, refresh_interval: "1.21 gigawats?!")
    end
  end
end
