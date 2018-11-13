defmodule SuperBasic do
  use CacheWorker, bucket: :super_basic
  def load(_), do: nil
end

defmodule CacheDisabled do
  use CacheWorker, bucket: :cache_disabled, cache_disabled: true

  def load(key) do
    require Logger
    Logger.info("loading #{inspect(key)}")
    4
  end
end

defmodule InitMap do
  use CacheWorker, bucket: :init_map
  def init(_), do: {:ok, %{x: 1, y: 2, z: 3}}
  def load(_), do: nil
end

defmodule InitWarn do
  use CacheWorker, bucket: :init_warn
  def init(_), do: {:warn, "Careful, mate!"}
  def load(_), do: nil
end

defmodule InitStop do
  use CacheWorker, bucket: :init_stop
  def init(_), do: {:stop, "Oh damn"}
  def load(_), do: nil
end

defmodule InitRaise do
  use CacheWorker, bucket: :init_stop
  def init(_), do: raise("Crap!!")
  def load(_), do: nil
end

defmodule LoadMap do
  use CacheWorker, bucket: :load_map
  def load(:b), do: {:ok, 2, %{a: 1, b: 2, c: 3}}
end

defmodule LoadError do
  use CacheWorker, bucket: :load_error
  def load(:a), do: {:error, "Whoopsie"}
end

defmodule LoadRaise do
  use CacheWorker, bucket: :load_raise
  def load(:r), do: raise("It broke")
end

defmodule WithFile do
  use CacheWorker, bucket: :wfile, file: "/tmp/cacheworker-bucket-wfile"
  def load(input), do: {:ok, "blah, #{input}"}
end

defmodule WithBadFile do
  use CacheWorker, bucket: :wfile, file: "/probably-no-permission"
  def load(input), do: {:ok, "meh, #{input}"}
end

defmodule FullRefresher do
  use CacheWorker, bucket: :full_refresher, refresh_interval: 0.02
  require Logger

  def load(:x), do: raise("x bad!")

  def load(input) do
    Logger.info("loading #{inspect(input)}")
    {:ok, nil}
  end
end

defmodule CacheWorkerTest do
  use ExUnit.Case
  alias CacheWorker.Config
  import ExUnit.CaptureLog
  import CompileTimeAssertions
  doctest CacheWorker

  test "child_spec" do
    opts =
      [refresh_interval: 8, file: "scoar"]
      |> FullRefresher.child_spec()
      |> Map.get(:start)
      |> elem(2)
      |> Enum.slice(1, 1)
      |> hd()

    assert 8 == Keyword.get(opts, :refresh_interval)
    assert "scoar" == Keyword.get(opts, :file)
    assert :full_refresher == Keyword.get(opts, :bucket)
  end

  test "basic" do
    launch(TestWorker)
    assert "The value for foo" == TestWorker.get(:foo)
    assert {:ok, "The value for bar"} == TestWorker.fetch(:bar)
    assert "on init" == TestWorker.get(:seeded)
    assert {:ok, [:bar, :foo, :seeded]} = TestWorker.keys()
    assert %Config{} = TestWorker.config()
    assert :test_worker == TestWorker.config(:bucket)
  end

  test "bad things" do
    assert_raise RuntimeError, fn -> SuperBasic.config() end
    assert_raise RuntimeError, fn -> SuperBasic.config(:hi) end

    assert_raise RuntimeError, fn ->
      SuperBasic.get(:hi, :failsauce)
    end
  end

  test "cache disabled" do
    launch(CacheDisabled)
    assert capture_log(fn -> CacheDisabled.get(:yo) end) =~ "loading :yo"
    assert capture_log(fn -> CacheDisabled.get(:yo) end) =~ "loading :yo"
    assert capture_log(fn -> CacheDisabled.get(:yo) end) =~ "loading :yo"
  end

  test "dump file" do
    file = "/tmp/cacheworker-bucket-wfile"
    File.rm(file)
    Process.flag(:trap_exit, true)
    {:ok, pid} = launch(WithFile)
    assert {:ok, "blah, one"} == WithFile.fetch("one")
    assert {:ok, "blah, two"} == WithFile.fetch("two")
    kill_and_wait(pid)
    launch(WithFile)
    assert {:ok, "blah, one"} == WithFile.fetch("one")
    assert {:ok, ~w(two one)} == WithFile.keys()
    File.rm(file)
  end

  test "fetch no save" do
    file = "/tmp/cacheworker-bucket-wfile"
    File.rm(file)
    Process.flag(:trap_exit, true)
    {:ok, pid} = launch(WithFile)
    assert {:ok, "blah, one", true} == WithFile.fetch_no_save("one")
    assert {:ok, "blah, two", true} == WithFile.fetch_no_save("two")
  end

  test "bad file for dump" do
    assert capture_log(fn -> launch(WithBadFile) end) =~ ":read_error"
  end

  test "init: map" do
    launch(InitMap)
    assert {:ok, ~w(x y z)a} == InitMap.keys()
  end

  test "init: warn" do
    assert capture_log(fn ->
             launch(InitWarn)
           end) =~ ~r/InitWarn.*warn.*Careful, mate/
  end

  test "init: stop" do
    {:error, "Oh damn"} = launch(InitStop)
  end

  test "init: raise" do
    assert capture_log(fn ->
             launch(InitRaise)
           end) =~ ~r/InitRaise.init.*error.*RuntimeError.*Crap/s
  end

  test "load: map" do
    launch(LoadMap)
    assert {:ok, 2} == LoadMap.fetch(:b)
    assert {:ok, ~w(a b c)a} == LoadMap.keys()
  end

  test "load: error" do
    launch(LoadError)
    assert capture_log(fn -> LoadError.get(:a) end) =~ "error: Whoopsie"
  end

  test "load: raise" do
    launch(LoadRaise)
    assert capture_log(fn -> LoadRaise.fetch(:r) end) =~ ~s/It broke/
  end

  test "full refresh and direct set/get" do
    launch(FullRefresher)
    assert nil == FullRefresher.get(:a)
    assert nil == FullRefresher.get(:b)
    assert nil == FullRefresher.get(:c)
    :timer.sleep(10)

    log = capture_log(fn -> :timer.sleep(40) end)

    assert log =~ "loading :a"
    assert log =~ "loading :b"
    assert log =~ "loading :c"

    assert capture_log(fn -> FullRefresher.get(:x) end) =~ "x bad!"

    assert :ok == FullRefresher.direct_set(:key, :val)
    assert :val == FullRefresher.direct_get(:key)
  end

  test "using" do
    assert_compile_time_raise(
      RuntimeError,
      ":bucket option must be defined directly in the use options!",
      quote do
        defmodule(Xx, do: use(CacheWorker, bucket: "oh hell no"))
      end
    )
  end

  defp launch(mod, opts \\ []) do
    {m, f, a} = Map.get(mod.child_spec(opts), :start)
    apply(m, f, a)
  end

  # Send a kill signal and wait for the process to be dead.
  # Make sure you have `Process.flag(:trap_exit, true)`
  defp kill_and_wait(pid) do
    Process.exit(pid, :kill)

    receive do
      {:EXIT, ^pid, :killed} -> :burp
    end
  end
end
