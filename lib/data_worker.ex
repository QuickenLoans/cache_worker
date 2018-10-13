defmodule DataWorker do
  @moduledoc ~S"""
  Defines a behavior to be implemented for managing data that should be held
  in the VM and periodically refreshed.

  The data is organized as a key/val data store called a bucket where the
  keys and vals can be any data type.

  If you wanted to keep track of some widgets, you might create your module
  thusly:

      defmodule WidgetStore do
        use DataWorker, bucket: :widgets, file: "/tmp/widget-dump"

        def load(key) do
          {:ok, Somewhere.get_widget(key)}
        end
      end

  Add the `WidgetStore` module to your supervision tree. This will add the data refresher
  agent to handle updating the keys on the refresh interval.

      children = [
        WidgetStore,
        ...,
      ]

      Supervisor.start_link(children, opts)

  When WidgetStore is started, it will load the dump file if one was
  configured. If a `:refresh_interval` was configured, a full refresh of all
  the data will be triggered at that interval, forever.

  The `&load/1` function on your `WidgetStore` module defines how the value
  for a given key is found and is called on whenever (1) a value is being
  requested and isn't yet in the bucket, or (2) is being refreshed by the
  data worker process.

  To get a widget, all a consumer needs to do is call `&WidgetStore.get(key)`
  and it will be returned. (`&fetch/1` does similarly, but provides more
  information.) The value from the cache will be used unless it's the first
  time for the particular key. In this case, `&WidgetStore.load/1` will be
  dispatched for the value, which will then be cached and returned.

  Steps Taken on a `&fetch(key)` Request:

      `&fetch("key")`
      --> [Cache] => `{:ok, "value"}`
          --> `&load("key")` => `{:error, "Something went wrong!"}`
              --> `{:ok, "Some data!"}`
                  --> *Saved to the cache* => `{:ok, "Some data!"}`
                      --> Data passed to `:return_fn`, if defined
                          => {:ok, "New value from return_fn"}

  ## Options for `use DataWorker`

  The `:bucket` option must be defined directly in the `use` options.

  The cascading logic for finding the final list of options is as follows,
  with each step having the opportunity to override the last:

  * `%DataWorker.Config{}` struct defaults
  * Any options provided to `use`
  * The `&data_worker_options/0` function. This one allows for options to be
    specified at runtime.

  In the `use` options, you may pass in any of the values described for the
  struct fields in `DataWorker.Config`.

  ## Options for `&fetch/1` and `&get/2`

  These functions allow access to the data in the data worker. The following
  options are available:

  * `:return_fn` - When defined as a function, it will be invoked after the
    value is gotten successfully (whether from the cache or via `&load/1`). The
    key and value are passed into this function, and the result will become
    the final return to the caller. This allows the caller to execute some
    code and return a new value to the caller, without affecting the cache
    itself. This also means that crashes will not affect the caching of the
    data.
  """

  use GenServer
  alias DataWorker.{Bucket, Config}
  require Logger

  @type bucket :: atom
  @type key :: term
  @type value :: term
  @type opts :: keyword
  @type init_return ::
          {:ok, map} | :ok | {:warn, String.t()} | {:stop, String.t()}
  @type load_return :: {:ok, value} | {:error, String.t()}
  @type fetch_return :: {:ok, value} | :no_bucket

  @doc "Returns the child_spec which should be used by the supervisor"
  @callback child_spec(keyword) :: Supervisor.child_spec()

  @doc "Give a DataWorker the opportunity to settle in"
  @callback init(Config.t()) :: init_return

  @doc "Actually procure the value for a key"
  @callback load(key) :: load_return

  @doc "Invoked on the `:refresh_interval` when the cache should be refreshed"
  @callback full_refresh :: :ok

  defmacro __using__(use_opts) do
    # Compile-time check with friendly reminder
    is_atom(Keyword.get(use_opts, :bucket, "not atom")) ||
      raise ":bucket option must be defined directly in the use options!"

    quote do
      alias DataWorker.Bucket

      @behaviour DataWorker

      @doc false
      @impl true
      @spec child_spec(keyword) :: Supervisor.child_spec()
      def child_spec(_) do
        DataWorker.child_spec(__MODULE__, unquote(use_opts))
      end

      @doc "Returns the `%DataWorker.Config{}`"
      @spec config :: Config.t()
      def config do
        Bucket.config(unquote(Keyword.get(use_opts, :bucket)))
      end

      @doc "Returns a particular value from the config"
      @spec config(key, term) :: term
      def config(key, default \\ nil) do
        Map.get(config(), key, default)
      end

      @doc """
      Initialize a DataWorker.

      If a cache file is not loaded on startup, this callback will be invoked
      with the DataWorker's `%Config{}`.

      This function should return `{:ok, map}` if a key/val set should be
      used to seed the cache, `:ok` if not, `{:warn, reason}` if a warning
      should be logged, and `{:stop, reason}` if initialization should be
      halted.
      """
      @impl true
      @spec init(%Config{}) :: DataWorker.init_return()
      def init(%Config{} = _config), do: :ok

      @doc "Call `&load/2` for each key in the cache. Returns `:ok`"
      @impl true
      @spec full_refresh :: :ok
      def full_refresh, do: DataWorker.full_refresh(__MODULE__)

      @doc "Fetches the value for a specific key in the bucket."
      @spec fetch(DataWorker.key(), DataWorker.opts()) ::
              DataWorker.fetch_return()
      def fetch(key, opts \\ []), do: DataWorker.fetch(__MODULE__, key, opts)

      @doc "Gets the value for a specific key in the bucket."
      @spec get(DataWorker.key(), DataWorker.value(), DataWorker.opts()) :: term
      def get(key, default \\ nil, opts \\ []),
        do: DataWorker.get(__MODULE__, key, default, opts)

      @doc "Gets a list of all keys in a given bucket."
      @spec keys :: [key]
      def keys, do: Bucket.keys(unquote(Keyword.get(use_opts, :bucket)))

      @doc """
      Do the work to procure the value for a given key.

      This callback is intended only for internal use.
      """
      @spec load(DataWorker.key()) ::
              {:ok, DataWorker.value()} | {:error, String.t()}
      def load(key), do: raise("#{__MODULE__} does not implement `&load/1`!")

      defoverridable init: 1, load: 1, full_refresh: 0
    end
  end

  @doc false
  def child_spec(mod, use_opts) do
    %{id: mod, start: {__MODULE__, :start_link, [mod, use_opts]}}
  end

  @doc false
  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.mod)
  end

  @doc false
  def init(%Config{} = config) do
    Bucket.init_bucket(config)

    # Skip init if we load a cache file
    ret =
      if maybe_load_file(config.bucket, config.file) do
        :ok
      else
        {config.mod, :init, [config]}
        |> invoke_carefully()
        |> handle_init_ret(config)
      end

    with :ok <- ret do
      schedule_full_refresh(config.refresh_interval)

      Logger.debug(fn ->
        l = config |> Map.from_struct() |> Enum.into([])
        "#{config.bucket}: Initialized DataWorker: #{inspect(l)}"
      end)
    end

    {:ok, nil}
  end

  @doc "Handle the signal to refresh the cache"
  def handle_info(:full_refresh, _) do
    this_worker_module().full_refresh()
    {:noreply, nil}
  end

  @doc "Fetch a value from the DataWorker"
  @spec fetch(module, key, opts) :: fetch_return
  def fetch(mod, key, opts \\ []) do
    with {:ok, value} <- do_fetch(mod.config(), key),
         fun when is_function(fun) <-
           Keyword.get(opts, :return_fn, {:ok, value}) do
      fun.(key, value)
    end
  end

  @doc "Get a value from the DataWorker"
  @spec get(module, key, value, opts) :: value
  def get(mod, key, default \\ nil, opts \\ []) do
    case fetch(mod, key, opts) do
      {:ok, val} -> val
      _ -> default
    end
  end

  @doc "Handle the refreshing of all keys for a given worker module"
  @spec full_refresh(module) :: :ok
  def full_refresh(mod) do
    %{bucket: bucket} = config = mod.config()

    case Bucket.keys(bucket) do
      {:ok, keys} ->
        refresh_for_keys(config, keys)

      :no_bucket ->
        Logger.error("#{bucket} bucket doesn't seem to exist!")
    end

    file_dump(bucket, config.file)

    schedule_full_refresh(config.refresh_interval)

    :ok
  end

  @doc """
  Get a value directly out of the cache (bucket), without calling `&load/1`
  """
  @spec direct_get(bucket, key) :: term
  def direct_get(bucket, key) do
    Bucket.get(bucket, key)
  end

  @doc "Set a value directly into the cache (bucket)"
  @spec direct_set(bucket, key, value) :: :ok | :no_bucket
  def direct_set(bucket, key, val) do
    Bucket.set(bucket, key, val)
  end

  defp do_fetch(%{bucket_enabled: false} = config, key) do
    run_load(config, key)
  end

  defp do_fetch(%{bucket: bucket} = config, key) do
    with :undefined <- Bucket.fetch(bucket, key) do
      run_load(config, key)
    end
  end

  defp invoke_carefully({mod, fun, args}) do
    apply(mod, fun, args)
  catch
    type, error -> {:caught, type, error}
  end

  @spec handle_init_ret(init_return | {:caught, atom, map}, Config.t()) ::
          :ok | {:stop, String.t()}
  defp handle_init_ret(init_ret, config) do
    case init_ret do
      {:ok, map} when is_map(map) ->
        store_map_into_cache(config, map)
        :ok

      :ok ->
        :ok

      {:warn, msg} ->
        Logger.warn(fn -> "#{config.mod} warns: #{msg}" end)
        :ok

      {:stop, msg} ->
        {:stop, msg}

      {:caught, type, error} ->
        Logger.warn(fn ->
          "#{config.mod}.init error: #{inspect(type)}, #{inspect(error)}"
        end)

        :ok

      wat ->
        {:stop,
         "Unrecognized `&init/1` return from #{config.mod}: #{inspect(wat)}"}
    end
  end

  # Optimizes & executes calling the `&load/1` function for the given keys
  @spec refresh_for_keys(Config.t(), [key]) :: :ok
  defp refresh_for_keys(config, keys) do
    parent = self()

    keys
    |> Enum.map(
      &spawn(fn ->
        try do
          run_load(config, &1)
        rescue
          e ->
            Logger.error("""
            Error processing CMS key #{inspect(&1)}: #{inspect(e)}\
            """)
        end

        send(parent, self())
      end)
    )
    # block until refresh is finished
    |> Enum.each(fn pid ->
      receive do
        ^pid -> nil
      end
    end)
  end

  # Refresh and return a particular value; log errors
  @spec run_load(Config.t(), key, opts) :: {:ok, value} | {:error, String.t()}
  defp run_load(%{mod: mod, bucket: bucket} = config, key, opts \\ []) do
    case invoke_carefully({mod, :load, [key]}) do
      {:ok, val} ->
        Logger.debug(fn ->
          ins = inspect(val, limit: 2, printable_limit: 100)
          "#{bucket}:#{inspect(key)}: Loaded: #{ins}"
        end)

        opts |> Keyword.get(:return_fn) |> maybe_call_fn([key, val])
        save_value(config, key, val)
        {:ok, val}

      {:ok, val, map} when is_map(map) ->
        opts |> Keyword.get(:return_fn) |> maybe_call_fn([key, val])
        store_map_into_cache(config, map)
        {:ok, val}

      {:error, error} ->
        msg = "#{mod}.load(#{inspect(key)}) error: #{error}"
        Logger.error(msg)
        {:error, msg}

      {:caught, type, error} ->
        msg = """
        #{mod}.load(#{inspect(key)}) error: #{inspect(type)}, #{inspect(error)}
        """

        Logger.warn(msg)

        {:error, msg}
    end
  end

  defp maybe_call_fn(fun, args) when is_function(fun),
    do: apply(fun, args)

  defp maybe_call_fn(_, _),
    do: :ok

  # Save a value to the bucket; log errors
  @spec save_value(module, key, value) :: :ok
  defp save_value(%{bucket: bucket}, key, val) do
    case Bucket.set(bucket, key, val) do
      {:ok, true} ->
        :ok

      :bucket_exists ->
        Logger.error("#{bucket}:#{key}: No cache found, trying to save value")
        :ok
    end
  end

  # Load an entire map into the cache
  @spec store_map_into_cache(Config.t(), map) :: :ok
  defp store_map_into_cache(%{bucket: bucket} = config, map) do
    Enum.each(Map.keys(map), fn key ->
      save_value(config, key, map[key])
    end)

    file_dump(bucket, config.file)

    Logger.debug(fn ->
      {:ok, keys} = Bucket.keys(bucket)
      "#{bucket}: Loaded cache with #{length(keys)} keys"
    end)

    :ok
  end

  # Dump an entire cache to disk log errors
  @spec file_dump(bucket, String.t()) :: :ok
  defp file_dump(_bucket, nil), do: :ok

  defp file_dump(bucket, file) do
    case Bucket.dump(bucket, file) do
      {:ok, true} ->
        Logger.debug(fn ->
          {:ok, keys} = Bucket.keys(bucket)
          "#{bucket}: Saved bucket to disk with #{length(keys)} keys"
        end)

        :ok

      {:error, error} ->
        Logger.error("#{bucket}: Failed to dump bucket: #{error}")
        :ok
    end
  end

  @spec maybe_load_file(bucket, String.t | nil) :: boolean
  defp maybe_load_file(bucket, file) when byte_size(file) > 0 do
    case Bucket.load(bucket, file) do
      {:ok, bucket} ->
        Logger.debug(fn ->
          {:ok, keys} = Bucket.keys(bucket)
          "#{bucket}: Loaded cache file with #{length(keys)} keys"
        end)

        true

      {:error, msg} ->
        Logger.warn("Failed to load cache file: #{file}: #{inspect(msg)}")
        false
    end
  end

  defp maybe_load_file(_, _), do: false

  # Get the module name for which this instance of DataWorker is running
  @spec this_worker_module :: module
  defp this_worker_module do
    self() |> Process.info() |> Keyword.get(:registered_name)
  end

  defp schedule_full_refresh(interval) when interval > 0 do
    Process.send_after(self(), :refresh, interval * 1_000)
  end

  defp schedule_full_refresh(_), do: nil
end
