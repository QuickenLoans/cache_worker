defmodule DataWorker do
  @moduledoc ~S"""
  Defines a behavior to be implemented for managing data that should be held
  in the VM and periodically refreshed.

  The data is organized as a key/val data store called a bucket where the
  keys and vals can be any data type.

  If you wanted to keep track of some widgets, you might create your module
  thusly:

      defmodule WidgetStore do
        use DataWorker, bucket: :widgets, file: "/tmp/widget-cache"

        def load(key) do
          {:ok, Somewhere.get_widget(key)}
        end
      end

  Add the `WidgetStore` module to your supervision tree. (This will add a
  supervisor process which will watch 2 processes: the `WidgetStore` data
  refresher agent and the accompanying Cachex process.)

      children = [
        WidgetStore,
        ...,
      ]

      Supervisor.start_link(children, opts)

  When WidgetStore is started, it will load the dump file if one was
  configured. If a `:refresh_interval` was configured, a full refresh of all
  the data will be triggered at that interval, forever.

  Your `WidgetStore` module defines how the value for a given key is found,
  but `DataWorker` takes care of parallelizing these calls and the caching
  functionality.

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
  alias DataWorker.Config
  require Logger

  @type key :: term
  @type value :: term
  @type opts :: keyword
  @type init_return ::
          {:ok, map} | :ok | {:warn, String.t()} | {:stop, String.t()}
  @type load_return :: {:ok, value} | {:error, String.t()}
  @type fetch_return :: {:ok, value} | {atom, term}

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
      raise ":bucket option must be defined in the use options!"

    quote do
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
        DataWorker.config(Keyword.get(use_opts, :bucket))
      end

      @doc "Returns a particular value from the config"
      @spec config(atom, atom) :: term
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

      @doc """
      Do the work to procure the value for a given key.

      This is intended only for internal use.
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
    Config.save_config_to_bucket!(config)

    ret =
      if maybe_load_file(config.bucket, config.file) do
        {:ok, nil}
      else
        {config.mod, :init, [config]}
        |> invoke_carefully()
        |> handle_init_ret(config)
      end

    with {:ok, nil} <- ret do
      maybe_schedule_full_refresh(config.refresh_interval)

      Logger.debug(fn ->
        l = config |> Map.from_struct() |> Enum.into([])
        "#{config.bucket}: Initialized DataWorker: #{inspect(l)}"
      end)
    end

    ret
  end

  @doc "Handle the signal to refresh the cache"
  def handle_info(:full_refresh, _) do
    this_worker_module().full_refresh()
    {:noreply, nil}
  end

  @doc "Fetch a value from the DataWorker"
  @spec fetch(module, key, opts) :: fetch_return
  def fetch(mod, key, opts \\ []) do
    %Config{cache_enabled: cache_enabled, bucket: bucket} = mod.config()

    ret =
      if cache_enabled do
        with {:ok, nil} <- Cachex.get(bucket, key) do
          {:ok, run_load(mod, key)}
        end
      else
        {:ok, run_load(mod, key)}
      end

    with {:ok, value} <- ret,
         fun when is_function(fun) <- Keyword.get(opts, :return_fn) do
      {:ok, fun.(key, value)}
    else
      _ -> ret
    end
  end

  @doc "Get a value from the DataWorker"
  @spec get(module, key, value, opts) :: value
  def get(mod, key, default \\ nil, opts \\ []) do
    case fetch(mod, key, default, opts) do
      {:ok, val} -> val
      _ -> default
    end
  end

  @doc "Handle the refreshing of all keys for a given worker module"
  @spec full_refresh(module, [any] | nil, opts) :: :ok
  def full_refresh(mod, keys \\ nil, opts \\ []) do
    %{bucket: bucket, refresh_interval: interval, file: file} = mod.config()

    keys = if keys, do: {:ok, keys}, else: keys(bucket)

    case keys do
      {:ok, keys} ->
        refresh_for_keys(mod, keys, Keyword.put(opts, :skip_dump?, true))

      {:error, error} ->
        Logger.error("#{bucket}: Error refreshing: #{inspect(error)}")
    end

    if file, do: cache_dump(bucket, file)

    maybe_schedule_full_refresh(interval)

    :ok
  end

  @doc """
  Get a value directly out of the cache (bucket), without calling `&load/1`
  """
  @spec direct_get(atom, any) :: {atom, any}
  def direct_get(bucket, key) do
    Cachex.get(bucket, key)
  end

  @doc "Put a value directly into the cache (bucket)"
  @spec direct_put(atom, any, any) :: {:ok, boolean} | {:error, boolean}
  def direct_put(bucket, key, val) do
    Cachex.put(bucket, key, val)
  end

  @doc "Get the cache keys in a bucket"
  @spec keys(atom) :: {:ok, [any]} | {:error, any}
  def keys(bucket) do
    with {:ok, keys} <- Cachex.keys(bucket) do
      {:ok, Enum.reject(keys, fn k -> k == :__config__ end)}
    end
  end

  defp config(bucket) do
    case DataWorker.direct_get(bucket, :__config__) do
      {:ok, %Config{} = config} ->
        config

      wat ->
        raise "Couldn't find config for #{__MODULE__}! Got #{inspect(wat)}"
    end
  end

  defp invoke_carefully({mod, fun, args}) do
    apply(mod, fun, args)
  catch
    type, error -> {:caught, type, error}
  end

  defp handle_init_ret(init_ret, config) do
    case init_ret do
      {:ok, map} when is_map(map) ->
        if config.cache_enabled, do: load_map_into_cache(config.mod, map)
        {:ok, nil}

      :ok ->
        {:ok, nil}

      {:warn, msg} ->
        Logger.warn(fn -> "#{config.mod} warns: #{msg}" end)
        {:ok, nil}

      {:stop, msg} ->
        {:stop, msg}

      {:caught, type, error} ->
        Logger.warn(fn ->
          "#{config.mod}.init error: #{inspect(type)}, #{inspect(error)}"
        end)

        {:ok, nil}

      wat ->
        {:stop,
         "Unrecognized `&init/1` return from #{config.mod}: #{inspect(wat)}"}
    end
  end

  # Optimizes & executes calling the `&load/1` function for the given keys
  @spec refresh_for_keys(module, [key], opts) :: :ok
  defp refresh_for_keys(mod, keys, opts) do
    parent = self()

    keys
    |> Enum.map(
      &spawn(fn ->
        try do
          run_load(mod, &1, opts)
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
    |> Enum.map(fn pid ->
      receive do
        ^pid -> nil
      end
    end)
  end

  # Refresh and return a particular value, On error, log and return `nil`
  @spec run_load(module, key, opts) :: value | nil
  defp run_load(mod, key, opts \\ []) do
    bucket = mod.config(:bucket)

    case invoke_carefully({mod, :load, [key]}) do
      {:ok, val} ->
        Logger.debug(fn ->
          ins = inspect(val, limit: 2, printable_limit: 100)
          "#{bucket}:#{inspect(key)}: Loaded: #{ins}"
        end)

        opts |> Keyword.get(:return_fn) |> maybe_call_fn([key, val])
        cache_save(mod, key, val, opts)
        val

      {:ok, val, map} when is_map(map) ->
        opts |> Keyword.get(:return_fn) |> maybe_call_fn([key, val])
        load_map_into_cache(mod, map)
        val

      :skip ->
        nil

      {:caught, type, error} ->
        Logger.warn("""
        #{mod}.load(#{inspect(key)}) error: #{inspect(type)}, #{inspect(error)}
        """)

        nil

      {:error, error} ->
        Logger.error("#{bucket}:#{inspect(key)}: Error loading: #{error}")
        nil
    end
  end

  defp maybe_call_fn(fun, args) when is_function(fun),
    do: apply(fun, args)

  defp maybe_call_fn(_, _),
    do: :ok

  # Save a value to the cache; log errors
  @spec cache_save(module, term, term, keyword) :: :ok
  defp cache_save(mod, key, val, opts) do
    bucket = mod.config(:bucket)

    case Cachex.put(bucket, key, val) do
      {:ok, true} ->
        file = mod.config(:file)
        skip_dump? = Keyword.get(opts, :skip_dump?, false)

        if file && not skip_dump?, do: cache_dump(bucket, file)

      {:error, :no_cache} ->
        Logger.error("#{bucket}:#{key}: No cache found, trying to save value")

      wat ->
        Logger.error("#{bucket}:#{key}: Problem saving: #{inspect(wat)}")
    end

    :ok
  end

  # Load an entire map into the cache; log errors
  @spec load_map_into_cache(module, map) :: :ok
  defp load_map_into_cache(mod, map) when is_atom(mod) and is_map(map) do
    Enum.each(Map.keys(map), fn key ->
      cache_save(mod, key, map[key], skip_dump?: true)
    end)

    Logger.debug(fn ->
      bucket = mod.config(:bucket)
      {:ok, keys} = keys(bucket)
      "#{bucket}: Loaded cache with #{length(keys)} keys"
    end)

    :ok
  end

  # Dump an entire cache to disk log errors
  @spec cache_dump(atom, String.t()) :: :ok
  defp cache_dump(bucket, file) do
    case Cachex.dump(bucket, file) do
      {:ok, true} ->
        Logger.debug(fn ->
          {:ok, keys} = keys(bucket)
          "#{bucket}: Saved bucket to disk with #{length(keys)} keys"
        end)

        :ok

      {:error, error} ->
        Logger.error("#{bucket}: Failed to dump bucket: #{error}")
        :ok
    end
  end

  # Get the module name for which this instance of DataWorker is running
  @spec this_worker_module :: module
  defp this_worker_module do
    self() |> Process.info() |> Keyword.get(:registered_name)
  end

  @spec maybe_load_file(atom, boolean) :: boolean
  defp maybe_load_file(_bucket, nil), do: false

  defp maybe_load_file(bucket, file) when byte_size(file) > 0 do
    case Cachex.load(bucket, file) do
      {:ok, _} ->
        Logger.debug(fn ->
          {:ok, keys} = keys(bucket)
          "#{bucket}: Loaded cache file with #{length(keys)} keys"
        end)

        true

      {:error, :unreachable_file} ->
        Logger.debug(fn -> "#{bucket}: No cache file found for bucket" end)
        false

      {:error, wat} ->
        Logger.warn("#{bucket}: Failed to load cache file: #{inspect(wat)}")
        false
    end
  end

  defp maybe_schedule_refresh(interval) when interval > 0 do
    Process.send_after(self(), :refresh, interval * 1_000)
  end

  defp maybe_schedule_refresh(_), do: :nah

  # Reads the config defined by :config_section
  defp maybe_read_config_section({:ok, {app, section}})
       when is_atom(app) and is_atom(section),
       do: Application.get_env(app, section) || []

  defp maybe_read_config_section(_), do: []
end
