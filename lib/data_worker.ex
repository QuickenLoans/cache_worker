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

  * `:skip_save` - When set to `true` and `&load/1` is invoked to load a value,
    that value will not be saved to the cache. Instead, it's expected that the
    calling code will manually do this via `&direct_set(key, value)`. This is
    useful if the new data needs to be tested in a pipeline before committed to
    the cache. Once confirmed, the value can be inserted via `&direct_set/2`.
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
  @type fetch_return :: {:ok, value} | {:error, String.t()} | :no_bucket

  @doc "Returns the child_spec which should be used by the supervisor"
  @callback child_spec(keyword) :: Supervisor.child_spec()

  @doc "Give a DataWorker the opportunity to settle in"
  @callback init(Config.t()) :: init_return

  @doc """
  Do the work to procure the value for a given key.

  This callback is intended only for internal use.
  """
  @callback load(key) :: load_return

  @doc "Invoked on the `:refresh_interval` when the cache should be refreshed"
  @callback full_refresh :: :ok

  defmacro __using__(use_opts) do
    # Compile-time check with friendly reminder
    is_atom(Keyword.get(use_opts, :bucket, "not atom")) ||
      raise ":bucket option must be defined directly in the use options!"

    bucket = Keyword.get(use_opts, :bucket)

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
      @spec config :: Config.t() | no_return
      def config do
        case Bucket.get(unquote(:"#{bucket}_config"), nil) do
          %Config{} = config ->
            config

          nil ->
            raise """
            No config available for #{unquote(bucket)} bucket. \
            Did you forget to start its DataWorker?\
            """
        end
      end

      @doc "Returns a particular value from the config"
      @spec config(DataWorker.key(), term) :: term | no_return
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
      @spec get(DataWorker.key(), DataWorker.opts()) :: term
      def get(key, opts \\ []),
        do: DataWorker.get(__MODULE__, key, opts)

      @doc "Set a key/val in the bucket directly"
      @spec direct_get(DataWorker.key()) :: term
      def direct_get(key), do: DataWorker.direct_get(__MODULE__, key)

      @doc "Get a key/val from the bucket directly"
      @spec direct_set(DataWorker.key(), DataWorker.value()) :: :ok | :no_bucket
      def direct_set(key, val), do: DataWorker.direct_set(__MODULE__, key, val)

      @doc "Gets a list of all keys in a given bucket."
      @spec keys :: [DataWorker.key()]
      def keys, do: Bucket.keys(unquote(bucket))

      defoverridable init: 1, full_refresh: 0
    end
  end

  @doc false
  def child_spec(mod, use_opts) do
    %{id: mod, start: {__MODULE__, :start_link, [mod, use_opts]}}
  end

  @doc false
  def start_link(mod, use_opts) do
    GenServer.start_link(__MODULE__, {mod, use_opts}, name: mod)
  end

  @doc false
  def init({mod, use_opts}) do
    %{bucket: bucket} = config = Config.normalize!(mod, use_opts)

    # Skip init if we load a cache file
    if maybe_load_file(bucket, config.file) do
      :ok
    else
      :ok = Bucket.new(bucket)

      {config.mod, :init, [config]}
      |> invoke_carefully()
      |> handle_init_ret(config)
    end
    |> case do
      :ok ->
        c_table = :"#{bucket}_config"
        :ok = Bucket.new(c_table)
        :ok = Bucket.set(c_table, nil, config)

        schedule_full_refresh(config.refresh_interval)

        Logger.debug(fn ->
          l = config |> Map.from_struct() |> Enum.into([])
          "#{bucket}: Initialized DataWorker: #{inspect(l)}"
        end)

        {:ok, nil}

      {:stop, msg} ->
        Bucket.delete(bucket)
        {:stop, msg}
    end
  end

  @doc "Handle the signal to refresh the cache"
  def handle_info(:full_refresh, _) do
    this_worker_module().full_refresh()
    {:noreply, nil}
  end

  @doc "Fetch a value from the DataWorker"
  @spec fetch(module, key, opts) :: fetch_return
  def fetch(mod, key, opts \\ []) do
    do_fetch(mod.config(), key, opts)
  end

  @doc "Get a value from the DataWorker"
  @spec get(module, key, opts) :: value | nil
  def get(mod, key, opts \\ []) do
    case fetch(mod, key, opts) do
      {:ok, val} -> val
      {:error, _} -> nil
    end
  end

  @doc "Handle the refreshing of all keys for a given worker module"
  @spec full_refresh(module) :: :ok | no_return
  def full_refresh(mod) do
    %{bucket: bucket} = config = mod.config()

    {:ok, keys} = Bucket.keys(bucket)

    Logger.info("refreshing #{inspect(config)}, #{inspect(keys)}")

    refresh_for_keys(config, keys)

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

  defp do_fetch(%{cache_enabled: false} = config, key, opts) do
    run_load(config, key, opts)
  end

  defp do_fetch(%{bucket: bucket} = config, key, opts) do
    with :undefined <- Bucket.fetch(bucket, key),
         {:ok, val} <- run_load(config, key, opts) do
      file_dump(bucket, config.file)
      {:ok, val}
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
          "#{config.mod}.init #{inspect(type)}, #{inspect(error)}"
        end)

        :ok

      wat ->
        Logger.warn(fn ->
          "Unrecognized `&init/1` return from #{config.mod}: #{inspect(wat)}"
        end)

        :ok
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
          "Loaded #{bucket}[#{inspect(key)}]: #{ins}"
        end)

        unless Keyword.get(opts, :skip_save?), do: save_value(config, key, val)
        {:ok, val}

      {:ok, val, map} when is_map(map) ->
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

      woah ->
        msg = """
        Something invalid was returned from \
        #{mod}.load(#{inspect(key)}): #{inspect(woah)}\
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
      :ok ->
        :ok

      :no_bucket ->
        Logger.error("#{bucket}:#{key}: Tried to save value, but no bucket!")
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
      :ok ->
        Logger.debug(fn ->
          {:ok, keys} = Bucket.keys(bucket)
          "#{bucket}: Saved bucket to disk with #{length(keys)} keys"
        end)

        :ok

      {:error, msg} ->
        Logger.error("#{bucket}: Failed to dump bucket: #{inspect(msg)}")
        :ok
    end
  end

  @spec maybe_load_file(bucket, String.t() | nil) :: boolean
  defp maybe_load_file(bucket, file) when byte_size(file) > 0 do
    case Bucket.load(bucket, file) do
      :ok ->
        Logger.debug(fn ->
          {:ok, keys} = Bucket.keys(bucket)
          "#{bucket}: Loaded cache file with #{length(keys)} keys"
        end)

        true

      {:error, msg} ->
        Logger.warn("""
        #{bucket}: Failed loading cache file: #{file}: #{inspect(msg)}\
        """)

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
    Process.send_after(self(), :full_refresh, Kernel.trunc(interval * 1_000))
  end

  defp schedule_full_refresh(_), do: nil
end
