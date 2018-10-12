defmodule DataWorker.Config do
  @moduledoc """
  Holds the `DataWorker` configuration

  * `:bucket` - The namespace (atom) for the cache store.
  * `:bucket_enabled` - Boolean indicating if caching is enabled. If disabled,
    all `&get/1` and `&fetch/1` calls will call `&load/1`. Defaults to `true`.
  * `:file` - Full path and filename where we should save the cache to
    whenever it is updated. If `nil`, this functionality is disabled. If
    defined, the data will be used to seed the cache on start.
  * `:refresh_interval` - The interval (in seconds) between full cache
    refreshes. When this occurs, it is done in the background and the data is
    atomically replaced. No processes are blocked from reading. If `nil`,
    refreshing is disabled. Default is 900 (15 minutes).
  * `:config_fn` - This may be set to a function which returns a keyword list
    to be merged with the inital_options for the actual set of options to use
    at run-time. Leave as the default `nil` if unneeded.
  * `:mod` - Internally defined. Set to the name of the module the DataWorker
    is based on.
  """

  alias DataWorker.Bucket
  require Logger

  defstruct mod: nil,
            bucket_enabled: true,
            bucket: nil,
            file: nil,
            refresh_interval: 900,
            config_fn: nil

  @type t :: %__MODULE__{
          mod: module,
          bucket_enabled: boolean,
          bucket: String.t(),
          file: String.t(),
          refresh_interval: integer,
          config_fn: function | nil
        }

  @doc """
  Given an options keyword list, return a `%DataWorker.Config{}`, warning and
  raising as appropriate.
  """
  @spec normalize!(module, keyword) :: %__MODULE__{} | no_return
  def normalize!(mod, opts) do
    __MODULE__
    |> struct(opts)
    |> normalize_mod!(mod)
    |> maybe_add_config_fn()
    |> normalize_bucket!()
    |> normalize_bucket_enabled()
    |> normalize_file()
    |> normalize_refresh_interval()
  end

  defp maybe_add_config_fn(%{config_fn: fun} = c) when is_function(fun) do
    opts_from_fn = fun.()

    if Keyword.keyword?(opts_from_fn) do
      struct(c, opts_from_fn)
    else
      Logger.warn("""
      Expected keyword list from #{c.mod}'s config_fn, #{inspect(fun)}, got \
      #{inspect(opts_from_fn)}. Carrying on anyway.\
      """)

      c
    end
  end

  defp maybe_add_config_fn(%{config_fn: fun} = c) when not is_nil(fun) do
    Logger.warn("#{c.mod} has bad config_fn setting: #{inspect(fun)}")
    c
  end

  defp maybe_add_config_fn(c), do: c

  defp normalize_mod!(c, mod) do
    case function_exported?(mod, :__info__, 1) do
      true -> %{c | mod: mod}
      false -> raise "Not a module: #{mod}"
    end
  end

  defp normalize_bucket!(c) do
    case Map.fetch(c, :bucket) do
      {:ok, bucket} when is_atom(bucket) ->
        %{c | bucket: bucket}

      {:ok, bad} ->
        raise("Bad bucket name for #{c.mod}: #{bad}")

      :error ->
        raise("Bucket name not defined for #{c.mod}")
    end
  end

  defp normalize_bucket_enabled(c) do
    %{c | bucket_enabled: c.bucket_enabled == true}
  end

  defp normalize_file(%{file: "/" <> _} = c), do: c

  defp normalize_file(%{file: nil} = c), do: c

  defp normalize_file(%{file: wat} = c) do
    Logger.warn("Weird :file value for #{c.mod}: #{wat}")
    c
  end

  defp normalize_refresh_interval(%{refresh_interval: nil} = c), do: c

  defp normalize_refresh_interval(%{refresh_interval: int} = c)
       when is_integer(int) and int >= 0,
       do: c

  defp normalize_refresh_interval(%{refresh_interval: wat} = c) do
    Logger.warn("Weird :refresh_interval value for #{c.mod}: #{wat}")
    c
  end
end
