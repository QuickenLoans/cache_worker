defmodule CacheWorker.Config do
  @moduledoc """
  Holds the `CacheWorker` configuration

  * `:bucket` - The namespace (atom) for the cache store.
  * `:cache_enabled` - Boolean indicating if caching is enabled. If disabled,
    all `&get/1` and `&fetch/1` calls will call `&load/1`. Defaults to `true`.
  * `:file` - Full path and filename where we should save the cache to
    whenever it is updated. If `nil`, this functionality is disabled. If
    defined, the data will be used to seed the cache on start.
  * `:refresh_interval` - The interval (in seconds) after the end of a full
    refresh (or init step) until a full_refresh is triggered. When this occurs,
    it is done in the background and the data is atomically replaced. No
    processes are blocked from reading. If `nil`, refreshing is disabled.
    Default is 900 (15 minutes).
  * `:config_fn` - This may be set to a function which returns a keyword list
    to be merged with the inital_options for the actual set of options to use
    at run-time. Leave as the default `nil` if unneeded.
  * `:mod` - Internally defined. Set to the name of the module the CacheWorker
    is based on.
  """

  require Logger

  defstruct mod: nil,
            cache_enabled: true,
            bucket: nil,
            file: nil,
            refresh_interval: 900,
            config_fn: nil

  @type t :: %__MODULE__{
          mod: module,
          cache_enabled: boolean,
          bucket: String.t(),
          file: String.t(),
          refresh_interval: integer,
          config_fn: function | nil
        }

  @doc """
  Given an options keyword list, return a `%CacheWorker.Config{}`, warning and
  raising as appropriate.
  """
  @spec normalize!(module, keyword) :: %__MODULE__{} | no_return
  def normalize!(mod, opts) do
    __MODULE__
    |> struct(opts)
    |> normalize_mod!(mod)
    |> maybe_add_config_fn!()
    |> normalize_bucket!()
    |> normalize_cache_enabled()
    |> normalize_file!()
    |> normalize_refresh_interval!()
  end

  defp maybe_add_config_fn!(%{config_fn: fun} = c) when is_function(fun) do
    opts_from_fn = fun.()

    if Keyword.keyword?(opts_from_fn) do
      struct(c, opts_from_fn)
    else
      raise """
      Expected keyword list from #{c.mod}'s config_fn, #{inspect(fun)} \
      got #{inspect(opts_from_fn)}!\
      """
    end
  end

  defp maybe_add_config_fn!(%{config_fn: nil} = c), do: c

  defp maybe_add_config_fn!(%{config_fn: bad} = c),
    do: raise("#{c.mod} has bad config_fn setting: #{inspect(bad)}")

  defp normalize_mod!(c, mod) do
    case function_exported?(mod, :__info__, 1) do
      true -> %{c | mod: mod}
      false -> raise "Not a module: #{mod}"
    end
  end

  defp normalize_bucket!(c) do
    case Map.fetch(c, :bucket) do
      {:ok, bucket} when is_atom(bucket) and not is_nil(bucket) ->
        %{c | bucket: bucket}

      {:ok, bad} ->
        raise("Bad bucket name for #{c.mod}: #{bad}")
    end
  end

  defp normalize_cache_enabled(c) do
    %{c | cache_enabled: c.cache_enabled == true}
  end

  defp normalize_file!(%{file: file} = c)
       when is_nil(file) or byte_size(file) > 0,
       do: c

  defp normalize_file!(%{file: wat} = c) do
    raise("Unrecognized file for #{c.mod}: #{inspect(wat)}")
  end

  defp normalize_refresh_interval!(%{refresh_interval: nil} = c), do: c

  defp normalize_refresh_interval!(%{refresh_interval: num} = c)
       when is_number(num) and num >= 0,
       do: c

  defp normalize_refresh_interval!(%{refresh_interval: wat} = c) do
    raise("Weird :refresh_interval value for #{c.mod}: #{wat}")
  end
end
