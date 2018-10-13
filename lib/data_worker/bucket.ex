defmodule DataWorker.Bucket do
  @moduledoc """
  The Bucket provides wrapper functionality around our data store, ETS.
  """

  @type bucket :: atom
  @type key :: term
  @type value :: term

  @ets_table_options [
    :set,
    :public,
    :named_table,
    read_concurrency: true,
    write_concurrency: true
  ]

  @spec new(bucket) :: :ok | :bucket_exists
  def new(table) do
    ^table = :ets.new(table, @ets_table_options)
  catch
    :error, :badarg -> :bucket_exists
  end

  @spec store(bucket, key, value) :: :ok | :no_bucket
  def store(bucket, key, val) do
    true = :ets.insert(bucket, {key, val})
    :ok
  catch
    :error, :badarg -> :no_bucket
  end

  @spec fetch(bucket, key) :: {:ok, value} | :undefined | :no_bucket
  def fetch(bucket, key) do
    case :ets.lookup(bucket, key) do
      [{_k, val}] -> {:ok, val}
      [] -> :undefined
    end
  catch
    :error, :badarg -> :no_bucket
  end

  @spec get(bucket, key, term) :: term
  def get(bucket, key, default \\ nil) do
    case fetch(bucket, key) do
      {:ok, val} -> val
      _ -> default
    end
  end

  @spec keys(bucket) :: [key] | :no_bucket
  def keys(bucket) do
    bucket |> key_stream() |> Enum.map(& &1)
  catch
    :error, :badarg -> :no_bucket
  end

  def key_stream(bucket) do
    Stream.resource(
      fn -> :ets.first(bucket) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        previous_key -> {[previous_key], :ets.next(bucket, previous_key)}
      end,
      fn _ -> :ok end
    )
  end

  @spec dump(bucket, String.t()) :: {:ok, bucket} | {:error, term}
  def dump(bucket, file) do
    :ets.tab2file(bucket, String.to_charlist(file))
  end

  @spec load(String.t()) :: :ok | {:error, term}
  def load(file) do
    :ets.file2tab(String.to_charlist(file))
  end

  @cache_bucket_config_key nil

  @doc """
  Sets up the ETS tables for a new bucket.

  For bucket `:foo`, the following tables are created:

    1. `:foo` to store the data
    2. `:foo_config` to store the `%DataWorker.Config{}`
  """
  @spec init_bucket(Config.t()) :: :ok | :no_bucket | :bucket_exists
  def init_bucket(%{bucket: bucket} = config) do
    with c_bucket <- :"#{bucket}_config",
         :ok <- new(bucket),
         :ok <- new(c_bucket) do
      store(c_bucket, @cache_bucket_config_key, config)
    end
  end

  def config(bucket) do
    get(:"#{bucket}_config", @cache_bucket_config_key)
  end
end
