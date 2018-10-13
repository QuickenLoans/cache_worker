defmodule DataWorker.Bucket do
  @moduledoc """
  The Bucket provides wrapper functionality around our data store, ETS.
  """

  require Logger

  @type bucket :: atom
  @type key :: term
  @type value :: term
  @type new_return :: :ok | :bucket_exists
  @type store_return :: :ok | :no_bucket
  @type fetch_return :: {:ok, value} | :undefined | :no_bucket
  @type keys_return :: [keys] | :no_bucket

  @ets_table_options [
    :set,
    :public,
    :named_table,
    read_concurrency: true,
    write_concurrency: true
  ]

  @spec new(bucket) :: new_return
  def new(table) do
    ^table = :ets.new(table, @ets_table_options)
  catch
    :error, :badarg ->
      Logger.warn("#{inspect(table)} table already exists!")
      :bucket_exists
  end

  @spec store(bucket, key, value) :: store_return
  def store(bucket, key, val) do
    true = :ets.insert(bucket, {key, val})
  catch
    :error, :badarg ->
      Logger.warn("#{inspect(bucket)} bucket doesn't exist!")
      :no_bucket
  end

  @spec fetch(bucket, key) :: fetch_return
  def fetch(bucket, key) do
    case :ets.lookup(bucket, key) do
      [{_k, val}] -> {:ok, val}
      [] -> :undefined
    end
  catch
    :error, :badarg ->
      Logger.warn("#{inspect(bucket)} bucket doesn't exist!")
      :no_bucket
  end

  @spec get(bucket, key, term) :: term
  def get(bucket, key, default \\ nil) do
    case fetch(bucket, key) do
      {:ok, val} -> val
      _ -> default
    end
  end

  @spec keys(bucket) :: keys_return
  def keys(bucket) do
    bucket |> key_stream() |> Enum.map(& &1)
  catch
    :error, :badarg ->
      Logger.warn("#{inspect(bucket)} bucket doesn't exist!")
      :no_bucket
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

  @doc """
  Sets up the ETS tables for a new bucket.

  For bucket `:foo`, the following tables are created:

    1. `:foo` to store the data
    2. `:foo_config` to store the `%DataWorker.Config{}`
  """
  @spec init_bucket(Config.t()) :: :ok
  def init_bucket(%{bucket: bucket} = config) do
    c_bucket = :"#{bucket}_config"

    new(bucket)
    new(c_bucket)
    store(c_bucket, nil, config)

    :ok
  end

  def config(bucket) do
    get(:"#{bucket}_config", nil)
  end
end
