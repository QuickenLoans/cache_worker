defmodule DataWorker.Bucket do
  @moduledoc """
  The Bucket provides wrapper functionality around our data store, ETS.
  """

  require Logger

  @type bucket :: atom
  @type key :: term
  @type value :: term
  @type new_return :: :ok
  @type store_return :: :ok | {:error, :no_bucket}
  @type fetch_return :: {:ok, value} | :undefined | {:error, :no_bucket}

  @ets_table_options [
    :set,
    :public,
    :named_table,
    read_concurrency: true,
    write_concurrency: true
  ]

  @spec new(bucket) :: new_return
  def new(bucket) do
    case :ets.new(bucket, @ets_table_options) do
      ^bucket -> :ok
    end
  end

  @spec store(bucket, key, value) :: store_return
  def store(bucket, key, val) do
    case :ets.insert(bucket, {key, val}) do
      true -> :ok
    end
  catch
    e in ArgumentError -> {:error, :no_bucket}
  end

  @spec fetch(bucket, key) :: fetch_return
  def fetch(bucket, key) do
    case :ets.lookup(bucket, key) do
      [] -> :undefined
      [{_k, val}] -> {:ok, val}
    end
  catch
    e in ArgumentError -> {:error, :no_bucket}
  end

  @doc """
  Save some configuration. Probably only fails if called more than once for
  the same bucket.
  """
  def store_config!(%{bucket: bucket} = config) do
    c_bucket = :"#{bucket}_config"

    try
      :ok = new(c_bucket)
    catch
      e in ArgumentError ->
        Logger.warn("""
        Perhaps &store_config!/1 was already called for #{bucket}? \
        Saving config anyhow...
        """)
    end

    store(c_bucket, nil, config)
  end

  def config(bucket) do
    with {:ok, config} <- fetch(bucket, :"#{bucket}_config") do
      config
    else
      _ -> nil
    end
  end
end
