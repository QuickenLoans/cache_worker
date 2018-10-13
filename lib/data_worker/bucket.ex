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

  @cache_bucket_config_key nil

  @doc """
  Sets up the ETS tables for a new bucket.

  `%DataWorker.Config{}` is recommended as the argument, but only a map is
  required. For bucket `:foo`, the following tables are created:

    1. `:foo` to store the data
    2. `:foo_config` to store the `%DataWorker.Config{}`
  """
  @spec init_bucket(map) :: :ok
  def init_bucket(%{bucket: bucket} = config) do
    c_table = :"#{bucket}_config"
    :ok = new(bucket)
    :ok = new(c_table)
    :ok = set(c_table, @cache_bucket_config_key, config)
  end

  @spec delete_bucket(bucket) :: :ok | :no_bucket
  def delete_bucket(bucket) do
    with c_table <- :"#{bucket}_config",
         :ok <- delete(bucket),
         :ok <- delete(c_table) do
      :ok
    else
      :no_table -> :no_bucket
    end
  end

  @doc "Create a new table, wiping out an existing table if needed."
  @spec new(bucket) :: :ok
  def new(table) do
    ^table = :ets.new(table, @ets_table_options)
    :ok
  catch
    :error, :badarg ->
      :ok = delete(table)
      ^table = :ets.new(table, @ets_table_options)
      :ok
  end

  @doc "Delete a table"
  @spec delete(bucket) :: :ok | :no_table
  def delete(table) do
    true = :ets.delete(table)
    :ok
  catch
    :error, :badarg -> :no_table
  end

  @doc """
  Set a key/val pair into a bucket.

  If the key already exists, its value is overwritten.
  """
  @spec set(bucket, key, value) :: :ok | :no_bucket
  def set(bucket, key, val) do
    true = :ets.insert(bucket, {key, val})
    :ok
  catch
    :error, :badarg -> :no_bucket
  end

  @doc "Fetch a value from the given bucket by its key."
  @spec fetch(bucket, key) :: {:ok, value} | :undefined | :no_bucket
  def fetch(bucket, key) do
    case :ets.lookup(bucket, key) do
      [{_k, val}] -> {:ok, val}
      [] -> :undefined
    end
  catch
    :error, :badarg -> :no_bucket
  end

  @doc "Get a value from the given bucket by its key."
  @spec get(bucket, key, term) :: term
  def get(bucket, key, default \\ nil) do
    case fetch(bucket, key) do
      {:ok, val} -> val
      _ -> default
    end
  end

  @doc "Get a list of all keys in a given bucket."
  @spec keys(bucket) :: [key] | :no_bucket
  def keys(bucket) do
    bucket |> key_stream() |> Enum.map(& &1)
  catch
    :error, :badarg -> :no_bucket
  end

  @doc "Get a stream of all keys for a given bucket."
  @spec key_stream(bucket) :: Enumerable.t()
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
  Dump all bucket data to disk. The file is connected to the bucket name.
  """
  @spec dump(bucket, String.t()) :: {:ok, bucket} | {:error, term}
  def dump(bucket, file) do
    :ets.tab2file(bucket, String.to_charlist(file))
  end

  @doc """
  Load a bucket's dump file back into memory, clobbering any existing bucket
  of the same name.

  An error will result from dumping a bucket with one name and loading it
  with another.
  """
  @spec load(bucket, String.t()) :: :ok | {:error, term}
  def load(bucket, file) do
    # Just in case
    delete(bucket)

    case :ets.file2tab(String.to_charlist(file)) do
      {:ok, ^bucket} ->
        :ok

      {:ok, other} ->
        {:error, "Tried to load #{other} dump into #{bucket} bucket!"}

      {:error, err} ->
        {:error, err}
    end
  end

  def config(bucket) do
    get(:"#{bucket}_config", @cache_bucket_config_key)
  end
end
