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
  @spec keys(bucket) :: {:ok, [key]} | :no_bucket
  def keys(bucket) do
    {:ok, bucket |> key_stream() |> Enum.map(& &1)}
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
  Load a bucket's dump file back into memory.

  An error will result if a table of the same name already exists. The
  operation will also fail if the file was saved with one bucket name and
  loaded it with another.
  """
  @spec load(bucket, String.t()) :: :ok | {:error, term}
  def load(bucket, file) do
    case :ets.file2tab(String.to_charlist(file)) do
      {:ok, ^bucket} ->
        :ok

      {:ok, other} ->
        {:error, "Tried to load #{other} dump into #{bucket} bucket!"}

      {:error, err} ->
        {:error, err}
    end
  end
end
