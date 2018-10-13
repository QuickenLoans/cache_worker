alias DataWorker, as: D
alias DataWorker.Bucket, as: B
alias DataWorker.Config, as: C

defmodule T do
  def t do
    B.new(:t)

    B.store(:t, :a, :A)
    B.store(:t, :b, :B)
    B.store(:t, :c, :C)
  end

  def k do
    B.keys(:t)
  end

  @ets_table_options [
    :set,
    :public,
    :named_table,
    read_concurrency: true,
    write_concurrency: true
  ]

  def x do
    bucket = :x
    :ets.new(bucket, @ets_table_options)
    :ets.new(bucket, @ets_table_options)
  catch
    a, b -> IO.inspect({a, b})
  end

  def y do
    bucket = :y
    :ets.new(bucket, @ets_table_options)
    B.store(bucket, :a, 1)
    B.store(bucket, :a, 1)
  catch
    a, b -> IO.inspect({a, b})
  end
end