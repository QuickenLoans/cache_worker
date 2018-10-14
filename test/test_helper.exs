# Thank you,
# https://github.com/OvermindDL1/typed_elixir/blob/master/test/test_helper.exs
defmodule CompileTimeAssertions do
  @moduledoc "Test helper functions"
  defmodule(DidNotRaise, do: defstruct(message: nil))

  defmacro assert_compile_time_raise(expected_exception, expected_message, fun) do
    actual_exception =
      try do
        fun
        |> Code.eval_quoted()
        |> Code.eval_quoted()

        %DidNotRaise{}
      rescue
        e -> e
      end

    quote do
      assert unquote(actual_exception.__struct__) ===
               unquote(expected_exception)

      assert unquote(actual_exception.message) === unquote(expected_message)
    end
  end

  defmacro assert_compile_time_throw(expected_error, fun) do
    actual_exception =
      try do
        Code.eval_quoted(fun)
        :DID_NOT_THROW
      catch
        e -> e
      end

    quote do
      assert unquote(expected_error) === unquote(Macro.escape(actual_exception))
    end
  end

  defmacro match_compile_time_raise(expected_exception, fun) do
    actual_exception =
      try do
        Code.eval_quoted(fun)
        %DidNotRaise{}
      rescue
        e -> e
      end

    quote do
      unquote(expected_exception) = unquote(actual_exception)
    end
  end

  defmacro match_compile_time_throw(expected_error, fun) do
    actual_exception =
      try do
        Code.eval_quoted(fun)
        :DID_NOT_THROW
      catch
        e -> e
      end

    quote do
      unquote(expected_error) = unquote(Macro.escape(actual_exception))
    end
  end
end

ExUnit.start()
