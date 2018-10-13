defmodule DataWorker.MixProject do
  @moduledoc false
  use Mix.Project

  def project do
    [
      app: :data_worker,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 0.9.1", only: [:dev, :test]},
      {:excoveralls, "~> 0.10", only: :test}
    ]
  end

  defp aliases do
    [
      lint: "credo --strict"
    ]
  end
end
