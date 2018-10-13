defmodule DataWorker.MixProject do
  use Mix.Project

  def project do
    [
      app: :data_worker,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
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
      {:cachex, "~> 3.0"}
    ]
  end

  defp aliases do
    [
      lint: "credo --strict"
    ]
  end
end
