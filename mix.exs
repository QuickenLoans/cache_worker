defmodule CacheWorker.MixProject do
  @moduledoc false
  use Mix.Project

  @github_url "https://github.com/QuickenLoans/cache_worker"

  def project do
    [
      # Missing metadata fields: description, licenses, links
      app: :cache_worker,
      version: "0.1.0",
      elixir: "~> 1.7",
      package: package(),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ],

      # Hex
      description: """
      Defines a behavior to be implemented for managing data that should be \
      held in the VM and periodically refreshed.\
      """,

      # Docs
      name: "CacheWorker",
      source_url: @github_url,
      homepage_url: @github_url,
      docs: [
        main: "CacheWorker",
        extras: ["README.md"]
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
      {:excoveralls, "~> 0.10", only: :test},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      lint: "credo --strict"
    ]
  end

  defp package do
    [
      maintainers: ["Adam Bellinson"],
      licenses: ["MIT"],
      links: %{"github" => @github_url},
      files: [
        "lib",
        "mix.exs",
        "README.md"
      ]
    ]
  end
end
