defmodule SnowflakeArrow.MixProject do
  use Mix.Project

  def project do
    [
      app: :snowflake_arrow,
      name: "snowflake_arrow",
      description: "Snowflake specific Arrow implementation",
      version: "0.1.0",
      elixir: "~> 1.13",
      deps: deps(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:rustler, "~> 0.25.0"},
      {:benchee, "~> 1.1", optional: true},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      files: [
        "lib",
        "native",
        "mix.exs",
        "LICENSE"
      ],
      licenses: ["Apache License 2.0"],
      links: %{"GitHub" => "https://github.com/joshuataylor/snowflake_arrow"},
      maintainers: ["Joshua Taylor"]
    ]
  end
end
