defmodule SnowflakeArrow.MixProject do
  use Mix.Project

  def project do
    [
      app: :snowflake_arrow,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:benchee, "~> 1.1", optional: true}
    ]
  end
end
