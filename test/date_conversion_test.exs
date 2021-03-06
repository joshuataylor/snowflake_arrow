defmodule SnowflakeArrow.DateConversionTest do
  use ExUnit.Case, async: true

  test "Can convert nulls and dates to correct with elixir types" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DATE.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.convert_snowflake_arrow_stream()

    assert values == [
             [
               nil,
               nil,
               nil,
               ~D[2024-05-26],
               nil,
               nil,
               nil,
               nil,
               nil,
               ~D[2022-07-30]
             ]
           ]
  end
end
