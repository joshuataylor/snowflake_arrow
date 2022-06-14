defmodule SnowflakeArrow.ChunkConversionTest do
  use ExUnit.Case, async: true

  test "Can convert nulls and dates to correct without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/x.arrow"
        ])
      )

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)
    assert values |> length == 1868
  end

  test "Can convert arrow to correct order of rows/columns with elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_ROWS.json"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: true)

    assert length(values) == 100

    # spot check some records

    row = values |> hd

    assert row ==
             [
               1,
               nil,
               nil,
               1_211_510_379,
               nil,
               6167.02,
               nil,
               nil,
               nil,
               nil,
               ~D[2023-11-11],
               nil,
               nil,
               nil
             ]
  end
end
