defmodule SnowflakeArrow.DecimalConversionTest do
  use ExUnit.Case, async: true

  test "Can convert decimals to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DECIMAL_38_2.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: true)

    assert values == [
             [21885.79],
             [nil],
             [nil],
             [7395.6],
             [26737.49],
             [7479.49],
             [21256.18],
             [nil],
             [17681.54],
             [nil]
           ]
  end
end
