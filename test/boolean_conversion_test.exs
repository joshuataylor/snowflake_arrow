defmodule SnowflakeArrow.BooleanConversionTest do
  use ExUnit.Case, async: true

  test "Can convert booleans to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_BOOLEAN.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: true)

    # Transpose
    assert values == [[nil], [nil], [nil], [nil], [nil], [false], [nil], [true], [nil], [false]]
  end
end
