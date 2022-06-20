defmodule SnowflakeArrow.BooleanConversionTest do
  use ExUnit.Case, async: true

  test "Can convert booleans to correct" do
    row_type = [
      %{
        "byteLength" => nil,
        "collation" => nil,
        "database" => "FOO",
        "length" => nil,
        "name" => "SF_BOOLEAN",
        "nullable" => true,
        "precision" => nil,
        "scale" => nil,
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "type" => "boolean"
      }
    ]

    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_BOOLEAN.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, row_type, cast: true)
    assert length(values) == 10

    # Transpose
    assert values == [[nil], [nil], [nil], [nil], [nil], [false], [nil], [true], [nil], [false]]
  end
end
