defmodule SnowflakeArrow.NumericConversionTest do
  use ExUnit.Case, async: true

  @row_type [
    %{
      "byteLength" => nil,
      "collation" => nil,
      "database" => "FOO",
      "length" => nil,
      "name" => "SF_FLOAT",
      "nullable" => true,
      "precision" => nil,
      "scale" => nil,
      "schema" => "BAR",
      "table" => "TEST_DATA",
      "type" => "fixed"
    }
  ]

  test "Can convert nulls and integer to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_INTEGER.arrow"
        ])
      )
      |> Base.decode64!()

    test_data = [
      [13_171_725_832],
      [nil],
      [nil],
      [nil],
      [16_395_724_444],
      [nil],
      [15_107_057_545],
      [nil],
      [17_910_485_500],
      [nil]
    ]

    assert SnowflakeArrow.convert_arrow_to_rows(data, @row_type, cast: true) == test_data
    assert SnowflakeArrow.convert_arrow_to_rows(data, cast: false) == test_data
  end
end
