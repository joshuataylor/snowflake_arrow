defmodule SnowflakeArrow.FloatConversionTest do
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
      "type" => "real"
    }
  ]

  test "Can convert nulls and floats to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_FLOAT.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, @row_type, cast: true)

    assert values == [
             [nil],
             [10362.79846742],
             [nil],
             [nil],
             [nil],
             [16728.54054275],
             [nil],
             [nil],
             [19064.12377525],
             [nil]
           ]

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)

    assert values == [
             [nil],
             [10362.79846742],
             [nil],
             [nil],
             [nil],
             [16728.54054275],
             [nil],
             [nil],
             [19064.12377525],
             [nil]
           ]
  end

  test "Can convert nulls and floats with 2 precision to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_FLOAT_TWO_PRECISION.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, @row_type, cast: true)

    assert values == [
             [3563.39],
             [nil],
             [26800.24],
             [13280.73],
             [nil],
             [nil],
             [9806.23],
             [18247.51],
             [19212.24],
             [nil]
           ]

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)

    assert values == [
             [3563.39],
             [nil],
             [26800.24],
             [13280.73],
             [nil],
             [nil],
             [9806.23],
             [18247.51],
             [19212.24],
             [nil]
           ]
  end
end
