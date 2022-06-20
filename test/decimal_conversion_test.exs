defmodule SnowflakeArrow.DecimalConversionTest do
  use ExUnit.Case, async: true

  @row_type [
    %{
      "byteLength" => nil,
      "collation" => nil,
      "database" => "FOO",
      "length" => nil,
      "name" => "SF_DECIMAL_38_2",
      "nullable" => true,
      "precision" => nil,
      "scale" => nil,
      "schema" => "BAR",
      "table" => "TEST_DATA",
      "type" => "fixed"
    }
  ]

  test "Can convert decimals to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DECIMAL_38_2.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, @row_type, cast: true)
    assert length(values) == 10

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

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)

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

    assert length(values) == 10
  end
end
