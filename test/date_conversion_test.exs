defmodule SnowflakeArrow.DateConversionTest do
  use ExUnit.Case, async: true

  @row_type [
    %{
      "byteLength" => nil,
      "collation" => nil,
      "database" => "FOO",
      "length" => nil,
      "name" => "SF_DATE",
      "nullable" => true,
      "precision" => nil,
      "scale" => nil,
      "schema" => "BAR",
      "table" => "TEST_DATA",
      "type" => "date"
    }
  ]

  test "Can convert nulls and dates to correct without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DATE.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)
    assert length(values) == 10

    assert values == [
             [nil],
             [nil],
             [nil],
             ["2024-05-26"],
             [nil],
             [nil],
             [nil],
             [nil],
             [nil],
             ["2022-07-30"]
           ]
  end

  test "Can convert nulls and dates to correct with elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DATE.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, @row_type, cast: true)

    assert values == [
             [nil],
             [nil],
             [nil],
             [~D[2024-05-26]],
             [nil],
             [nil],
             [nil],
             [nil],
             [nil],
             [~D[2022-07-30]]
           ]
  end
end
