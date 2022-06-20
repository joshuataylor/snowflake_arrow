defmodule SnowflakeArrow.ChunkConversionTest do
  use ExUnit.Case, async: true

  test "Can convert arrow to correct order of rows/columns with elixir types" do
    arrow_data_large = File.read!("benchmark/large_arrow")

    row_type = [
      %{
        "name" => "ROW_NUMBER",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => 0,
        "precision" => 18,
        "type" => "fixed",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_BOOLEAN",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "boolean",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_VARCHAR",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "text",
        "byteLength" => 16_777_216,
        "length" => 16_777_216,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_INTEGER",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => 0,
        "precision" => 38,
        "type" => "fixed",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_FLOAT",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "real",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_FLOAT_TWO_PRECISION",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "real",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_DECIMAL_38_2",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => 2,
        "precision" => 38,
        "type" => "fixed",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_TIMESTAMP_NTZ",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => 9,
        "precision" => 0,
        "type" => "timestamp_ntz",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_TIMESTAMP_LTZ",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => 9,
        "precision" => 0,
        "type" => "timestamp_ltz",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_TIMESTAMP",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => 9,
        "precision" => 0,
        "type" => "timestamp_ntz",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_DATE",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "date",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_VARIANT_JSON",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "variant",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_ARRAY",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "array",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_OBJECT",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "object",
        "byteLength" => nil,
        "length" => nil,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_HEX_BINARY",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "binary",
        "byteLength" => 8_388_608,
        "length" => 8_388_608,
        "nullable" => true,
        "collation" => nil
      },
      %{
        "name" => "SF_BASE64_BINARY",
        "database" => "FOO",
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "scale" => nil,
        "precision" => nil,
        "type" => "binary",
        "byteLength" => 8_388_608,
        "length" => 8_388_608,
        "nullable" => true,
        "collation" => nil
      }
    ]

    values = SnowflakeArrow.convert_arrow_to_rows(arrow_data_large, row_type, cast: true)

    #    IO.inspect(values)

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
