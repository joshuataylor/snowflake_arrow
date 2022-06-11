defmodule SnowflakeArrow.ChunkConversionTest do
  use ExUnit.Case, async: true
  alias SnowflakeArrow.Native

  test "Can convert nulls and dates to correct without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/x.arrow"
        ])
      )

    values = Native.convert_arrow_stream_to_rows(data, true)
    assert values |> length == 1868
  end

  test "Can convert arrow to correct order of rows/columns without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_ROWS.json"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream_to_rows(data, true)

    assert length(values) == 100

    # spot check some records

    row = values |> hd

    IO.inspect(row)

    [
      row_number,
      sf_boolean,
      sf_varchar,
      sf_integer,
      sf_float,
      sf_float_tinto_iterwo_precision,
      sf_decimal_38_2,
      sf_timestamp_ntz,
      sf_timestamp_ltz,
      sf_timestamp,
      sf_date,
      sf_variant_json,
      sf_array,
      sf_object,
      sf_hex_binary,
      sf_base64_binary
    ] = row

    assert row_number == 1
  end
end
