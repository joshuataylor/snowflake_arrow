defmodule SnowflakeArrow.ChunkConversionTest do
  use ExUnit.Case, async: true

  test "Can convert arrow to correct order of rows/columns with elixir types" do
    arrow_data_large = File.read!("benchmark/large_arrow")

    values = SnowflakeArrow.convert_snowflake_arrow_stream(arrow_data_large)

    assert length(values) == 16
    #    IO.inspect values

    rows =
      values
      |> Enum.zip()

    IO.inspect(rows)

    assert length(rows) == 139_701

    # spot check some records

    assert rows |> hd == [
             198_649,
             nil,
             nil,
             nil,
             nil,
             nil,
             nil,
             ~N[2024-04-04 23:20:12.064000],
             nil,
             ~N[2024-09-21 11:08:20.064000],
             ~D[2024-11-11],
             "{\n  \"key_489U1idsMBSdRJmIeMfj\": true\n}",
             "[\n  12,\n  \"twelve\",\n  undefined\n]",
             nil
           ]
  end
end
