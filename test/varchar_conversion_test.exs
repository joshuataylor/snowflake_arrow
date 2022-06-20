defmodule SnowflakeArrow.TextConversionTest do
  use ExUnit.Case, async: true

  test "Can convert text to string without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_VARCHAR.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)

    assert values == [
             [nil],
             [nil],
             ["fF7Lk7YKfXtYFsY2fVXN"],
             [nil],
             ["dvhJoe9A0qtgRJwHFMHN"],
             [nil],
             [nil],
             ["dBmT57wW3U0pLaJp9PGV"],
             [nil],
             ["XRFjUy5wdbzE7k1z9SSa"]
           ]
  end

  test "Can read chunked rows correctly" do
    arrow_data_large = File.read!("benchmark/large_arrow")

    parsed =
      SnowflakeArrow.Native.convert_arrow_stream_to_columns(arrow_data_large, false)
      |> Enum.at(2)

    assert length(parsed) == 139_701
  end
end
