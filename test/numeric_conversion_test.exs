defmodule SnowflakeArrow.NumericConversionTest do
  use ExUnit.Case, async: true
  alias SnowflakeArrow.Native

  test "Can convert nulls and integer to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_INTEGER.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream_to_rows(data, true)

    assert values == [[13171725832], [nil], [nil], [nil], [16395724444], [nil], [15107057545], [nil], [17910485500], [nil]]
  end
end
