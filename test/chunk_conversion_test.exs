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

    values = Native.convert_arrow_stream(data, false)
    assert values["SF_VARCHAR"] |> length == 1868
  end
end
