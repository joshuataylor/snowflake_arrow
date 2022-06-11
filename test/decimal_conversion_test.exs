defmodule SnowflakeArrow.DecimalConversionTest do
  use ExUnit.Case, async: true
  alias SnowflakeArrow.Native

  test "Can convert decimals to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DECIMAL_38_2.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream_to_rows(data, true)
    require IEx
    IEx.pry

    assert values== [[2188579], [nil], [nil], [739560], [2673749], [747949], [2125618], [nil], [1768154], [nil]]
  end
end
