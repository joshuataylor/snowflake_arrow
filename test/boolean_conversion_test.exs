defmodule SnowflakeArrow.BooleanConversionTest do
  use ExUnit.Case, async: true
  alias SnowflakeArrow.Native

  test "Can convert booleans to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_BOOLEAN.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, true, false)

    assert values["SF_BOOLEAN"] == [
             nil,
             nil,
             nil,
             nil,
             nil,
             false,
             nil,
             true,
             nil,
             false
           ]

    values = Native.convert_arrow_stream(data, true, true)

    # Transpose
    assert values == [
             [nil],
             [nil],
             [nil],
             [nil],
             [nil],
             [false],
             [nil],
             [true],
             [nil],
             [false]
           ]
  end
end
