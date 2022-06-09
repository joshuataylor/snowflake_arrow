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

    values = Native.convert_arrow_stream(data, true, false)

    assert values["SF_INTEGER"] ==
             [
               13_171_725_832,
               nil,
               nil,
               nil,
               16_395_724_444,
               nil,
               15_107_057_545,
               nil,
               17_910_485_500,
               nil
             ]

    values = Native.convert_arrow_stream(data, true, true)

    assert values == [
             [13_171_725_832],
             [nil],
             [nil],
             [nil],
             [16_395_724_444],
             [nil],
             [15_107_057_545],
             [nil],
             [17_910_485_500],
             [nil]
           ]
  end
end
