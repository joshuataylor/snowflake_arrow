defmodule SnowflakeArrow.FloatConversionTest do
  use ExUnit.Case, async: true
  alias SnowflakeArrow.Native

  test "Can convert nulls and floats to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_FLOAT.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, true)

    assert values["SF_FLOAT"] == [
             nil,
             10362.79846742,
             nil,
             nil,
             nil,
             16728.54054275,
             nil,
             nil,
             19064.12377525,
             nil
           ]
  end

  test "Can convert nulls and floats with 2 precision to correct" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_FLOAT_TWO_PRECISION.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, true)

    assert values["SF_FLOAT_TWO_PRECISION"] == [
             3563.39,
             nil,
             26800.24,
             13280.73,
             nil,
             nil,
             9806.23,
             18247.51,
             19212.24,
             nil
           ]
  end
end
