defmodule SnowflakeArrow.FloatConversionTest do
  use ExUnit.Case, async: true

  test "Can convert nulls and floats to correct data type" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_FLOAT.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.read_arrow_stream_to_columns!()

    assert values == [
             {
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
             }
           ]
  end

  test "Can convert nulls and floats with 2 precision to correct" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_FLOAT_TWO_PRECISION.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.read_arrow_stream_to_columns!()

    assert values == [
             {
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
             }
           ]
  end
end
