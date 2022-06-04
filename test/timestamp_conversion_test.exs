defmodule SnowflakeArrow.TimestampConversionTest do
  use ExUnit.Case, async: true
  alias SnowflakeArrow.Native

  # Includes tests for NTZ/LTZ and normal timestamp conversions

  test "Can convert TIMESTAMP to string without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, false)

    assert values == [
             [
               nil,
               nil,
               "2022-12-29 07:22:31",
               "2023-04-10 18:32:21",
               nil,
               "2023-11-03 20:57:20",
               "2024-04-13 20:06:31",
               nil,
               "2024-11-02 04:19:53",
               "2022-06-08 09:48:05"
             ]
           ]
  end

  test "Can convert TIMESTAMP_NTZ to string without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP_NTZ.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, false)

    assert values == [
             [
               "2024-07-22 07:16:17.484",
               nil,
               "2024-12-17 06:52:42.484",
               nil,
               nil,
               nil,
               "2024-05-26 09:12:14.484",
               "2025-01-24 09:13:42.484",
               "2024-12-24 07:38:38.484",
               "2025-01-08 07:31:12.484"
             ]
           ]
  end

  test "Can convert TIMESTAMP_LTZ to string without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP_LTZ.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, false)

    assert values == [
             [
               nil,
               "2023-09-06 04:56:28",
               "2025-02-15 00:35:26",
               "2023-10-01 15:39:37",
               nil,
               "2024-10-10 02:49:14",
               "2022-06-11 10:05:37",
               nil,
               "2022-12-08 01:10:25",
               nil
             ]
           ]
  end
end
