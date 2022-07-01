defmodule SnowflakeArrow.TimestampConversionTest do
  use ExUnit.Case, async: true

  # Includes tests for NTZ/LTZ and normal timestamp conversions

  test "Can convert nulls and TIMESTAMP to elixir types" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.read_arrow_stream_to_columns!()

    assert values == [
             {
               nil,
               nil,
               ~N[2022-12-29 07:22:31.484000],
               ~N[2023-04-10 18:32:21.484000],
               nil,
               ~N[2023-11-03 20:57:20.484000],
               ~N[2024-04-13 20:06:31.484000],
               nil,
               ~N[2024-11-02 04:19:53.484000],
               ~N[2022-06-08 09:48:05.484000]
             }
           ]
  end

  test "Can convert nulls and TIMESTAMP_NTZ to elixir types" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP_NTZ.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.read_arrow_stream_to_columns!()

    assert values == [
             {
               nil,
               nil,
               ~N[2025-07-14 02:12:48.064000],
               ~N[2024-12-05 18:07:46.064000],
               ~N[2025-06-12 01:31:27.064000],
               nil,
               nil,
               nil,
               ~N[2023-03-17 21:56:41.064000],
               nil
             }
           ]
  end

  test "Can convert nulls and TIMESTAMP_LTZ with elixir types" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP_LTZ.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.read_arrow_stream_to_columns!()

    # NOTE THIS IS WRONG, BUT JUST TESTING THAT IT CONVERTS CORRECTLY!
    assert values == [
             {
               nil,
               ~N[2023-09-06 04:56:28.484000],
               ~N[2025-02-15 00:35:26.484000],
               ~N[2023-10-01 15:39:37.484000],
               nil,
               ~N[2024-10-10 02:49:14.484000],
               ~N[2022-06-11 10:05:37.484000],
               nil,
               ~N[2022-12-08 01:10:25.484000],
               nil
             }
           ]
  end
end
