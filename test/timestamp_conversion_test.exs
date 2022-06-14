defmodule SnowflakeArrow.TimestampConversionTest do
  use ExUnit.Case, async: true

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

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)

    assert values == [
             [nil],
             [nil],
             ["2022-12-29 07:22:31.484"],
             ["2023-04-10 18:32:21.484"],
             [nil],
             ["2023-11-03 20:57:20.484"],
             ["2024-04-13 20:06:31.484"],
             [nil],
             ["2024-11-02 04:19:53.484"],
             ["2022-06-08 09:48:05.484"]
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

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)

    assert values == [
             [nil],
             ["2024-07-22 07:16:17.484"],
             ["2024-12-17 06:52:42.484"],
             [nil],
             [nil],
             [nil],
             ["2024-05-26 09:12:14.484"],
             ["2025-01-24 09:13:42.484"],
             ["2024-12-24 07:38:38.484"],
             ["2025-01-08 07:31:12.484"]
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

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: false)

    assert values == [
             [nil],
             ["2023-09-06 04:56:28.484"],
             ["2025-02-15 00:35:26.484"],
             ["2023-10-01 15:39:37.484"],
             [nil],
             ["2024-10-10 02:49:14.484"],
             ["2022-06-11 10:05:37.484"],
             [nil],
             ["2022-12-08 01:10:25.484"],
             [nil]
           ]
  end

  test "Can convert TIMESTAMP to string with elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: true)

    assert values == [
             [nil],
             [nil],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 29,
                 minute: 22,
                 month: 12,
                 second: 31,
                 year: 2022
               }
             ],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 10,
                 minute: 32,
                 month: 4,
                 second: 21,
                 year: 2023
               }
             ],
             [nil],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 3,
                 minute: 57,
                 month: 11,
                 second: 20,
                 year: 2023
               }
             ],
             [
               %{__struct__: ElixirDateTime, day: 13, minute: 6, month: 4, second: 31, year: 2024}
             ],
             [nil],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 2,
                 minute: 19,
                 month: 11,
                 second: 53,
                 year: 2024
               }
             ],
             [%{__struct__: ElixirDateTime, day: 8, minute: 48, month: 6, second: 5, year: 2022}]
           ]
  end

  test "Can convert TIMESTAMP_NTZ to string with elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP_NTZ.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: true)

    assert values == [
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 22,
                 minute: 16,
                 month: 7,
                 second: 17,
                 year: 2024
               }
             ],
             [nil],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 17,
                 minute: 52,
                 month: 12,
                 second: 42,
                 year: 2024
               }
             ],
             [nil],
             [nil],
             [nil],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 26,
                 minute: 12,
                 month: 5,
                 second: 14,
                 year: 2024
               }
             ],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 24,
                 minute: 13,
                 month: 1,
                 second: 42,
                 year: 2025
               }
             ],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 24,
                 minute: 38,
                 month: 12,
                 second: 38,
                 year: 2024
               }
             ],
             [%{__struct__: ElixirDateTime, day: 8, minute: 31, month: 1, second: 12, year: 2025}]
           ]
  end

  test "Can convert TIMESTAMP_LTZ to string with elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP_LTZ.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, cast: true)

    # NOTE THIS IS WRONG, BUT JUST TESTING THAT IT CONVERTS CORRECTLY!
    assert values == [
             [nil],
             [
               %{__struct__: ElixirDateTime, day: 6, minute: 56, month: 9, second: 28, year: 2023}
             ],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 15,
                 minute: 35,
                 month: 2,
                 second: 26,
                 year: 2025
               }
             ],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 1,
                 minute: 39,
                 month: 10,
                 second: 37,
                 year: 2023
               }
             ],
             [nil],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 10,
                 minute: 49,
                 month: 10,
                 second: 14,
                 year: 2024
               }
             ],
             [
               %{__struct__: ElixirDateTime, day: 11, minute: 5, month: 6, second: 37, year: 2022}
             ],
             [nil],
             [
               %{
                 __struct__: ElixirDateTime,
                 day: 8,
                 minute: 10,
                 month: 12,
                 second: 25,
                 year: 2022
               }
             ],
             [nil]
           ]
  end
end
