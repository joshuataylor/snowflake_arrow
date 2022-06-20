defmodule SnowflakeArrow.TimestampConversionTest do
  use ExUnit.Case, async: true

  # Includes tests for NTZ/LTZ and normal timestamp conversions

  test "Can convert TIMESTAMP to string without elixir types" do
    row_type = [
      %{
        "byteLength" => nil,
        "collation" => nil,
        "database" => "FOO",
        "length" => nil,
        "name" => "SF_TIMESTAMP",
        "nullable" => true,
        "precision" => 0,
        "scale" => 9,
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "type" => "timestamp_ntz"
      }
    ]

    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, row_type, cast: false)

    assert length(values) == 10

    assert values == [
             [nil],
             [nil],
             ["2022-12-29 07:22:31.484000000"],
             ["2023-04-10 18:32:21.484000000"],
             [nil],
             ["2023-11-03 20:57:20.484000000"],
             ["2024-04-13 20:06:31.484000000"],
             [nil],
             ["2024-11-02 04:19:53.484000000"],
             ["2022-06-08 09:48:05.484000000"]
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
             [nil],
             ["2025-07-14 02:12:48.064000000"],
             ["2024-12-05 18:07:46.064000000"],
             ["2025-06-12 01:31:27.064000000"],
             [nil],
             [nil],
             [nil],
             ["2023-03-17 21:56:41.064000000"],
             [nil]
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
             ["2023-09-06 04:56:28.484000000"],
             ["2025-02-15 00:35:26.484000000"],
             ["2023-10-01 15:39:37.484000000"],
             [nil],
             ["2024-10-10 02:49:14.484000000"],
             ["2022-06-11 10:05:37.484000000"],
             [nil],
             ["2022-12-08 01:10:25.484000000"],
             [nil]
           ]
  end

  test "Can convert TIMESTAMP to string with elixir types" do
    row_type = [
      %{
        "byteLength" => nil,
        "collation" => nil,
        "database" => "FOO",
        "length" => nil,
        "name" => "SF_TIMESTAMP",
        "nullable" => true,
        "precision" => 0,
        "scale" => 9,
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "type" => "timestamp_ntz"
      }
    ]

    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, row_type, cast: true)

    assert values == [
             [nil],
             [nil],
             [~N[2022-12-29 07:22:31.484000]],
             [~N[2023-04-10 18:32:21.484000]],
             [nil],
             [~N[2023-11-03 20:57:20.484000]],
             [~N[2024-04-13 20:06:31.484000]],
             [nil],
             [~N[2024-11-02 04:19:53.484000]],
             [~N[2022-06-08 09:48:05.484000]]
           ]
  end

  test "Can convert TIMESTAMP_NTZ to string with elixir types" do
    row_type = [
      %{
        "byteLength" => nil,
        "collation" => nil,
        "database" => "FOO",
        "length" => nil,
        "name" => "SF_TIMESTAMP",
        "nullable" => true,
        "precision" => 0,
        "scale" => 9,
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "type" => "timestamp_ntz"
      }
    ]

    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP_NTZ.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, row_type, cast: true)

    assert values == [
             [~N[2024-07-22 07:16:17.484000]],
             [nil],
             [~N[2024-12-17 06:52:42.484000]],
             [nil],
             [nil],
             [nil],
             [~N[2024-05-26 09:12:14.484000]],
             [~N[2025-01-24 09:13:42.484000]],
             [~N[2024-12-24 07:38:38.484000]],
             [~N[2025-01-08 07:31:12.484000]]
           ]
  end

  test "Can convert TIMESTAMP_LTZ to string with elixir types" do
    row_type = [
      %{
        "byteLength" => nil,
        "collation" => nil,
        "database" => "FOO",
        "length" => nil,
        "name" => "SF_TIMESTAMP",
        "nullable" => true,
        "precision" => 0,
        "scale" => 9,
        "schema" => "BAR",
        "table" => "TEST_DATA",
        "type" => "timestamp_ltz"
      }
    ]

    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_TIMESTAMP_LTZ.arrow"
        ])
      )
      |> Base.decode64!()

    values = SnowflakeArrow.convert_arrow_to_rows(data, row_type, cast: true)

    # NOTE THIS IS WRONG, BUT JUST TESTING THAT IT CONVERTS CORRECTLY!
    assert values == [
             [nil],
             [~N[2023-09-06 04:56:28.484000]],
             [~N[2025-02-15 00:35:26.484000]],
             [~N[2023-10-01 15:39:37.484000]],
             [nil],
             [~N[2024-10-10 02:49:14.484000]],
             [~N[2022-06-11 10:05:37.484000]],
             [nil],
             [~N[2022-12-08 01:10:25.484000]],
             [nil]
           ]
  end
end
