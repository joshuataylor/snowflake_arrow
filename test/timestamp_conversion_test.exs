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

    values = Native.convert_arrow_stream_to_rows(data, false)

    assert values == []
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

    values = Native.convert_arrow_stream_to_rows(data, false)

    assert values == []
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

    values = Native.convert_arrow_stream_to_rows(data, false)

    assert values == []
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

    values = Native.convert_arrow_stream_to_rows(data, true)

    assert values == []
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

    values = Native.convert_arrow_stream_to_rows(data, true)

    assert values == []
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

    values = Native.convert_arrow_stream_to_rows(data, true)

    assert values == []
  end
end
