defmodule SnowflakeArrow.DateConversionTest do
  use ExUnit.Case, async: true
  alias SnowflakeArrow.Native

  test "Can convert nulls and dates to correct without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DATE.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, false, false)

    assert values["SF_DATE"] == [
             nil,
             nil,
             nil,
             "2024-05-26",
             nil,
             nil,
             nil,
             nil,
             nil,
             "2022-07-30"
           ]
  end

  test "Can convert nulls and dates to correct with elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DATE.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, true, false)

    assert values["SF_DATE"] == [nil, nil, nil, ~D[2024-05-26], nil, nil, nil, nil, nil, ~D[2022-07-30]]
  end

end
