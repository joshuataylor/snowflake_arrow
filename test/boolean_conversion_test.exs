defmodule SnowflakeArrow.BooleanConversionTest do
  use ExUnit.Case, async: true

  test "Can convert nil and booleans to correct format" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_BOOLEAN.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.convert_snowflake_arrow_stream()

    assert values == [[nil, nil, nil, nil, nil, false, nil, true, nil, false]]
  end
end
