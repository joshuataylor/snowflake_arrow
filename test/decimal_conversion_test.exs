defmodule SnowflakeArrow.DecimalConversionTest do
  use ExUnit.Case, async: true

  test "Can convert nulls and decimals to correct" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_DECIMAL_38_2.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.read_arrow_stream_to_columns!()

    assert values == [
             [
               21885.79,
               nil,
               nil,
               7395.6,
               26737.49,
               7479.49,
               21256.18,
               nil,
               17681.54,
               nil
             ]
           ]
  end
end
