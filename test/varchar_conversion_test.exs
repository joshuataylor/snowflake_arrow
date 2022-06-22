defmodule SnowflakeArrow.TextConversionTest do
  use ExUnit.Case, async: true

  test "Can convert text to string without elixir types" do
    values =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_VARCHAR.arrow"
        ])
      )
      |> Base.decode64!()
      |> SnowflakeArrow.read_arrow_stream_to_columns!()

    assert values == [
             [
               nil,
               nil,
               "fF7Lk7YKfXtYFsY2fVXN",
               nil,
               "dvhJoe9A0qtgRJwHFMHN",
               nil,
               nil,
               "dBmT57wW3U0pLaJp9PGV",
               nil,
               "XRFjUy5wdbzE7k1z9SSa"
             ]
           ]
  end
end
