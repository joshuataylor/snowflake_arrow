defmodule SnowflakeArrow.TextConversionTest do
  use ExUnit.Case, async: true
  alias SnowflakeArrow.Native

  test "Can convert text to string without elixir types" do
    data =
      File.read!(
        Path.join([
          :code.priv_dir(:snowflake_arrow),
          "testing/base64/SF_VARCHAR.arrow"
        ])
      )
      |> Base.decode64!()

    values = Native.convert_arrow_stream(data, false)

    assert values["SF_VARCHAR"] == [
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
