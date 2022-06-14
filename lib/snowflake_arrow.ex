defmodule SnowflakeArrow do
  alias SnowflakeArrow.Native
  # Convert to rows
  # opts is a keyword list, right now it's just [cast: true/false]
  def convert_arrow_to_rows(data, opts \\ []) do
    data
    |> Native.convert_arrow_stream_to_rows_chunked(Keyword.get(opts, :cast, false))
    |> Enum.zip_with(& &1)
  end
end
