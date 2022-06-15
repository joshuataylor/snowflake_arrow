defmodule SnowflakeArrow do
  alias SnowflakeArrow.Native

  @spec convert_arrow_to_rows(boolean(), [cast: boolean()]) :: list
  def convert_arrow_to_rows(data, opts \\ []) when is_binary(data) do
    data
    |> Native.convert_arrow_stream_to_rows_chunked(Keyword.get(opts, :cast, false))
    |> Enum.zip_with(& &1)
  end
end
