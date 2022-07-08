defmodule SnowflakeArrow do
  alias SnowflakeArrow.Native

  # Converts an Arrow IPC Streaming file of Snowflake Arrow data to rows of tuples.
  def convert_snowflake_arrow_stream(data) when is_binary(data) do
    with {:ok, data} <- Native.convert_snowflake_arrow_stream(data) do
      data
    end
  end
end
