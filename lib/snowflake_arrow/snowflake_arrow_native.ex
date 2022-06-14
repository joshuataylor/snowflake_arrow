defmodule SnowflakeArrow.Native do
  use Rustler, otp_app: :snowflake_arrow, crate: :snowflake_arrow, mode: :release

  def convert_arrow_stream_to_rows_chunked("", _cast), do: []
  def convert_arrow_stream_to_rows_chunked(_binary, _cast), do: error()
  defp error, do: :erlang.nif_error(:nif_not_loaded)
end
