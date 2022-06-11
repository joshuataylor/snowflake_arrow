defmodule SnowflakeArrow.Native do
  use Rustler, otp_app: :snowflake_arrow, crate: :snowflake_arrow, mode: :release

  def convert_arrow_stream_to_rows("", _cast), do: []
  def convert_arrow_stream_to_rows(binary, _cast), do: error()
  #  def convert_arrow_stream_rows(binary, _cast), do: error()
  #  def convert_arrow_stream_single(_options, _cast), do: error()
  #  def convert_arrow_stream_no_cast(_options), do: error()
  defp error, do: :erlang.nif_error(:nif_not_loaded)
end
