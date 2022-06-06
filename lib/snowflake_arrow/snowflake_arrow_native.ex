defmodule SnowflakeArrow.Native do
  # , mode: :release
  use Rustler, otp_app: :snowflake_arrow, crate: :snowflake_arrow

  def convert_arrow_stream(_options, _cast), do: error()
  #  def convert_arrow_stream_no_cast(_options), do: error()
  defp error, do: :erlang.nif_error(:nif_not_loaded)
end
