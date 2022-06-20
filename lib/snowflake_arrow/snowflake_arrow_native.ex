defmodule SnowflakeArrow.Native do
  # , mode: :release
  use Rustler, otp_app: :snowflake_arrow, crate: :snowflake_arrow
  #  use JemallocInfo.RustlerMixin

  def convert_arrow_stream_to_columns("", _cast), do: []
  def convert_arrow_stream_to_columns(_binary, _cast), do: error()
  #  def arrow_ffi(_binary), do: error()
  defp error, do: :erlang.nif_error(:nif_not_loaded)
end
