defmodule SnowflakeArrow.Native do
  use Rustler, otp_app: :snowflake_arrow, crate: :snowflake_arrow, mode: :debug

  def convert_snowflake_arrow_stream(_data), do: error()
  defp error, do: :erlang.nif_error(:nif_not_loaded)
end
