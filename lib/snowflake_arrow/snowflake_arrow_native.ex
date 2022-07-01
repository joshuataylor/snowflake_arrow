defmodule SnowflakeArrow.Native do
  use Rustler, otp_app: :snowflake_arrow, crate: :snowflake_arrow#, mode: :release

  def convert_snowflake_arrow_stream_to_df(_array), do: error()
  def convert_snowflake_arrow_stream_to_df_owned(_array), do: error()
  def append_snowflake_arrow_stream_to_df(_resource, _df2), do: error()
  def convert_snowflake_arrow_stream(_data), do: error()
  def get_column_at(_df, _index, _row_index), do: error()
  def to_owned(_resource), do: error()
  def get_column(_resource, _column_name), do: error()
  def get_column_names(_resource), do: error()
  defp error, do: :erlang.nif_error(:nif_not_loaded)
end
