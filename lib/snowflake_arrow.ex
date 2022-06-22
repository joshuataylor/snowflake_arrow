defmodule SnowflakeArrow do
  alias SnowflakeArrow.Native

  def convert_arrow_to_df(data) when is_binary(data) do
    Native.convert_snowflake_arrow_stream_to_df(data)
  end

  # Once explorer has mutable dataframes in, we can remove these.
  def append_snowflake_arrow_to_df(ref, data) when is_binary(data) do
    Native.append_snowflake_arrow_stream_to_df(ref, data)
  end

  def to_owned(resource) do
    Native.to_owned(resource)
  end

  def get_column(resource, column_name) do
    Native.get_column(resource, column_name)
  end

  def get_column_names(resource) do
    Native.get_column_names(resource)
  end

  def read_arrow_stream_to_columns!(data) when is_binary(data) do
    with {:ok, ref} <- convert_arrow_to_df(data),
         {:ok, owned_ref} <- to_owned(ref),
         columns <- get_columns(owned_ref) do
      columns
    end
  end

  def get_columns(resource) do
    {:ok, names} = get_column_names(resource)

    names
    |> Stream.map(fn name ->
      {:ok, data} = get_column(resource, name)
      data
    end)
    |> Enum.to_list()
  end
end
