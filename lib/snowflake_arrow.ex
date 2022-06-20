defmodule SnowflakeArrow do
  alias SnowflakeArrow.Native

  @spec convert_arrow_to_rows(binary, Map.t(), cast: boolean()) :: list
  def convert_arrow_to_rows(data, snowflake_column_data, cast: true) when is_binary(data) do
    data
    |> Native.convert_arrow_stream_to_columns(true)
    |> Enum.map(&process_columns(&1, hd(&1)))
    |> Enum.zip_with(& &1)
  end

  def convert_arrow_to_rows(data, _, cast: false) when is_binary(data) do
    convert_arrow_to_rows(data, cast: false)
  end

  @spec convert_arrow_to_rows(binary, Map.t(), cast: boolean()) :: list
  def convert_arrow_to_rows(data, cast: false) when is_binary(data) do
    data
    |> Native.convert_arrow_stream_to_columns(false)
#    |> Stream.map(&process_columns(&1, hd(&1)))
    |> Enum.zip_with(& &1)
#    |> Enum.to_list()
  end

  def process_columns(rows, type) when is_tuple(type) and tuple_size(type) == 3 do
    rows
    |> Enum.map(&remap_dates/1)
  end

  def process_columns(rows, type) when is_tuple(type) and tuple_size(type) == 7 do
    rows
    |> Enum.map(&remap_datetimes/1)
  end

  def process_columns(rows, _type), do: rows

  def remap_datetimes(nil), do: nil

  def remap_datetimes({year, month, day, hour, minute, second, microsecond}) do
    %DateTime{
      calendar: Calendar.ISO,
      year: year,
      month: month,
      day: day,
      hour: hour,
      minute: minute,
      second: second,
      microsecond: {microsecond, 6},
      std_offset: 0,
      utc_offset: 0,
      zone_abbr: "UTC",
      time_zone: "Etc/UTC"
    }
  end

  def remap_dates(nil), do: nil

  def remap_dates({year, month, day}) do
    %Date{
      calendar: Calendar.ISO,
      year: year,
      month: month,
      day: day
    }
  end
end
