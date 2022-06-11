defmodule ElixirDateTime do
  defstruct [
    :year,
    :month,
    :day,
    :hour,
    :minute,
    :second,
    :time_zone,
    :zone_abbr,
    :utc_offset,
    :std_offset
  ]

  def foo() do
    require IEx
    IEx.pry
  end
end
