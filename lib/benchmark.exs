arrow_data_small = File.read!("benchmark/small")
arrow_data_large = File.read!("benchmark/large")

Benchee.run(
  %{
    "arrow_small (368kb)" => fn -> SnowflakeArrow.Native.convert_arrow_stream(arrow_data_small, true) end,
    "arrow_large (9.4mb)" => fn -> SnowflakeArrow.Native.convert_arrow_stream(arrow_data_large, true) end,
  },
  time: 10
)
