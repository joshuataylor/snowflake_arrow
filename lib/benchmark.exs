arrow_data_small = File.read!("benchmark/small_arrow")
arrow_data_large = File.read!("benchmark/large_arrow")
jason_data_large = File.read!("benchmark/large_json.json")
jason_data_small = File.read!("benchmark/small_json.json")

Benchee.run(
  %{
    "arrow_small (368kb)" => fn -> SnowflakeArrow.Native.convert_arrow_stream(arrow_data_small, true) end,
    "arrow_large (9.4mb)" => fn -> SnowflakeArrow.Native.convert_arrow_stream(arrow_data_large, true) end,
    "jason_decode (8.8mb)" => fn -> SnowflakeArrow.Native.convert_arrow_stream(jason_data_large, true) end,
    "jason_decode (468Kb)" => fn -> SnowflakeArrow.Native.convert_arrow_stream(jason_data_small, true) end,
  },
  time: 5
)
