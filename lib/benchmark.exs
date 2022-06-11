arrow_data_small = File.read!("benchmark/small_arrow")
arrow_data_large = File.read!("benchmark/large_arrow")
jason_data_large = File.read!("benchmark/large_json.json")
jason_data_small = File.read!("benchmark/small_json.json")

Benchee.run(
  %{
    "arrow_small transpose (368kb)" => fn ->
      SnowflakeArrow.Native.convert_arrow_stream_to_rows(arrow_data_small, true)
    end,
    "arrow_large transpose (9.4mb)" => fn ->
      SnowflakeArrow.Native.convert_arrow_stream_to_rows(arrow_data_large, true)
    end,
    "arrow_small no transpose (368kb)" => fn ->
      SnowflakeArrow.Native.convert_arrow_stream_to_rows(arrow_data_small, true)
    end,
    "arrow_large no transpose (9.4mb)" => fn ->
      SnowflakeArrow.Native.convert_arrow_stream_to_rows(arrow_data_large, true)
    end
    #    "jason_decode (8.8mb)" => fn -> Jason.decode!("[#{jason_data_large}]") end,
    #    "jason_decode (468Kb)" => fn -> Jason.decode!("[#{jason_data_small}]") end
  },
  time: 5
)
