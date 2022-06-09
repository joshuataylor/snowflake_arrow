arrow_data_small = File.read!("benchmark/small_arrow")
arrow_data_large = File.read!("benchmark/large_arrow")
jason_data_large = File.read!("benchmark/large_json.json")
jason_data_small = File.read!("benchmark/small_json.json")

Benchee.run(
  %{
    "arrow_small par_iter (368kb)" => fn ->
      SnowflakeArrow.Native.convert_arrow_stream(arrow_data_small, true)
    end,
    "arrow_large par_iter (9.4mb)" => fn ->
      SnowflakeArrow.Native.convert_arrow_stream(arrow_data_large, true)
    end,
    "jason_decode (8.8mb)" => fn -> Jason.decode!("[#{jason_data_large}]") end,
    "jason_decode (468Kb)" => fn -> Jason.decode!("[#{jason_data_small}]") end,
  },
  time: 10
)
