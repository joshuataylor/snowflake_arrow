arrow_data_small = File.read!("benchmark/small_arrow")
arrow_data_large = File.read!("benchmark/large_arrow")
jason_data_large = File.read!("benchmark/large_json.json")
jason_data_small = File.read!("benchmark/small_json.json")

Benchee.run(
  %{
    "arrow_small cast (368kb)" => fn ->
      SnowflakeArrow.convert_arrow_to_rows(arrow_data_small, cast: true)
    end,
    "arrow_large cast (9.4mb)" => fn ->
      SnowflakeArrow.convert_arrow_to_rows(arrow_data_large, cast: true)
    end,
    "arrow_small no cast (368kb)" => fn ->
      SnowflakeArrow.convert_arrow_to_rows(arrow_data_small, cast: false)
    end,
    "arrow_large no cast (9.4mb)" => fn ->
      SnowflakeArrow.convert_arrow_to_rows(arrow_data_large, cast: false)
    end,
    "jason_decode (8.8mb)" => fn -> Jason.decode!("[#{jason_data_large}]") end,
    "jason_decode (468Kb)" => fn -> Jason.decode!("[#{jason_data_small}]") end
  },
  time: 5
)
