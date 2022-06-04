#json_data = File.read!("/Users/joshtaylor/dev/snowflake_arrow/snowflake_json.json")

arrow_data =
  File.read!(
    "/home/josh/dev/req_snowflake/arrow/l6vz-s-aust4888results01a4b8be-3200-f010-0000-c20d00025aee0maindata061.s3_arrow"
  )

Benchee.run(
  %{
    #    "decode" => fn -> :zlib.gunzip(arrow_data_gzipped) end,
    #    "decode_rust" => fn -> SnowflakeArrow.Native.convert_arrow_stream(arrow_data_gzipped, false) end,
    "naive_convert_arrow_stream" => fn ->
      SnowflakeArrow.Native.convert_arrow_stream(arrow_data, true)
    end
    #    "naive_convert_arrow_stream_no_cast" => fn -> SnowflakeArrow.Native.convert_arrow_stream(arrow_data, false) end,
    #    "naive_convert_arrow_stream_no_cast_gzipped" => fn -> SnowflakeArrow.Native.convert_arrow_stream(arrow_data_gzipped, true, true) end,
    #    "jason_decode_snowflake_response" => fn ->
    #      Jason.decode!("[#{json_data}]")
    #    end,
  },
  time: 5
)
