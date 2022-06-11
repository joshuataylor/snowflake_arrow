mod tests {

    // use snowflake_arrow::serialize::new_serializer;

    // use snowflake_arrow::read_arrow_stream;
    use snowflake_arrow::convert_chunks_to_snowflake;
    use std::fs::File;
    use std::io::BufReader;

    // use arrow2_convert::{deserialize::TryIntoCollection, serialize::TryIntoArrow, ArrowField};

    // use snowflake_arrow::convert;
    // use snowflake_arrow::{convert, ReturnType};

    // serialize to rust type
    #[test]
    fn serialize_rust() {
        let f = File::open("/home/josh/dev/snowflake_arrow/benchmark/large_arrow").unwrap();
        let mut reader = BufReader::new(f);
        convert_chunks_to_snowflake(&mut reader, true);
    }
}
