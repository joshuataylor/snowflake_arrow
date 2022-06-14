pub mod helpers;
mod rows;

rustler::init!(
    "Elixir.SnowflakeArrow.Native",
    [rows::convert_arrow_stream_to_rows_chunked]
);
