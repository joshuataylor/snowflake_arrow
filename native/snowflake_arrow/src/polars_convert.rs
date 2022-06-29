use rustler::Binary;

use polars::export::arrow::datatypes::Metadata;

use polars::datatypes::{AnyValue, DataType as PolarsDataType, DatetimeChunked};
use polars::export::chrono::NaiveDateTime;
use polars::export::rayon::prelude::*;
use polars::prelude::{DataFrame, IpcStreamReader, SerReader, Series, TimeUnit};
use polars::series::IntoSeries;
use std::collections::HashMap;
use std::io::Cursor;

pub fn snowflake_arrow_ipc_streaming_binary_to_dataframe(binary: &Binary) -> DataFrame {
    let c = Cursor::new(binary.as_ref());

    let mut stream_reader = IpcStreamReader::new(c);
    let schema = stream_reader.arrow_schema().unwrap();
    let df = stream_reader.finish().unwrap();
    let mut column_metadata: HashMap<&str, &Metadata> = HashMap::new();

    // We need the field metadata for the timestamp info later.
    for field in schema.fields.iter() {
        column_metadata.insert(&field.name, &field.metadata);
    }

    let new_df: Vec<Series> = df
        .get_columns()
        .clone()
        .into_par_iter()
        .map(|series| {
            match series.dtype() {
                PolarsDataType::Struct(_str) => {
                    let fm = column_metadata.get(series.name()).unwrap();
                    let logical_type = fm.get("logicalType").unwrap().as_str();
                    match logical_type {
                        "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                            let fields = series.struct_().unwrap().fields();
                            let epoch_series = fields.get(0).unwrap();
                            let fraction_series = fields.get(1).unwrap();


                            // We need to use from_timestamp as we get them back in a struct
                            let datetimes = epoch_series.iter().zip(fraction_series.iter()).map(
                                |(a, b)| match a {
                                    AnyValue::Int64(epoch) => {
                                        let fraction = match b {
                                            AnyValue::Int32(x) => x,
                                            _ => unreachable!(),
                                        };
                                        Some(NaiveDateTime::from_timestamp(epoch, fraction as u32z))
                                    }
                                    _ => None,
                                },
                            );

                            DatetimeChunked::from_naive_datetime_options(
                                series.name(),
                                datetimes,
                                TimeUnit::Milliseconds,
                            )
                            .into_series()
                        }
                        _ => series,
                    }
                }
                _ => series,
            }
        })
        .collect();

    DataFrame::new(new_df).unwrap()
}
