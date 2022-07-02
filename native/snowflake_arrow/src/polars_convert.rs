use polars::datatypes::{AnyValue, DataType as PolarsDataType, DatetimeChunked};
use polars::export::arrow::datatypes::Metadata;
use polars::export::chrono::NaiveDateTime;
use polars::export::rayon::prelude::*;
use polars::prelude::Result as PolarsResult;
use polars::prelude::{DataFrame, IpcStreamReader, NamedFrom, SerReader, Series, TimeUnit};
use polars::series::IntoSeries;
use rustler::Binary;
use std::collections::HashMap;
use std::io::Cursor;

#[inline]
pub fn snowflake_arrow_ipc_streaming_binary_to_dataframe(
    binary: &Binary,
) -> PolarsResult<DataFrame> {
    // Cursor seems like a fine way to do this?
    let binary_cursor = Cursor::new(binary.as_ref());

    let mut stream_reader = IpcStreamReader::new(binary_cursor);
    let schema = stream_reader.arrow_schema()?;
    let df = stream_reader.finish()?;
    let mut column_metadata: HashMap<&str, &Metadata> = HashMap::new();

    // We need the field metadata for the timestamp info later.
    for field in &schema.fields {
        column_metadata.insert(&field.name, &field.metadata);
    }

    // Creating a new par iter like this is the fastest way I've found so far.
    // doing a df replace seems to be slower.
    // Maybe use more allocations though?
    let df_series: Vec<Series> = df
        .get_columns()
        .clone()
        .into_par_iter()
        .map(move |series| {
            match series.dtype() {
                PolarsDataType::Int32 => {
                    // fine to use unwrap here as we know this field exists.
                    let fm = column_metadata.get(series.name()).unwrap();

                    if fm.get("scale").unwrap() == "0" {
                        series
                    } else {
                        // build f64 from int32
                        let scale = fm.get("scale").unwrap().parse::<i32>().unwrap();

                        // Then convert to vec
                        let float64_vec = Vec::from(series.i32().unwrap())
                            .iter()
                            .map(|v| v.map(|x| f64::from(x) / 10f64.powi(scale) as f64))
                            .collect::<Vec<Option<f64>>>();
                        Series::new(series.name(), &float64_vec)
                    }
                }

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
                                        Some(NaiveDateTime::from_timestamp(epoch, fraction as u32))
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

    DataFrame::new(df_series)
}
