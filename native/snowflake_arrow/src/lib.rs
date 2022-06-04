mod elixir_types;
use arrow2::array::{Array, BinaryArray, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveArray, StructArray, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Metadata};
use arrow2::error::{ArrowError as Error, Result};
use arrow2::io::ipc::read;
use arrow2::temporal_conversions::date32_to_date;
use chrono::{NaiveDateTime};
use rustler::types::binary::Binary;
use rustler::{Encoder, Env, Term};
use std::any::Any;
use std::sync::Arc;
use arrow2::io::ipc::read::StreamMetadata;
use crate::ArrowType::{ABinaryArray, ABooleanArray, AF64Array, AFloat64Array, AInt16Array, AInt32Array, AInt64Array, AInt8Array, AMissing, AStringVec, AUtf8Array};
use crate::atoms::nil;

mod atoms {
    rustler::atoms! {
      ok,
      error,
      __struct__,
      nil
    }
}

rustler::init!("Elixir.SnowflakeArrow.Native", [convert_arrow_stream]);
pub enum ArrowType<'a> {
    AInt64Array(Option<&'a PrimitiveArray<i64>>),
    AInt32Array(Option<&'a PrimitiveArray<i32>>),
    AInt16Array(Option<&'a PrimitiveArray<i16>>),
    AInt8Array(Option<&'a PrimitiveArray<i8>>),
    ABooleanArray(Option<&'a BooleanArray>),
    AUtf8Array(Option<&'a Utf8Array<i32>>),
    AStringVec(Option<Vec<Option<String>>>),
    // ADateVec(Option<Vec<Option<ElixirDate>>>),
    ABinaryArray(Option<&'a BinaryArray<i32>>),
    AFloat64Array(Option<&'a PrimitiveArray<f64>>),
    AMissing(String),
    AF64Array(Option<Vec<Option<f64>>>)
}

fn metadata_for_field(metadata: StreamMetadata, index: u32) -> Metadata {
    return metadata.schema.fields.get(index as usize).unwrap().metadata.clone();
}

#[rustler::nif]
pub fn convert_arrow_stream<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
    cast_elixir_types: bool
) -> Term<'a> {
    let (metadata, chunk) = convert_binary(arrow_stream_data).unwrap();
    let mut vec: Vec<ArrowType> = Vec::new();

    let mut i = 0;
    for field in chunk.arrays() {
        // get column metadata
        let field_metadata = metadata_for_field(metadata.clone(), i);

        let column = field.as_any();
        match field.data_type() {
            DataType::Int64 => vec.push(AInt64Array(column.downcast_ref::<Int64Array>())),
            DataType::Int32 => {
                if field_metadata.get("scale").unwrap() == "0" {
                    vec.push(AInt32Array(column.downcast_ref::<Int32Array>()));
                } else {
                    let scale = field_metadata.get("scale").unwrap().parse::<i32>().unwrap();
                    let dc = column.downcast_ref::<Int32Array>().unwrap();
                    let mut float_vecs: Vec<Option<f64>> = Vec::new();

                    let mut float_index = 0;
                    for valid in dc {
                        if dc.is_null(float_index) {
                            float_vecs.push(None);
                        } else {
                            let f = (*valid.unwrap() as f64) / (10.0f64.powi(scale as i32));
                            float_vecs.push(Some(f));
                        }
                        float_index += 1;
                    }

                    vec.push(AF64Array(Some(float_vecs)));
                }
            },
            DataType::Int16 => vec.push(AInt16Array(column.downcast_ref::<Int16Array>())),
            DataType::Int8 => vec.push(AInt8Array(column.downcast_ref::<Int8Array>())),
            DataType::Boolean => vec.push(ABooleanArray(column.downcast_ref::<BooleanArray>())),
            DataType::Utf8 => { vec.push(AUtf8Array(column.downcast_ref::<Utf8Array<i32>>())) },
            DataType::Float64 => vec.push(AFloat64Array(column.downcast_ref::<PrimitiveArray<f64>>())),
            DataType::Date32 => {
                if cast_elixir_types {
                    // let dates = convert_dates(column);
                    // vec.push(ADateVec(Some(dates)));
                } else {
                    let mut date_vec: Vec<Option<String>> = Vec::new();
                    for date in column.downcast_ref::<Int32Array>().unwrap() {
                        match date {
                            Some(d) => {
                                let value = date32_to_date(*d as i32).to_string();
                                date_vec.push(Some(value));
                            },
                            None => date_vec.push(None)
                        }
                    }
                    vec.push(AStringVec(Some(date_vec)));
                }
            },
            DataType::Struct(_f) => {
                let logical_type = field_metadata.get("logicalType").unwrap().as_str();
                match logical_type {
                    "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => {
                        let date_vec = convert_timestamps(column);
                        vec.push(AStringVec(Some(date_vec)));
                    }
                    _ => {}
                }
            },
            DataType::Binary => { vec.push(ABinaryArray(column.downcast_ref::<BinaryArray<i32>>())) },
            data_type => {
                vec.push(AMissing(format!("{:?}", data_type)));
            }
            // When using local branch, I added formatting to arrow2 to show the datatype as a string,
            // so I knew the types that weren't working yet
            // vec.push(to_term(env, data_type.to_string()).unwrap()),
        }

        i+=1;
    }

    return vec.encode(env);
}

// Function to convert dates to Elixir dates.
// fn convert_dates(column: &dyn Any) -> Vec<Option<ElixirDate>> {
//     let dates_array = column.downcast_ref::<Int32Array>().unwrap();
//
//     let mut date_vec: Vec<Option<ElixirDate>> = Vec::new();
//     for date in dates_array {
//         match date {
//             Some(d) => {
//                 let date = date32_to_date(*d as i32);
//                 // Code to convert to an ElixirDate
//                 let date = ElixirDate {
//                     year: date.year(),
//                     month: date.month(),
//                     day: date.day(),
//                 };
//                 date_vec.push(Some(date));
//             }
//             None => {
//                 date_vec.push(None);
//             }
//         }
//     }
//     return date_vec;
// }

fn convert_timestamps(column: &dyn Any) -> Vec<Option<String>> {
    let mut date_vec: Vec<Option<String>> = Vec::new();

    let (_fields, arrays, _bitmap) = column.downcast_ref::<StructArray>().unwrap().clone().into_data();

    // get the epochs from the first array
    let epochs = arrays[0].as_any().downcast_ref::<Int64Array>().unwrap();

    // Get the fractional parts from the second array
    let fractions = arrays[1].as_any().downcast_ref::<Int32Array>().unwrap();

    let i = 0;
    for epoch in epochs {
        match epoch {
            Some(epoch) => {
                let value1 = *epoch as i64;
                let value2 = fractions.value(i);
                date_vec.push(Some(NaiveDateTime::from_timestamp(value1, value2 as u32).to_string()));
            }
            None => {
                date_vec.push(None);
            }
        }
    }
    return date_vec;
}

// Converts a Elixir binary to Chunk<Arc<dyn Array>> using arrow2
pub fn convert_binary(arrow_stream_data: Binary) -> Result<(StreamMetadata, Chunk<Arc<dyn Array>>)> {
    let mut binary = arrow_stream_data.as_ref();

    // We want the metadata later for checking types
    let metadata = read::read_stream_metadata(&mut binary).unwrap();
    let mut stream = read::StreamReader::new(&mut binary, metadata.clone());

    // Could just return the chunk via unwrap(), but it's a negligible difference
    loop {
        match stream.next() {
            Some(x) => match x {
                Ok(read::StreamState::Some(b)) => {
                    return Ok((metadata, b));
                }
                Ok(read::StreamState::Waiting) => break,
                Err(l) => return Err(l),
            },
            None => break,
        };
    }

    return Err(Error::OutOfSpec(
        "Couldn't parse file due to unknown reason".to_string(),
    ));
}

impl<'b> Encoder for ArrowType<'b> {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        return match self {
            AFloat64Array(dt) => dt.unwrap().iter().collect::<Vec<_>>().encode(env),
            AInt32Array(dt) => dt.unwrap().iter().collect::<Vec<_>>().encode(env),
            AInt64Array(dt) => dt.unwrap().iter().collect::<Vec<_>>().encode(env),
            AInt16Array(dt) => dt.unwrap().iter().collect::<Vec<_>>().encode(env),
            AInt8Array(dt) => dt.unwrap().iter().collect::<Vec<_>>().encode(env),
            ABooleanArray(dt) => dt.unwrap().iter().collect::<Vec<_>>().encode(env),
            AUtf8Array(dt) => dt.unwrap().iter().collect::<Vec<_>>().encode(env),
            ABinaryArray(dt) => dt.unwrap().iter().collect::<Vec<_>>().encode(env),
            AStringVec(dt) => {
                match dt {
                    Some(x) => x.iter().collect::<Vec<_>>().encode(env),
                    None => nil().encode(env),
                }
            }
            AF64Array(dt) => dt.iter().collect::<Vec<_>>().encode(env),
            AMissing(missing) => missing.encode(env)
        };
    }
}
