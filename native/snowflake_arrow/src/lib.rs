mod elixir_types;
use arrow2::array::{
    Array, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StructArray,
    Utf8Array,
};
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

// #[derive(Serialize)]
pub enum ReturnType<'a> {
    Int32(Vec<Option<&'a i32>>),
    Int64(Vec<Option<&'a i64>>),
    Float64(Vec<Option<&'a f64>>),
    Int16(Vec<Option<&'a i16>>),
    Int8(Vec<Option<&'a i8>>),
    Utf8(Vec<Option<&'a str>>),
    Boolean(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    Missing,
}

mod atoms {
    rustler::atoms! {
      ok,
      error,
      __struct__,
      nil
    }
}

rustler::init!("Elixir.SnowflakeArrow.Native", [convert_arrow_stream]);
// use rayon::prelude::*;

fn metadata_for_field(metadata: StreamMetadata, index: u32) -> Metadata {
    return metadata
        .schema
        .fields
        .get(index as usize)
        .unwrap()
        .metadata
        .clone();
}

#[rustler::nif]
pub fn convert_arrow_stream<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
    _cast_elixir_types: bool,
) -> Term<'a> {
    let (metadata, chunk) = convert_binary(arrow_stream_data).unwrap();
    chunk
        .iter()
        .enumerate()
        .map(|(field_index, field)| {
            let field_metadata = metadata_for_field(metadata.clone(), field_index as u32);

            let column = field.as_any();
            match field.data_type() {
                DataType::Int64 => ReturnType::Int64(
                    column
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                ),
                // // .encode(env),
                DataType::Int32 => ReturnType::Int32(
                    column
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                ),
                DataType::Float64 => ReturnType::Float64(
                    column
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                ),
                // .encode(env),
                DataType::Int16 => ReturnType::Int16(
                    column
                        .downcast_ref::<Int16Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                ),
                DataType::Int8 => ReturnType::Int8(
                    column
                        .downcast_ref::<Int8Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                ),
                // // .encode(env),
                DataType::Utf8 => ReturnType::Utf8(
                    column
                        .downcast_ref::<Utf8Array<i32>>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                ),
                // // .encode(env),
                DataType::Date32 =>
                // Not sure if this is the best way to convert into NaiveDates
                {
                    ReturnType::String(
                        column
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .iter()
                            .map(|t| match t {
                                Some(v) => Some(date32_to_date(*v).to_string()),
                                None => None,
                            })
                            .collect::<Vec<Option<String>>>(),
                    )
                }
                DataType::Boolean => ReturnType::Boolean(
                    column
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                ),
                DataType::Struct(_f) => {
                    let logical_type = field_metadata.get("logicalType").unwrap().as_str();
                    match logical_type {
                        "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" => {
                            return ReturnType::String(convert_timestamps(column));
                        }
                        _ => ReturnType::Missing,
                    }
                }
                _ => ReturnType::Missing, //field.data_type().to_string(),
            }
        })
        .collect::<Vec<ReturnType>>()
        .encode(env)
}

fn convert_timestamps(column: &dyn Any) -> Vec<Option<String>> {
    let mut date_vec: Vec<Option<String>> = Vec::new();

    let (_fields, arrays, _bitmap) = column
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone()
        .into_data();

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
                date_vec.push(Some(
                    NaiveDateTime::from_timestamp(value1, value2 as u32).to_string(),
                ));
            }
            None => {
                date_vec.push(None);
            }
        }
    }
    return date_vec;
}

// Converts a Elixir binary to Chunk<Arc<dyn Array>> using arrow2
pub fn convert_binary(
    arrow_stream_data: Binary,
) -> Result<(StreamMetadata, Chunk<Arc<dyn Array>>)> {
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

impl<'b> Encoder for ReturnType<'b> {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            ReturnType::Int32(a) => a.encode(env),
            ReturnType::Int64(a) => a.encode(env),
            ReturnType::Float64(a) => a.encode(env),
            ReturnType::Int16(a) => a.encode(env),
            ReturnType::Int8(a) => a.encode(env),
            ReturnType::Utf8(a) => a.encode(env),
            ReturnType::String(a) => a.encode(env),
            ReturnType::Boolean(a) => a.encode(env),
            ReturnType::Missing => "missing".encode(env),
        }
    }
}
