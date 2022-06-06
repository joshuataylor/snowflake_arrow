mod elixir_types;
use crate::atoms::nil;
use arrow2::array::{
    BinaryArray, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StructArray, Utf8Array,
};
use arrow2::datatypes::{DataType, Metadata};
use arrow2::io::ipc::read;
use arrow2::io::ipc::read::StreamMetadata;
use arrow2::temporal_conversions::date32_to_date;
use chrono::NaiveDateTime;
use rustler::types::binary::Binary;
use rustler::{Encoder, Env, Term};
use std::any::Any;
use std::collections::HashMap;

#[derive(Clone)]
pub enum ReturnType<'a> {
    Int32(Vec<Option<&'a i32>>),
    Int64(Vec<Option<&'a i64>>),
    Float64(Vec<Option<&'a f64>>),
    Int16(Vec<Option<&'a i16>>),
    Int8(Vec<Option<&'a i8>>),
    Utf8(Vec<Option<&'a str>>),
    Boolean(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    Binary(Vec<Option<&'a [u8]>>),
    Missing(String),
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

fn metadata_for_field(metadata: &StreamMetadata, index: u32) -> Metadata {
    return metadata
        .schema
        .fields
        .get(index as usize)
        .unwrap()
        .metadata
        .clone();
}

fn name_for_field(metadata: &StreamMetadata, index: u32) -> &String {
    return &metadata.schema.fields.get(index as usize).unwrap().name;
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn convert_arrow_stream<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
    _cast_elixir_types: bool,
) -> Term<'a> {
    let mut binary = arrow_stream_data.as_ref();
    let mut columns: HashMap<String, Vec<Term>> = HashMap::new();

    // We want the metadata later for checking types
    let metadata = read::read_stream_metadata(&mut binary).unwrap();
    for field in &metadata.schema.fields {
        columns.insert(field.name.clone(), vec![]);
    }
    let mut stream = read::StreamReader::new(&mut binary, metadata.clone());
    let _foo: Vec<Vec<Option<&[u8]>>> = vec![];

    loop {
        match stream.next() {
            Some(x) => match x {
                Ok(read::StreamState::Some(chunk)) => {
                    let mut fields_hashmaps2: HashMap<String, Vec<Term>> = HashMap::new();
                    for field in &metadata.schema.fields {
                        fields_hashmaps2.insert(field.name.clone(), vec![]);
                    }

                    let mut field_index = 0;

                    for field in chunk.arrays() {
                        let total_columns = field.len();
                        let field_metadata = metadata_for_field(&metadata, field_index as u32);
                        let field_name = name_for_field(&metadata, field_index as u32);
                        let column = field.as_any();

                        let encoded = match field.data_type() {
                            DataType::Int64 => column
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>()
                                .encode(env),
                            DataType::Int32 => {
                                if field_metadata.get("scale").unwrap() == "0" {
                                    column
                                        .downcast_ref::<Int32Array>()
                                        .unwrap()
                                        .iter()
                                        .collect::<Vec<_>>()
                                        .encode(env)
                                } else {
                                    let scale = field_metadata
                                        .get("scale")
                                        .unwrap()
                                        .parse::<i32>()
                                        .unwrap();
                                    let int32_array = column.downcast_ref::<Int32Array>().unwrap();
                                    let mut float_vecs: Vec<Option<f64>> = Vec::new();

                                    for valid in int32_array {
                                        match valid {
                                            Some(x) => {
                                                float_vecs.push(Some(
                                                    x.clone() as f64 / 10f64.powi(scale),
                                                ));
                                            }
                                            None => float_vecs.push(None),
                                        }
                                    }

                                    float_vecs.encode(env)
                                }
                            }
                            DataType::Float64 => column
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>()
                                .encode(env),
                            DataType::Int16 => column
                                .downcast_ref::<Int16Array>()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>()
                                .encode(env),
                            DataType::Int8 => column
                                .downcast_ref::<Int8Array>()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>()
                                .encode(env),
                            DataType::Utf8 => column
                                .downcast_ref::<Utf8Array<i32>>()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>()
                                .encode(env),

                            DataType::Date32 => column
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .iter()
                                .map(|t| match t {
                                    Some(v) => Some(date32_to_date(*v).to_string()),
                                    None => None,
                                })
                                .collect::<Vec<Option<String>>>()
                                .encode(env),

                            DataType::Boolean => column
                                .downcast_ref::<BooleanArray>()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>()
                                .encode(env),

                            DataType::Binary => column
                                .downcast_ref::<BinaryArray<i32>>()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>()
                                .encode(env),

                            DataType::Struct(_f) => {
                                let logical_type =
                                    field_metadata.get("logicalType").unwrap().as_str();
                                match logical_type {
                                    "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                                        convert_timestamps(column).encode(env)
                                    }
                                    _a => vec![nil(); total_columns].encode(env),
                                }
                            }
                            // a => a.to_string().encode(env), //field.data_type().to_string().encode(env), //,
                            _a => vec![nil(); total_columns].encode(env),
                        };
                        columns.get_mut(field_name).unwrap().push(encoded);
                        field_index += 1;
                    }
                }
                Ok(read::StreamState::Waiting) => break,
                Err(_l) => break,
            },
            None => break,
        };
    }

    return columns.encode(env);
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