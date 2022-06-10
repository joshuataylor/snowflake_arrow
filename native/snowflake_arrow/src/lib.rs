#![feature(type_ascription)]
pub mod serialize;

use crate::atoms::elixir_calendar_iso;
use crate::serialize::new_serializer;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Metadata};
use arrow2::io::ipc::read;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use indexmap::IndexMap;
use rayon::prelude::*;
use rustler::types::binary::Binary;
use rustler::NifStruct;
use rustler::NifUntaggedEnum;
use rustler::{Encoder, Env, Term};
use std::sync::Arc;

#[derive(NifStruct, Debug)]
#[module = "Date"]
pub struct ElixirDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub calendar: rustler::Atom,
}

#[derive(NifStruct, Debug)]
#[module = "ElixirDateTime"]
pub struct ElixirDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub minute: u32,
    pub second: u32,
}

impl From<NaiveDate> for ElixirDate {
    fn from(d: NaiveDate) -> Self {
        ElixirDate {
            year: d.year(),
            month: d.month(),
            day: d.day(),
            calendar: elixir_calendar_iso(),
        }
    }
}

impl From<NaiveDateTime> for ElixirDateTime {
    fn from(d: NaiveDateTime) -> Self {
        ElixirDateTime {
            year: d.year(),
            month: d.month(),
            day: d.day(),
            minute: d.minute(),
            second: d.second(),
        }
    }
}

struct ArrowIndexMap(IndexMap<String, Vec<ReturnType>>);

#[derive(Debug, NifUntaggedEnum)]
pub enum ReturnType {
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float64(Option<f64>),
    Float642(Option<f64>),
    Int16(Option<i16>),
    Int8(Option<i8>),
    Boolean(Option<bool>),
    Binary(Option<Vec<u8>>),
    String(Option<String>),
    Date(Option<ElixirDate>),
    DateTime(Option<ElixirDateTime>),
    Missing(Option<String>),
}

impl From<DataType> for ReturnType {
    fn from(dt: DataType) -> Self {
        match dt {
            // DataType::Null => {}
            DataType::Boolean => ReturnType::Boolean(None),
            DataType::Int8 => ReturnType::Int8(None),
            DataType::Int16 => ReturnType::Int16(None),
            DataType::Int32 => ReturnType::Int32(None),
            DataType::Int64 => ReturnType::Int64(None),
            DataType::Float64 => ReturnType::Float64(None),
            DataType::Date32 => ReturnType::Date(None),
            DataType::Utf8 => ReturnType::String(None),
            _ => unreachable!(),
        }
    }
}

mod atoms {
    rustler::atoms! {
      ok,
      error,
      __struct__,
      nil,
      elixir_calendar_iso = "Elixir.Calendar.ISO"
    }
}

rustler::init!("Elixir.SnowflakeArrow.Native", [convert_arrow_stream]);

#[rustler::nif]
#[inline]
fn convert_arrow_stream<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
    cast_elixir_types: bool,
    transpose_rows: bool,
) -> Term<'a> {
    let mut arrow_data = arrow_stream_data.as_ref();

    // We want the metadata later for checking types
    let metadata = read::read_stream_metadata(&mut arrow_data).unwrap();

    let mut field_metadata: IndexMap<usize, Metadata> = IndexMap::new();

    for (i, field) in metadata.schema.fields.iter().enumerate() {
        field_metadata.insert(i, field.metadata.clone());
    }

    let stream = read::StreamReader::new(&mut arrow_data, metadata.clone());
    let mut chunks: Vec<Chunk<Arc<dyn Array>>> = vec![];
    let mut total_rows = 0;

    for stream_state in stream {
        match stream_state {
            Ok(read::StreamState::Some(chunk)) => {
                total_rows += chunk.len();
                chunks.push(chunk)
            }
            Ok(read::StreamState::Waiting) => break,
            Err(_l) => break,
        }
    }

    let return_type_hashmap: IndexMap<String, Vec<ReturnType>> = chunks
        .par_iter()
        .flat_map(|chunk| {
            chunk
                .iter()
                .enumerate()
                .map(|(field_index, array)| {
                    let field_name = metadata.schema.fields[field_index].name.clone();
                    let fm = field_metadata.get(&field_index).unwrap();
                    (field_name, new_serializer(fm, array, cast_elixir_types))
                })
                .collect::<IndexMap<String, Vec<ReturnType>>>()
        })
        .fold(IndexMap::new, |mut acc, (key, rt)| {
            acc.entry(key).or_insert_with(std::vec::Vec::new).extend(rt);
            acc
        })
        .reduce_with(|mut m1, m2| {
            for (k, v) in m2 {
                m1.entry(k).or_insert_with(Vec::new).extend(v);
            }
            m1
        })
        .unwrap();

    // total values in return_type_hashmap
    let total_columns = return_type_hashmap.len();

    // As we need to return rows back to Elixir, we need to transpose the data for columns tro rows.
    // This is probably a stupid way to do this, but it works.
    if transpose_rows {
        return (0..total_rows)
            .into_iter()
            .map(|row_index| {
                (0..total_columns)
                    .into_iter()
                    .map(|column_index| {
                        // println!("{}", column_index);
                        let column_name = return_type_hashmap.values().nth(column_index).unwrap();
                        column_name.get(row_index).unwrap()
                    })
                    .collect::<Vec<&ReturnType>>()
            })
            .collect::<Vec<Vec<&ReturnType>>>()
            .encode(env);
    }

    // We need to implement ArrowIndexMap here, as we need to encode it later but will get an error otherwise.
    ArrowIndexMap(return_type_hashmap).encode(env)
}

impl Encoder for ArrowIndexMap {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let (keys, values): (Vec<_>, Vec<_>) = self
            .0
            .iter()
            .map(|(k, v)| (k.encode(env), v.encode(env)))
            .unzip();
        Term::map_from_arrays(env, &keys, &values).unwrap()
    }
}
