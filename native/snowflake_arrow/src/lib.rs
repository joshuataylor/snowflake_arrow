#![feature(type_ascription)]
pub mod serialize;

use crate::serialize::new_serializer;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Metadata;
use arrow2::io::ipc::read;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use rayon::prelude::*;
use rustler::types::binary::Binary;
use rustler::NifStruct;

use rustler::{Encoder, Env, Term};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(NifStruct, Debug)]
#[module = "ElixirDate"]
pub struct ElixirDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

#[derive(NifStruct, Debug)]
#[module = "ElixirDateTime"]
pub struct ElixirDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub minute: u32,
    pub second: u32,
    // pub microsecond: microsecond,
}

impl From<NaiveDate> for ElixirDate {
    fn from(d: NaiveDate) -> Self {
        ElixirDate {
            year: d.year(),
            month: d.month(),
            day: d.day(),
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

#[derive(Debug)]
pub enum ReturnType<'a> {
    Int32(Option<&'a i32>),
    Int64(Option<&'a i64>),
    Float64(Option<&'a f64>),
    Str(Option<&'a str>),
    Float642(Option<f64>),
    Int16(Option<&'a i16>),
    Int8(Option<&'a i8>),
    Utf8(Option<&'a str>),
    Boolean(Option<bool>),
    String(Option<String>),
    Date(Option<ElixirDate>),
    DateTime(Option<ElixirDateTime>),
    Binary(Option<&'a [u8]>),
    Missing(Option<String>),
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

    let mut field_metadata: HashMap<String, Metadata> = HashMap::new();

    for (_i, field) in metadata.schema.fields.iter().enumerate() {
        field_metadata.insert(field.name.to_string(), field.metadata.clone());
    }

    let mut stream = read::StreamReader::new(&mut arrow_data, metadata.clone());
    let mut chunks: Vec<Chunk<Arc<dyn Array>>> = vec![];
    let mut total_rows = 0;

    loop {
        match stream.next() {
            Some(x) => match x {
                Ok(read::StreamState::Some(chunk)) => {
                    total_rows += chunk.len();
                    chunks.push(chunk)
                }
                Ok(read::StreamState::Waiting) => break,
                Err(_l) => break,
            },
            None => break,
        }
    }

    let return_type_hashmap: HashMap<String, Vec<ReturnType>> = chunks
        .par_iter()
        .flat_map(|chunk| {
            chunk
                .iter()
                .enumerate()
                .map(|(field_index, array)| {
                    let field_name = metadata.schema.fields[field_index].name.clone();
                    let fm = field_metadata.get(&field_name).unwrap();
                    (field_name, new_serializer(fm, array, cast_elixir_types))
                })
                .collect::<HashMap<String, Vec<ReturnType<'_>>>>()
        })
        .fold(HashMap::new, |mut acc, (key, rt)| {
            acc.entry(key).or_insert_with(|| vec![]).extend(rt);
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

    // As we need to return rows back to Elixir, we need to transpose the data.
    // This is probably a stupid way to do this, but it works, and much faster than doing it
    // in elixir
    // Elixir code would look like this (it takes multiple seconds to run for me):
    //     total_columns = length(Map.keys(columns))
    //     total = length(columns[hd(Map.keys(columns))])
    //     list = Map.to_list(columns)
    //
    //     Enum.map(0..(total - 1), fn row_index ->
    //       Enum.map(0..(total_columns - 1), fn column_index ->
    //         {key, values} = Enum.at(list, column_index)
    //         Enum.at(values, row_index)
    //       end)
    //     end)
    if transpose_rows {
        return (0..total_rows)
            .into_iter()
            .map(|row_index| {
                (0..total_columns)
                    .into_iter()
                    .map(|column_index| {
                        let column_name = return_type_hashmap.values().nth(column_index).unwrap();
                        column_name.get(row_index).unwrap()
                    })
                    .collect::<Vec<&ReturnType<'_>>>()
            })
            .collect::<Vec<Vec<&ReturnType>>>()
            .encode(env);
    }

    return_type_hashmap.encode(env)
}

impl<'b> Encoder for ReturnType<'_> {
    #[inline]
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            ReturnType::Int64(a) => a.encode(env),
            ReturnType::Binary(a) => a.encode(env),
            ReturnType::Int32(a) => a.encode(env),
            ReturnType::Float64(a) => a.encode(env),
            ReturnType::Int16(a) => a.encode(env),
            ReturnType::Int8(a) => a.encode(env),
            ReturnType::Utf8(a) => a.encode(env),
            ReturnType::String(a) => a.encode(env),
            ReturnType::Boolean(a) => a.encode(env),
            ReturnType::Missing(x) => x.encode(env),
            ReturnType::Float642(x) => x.encode(env),
            ReturnType::Str(x) => x.encode(env),
            ReturnType::Date(x) => x.encode(env),
            ReturnType::DateTime(x) => x.encode(env),
        }
    }
}
