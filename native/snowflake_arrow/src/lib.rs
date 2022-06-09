#![feature(type_ascription)]
pub mod serialize;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Metadata;
use arrow2::io::ipc::read;
use rayon::prelude::*;
use rustler::types::binary::Binary;
use rustler::{Encoder, Env, Term};
use std::collections::HashMap;

use std::sync::Arc;

use crate::serialize::new_serializer;

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
    _cast_elixir_types: bool,
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

    loop {
        match stream.next() {
            Some(x) => match x {
                Ok(read::StreamState::Some(chunk)) => chunks.push(chunk),
                Ok(read::StreamState::Waiting) => break,
                Err(_l) => break,
            },
            None => break,
        }
    }

    chunks
        .par_iter()
        .flat_map(|chunk| {
            chunk
                .iter()
                .enumerate()
                .map(|(field_index, array)| {
                    let field_name = metadata.schema.fields[field_index].name.clone();
                    let fm = field_metadata.get(&field_name).unwrap();
                    (field_name, new_serializer(fm, array))
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
        .unwrap()
        .encode(env)
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
        }
    }
}
