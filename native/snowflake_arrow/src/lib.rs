mod serialize;
use crate::serialize::new_serializer;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Metadata;
use arrow2::io::ipc::read;
use rayon::prelude::*;
use rustler::types::binary::Binary;
use rustler::{Encoder, Env, Term};
use std::collections::HashMap;
use std::sync::Arc;

pub enum ReturnType<'a> {
    Int32(Vec<Option<&'a i32>>),
    Int64(Vec<Option<&'a i64>>),
    // Float64(Vec<Option<f64>>),
    Float64(Vec<Option<f64>>),
    Int16(Vec<Option<&'a i16>>),
    Int8(Vec<Option<&'a i8>>),
    Utf8(Vec<Option<&'a str>>),
    Boolean(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    Binary(Vec<Option<&'a [u8]>>),
    Missing(Vec<Option<String>>),
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
pub fn convert_arrow_stream<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
    _cast_elixir_types: bool,
) -> Term<'a> {
    let mut d = arrow_stream_data.as_ref();

    // We want the metadata later for checking types
    let metadata = read::read_stream_metadata(&mut d).unwrap();

    let mut field_metadata: HashMap<usize, Metadata> = HashMap::new();

    for (i, field) in metadata.schema.fields.iter().enumerate() {
        field_metadata.insert(i, field.metadata.clone());
    }

    let mut stream = read::StreamReader::new(&mut d, metadata.clone());
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
                    let fm = field_metadata.get(&field_index).unwrap();
                    let return_type = new_serializer(&fm, array);
                    let field_name = metadata.schema.fields[field_index].name.clone();

                    (field_name, return_type)
                })
                .collect::<HashMap<String, ReturnType>>()
        })
        .collect::<HashMap<String, ReturnType<'_>>>()
        .encode(env)
}

impl<'b> Encoder for ReturnType<'_> {
    #[inline]
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
            ReturnType::Missing(x) => x.encode(env),
            _ => "x".encode(env),
        }
    }
}
