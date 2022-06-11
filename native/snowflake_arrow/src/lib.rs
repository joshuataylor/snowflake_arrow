mod serialize;

use crate::serialize::serialize_to_elixir;
use arrow2::datatypes::Metadata;

use arrow2::io::ipc::read;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use rayon::prelude::*;
use std::collections::HashMap;

use crate::atoms::elixir_calendar_iso;
use rustler::NifStruct;
use rustler::NifUntaggedEnum;
use rustler::{Binary, Encoder, Env, Term};
use std::io::Read;

#[derive(Debug, Clone, NifUntaggedEnum)]
pub enum SnowflakeReturnType {
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

#[derive(Debug, Clone, NifStruct)]
#[module = "Date"]
pub struct ElixirDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub calendar: rustler::Atom,
}

#[derive(Debug, Clone, NifStruct)]
#[module = "NaiveDateTime"]
pub struct ElixirDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: u32,
    pub time_zone: String,
    pub zone_abbr: String,
    pub utc_offset: usize,
    pub std_offset: usize,
    pub calendar: rustler::Atom,
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
            microsecond: d.timestamp_subsec_micros(),
            time_zone: "Etc/UTC".to_string(),
            zone_abbr: "UTC".to_string(),
            utc_offset: 0,
            std_offset: 0,
            calendar: elixir_calendar_iso(),
        }
    }
}

pub type ColumnMetadata = HashMap<usize, Metadata>;

pub fn convert_chunks_to_snowflake<R: Read>(
    arrow_data: &mut R,
    cast_elixir_types: bool,
) -> Vec<Vec<SnowflakeReturnType>> {
    let metadata = read::read_stream_metadata(arrow_data).unwrap();

    let stream = read::StreamReader::new(arrow_data, metadata.clone());
    let mut field_metadata: ColumnMetadata = HashMap::new();

    // We need the field metadata for the timestamp info later.
    for (i, field) in metadata.schema.fields.iter().enumerate() {
        field_metadata.insert(i, field.metadata.clone());
    }

    let mut chunks = vec![];

    for stream_state in stream {
        match stream_state {
            Ok(read::StreamState::Some(chunk)) => chunks.push(chunk),
            Ok(read::StreamState::Waiting) => break,
            Err(_l) => break,
        }
    }

    chunks
        .par_iter()
        .flat_map(|chunk| serialize_to_elixir(&field_metadata, chunk, cast_elixir_types).unwrap())
        .collect::<Vec<Vec<SnowflakeReturnType>>>()
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

rustler::init!(
    "Elixir.SnowflakeArrow.Native",
    [convert_arrow_stream_to_rows]
);

#[rustler::nif]
#[inline]
fn convert_arrow_stream_to_rows<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
    cast_elixir_types: bool,
) -> Term<'a> {
    let result = convert_chunks_to_snowflake(&mut arrow_stream_data.as_ref(), cast_elixir_types);

    let rows = result.len();
    println!("{}", rows);

    "x".encode(env)
}
