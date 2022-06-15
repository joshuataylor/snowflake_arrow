use crate::helpers::get_chunks_for_data;
use crate::rows::atoms::elixir_calendar_iso;
use arrow2::array::{
    Array, BooleanArray, Int32Array, Int64Array, PrimitiveArray, StructArray, Utf8Array,
};
use arrow2::datatypes::DataType;
use arrow2::temporal_conversions::date32_to_date;
use bytes::{BufMut, BytesMut};
use chrono::{Datelike, Timelike};
use chrono::{NaiveDate, NaiveDateTime};
use indexmap::IndexMap;
use rustler::NifStruct;
use rustler::{Binary, Encoder, Env, NewBinary, Term};
use std::any::Any;
use std::io::Write;

mod atoms {
    rustler::atoms! {
      ok,
      error,
      __struct__,
      nil,
      elixir_calendar_iso = "Elixir.Calendar.ISO"
    }
}

#[derive(NifStruct, Debug)]
#[module = "Date"]
pub struct ElixirDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub calendar: rustler::Atom,
}

#[derive(NifStruct, Debug)]
#[module = "NaiveDateTime"]
pub struct ElixirDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: (u32, usize),
    pub calendar: rustler::Atom,
    pub hour: u32,
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
            hour: d.hour(),
            minute: d.minute(),
            second: d.second(),
            microsecond: (d.timestamp_subsec_micros(), 6),
            calendar: elixir_calendar_iso(),
        }
    }
}

pub struct ArrowErlangBinary<'a> {
    pub values: Vec<Option<Binary<'a>>>,
}

pub struct ArrowBoolean {
    pub values: Vec<Option<bool>>,
}

pub struct ArrowInt32 {
    pub values: Vec<Option<i32>>,
}

pub struct ArrowInt64 {
    pub values: Vec<Option<i64>>,
}

pub struct ArrowInt16 {
    pub values: Vec<Option<i16>>,
}

// Yes arrow doesn't support float 64, technically this is a decimal.
// Probably should move it to decimal.
pub struct ArrowFloat64 {
    pub values: Vec<Option<f64>>,
}

pub struct ArrowInt8 {
    pub values: Vec<Option<i8>>,
}

pub struct ArrowNaiveDate {
    pub values: Vec<Option<NaiveDate>>,
}

pub struct ArrowNaiveDateTime {
    pub values: Vec<Option<NaiveDateTime>>,
}

pub enum ArrowColumn<'a> {
    ArrowErlangBinary(ArrowErlangBinary<'a>),
    ArrowBoolean(ArrowBoolean),
    ArrowInt8(ArrowInt8),
    ArrowInt16(ArrowInt16),
    ArrowInt32(ArrowInt32),
    ArrowFloat64(ArrowFloat64),
    ArrowNaiveDate(ArrowNaiveDate),
    ArrowNaiveDateTime(ArrowNaiveDateTime),
    ArrowInt64(ArrowInt64),
}

#[rustler::nif]
#[inline(always)]
pub fn convert_arrow_stream_to_rows_chunked<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
    cast_elixir_types: bool,
) -> Term<'a> {
    let (chunks, field_metadata, _total_rows) =
        get_chunks_for_data(&mut arrow_stream_data.as_ref());
    let mut array_hashmap: IndexMap<usize, ArrowColumn> = IndexMap::new();

    for chunk in chunks {
        for (array_index, array) in chunk.arrays().iter().enumerate() {
            let fm = field_metadata.get(&array_index).unwrap();

            match array.data_type() {
                DataType::Utf8 => {
                    let column = array_hashmap.entry(array_index).or_insert_with(|| {
                        ArrowColumn::ArrowErlangBinary(ArrowErlangBinary { values: vec![] })
                    });

                    let utf8_data = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

                    let mut values_binary = NewBinary::new(env, utf8_data.values().len());
                    values_binary
                        .as_mut_slice()
                        .write_all(utf8_data.values())
                        .unwrap();

                    let binary: Binary = values_binary.into();
                    let d = build_binary_vec_from_binary_offsets(
                        binary,
                        utf8_data.offsets().to_vec(),
                        env,
                    );

                    if let ArrowColumn::ArrowErlangBinary(u) = column {
                        u.values.extend(d)
                    }
                }
                DataType::Boolean => {
                    let column = array_hashmap.entry(array_index).or_insert_with(|| {
                        ArrowColumn::ArrowBoolean(ArrowBoolean { values: vec![] })
                    });

                    let data: Vec<Option<bool>> = array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect();

                    if let ArrowColumn::ArrowBoolean(u) = column {
                        u.values.extend(data)
                    }
                }
                DataType::Int32 => {
                    if fm.get("scale").unwrap() == "0" {
                        let column = array_hashmap.entry(array_index).or_insert_with(|| {
                            ArrowColumn::ArrowInt32(ArrowInt32 { values: vec![] })
                        });

                        let data = array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .iter()
                            .map(|x| x.copied())
                            .collect::<Vec<Option<i32>>>();

                        if let ArrowColumn::ArrowInt32(u) = column {
                            u.values.extend(data)
                        }
                    } else {
                        let column = array_hashmap.entry(array_index).or_insert_with(|| {
                            ArrowColumn::ArrowFloat64(ArrowFloat64 { values: vec![] })
                        });
                        let scale = fm.get("scale").unwrap().parse::<i32>().unwrap();
                        let data = float_to_vecs(array, scale);
                        if let ArrowColumn::ArrowFloat64(u) = column {
                            u.values.extend(data)
                        }
                    }
                }
                DataType::Int8 => {
                    let column = array_hashmap
                        .entry(array_index)
                        .or_insert_with(|| ArrowColumn::ArrowInt8(ArrowInt8 { values: vec![] }));

                    let data = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i8>>()
                        .unwrap()
                        .iter()
                        .map(|x| x.copied())
                        .collect::<Vec<Option<i8>>>();

                    if let ArrowColumn::ArrowInt8(u) = column {
                        u.values.extend(data)
                    }
                }
                DataType::Int16 => {
                    let column = array_hashmap
                        .entry(array_index)
                        .or_insert_with(|| ArrowColumn::ArrowInt16(ArrowInt16 { values: vec![] }));

                    let data = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i16>>()
                        .unwrap()
                        .iter()
                        .map(|x| x.copied())
                        .collect::<Vec<Option<i16>>>();

                    if let ArrowColumn::ArrowInt16(u) = column {
                        u.values.extend(data)
                    }
                }
                DataType::Int64 => {
                    let column = array_hashmap
                        .entry(array_index)
                        .or_insert_with(|| ArrowColumn::ArrowInt64(ArrowInt64 { values: vec![] }));

                    let data = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .unwrap()
                        .iter()
                        .map(|x| x.copied())
                        .collect::<Vec<Option<i64>>>();

                    if let ArrowColumn::ArrowInt64(u) = column {
                        u.values.extend(data)
                    }
                }
                DataType::Float64 => {
                    let column = array_hashmap.entry(array_index).or_insert_with(|| {
                        ArrowColumn::ArrowFloat64(ArrowFloat64 { values: vec![] })
                    });

                    let data = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f64>>()
                        .unwrap()
                        .iter()
                        .map(|x| x.copied())
                        .collect::<Vec<Option<f64>>>();

                    if let ArrowColumn::ArrowFloat64(u) = column {
                        u.values.extend(data)
                    }
                }
                DataType::Date32 => {
                    let data = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .unwrap()
                        .iter()
                        .map(|t| t.map(|x| date32_to_date(*x)))
                        .collect::<Vec<Option<NaiveDate>>>();

                    if cast_elixir_types {
                        let column = array_hashmap.entry(array_index).or_insert_with(|| {
                            ArrowColumn::ArrowNaiveDate(ArrowNaiveDate { values: vec![] })
                        });

                        if let ArrowColumn::ArrowNaiveDate(u) = column {
                            u.values.extend(data);
                        }
                    } else {
                        let column = array_hashmap.entry(array_index).or_insert_with(|| {
                            ArrowColumn::ArrowErlangBinary(ArrowErlangBinary { values: vec![] })
                        });
                        let date_strings = data
                            .iter()
                            .map(|x| x.as_ref().map(|d| d.to_string()))
                            .collect::<Vec<Option<String>>>();

                        let erlang_binaries = build_binary_vec_from_strings(date_strings, env);

                        if let ArrowColumn::ArrowErlangBinary(u) = column {
                            u.values.extend(erlang_binaries);
                        }
                    }
                }
                // DataType::Binary => array
                //     .as_any()
                //     .downcast_ref::<BinaryArray<i32>>()
                //     .unwrap()
                //     .iter()
                //     .map(|x| ReturnType::Binary(x.map(|t| t.to_vec())))
                //     .collect::<Vec<ReturnType>>(),
                DataType::Struct(_f) => {
                    let logical_type = fm.get("logicalType").unwrap().as_str();
                    match logical_type {
                        "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                            let timestamps = convert_timestamps(array.as_any());

                            if cast_elixir_types {
                                let column =
                                    array_hashmap.entry(array_index).or_insert_with(|| {
                                        ArrowColumn::ArrowNaiveDateTime(ArrowNaiveDateTime {
                                            values: vec![],
                                        })
                                    });

                                if let ArrowColumn::ArrowNaiveDateTime(u) = column {
                                    u.values.extend(timestamps);
                                }
                            } else {
                                let column =
                                    array_hashmap.entry(array_index).or_insert_with(|| {
                                        ArrowColumn::ArrowErlangBinary(ArrowErlangBinary {
                                            values: vec![],
                                        })
                                    });
                                let datetime_strings = timestamps
                                    .iter()
                                    .map(|x| x.as_ref().map(|d| d.to_string()))
                                    .collect::<Vec<Option<String>>>();

                                let erlang_binaries =
                                    build_binary_vec_from_strings(datetime_strings, env);

                                if let ArrowColumn::ArrowErlangBinary(u) = column {
                                    u.values.extend(erlang_binaries);
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                _ => {
                    // let column = array_hashmap.entry(array_index).or_insert_with(|| {
                    //     ArrowColumn::Missing(vec![None; chunk.len()])
                    // });
                }
            }
        }
    }

    let mut processed_columns = vec![];
    for (_column_index, array) in array_hashmap {
        processed_columns.push(match array {
            ArrowColumn::ArrowErlangBinary(x) => x.values.encode(env),
            ArrowColumn::ArrowBoolean(b) => b.values.encode(env),
            ArrowColumn::ArrowInt8(x) => x.values.encode(env),
            ArrowColumn::ArrowInt16(x) => x.values.encode(env),
            ArrowColumn::ArrowInt32(x) => x.values.encode(env),
            ArrowColumn::ArrowFloat64(x) => x.values.encode(env),
            ArrowColumn::ArrowNaiveDate(x) => x
                .values
                .iter()
                .map(|d| d.as_ref().map(|d| (*d).into()))
                .collect::<Vec<Option<ElixirDate>>>()
                .encode(env),
            ArrowColumn::ArrowNaiveDateTime(x) => x
                .values
                .iter()
                .map(|d| d.as_ref().map(|d| (*d).into()))
                .collect::<Vec<Option<ElixirDateTime>>>()
                .encode(env),
            ArrowColumn::ArrowInt64(x) => x.values.encode(env),
        })
    }

    processed_columns.encode(env)
}

#[inline(always)]
pub fn build_binary_vec_from_strings<'a>(
    strings: Vec<Option<String>>,
    env: Env<'a>,
) -> Vec<Option<Binary<'a>>> {
    // Loop through the strings, building them as bytes
    let mut offsets: Vec<i32> = vec![];
    let mut current_offset = 0;
    let mut bytes_buffer = BytesMut::new();

    for string in strings {
        match string {
            None => offsets.push(current_offset),
            Some(string) => {
                let parsed_bytes = string.as_bytes();
                bytes_buffer.put(parsed_bytes);
                current_offset += parsed_bytes.len() as i32;
                offsets.push(current_offset);
            }
        }
    }
    let b1 = bytes_buffer.freeze();

    // Create a new erlang binary from the bytes
    let mut values_binary = NewBinary::new(env, b1.len());
    values_binary.as_mut_slice().write_all(&b1).unwrap();
    let binary: Binary = values_binary.into();

    build_binary_vec_from_binary_offsets(binary, offsets, env)
}

#[inline(always)]
fn build_binary_vec_from_binary_offsets<'a>(
    binary: Binary<'a>,
    offsets: Vec<i32>,
    _env: Env<'a>,
) -> Vec<Option<Binary<'a>>> {
    let mut current_offset: i32 = -1;
    offsets
        .iter()
        .map(|&offset| {
            // If the offset is 0, we know the value is None
            // If the previous number is the same as the current offset, it's none
            // We also don't want to add the first iteration, so we start current offset at -1
            if current_offset >= 0 && (offset == 0 || current_offset == offset) {
                // values.push(None);
                current_offset = offset;
                None
            } else if current_offset >= 0 {
                let slice = binary
                    .make_subbinary(current_offset as usize, (offset - current_offset) as usize)
                    .unwrap();
                current_offset = offset;
                Some(slice)
            } else {
                current_offset = 0;
                None
            }
        })
        .collect::<Vec<Option<Binary>>>()
}

#[inline]
pub fn float_to_vecs(array: &Box<dyn Array>, scale: i32) -> Vec<Option<f64>> {
    array
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .map(|t| t.map(|&v| v as f64 / 10f64.powi(scale)))
        .collect::<Vec<Option<f64>>>()
}

#[inline]
pub fn convert_timestamps(column: &dyn Any) -> Vec<Option<NaiveDateTime>> {
    let (_fields, arrays, _bitmap) = column
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone()
        .into_data();

    // Get the fractional parts from the second array
    let fractions = arrays[1].as_any().downcast_ref::<Int32Array>().unwrap();

    // Loop the epochs and get the fraction to get the final timestamp
    arrays[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, epoch)| match epoch {
            Some(epoch) => {
                let value1 = *epoch as i64;
                let value2 = fractions.value(i);
                Some(NaiveDateTime::from_timestamp(value1, value2 as u32))
            }
            None => None,
        })
        .collect::<Vec<Option<NaiveDateTime>>>()
}
