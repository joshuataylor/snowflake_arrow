use crate::error::SnowflakeArrowError;
use crate::polars_convert::snowflake_arrow_ipc_streaming_binary_to_dataframe;
use crate::rustler_helper::atoms::{
    calendar, day, elixir_calendar_iso, hour, microsecond, minute, month, second, year,
};
use chrono::{Datelike, Timelike};
use polars::datatypes::DataType;
use polars::export::arrow::array::Array;
use polars::prelude::ChunkLen;
use rustler::types::atom;
use rustler::types::atom::nil;
use rustler::wrapper::list::make_list;
use rustler::wrapper::{map, NIF_TERM};
use rustler::{Atom, Binary, Encoder, Env, NewBinary, Term};

macro_rules! encode {
    ($s:ident, $env:ident, $convert_function:ident, $out_type:ty) => {
        $s.$convert_function()
            .unwrap()
            .into_iter()
            .map(|x| x.map(|d| d as $out_type).encode($env).as_c_arg())
            .collect::<Vec<NIF_TERM>>()
    };
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn convert_snowflake_arrow_stream<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
) -> Result<Term<'a>, SnowflakeArrowError> {
    let df = snowflake_arrow_ipc_streaming_binary_to_dataframe(&arrow_stream_data)?;
    // Here we build the Date struct manually, as it's much faster than using Date NifStruct
    // This is because we already have the keys (we know this at compile time), and the types,
    // so we can build the struct directly.
    let date_struct_keys = [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        day().encode(env).as_c_arg(),
        month().encode(env).as_c_arg(),
        year().encode(env).as_c_arg(),
    ];

    // // This sets the value in the map to "Elixir.Calendar.ISO", which must be an atom.
    let calendar_iso_c_arg = elixir_calendar_iso().encode(env).as_c_arg();

    // This is used for the map to know that it's a struct. Define it here so it's not redefined in the loop.
    let date_module_atom = Atom::from_str(env, "Elixir.Date")
        .unwrap()
        .encode(env)
        .as_c_arg();

    // Here we build the NaiveDateTime struct manually, as it's about 4x faster than using NifStruct.
    // It's faster because we already have the keys (we know this at compile time), and the type.
    let datetime_struct_keys = [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        microsecond().encode(env).as_c_arg(),
        day().encode(env).as_c_arg(),
        month().encode(env).as_c_arg(),
        year().encode(env).as_c_arg(),
        hour().encode(env).as_c_arg(),
        minute().encode(env).as_c_arg(),
        second().encode(env).as_c_arg(),
    ];

    // This is used for the map to know that it's a struct. Define it here so it's not redefined in the loop.
    let datetime_module_atom = Atom::from_str(env, "Elixir.NaiveDateTime")
        .unwrap()
        .encode(env)
        .as_c_arg();
    let nil = nil().encode(env).as_c_arg();
    let env_carg = env.as_c_arg();

    // loop over columns to return as a list of tuples.
    // We can't use par_iter here as we need to env to encode the tuples to c_arg.
    // @todo can we somehow make this parallel later by using a generic type that holds the iter?
    let columns: Vec<NIF_TERM> = df
        .iter()
        .map(|series| unsafe {
            let tuples = match series.0.dtype() {
                DataType::Int32 => encode!(series, env, i32, i32),
                DataType::Boolean => encode!(series, env, bool, bool),
                DataType::Int8 => encode!(series, env, i8, i8),
                DataType::Int16 => encode!(series, env, i16, i16),
                DataType::Int64 => encode!(series, env, i64, i64),
                DataType::Float64 => encode!(series, env, f64, f64),
                DataType::Date => series
                    .date()
                    .unwrap()
                    .as_date_iter()
                    .map(|x| match x {
                        None => nil,
                        Some(dt) => {
                            let values = [
                                date_module_atom,
                                calendar_iso_c_arg,
                                dt.day().encode(env).as_c_arg(),
                                dt.month().encode(env).as_c_arg(),
                                dt.year().encode(env).as_c_arg(),
                            ];
                            Term::new(
                                env,
                                map::make_map_from_arrays(
                                    env.as_c_arg(),
                                    &date_struct_keys,
                                    &values,
                                )
                                .unwrap(),
                            )
                            .as_c_arg()
                        }
                    })
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Datetime(_tu, _tz) => series
                    .datetime()
                    .unwrap()
                    .as_datetime_iter()
                    .map(|dt_item| match dt_item {
                        None => nil,
                        Some(dt) => {
                            let mut microseconds = dt.timestamp_subsec_micros();
                            if microseconds > 999_999 {
                                microseconds = 999_999;
                            }

                            let values = &[
                                datetime_module_atom,
                                calendar_iso_c_arg,
                                (microseconds, 6).encode(env).as_c_arg(),
                                dt.day().encode(env).as_c_arg(),
                                dt.month().encode(env).as_c_arg(),
                                dt.year().encode(env).as_c_arg(),
                                dt.hour().encode(env).as_c_arg(),
                                dt.minute().encode(env).as_c_arg(),
                                dt.second().encode(env).as_c_arg(),
                            ];
                            Term::new(
                                env,
                                map::make_map_from_arrays(
                                    env.as_c_arg(),
                                    &datetime_struct_keys,
                                    values,
                                )
                                .unwrap(),
                            )
                            .as_c_arg()
                        }
                    })
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Utf8 => {
                    // This is goofy and stupid, but drastically speeds up converting binaries.
                    // What we do here is take all the values/offsets, create a binary, then subslice into it.
                    // @todo fix this later to be less terrible.
                    let utf8 = series.utf8().unwrap();
                    let len = utf8.len();
                    let mut values = Vec::with_capacity(len);
                    let mut offsets: Vec<usize> = Vec::with_capacity(len);
                    let mut last_offset: usize = 0;

                    // Since we don't slice, we are safe to build the offsets this way.
                    for array in series.utf8().unwrap().downcast_iter() {
                        let utf8_array = array
                            .as_any()
                            .downcast_ref::<polars::export::arrow::array::Utf8Array<i64>>()
                            .unwrap();

                        let mut offset_loop: usize = 0;
                        for offset in utf8_array.offsets().iter().skip(1) {
                            offset_loop = *offset as usize;
                            offsets.push(last_offset + offset_loop);
                        }

                        values.extend_from_slice(utf8_array.values().as_slice());

                        last_offset += offset_loop;
                    }
                    values.shrink_to_fit();

                    // Can we make a binary straight from the slice?
                    // This **might** be faster, but I think that allocating it this way is okay
                    // as we know the length already?
                    let mut values_binary = NewBinary::new(env, values.len());
                    values_binary.copy_from_slice(&values);

                    let binary: Binary = values_binary.into();
                    let binary_carg = binary.to_term(env).as_c_arg();

                    // Now we slice into the binary.
                    let mut last_offset: usize = 0;
                    let mut sub_binaries: Vec<NIF_TERM> = Vec::with_capacity(offsets.len());
                    for offset in offsets {
                        if last_offset == offset {
                            last_offset = offset;
                            sub_binaries.push(nil);
                        } else {
                            let raw_term = rustler_sys::enif_make_sub_binary(
                                env_carg,
                                binary_carg,
                                last_offset,
                                offset - last_offset,
                            );

                            sub_binaries.push(raw_term);
                            last_offset = offset;
                        }
                    }

                    sub_binaries
                }
                _ => {
                    vec![nil; series.len()]
                }
            };
            Term::new(env, make_list(env.as_c_arg(), &tuples)).as_c_arg()
        })
        .collect();

    Ok(unsafe { Term::new(env, make_list(env.as_c_arg(), &columns)) })
}