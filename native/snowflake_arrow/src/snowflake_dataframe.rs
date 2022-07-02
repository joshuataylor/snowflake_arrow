use crate::error::SnowflakeArrowError;
use crate::polars_convert::snowflake_arrow_ipc_streaming_binary_to_dataframe;
use crate::rustler_helper::atoms::{
    calendar, day, elixir_calendar_iso, hour, microsecond, minute, month, second, year,
};
use crate::rustler_helper::make_subbinary;
use chrono::{Datelike, Timelike};
use polars::datatypes::DataType;
use polars::export::arrow::temporal_conversions::{date32_to_date, timestamp_ms_to_datetime};
use polars::prelude::ChunkLen;
use rustler::types::atom;
use rustler::types::atom::nil;
use rustler::wrapper::list::make_list;
use rustler::wrapper::{map, tuple, NIF_TERM};
use rustler::{Atom, Binary, Encoder, Env, NewBinary, Term};

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

    // This sets the value in the map to "Elixir.Calendar.ISO", which must be an atom.
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

    // loop over columns to return as a list of tuples.
    // We can't use par_iter here as we need to env to encode the tuples to c_arg.
    // @todo can we somehow make this parallel later by using a generic type that holds the iter?
    let columns: Vec<NIF_TERM> = df
        .iter()
        .map(|series| unsafe {
            // let tuples = get_column_as_c_arg(env, &series);
            let tuples = match series.0.dtype() {
                DataType::Int32 => series
                    .i32()
                    .expect("not i32")
                    .into_iter()
                    .map(|x| x.map(|d| d as i32).encode(env).as_c_arg())
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Boolean => series
                    .bool()
                    .expect("not bool")
                    .into_iter()
                    .map(|x| x.map(|d| d as bool).encode(env).as_c_arg())
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Int8 => series
                    .i8()
                    .expect("not bool")
                    .into_iter()
                    .map(|x| x.map(|d| d as i8).encode(env).as_c_arg())
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Int16 => series
                    .i16()
                    .expect("not bool")
                    .into_iter()
                    .map(|x| x.map(|d| d as i16).encode(env).as_c_arg())
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Int64 => series
                    .i64()
                    .expect("not bool")
                    .into_iter()
                    .map(|x| x.map(|d| d as i64).encode(env).as_c_arg())
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Float64 => series
                    .f64()
                    .expect("not bool")
                    .into_iter()
                    .map(|x| x.map(|d| d as f64).encode(env).as_c_arg())
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Date => series
                    .date()
                    .unwrap()
                    .0
                    .into_iter()
                    .map(|x| {
                        x.map(|date| {
                            let dt = date32_to_date(date);
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
                        })
                        .encode(env)
                        .as_c_arg()
                    })
                    .collect::<Vec<NIF_TERM>>(),
                DataType::Datetime(_tu, _tz) => series
                    .datetime()
                    .unwrap()
                    .0
                    .into_iter()
                    .map(|x| {
                        x.map(|dt_value| {
                            let dt = timestamp_ms_to_datetime(dt_value);
                            let values = &[
                                datetime_module_atom,
                                calendar_iso_c_arg,
                                (dt.timestamp_subsec_micros(), 6).encode(env).as_c_arg(),
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
                        })
                        .encode(env)
                        .as_c_arg()
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

                    for array in utf8.downcast_iter() {
                        let mut offset_loop: usize = 0;
                        for offset in array.offsets().iter().skip(1) {
                            offset_loop = *offset as usize;
                            offsets.push(last_offset + offset_loop);
                        }

                        last_offset += offset_loop;
                        values.extend(array.values().as_slice());
                    }

                    // Can we make a binary straight from the slice?
                    // This **might** be faster, but I think that allocating it this way is okay
                    // as we know the length already?
                    let mut values_binary = NewBinary::new(env, values.len());
                    values_binary.copy_from_slice(values.as_slice());

                    let binary: Binary = values_binary.into();

                    // Now we slice into the binary.
                    let mut last_offset: usize = 0;
                    let mut binaries: Vec<NIF_TERM> = Vec::with_capacity(offsets.len());
                    for offset in offsets {
                        if last_offset == offset {
                            last_offset = offset;
                            binaries.push(nil);
                        } else {
                            let slice =
                                make_subbinary(env, &binary, last_offset, offset - last_offset)
                                    .unwrap()
                                    .as_c_arg();
                            binaries.push(slice);
                            last_offset = offset;
                        }
                    }

                    binaries
                }
                DataType::UInt8 => series
                    .u8()
                    .expect("not u8")
                    .into_iter()
                    .map(|x| x.map(|d| d as u8).encode(env).as_c_arg())
                    .collect::<Vec<NIF_TERM>>(),
                _ => vec![nil; series.len()], // @todo fix this
            };

            Term::new(env, tuple::make_tuple(env.as_c_arg(), &tuples)).as_c_arg()
        })
        .collect();

    Ok(unsafe { Term::new(env, make_list(env.as_c_arg(), &columns)) })
}
