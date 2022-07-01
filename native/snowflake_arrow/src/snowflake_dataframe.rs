use crate::atoms::{
    calendar, day, hour, lock_fail, microsecond, minute, month, no_column, no_dataframe, ok,
    second, year,
};
use crate::polars_convert::snowflake_arrow_ipc_streaming_binary_to_dataframe;
use crate::rustler_helper::atoms::elixir_calendar_iso;
use crate::rustler_helper::{ElixirNaiveDateTime, make_subbinary};
use crate::{
    atoms, MutableSnowflakeArrowDataframeArc, MutableSnowflakeArrowDataframeResource,
    SnowflakeArrowDataframeArc, SnowflakeArrowDataframeResource,
};
// use chrono::{Datelike, NaiveDateTime, Timelike};
use polars::datatypes::{AnyValue, DataType};
use polars::export::arrow::temporal_conversions::{date32_to_date, timestamp_ms_to_datetime};
use polars::series::Series;
use rustler::types::atom;
use rustler::types::atom::nil;
use rustler::wrapper::list::make_list;
use rustler::wrapper::{map, tuple, NIF_TERM};
use rustler::{Atom, Binary, Encoder, Env, NewBinary, ResourceArc, Term};
use std::sync::Mutex;
use chrono::Timelike;
use eetf::Map;
// use erlang_term::Term as ErlangTerm;
use polars::export::chrono::{Datelike, NaiveDateTime};
use eetf::{Term as ETFTerm, Atom as ETFAtom};

#[rustler::nif(schedule = "DirtyIo")]
pub fn convert_snowflake_arrow_stream_to_df(
    arrow_stream_data: Binary,
) -> (Atom, MutableSnowflakeArrowDataframeArc) {
    let resource = ResourceArc::new(MutableSnowflakeArrowDataframeResource(Mutex::new(
        snowflake_arrow_ipc_streaming_binary_to_dataframe(&arrow_stream_data),
    )));

    (atoms::ok(), resource)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn convert_snowflake_arrow_stream_to_df_owned(
    arrow_stream_data: Binary,
) -> Result<SnowflakeArrowDataframeArc, Atom> {
    let df = snowflake_arrow_ipc_streaming_binary_to_dataframe(&arrow_stream_data);

    let resource = ResourceArc::new(SnowflakeArrowDataframeResource(df));

    Ok(resource)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn append_snowflake_arrow_stream_to_df(
    resource: ResourceArc<MutableSnowflakeArrowDataframeResource>,
    arrow_stream_data: Binary,
) -> Atom {
    let mut original_df = match resource.0.try_lock() {
        Err(_) => return lock_fail(),
        Ok(guard) => guard,
    };

    let new_df = snowflake_arrow_ipc_streaming_binary_to_dataframe(&arrow_stream_data);

    match original_df.vstack_mut(&new_df) {
        Ok(_x) => ok(),
        Err(_x) => no_dataframe(),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn get_column_names(
    env: Env,
    resource: ResourceArc<SnowflakeArrowDataframeResource>,
) -> Result<Term, Atom> {
    let df = &resource.0;
    Ok(df.get_column_names().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn to_owned(
    resource: ResourceArc<MutableSnowflakeArrowDataframeResource>,
) -> Result<SnowflakeArrowDataframeArc, Atom> {
    let mut df = match resource.0.try_lock() {
        Err(_) => return Err(nil()),
        Ok(guard) => guard,
    };

    // We should rechunk here, as we've added data to it. We need to make sure that the data is in the right order.
    df.rechunk();

    // As the dataframe does not have copy, we have to use a clone. Is this bad?
    // @todo can we now drop the mutable one, and just return the immutable one? I think it'll be GCd?
    let immutable_df = df.clone();

    let resource = ResourceArc::new(SnowflakeArrowDataframeResource(immutable_df));

    Ok(resource)
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn get_column(
    env: Env,
    resource: ResourceArc<SnowflakeArrowDataframeResource>,
    column_name: String,
) -> Result<Term, Atom> {
    let df = &resource.0;

    // check that the column exists in the dataframe. if it does not, throw an error
    match df.find_idx_by_name(&column_name) {
        Some(_col) => {
            let series = df.column(&column_name).unwrap();
            let data = get_column_as_c_arg(env, series);
            Ok(unsafe { Term::new(env, tuple::make_tuple(env.as_c_arg(), &data)) })
        }
        None => Err(no_column()),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn convert_snowflake_arrow_stream<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
) -> Result<Term<'a>, Atom> {
    let df = snowflake_arrow_ipc_streaming_binary_to_dataframe(&arrow_stream_data);

    // loop over columns to return as a list of tuples.
    let columns: Vec<NIF_TERM> = df
        .iter()
        .map(|series| unsafe {
            tuple::make_tuple(env.as_c_arg(), &get_column_as_c_arg(env, series))
        })
        .collect();

    // build into into a list
    Ok(unsafe { Term::new(env, make_list(env.as_c_arg(), &columns)) })
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn arrow_to_binary_term_format<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
) -> Result<Term<'a>, Atom> {
    let df = snowflake_arrow_ipc_streaming_binary_to_dataframe(&arrow_stream_data);
    // println!("{:?}", df.get_column_names());

    // get the series for the df we care about
    let series = df.column("ORDER_PLACED_AT").unwrap();

    // Here we build the NaiveDateTime struct manually, as it's about 4x faster than using NifStruct.
    // It's faster because we already have the keys (we know this at compile time), and the type.
    // let struct_keys = [
    //     atom::__struct__().encode(env).as_c_arg(),
    //     calendar().encode(env).as_c_arg(),
    //     microsecond().encode(env).as_c_arg(),
    //     day().encode(env).as_c_arg(),
    //     month().encode(env).as_c_arg(),
    //     year().encode(env).as_c_arg(),
    //     hour().encode(env).as_c_arg(),
    //     minute().encode(env).as_c_arg(),
    //     second().encode(env).as_c_arg(),
    // ];

    let d = ETFTerm::from(ETFAtom::from("microsecond"));

    // This sets the value in the map to "Elixir.Calendar.ISO", which must be an atom.
    // let calendar_iso_c_arg = elixir_calendar_iso().encode(env).as_c_arg();
    //
    // // This is used for the map to know that it's a struct. Define it here so it's not redefined in the loop.
    // let module_atom = Atom::from_str(env, "Elixir.NaiveDateTime")
    //     .unwrap()
    //     .encode(env)
    //     .as_c_arg();

    let v = series
        .datetime()
        .unwrap()
        .0
        .into_iter()
        .map(|x| {
            x.map(|dt_value| {
                let x = timestamp_ms_to_datetime(dt_value);
                Map::from(vec![(d, Term)])
            })
        })
        .collect::<Vec<Option<ElixirNaiveDateTime>>>();

    // let bytes = to_bytes(&v).unwrap();

    // convert to json
    // let foo = serde_json::value::to_value(df).unwrap();

    // let json = serde_json::to_value(&df).unwrap();
    // let new_term: ErlangTerm = serde_json::from_value(json).unwrap();
    // let output = new_term.to_bytes();



    let mut values_binary = NewBinary::new(env, bytes.len());
    values_binary.copy_from_slice(bytes.as_slice());

    let binary: Binary = values_binary.into();


    return Ok(binary.to_term(env));

    // return output;

    // return json;


    // loop over columns, gathering them as their parts.
    // let columns: Vec<NIF_TERM> = df
    //     .iter()
    //     .map(|series| unsafe {
    //         tuple::make_tuple(env.as_c_arg(), &get_column_as_c_arg(env, series))
    //     })
    //     .collect();

    // build into into a list
    // Ok(unsafe { Term::new(env, make_list(env.as_c_arg(), &columns)) })
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn get_column_at(
    env: Env,
    resource: ResourceArc<SnowflakeArrowDataframeResource>,
    column_name: String,
    row_index: usize,
) -> Result<Term, Atom> {
    let df = &resource.0;

    // check that the column exists in the dataframe. if it does not, throw an error
    match df.find_idx_by_name(&column_name) {
        Some(_col) => {
            // let data = get_column_as_c_arg(env, column_name, df);
            let value = df
                .column(&column_name)
                .unwrap()
                .iter()
                .nth(row_index)
                .unwrap();
            let encoded = match value {
                AnyValue::Null => nil().encode(env),
                AnyValue::Datetime(_a, _b, _c) => {
                    32.encode(env)
                    // <NaiveDateTime as Into<ElixirNaiveDateTime>>::into(timestamp_ms_to_datetime(a)).encode(env)
                }
                AnyValue::Boolean(x) => x.encode(env),
                AnyValue::Utf8(x) => x.encode(env),
                AnyValue::UInt8(x) => x.encode(env),
                AnyValue::UInt16(x) => x.encode(env),
                AnyValue::UInt32(x) => x.encode(env),
                AnyValue::UInt64(x) => x.encode(env),
                AnyValue::Int8(x) => x.encode(env),
                AnyValue::Int16(x) => x.encode(env),
                AnyValue::Int32(x) => x.encode(env),
                AnyValue::Int64(x) => x.encode(env),
                AnyValue::Float32(x) => x.encode(env),
                AnyValue::Float64(x) => x.encode(env),
                AnyValue::Date(x) => x.encode(env),
                AnyValue::Time(x) => x.encode(env),
                AnyValue::Utf8Owned(x) => x.encode(env),
                AnyValue::List(x) => get_column_as_c_arg(env, &x).encode(env),
                _ => nil().encode(env),
            };
            Ok(encoded)
        }
        None => Err(no_column()),
    }
}

fn get_column_as_c_arg(env: Env, series: &Series) -> Vec<NIF_TERM> {
    return match series.0.dtype() {
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
        DataType::Date => {
            // Here we build the Date struct manually, as it's much faster than using Date NifStruct
            // This is because we already have the keys (we know this at compile time), and the types,
            // so we can build the struct directly.
            let struct_keys = [
                atom::__struct__().encode(env).as_c_arg(),
                calendar().encode(env).as_c_arg(),
                day().encode(env).as_c_arg(),
                month().encode(env).as_c_arg(),
                year().encode(env).as_c_arg(),
            ];

            // This sets the value in the map to "Elixir.Calendar.ISO", which must be an atom.
            let calendar_iso_c_arg = elixir_calendar_iso().encode(env).as_c_arg();

            // This is used for the map to know that it's a struct. Define it here so it's not redefined in the loop.
            let module_atom = Atom::from_str(env, "Elixir.Date")
                .unwrap()
                .encode(env)
                .as_c_arg();

            series
                .date()
                .unwrap()
                .0
                .into_iter()
                .map(|x| {
                    let bar = x.map(|date| {
                        let dt = date32_to_date(date);
                        let values = [
                            module_atom,
                            calendar_iso_c_arg,
                            dt.day().encode(env).as_c_arg(),
                            dt.month().encode(env).as_c_arg(),
                            dt.year().encode(env).as_c_arg(),
                        ];
                        unsafe {
                            Term::new(
                                env,
                                map::make_map_from_arrays(env.as_c_arg(), &struct_keys, &values)
                                    .unwrap(),
                            )
                        }
                    });
                    bar.encode(env).as_c_arg()
                })
                .collect::<Vec<NIF_TERM>>()
        }
        DataType::Datetime(_tu, _tz) => {
            // Here we build the NaiveDateTime struct manually, as it's about 4x faster than using NifStruct.
            // It's faster because we already have the keys (we know this at compile time), and the type.
            let struct_keys = [
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

            // This sets the value in the map to "Elixir.Calendar.ISO", which must be an atom.
            let calendar_iso_c_arg = elixir_calendar_iso().encode(env).as_c_arg();

            // This is used for the map to know that it's a struct. Define it here so it's not redefined in the loop.
            let module_atom = Atom::from_str(env, "Elixir.NaiveDateTime")
                .unwrap()
                .encode(env)
                .as_c_arg();

            series
                .datetime()
                .unwrap()
                .0
                .into_iter()
                .map(|x| {
                    x.map(|dt_value| {
                        let dt = timestamp_ms_to_datetime(dt_value);
                        let values = &[
                            module_atom,
                            calendar_iso_c_arg,
                            (dt.timestamp_subsec_micros(), 6).encode(env).as_c_arg(),
                            dt.day().encode(env).as_c_arg(),
                            dt.month().encode(env).as_c_arg(),
                            dt.year().encode(env).as_c_arg(),
                            dt.hour().encode(env).as_c_arg(),
                            dt.minute().encode(env).as_c_arg(),
                            dt.second().encode(env).as_c_arg(),
                        ];
                        unsafe {
                            Term::new(
                                env,
                                map::make_map_from_arrays(env.as_c_arg(), &struct_keys, values)
                                    .unwrap(),
                            )
                        }
                    })
                    .encode(env)
                    .as_c_arg()
                })
                .collect::<Vec<NIF_TERM>>()
        }
        DataType::Utf8 => {
            // This is goofy and stupid, but drastically speeds up converting binaries to strings.
            // What we do here is take all the values/offsets, create a binary, then subslice into it.
            let mut values = Vec::new();
            let mut last_offset: usize = 0;
            let mut offsets: Vec<usize> = Vec::new();

            for array in series.utf8().unwrap().downcast_iter() {
                let mut offset_loop: usize = 0;
                for offset in array.offsets().iter().skip(1) {
                    offset_loop = *offset as usize;
                    offsets.push(last_offset + offset_loop);
                }

                last_offset += offset_loop;
                values.extend(array.values().as_slice());
            }

            let mut values_binary = NewBinary::new(env, values.len());
            values_binary.copy_from_slice(values.as_slice());

            let binary: Binary = values_binary.into();

            let mut last_offset: usize = 0;
            let mut binaries: Vec<NIF_TERM> = Vec::with_capacity(offsets.len());
            for offset in offsets {
                if last_offset == offset {
                    last_offset = offset;
                    binaries.push(nil().encode(env).as_c_arg());
                } else {
                    let slice = make_subbinary(env, &binary, last_offset, offset - last_offset)
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
        _ => vec![nil().encode(env).as_c_arg(); series.len()],
    };
}
