use crate::{
    atoms, DataFrame, MutableSnowflakeArrowDataframeArc, MutableSnowflakeArrowDataframeResource,
    SnowflakeArrowDataframeArc, SnowflakeArrowDataframeResource,
};
use polars::datatypes::DataType;
use polars::export::arrow::array::Array;

use rustler::types::atom::nil;

use crate::atoms::{lock_fail, no_column, no_dataframe, ok};
use crate::rustler_helper::{make_subbinary, ElixirDate, ElixirNaiveDateTime};
use chrono::{NaiveDate, NaiveDateTime};
use polars::export::arrow::temporal_conversions::{date32_to_date, timestamp_ms_to_datetime};
use rustler::wrapper::list::make_list;
use rustler::wrapper::NIF_TERM;
use rustler::{Atom, Binary, Encoder, Env, NewBinary, ResourceArc, Term};
use std::sync::Mutex;
// use crate::rustler_helper::SnowflakeArrowBinary;
use crate::polars_convert::snowflake_arrow_ipc_streaming_binary_to_dataframe;

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
            let data = get_column_as_c_arg(env, column_name, df);

            // Ok(unsafe { Term::new(env, data) })
            Ok(unsafe { Term::new(env, make_list(env.as_c_arg(), &data)) })
        }
        None => Err(no_column()),
    }
}

fn get_column_as_c_arg(env: Env, column_name: String, df: &DataFrame) -> Vec<NIF_TERM> {
    let series = df.column(&column_name).unwrap();
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
        DataType::Date => series
            .date()
            .unwrap()
            .0
            .into_iter()
            .map(|x| {
                x.map(|date| <NaiveDate as Into<ElixirDate>>::into(date32_to_date(date)))
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
                x.map(|date| {
                    <NaiveDateTime as Into<ElixirNaiveDateTime>>::into(timestamp_ms_to_datetime(
                        date,
                    ))
                })
                .encode(env)
                .as_c_arg()
            })
            .collect::<Vec<NIF_TERM>>(),
        DataType::Utf8 => {
            // This is goofy and stupid, but drastically speeds up converting binaries to strings.
            // This function should be optimised later.

            let mut values: Vec<u8> = Vec::new();
            let mut last_offset: usize = 0;
            let mut offsets: Vec<usize> = Vec::new();

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

                values.extend(utf8_array.values().as_slice());

                last_offset += offset_loop;
            }

            let mut values_binary = NewBinary::new(env, values.len());
            values_binary
                .as_mut_slice()
                .copy_from_slice(values.as_slice());

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
        dt => vec![format!("{:?}", dt).encode(env).as_c_arg()],
    };
}
