// As we need to return a lot of data back to Elixir, we need to do some tricks to make this not use all the resources.
// Returning it as a vec uses all memory, as a lot of binaries are created. To work around this, we create sub binaries
// and other tricks to return data as effectively as possible to Elixir.
// @todo move this into a separate crate

use crate::rustler_helper::atoms::elixir_calendar_iso;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use rustler::Atom;
use rustler::NifStruct;
use rustler::{Binary, Env, Error, NifResult, Term};
use serde::{Serialize, Deserialize};

pub mod atoms {
    rustler::atoms! {
      elixir_calendar_iso = "Elixir.Calendar.ISO"
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ElixirDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    // pub calendar: Atom,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ElixirNaiveDateTime {
    // pub calendar: Atom,
    pub day: u32,
    pub month: u32,
    pub year: i32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: (u32, u32),
}

impl From<NaiveDate> for ElixirDate {
    fn from(d: NaiveDate) -> Self {
        ElixirDate {
            year: d.year(),
            month: d.month(),
            day: d.day(),
            // calendar: elixir_calendar_iso(),
        }
    }
}

impl From<NaiveDateTime> for ElixirNaiveDateTime {
    fn from(dt: NaiveDateTime) -> Self {
        ElixirNaiveDateTime {
            // calendar: elixir_calendar_iso(),
            day: dt.day(),
            month: dt.month(),
            year: dt.year(),
            hour: dt.hour(),
            minute: dt.minute(),
            second: dt.second(),
            microsecond: (dt.timestamp_subsec_micros(), 6),
        }
    }
}

#[inline(always)]
pub fn make_subbinary<'a>(
    env: Env<'a>,
    binary: &Binary,
    offset: usize,
    length: usize,
) -> NifResult<Term<'a>> {
    let min_len = length.checked_add(offset);
    if min_len.ok_or(Error::BadArg)? > binary.len() {
        return Err(Error::BadArg);
    }
    let term = binary.to_term(env);

    let raw_term = unsafe {
        rustler_sys::enif_make_sub_binary(
            term.get_env().as_c_arg(),
            term.as_c_arg(),
            offset,
            length,
        )
    };

    let bar = unsafe { Term::new(env, raw_term) };

    Ok(bar)
}
