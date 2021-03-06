extern crate core;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use polars::prelude::DataFrame;
use rustler::{Env, ResourceArc, Term};
use std::sync::Mutex;

pub mod error;
pub mod polars_convert;
mod rustler_helper;
mod snowflake_dataframe;
pub struct MutableSnowflakeArrowDataframeResource(pub Mutex<DataFrame>);
pub type MutableSnowflakeArrowDataframeArc = ResourceArc<MutableSnowflakeArrowDataframeResource>;
pub struct SnowflakeArrowDataframeResource(pub DataFrame);
pub type SnowflakeArrowDataframeArc = ResourceArc<SnowflakeArrowDataframeResource>;

fn load(env: Env, _info: Term) -> bool {
    rustler::resource!(MutableSnowflakeArrowDataframeResource, env);
    rustler::resource!(SnowflakeArrowDataframeResource, env);
    true
}

mod atoms {
    rustler::atoms! {
        // Common Atoms
        ok,
        error,

        // Resource Atoms
        bad_reference,
        lock_fail,

        no_dataframe,
        no_column,
    }
}

rustler::init!(
    "Elixir.SnowflakeArrow.Native",
    [snowflake_dataframe::convert_snowflake_arrow_stream],
    load = load
);
