extern crate core;

pub mod helpers;
mod rows;

#[cfg(not(any(
all(windows, target_env = "gnu"),
all(target_os = "linux", target_env = "musl")
)))]
use mimalloc::MiMalloc;
use rustler::{Env, Term};

#[cfg(not(any(
all(windows, target_env = "gnu"),
all(target_os = "linux", target_env = "musl")
)))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

rustler::init!(
    "Elixir.SnowflakeArrow.Native",
    [rows::convert_arrow_stream_to_columns]
);
