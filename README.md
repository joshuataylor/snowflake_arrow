# SnowflakeArrow

Snowflake specific Arrow implementation, which only implements the [Data types Snowflake supports](https://docs.snowflake.com/en/sql-reference/data-types.html).

This has been designed to work with [req_snowflake](https://github.com/joshuataylor/req_snowflake), which when added to a project with `req_snowflake` will
allow Snowflake to send back Arrow results which this library will parse.

This library uses [Rustler](https://github.com/rusterlium/rustler) to create a Rust binding to [Arrow2](https://github.com/jorgecarleitao/arrow2) library.

Performance has been given a priority, so some workarounds have had to be made to get good performance, as we are returning datasets.

Data is returned from [Polars](https://github.com/pola-rs/polars), which is what [Explorer](https://github.com/elixir-nx/explorer) uses.

## Installation

```elixir
def deps do
  [
    {:snowflake_arrow, github: "joshuataylor/snowflake_arrow"}
  ]
end
```

## Features

- Cast to [DateTime](https://hexdocs.pm/elixir/1.12.3/DateTime.html) & [Date](https://hexdocs.pm/elixir/1.13/Date.html) from within Rust.
- Custom Snowflake types (`TIMESTAMP_TZ`, `TIMESTAMP_NTZ`, `TIMESTAMP_LTZ`) supported.
- Optimised for speed

## Snowflake Notes
Snowflake send back Timestamps in Structs which include the timezone. This is casted to their proper types during import,
which Polars makes incredibly easy.

## Benchmarks

Benchmarks are hard to compare against the JSON implementation of Snowflake, as the JSON implementation doesn't return the same results as the Arrow implementation.