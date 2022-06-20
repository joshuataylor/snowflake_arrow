# SnowflakeArrow

Snowflake specific Arrow implementation, which only implements the [Data types Snowflake supports](https://docs.snowflake.com/en/sql-reference/data-types.html).

This has been designed to work with [req_snowflake](https://github.com/joshuataylor/req_snowflake), which when added to a project with `req_snowflake` will
allow Snowflake to send back Arrow results which this library will parse.

This library uses [Rustler](https://github.com/rusterlium/rustler) to create a Rust binding to [Arrow2](https://github.com/jorgecarleitao/arrow2) library.

Arrow2 has been chosen as it also the underlying library for [Polars](https://github.com/pola-rs/polars), which is what [Explorer](https://github.com/elixir-nx/explorer) uses.

## Installation

```elixir
def deps do
  [
    {:snowflake_arrow, github: "joshuataylor/snowflake_arrow"}
  ]
end
```

## Features

- Cast to to [DateTime](https://hexdocs.pm/elixir/1.12.3/DateTime.html) & [Date](https://hexdocs.pm/elixir/1.13/Date.html) from within Rust, which will allow you to not have to cast them yourself (if not set, will cast to strings with the date/datetime value)

## Snowflake Notes
- Snowflake seems to have been an early adopter for Arrow, and some of their design decisions around timestamps are now Arrow types. We can't directly parse a Snowflake Arrow Streaming file with something like Explorer, as timestamps etc won't work.

## Benchmarks

Benchmarks are hard to compare against the JSON implementation of Snowflake, as the JSON implementation doesn't return the same results as the Arrow implementation.

Using Rustler, sending back timestamps as unix timestamps is quicker than sending back timestamps as Datetime structs.