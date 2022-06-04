# SnowflakeArrow

Snowflake specific arrow implementation, which only implements the [Data types Snowflake supports](https://docs.snowflake.com/en/sql-reference/data-types.html).

This has been designed to work with [req_snowflake](https://github.com/joshuataylor/req_snowflake), which when added to a project with `req_snowflake` will
allow Snowflake to send back Arrow results which this library will parse.

This library uses [Rustler](https://github.com/rusterlium/rustler) to create a Rust binding to [Arrow2](https://github.com/jorgecarleitao/arrow2) library.

Arrow2 has been chosen as the performance is incredible (from my basic rust knowledge I could get some very good results), and is being used more and more.

Nothing against arrow-rs, I just went with Arrow2.

## Installation

```elixir
def deps do
  [
    {:snowflake_arrow, github: "joshuataylor/snowflake_arrow"}
  ]
end
```

## Features

- Optional setting to cast to Elixir types from within Rust, which will allow you to not have to cast them yourself (if not set, will cast to strings, integers, floats)
- 

## Benchmarks

Benchmarks are hard to compare against the JSON implementation of Snowflake, as we also have to cast and do other things to get the same results.

I have benchmarks against the files Snowflake sends back (they send back files ranging from a few kb to 20mb), and the parsing results are pretty incredible.
Once we get the library cleaned up, public benchmarks will be made available, comparing the full deserialization of the JSON using Elixir to the Rust implementation with
casting to Elixir Types.

## Help please

My Rust knowledge is basic, so I just went with what felt right. I am not a Rustacean (yet! 🦀), so please feel free to suggest improvements.

Please see the pull request for my comments around the quick Rust implementation, I wrote this in a day after studying and playing with Arrow & Arrow2.

It's also single threaded, as I'm unsure how much improvement could be made with multi-threading.