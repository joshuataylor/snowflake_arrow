// Once we get the base library right, I'll re-add casting to dates.

// use serde::{Deserialize, Serialize};

// For some reason, I can't convert to a pure Elixir module, so we just created a test struct to test the conversion
// #[derive(Debug, Serialize, Deserialize, Copy, Clone)]
// #[serde(rename = "Elixir.TestDate")]
// pub struct ElixirDate {
//     pub(crate) year: i32,
//     pub(crate) month: u32,
//     pub(crate) day: u32,
// }