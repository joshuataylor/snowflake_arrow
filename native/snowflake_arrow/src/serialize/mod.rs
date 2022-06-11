mod iterator;
mod rust_serialize;

use arrow2::array::Array;
use arrow2::chunk::Chunk;

use crate::serialize::rust_serialize::new_serializer;
use crate::{ColumnMetadata, SnowflakeReturnType};
use arrow2::error::Result;
use streaming_iterator::StreamingIterator;

/// Creates serializers that iterate over each column that serializes each item according
/// to `options`.
pub fn new_serializers<'a, A: AsRef<dyn Array>>(
    column_metadata: &ColumnMetadata,
    columns: &'a [A],
    cast_elixir_types: bool,
) -> Result<Vec<Box<dyn StreamingIterator<Item = Vec<SnowflakeReturnType>> + 'a>>> {
    columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            let column_metadata = column_metadata.get(&column_index).unwrap();
            new_serializer(column_metadata, column.as_ref(), cast_elixir_types)
        })
        .collect()
}

/// Serializes [`Chunk`] to a vector of rows.
/// The vector is guaranteed to have `columns.len()` entries.
/// Each `row` is guaranteed to have `columns.array().len()` fields.
pub fn serialize_to_elixir<A: AsRef<dyn Array>>(
    field_metadata: &ColumnMetadata,
    columns: &Chunk<A>,
    cast_elixir_types: bool,
) -> Result<Vec<Vec<SnowflakeReturnType>>> {
    let mut serializers = new_serializers(field_metadata, columns, cast_elixir_types)?;

    let mut rows = Vec::with_capacity(columns.len());
    let mut row = Vec::with_capacity(columns.arrays().len());

    // this is where the (expensive) transposition happens: the outer loop is on rows, the inner on columns
    (0..columns.len()).try_for_each(|_| {
        serializers.iter_mut().for_each(|iter| {
            let field = iter.next().unwrap();
            row.extend_from_slice(field);
        });
        rows.push(row.clone());
        row.clear();
        Result::Ok(())
    })?;

    Ok(rows)
}
