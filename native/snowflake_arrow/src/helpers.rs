use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Metadata;
use arrow2::io::ipc::read;
use arrow2::io::ipc::read::StreamMetadata;
use std::collections::HashMap;
use std::io::Read;

pub fn get_chunks_for_data<R: Read>(
    arrow_data: &mut R,
) -> (
    Vec<Chunk<Box<dyn Array>>>,
    HashMap<usize, Metadata>,
    StreamMetadata,
    usize,
) {
    let stream_metadata = read::read_stream_metadata(arrow_data).unwrap();

    let mut column_metadata: HashMap<usize, Metadata> = HashMap::new();

    // We need the field metadata for the timestamp info later.
    for (i, field) in stream_metadata.schema.fields.iter().enumerate() {
        column_metadata.insert(i, field.metadata.clone());
    }

    let stream = read::StreamReader::new(arrow_data, stream_metadata.clone());
    let mut chunks = vec![];
    let mut total_rows = 0;

    for stream_state in stream {
        match stream_state {
            Ok(read::StreamState::Some(chunk)) => {
                total_rows += chunk.len();
                chunks.push(chunk);
            }
            Ok(read::StreamState::Waiting) => break,
            Err(_l) => break,
        }
    }
    (chunks, column_metadata, stream_metadata, total_rows)
}
