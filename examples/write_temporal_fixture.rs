use std::env;
use std::fs::File;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args_os()
        .nth(1)
        .ok_or("usage: cargo run --example write_temporal_fixture -- OUTPUT")?;
    let schema = Arc::new(parse_message_type(
        "message schema {
            REQUIRED INT64 utc_timestamp (TIMESTAMP(MILLIS,true));
            REQUIRED INT64 local_timestamp (TIMESTAMP(MILLIS,false));
            REQUIRED INT32 utc_time (TIME(MILLIS,true));
            REQUIRED INT32 local_time (TIME(MILLIS,false));
        }",
    )?);
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(false)
            .set_created_by("sigil-plugin-parquet temporal fixture".to_owned())
            .build(),
    );
    let mut writer = SerializedFileWriter::new(File::create(path)?, schema, properties)?;
    let mut row_group = writer.next_row_group()?;

    for values in [
        [1_788_484_682_000_i64, 1_788_484_682_001],
        [1_788_484_682_000_i64, 1_788_484_682_001],
    ] {
        let mut column = row_group.next_column()?.ok_or("missing timestamp column")?;
        column
            .typed::<Int64Type>()
            .write_batch(&values, None, None)?;
        column.close()?;
    }

    for values in [[1_234_i32, 1_235], [1_234_i32, 1_235]] {
        let mut column = row_group.next_column()?.ok_or("missing time column")?;
        column
            .typed::<Int32Type>()
            .write_batch(&values, None, None)?;
        column.close()?;
    }

    if row_group.next_column()?.is_some() {
        return Err("unexpected temporal fixture column".into());
    }
    row_group.close()?;
    writer.close()?;
    Ok(())
}
