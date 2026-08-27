use std::env;
use std::fs::File;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, DoubleType};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args_os()
        .nth(1)
        .ok_or("usage: cargo run --example write_fixture -- OUTPUT")?;
    let schema = Arc::new(parse_message_type(
        "message schema {
            REQUIRED DOUBLE total;
            OPTIONAL BYTE_ARRAY status (UTF8);
        }",
    )?);
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_dictionary_enabled(true)
            .set_created_by("sigil-plugin-parquet fixture".to_owned())
            .build(),
    );
    let mut writer = SerializedFileWriter::new(File::create(path)?, schema, properties)?;
    let mut row_group = writer.next_row_group()?;

    let mut column = row_group.next_column()?.ok_or("missing total column")?;
    column
        .typed::<DoubleType>()
        .write_batch(&[12.5, 27.75, 41.0], None, None)?;
    column.close()?;

    let mut column = row_group.next_column()?.ok_or("missing status column")?;
    column.typed::<ByteArrayType>().write_batch(
        &[ByteArray::from("pending"), ByteArray::from("paid")],
        Some(&[1, 0, 1]),
        None,
    )?;
    column.close()?;

    if row_group.next_column()?.is_some() {
        return Err("unexpected fixture column".into());
    }
    row_group.close()?;
    writer.close()?;
    Ok(())
}
