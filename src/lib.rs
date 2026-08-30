#![deny(unsafe_code)]

#[allow(unsafe_code, clippy::all, clippy::nursery, clippy::pedantic)]
mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "parquet",
        generate_all,
    });
}

mod decode;
mod dictionary;
mod value;

use std::io::Cursor;

use bindings::exports::sigil::parquet::reader::{
    Cell, ColumnInfo, ColumnReadOptions, Error, ErrorClass, FileInfo, Guest, ReadOptions,
};
use parquet2::FallibleStreamingIterator;
use parquet2::metadata::{ColumnDescriptor, FileMetaData, RowGroupMetaData};
use parquet2::page::{DataPage, DictPage, Page};
use parquet2::read::{BasicDecompressor, get_page_iterator, read_metadata};
use parquet2::schema::types::{
    PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};

const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_ROWS: usize = 1_000_000;
const MAX_ROW_GROUPS: usize = 4_096;
const MAX_COLUMNS: usize = 1_024;
const MAX_COLUMN_PATH_BYTES: usize = 1_024;
const MAX_PAGE_BYTES: usize = 1024 * 1024;
const MAX_COLUMN_CHUNK_BYTES: usize = 32 * 1024 * 1024;
const MAX_COLUMN_RESULT_CELLS: usize = 100_000;
const MAX_COLUMN_RESULT_BYTES: usize = 16 * 1024 * 1024;

struct Parquet;

struct Parsed {
    input: Vec<u8>,
    metadata: FileMetaData,
}

fn error(class: ErrorClass, message: &'static str) -> Error {
    Error {
        class,
        message: message.to_owned(),
    }
}

fn invalid_format() -> Error {
    error(
        ErrorClass::InvalidFormat,
        "input is not a valid Parquet file",
    )
}

#[inline(never)]
fn parse(input: Vec<u8>) -> Result<Parsed, Error> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(error(
            ErrorClass::Limit,
            "Parquet input exceeds the 16 MiB limit",
        ));
    }
    if input.len() < 12 || !input.starts_with(b"PAR1") || !input.ends_with(b"PAR1") {
        return Err(invalid_format());
    }
    let mut cursor = Cursor::new(input.as_slice());
    let metadata = read_metadata(&mut cursor).map_err(|_error| invalid_format())?;
    validate_metadata(&metadata)?;
    Ok(Parsed { input, metadata })
}

#[inline(never)]
fn validate_metadata(metadata: &FileMetaData) -> Result<(), Error> {
    if metadata.num_rows > MAX_ROWS {
        return Err(error(
            ErrorClass::Limit,
            "Parquet file exceeds the one million row limit",
        ));
    }
    if metadata.row_groups.len() > MAX_ROW_GROUPS {
        return Err(error(
            ErrorClass::Limit,
            "Parquet file exceeds the row-group limit",
        ));
    }
    let declared_rows = metadata
        .row_groups
        .iter()
        .try_fold(0usize, |rows, group| rows.checked_add(group.num_rows()))
        .ok_or_else(|| {
            error(
                ErrorClass::Limit,
                "Parquet row-group row count exceeds the supported limit",
            )
        })?;
    if declared_rows != metadata.num_rows {
        return Err(invalid_format());
    }
    let columns = metadata.schema().columns();
    if columns.len() > MAX_COLUMNS {
        return Err(error(
            ErrorClass::Limit,
            "Parquet file exceeds the column limit",
        ));
    }
    for column in columns {
        if column.path_in_schema.join(".").len() > MAX_COLUMN_PATH_BYTES {
            return Err(error(
                ErrorClass::Limit,
                "Parquet column path exceeds the metadata limit",
            ));
        }
    }
    Ok(())
}

#[inline(never)]
fn physical_type_name(value: PhysicalType) -> &'static str {
    match value {
        PhysicalType::Boolean => "boolean",
        PhysicalType::Int32 => "int32",
        PhysicalType::Int64 => "int64",
        PhysicalType::Int96 => "int96",
        PhysicalType::Float => "float",
        PhysicalType::Double => "double",
        PhysicalType::ByteArray => "byte-array",
        PhysicalType::FixedLenByteArray(_) => "fixed-len-byte-array",
    }
}

#[inline(never)]
fn logical_type_name(value: &PrimitiveLogicalType) -> String {
    match value {
        PrimitiveLogicalType::String => "string".to_owned(),
        PrimitiveLogicalType::Enum => "enum".to_owned(),
        PrimitiveLogicalType::Decimal(precision, scale) => {
            format!("decimal(precision={precision},scale={scale})")
        }
        PrimitiveLogicalType::Date => "date".to_owned(),
        PrimitiveLogicalType::Time { unit, .. } => format!("time({unit:?})").to_lowercase(),
        PrimitiveLogicalType::Timestamp { unit, .. } => {
            format!("timestamp({unit:?})").to_lowercase()
        }
        PrimitiveLogicalType::Integer(value) => format!("{value:?}").to_lowercase(),
        PrimitiveLogicalType::Unknown => "unknown".to_owned(),
        PrimitiveLogicalType::Json => "json".to_owned(),
        PrimitiveLogicalType::Bson => "bson".to_owned(),
        PrimitiveLogicalType::Uuid => "uuid".to_owned(),
    }
}

#[inline(never)]
fn is_supported(column: &ColumnDescriptor) -> bool {
    column.path_in_schema.len() == 1
        && column.descriptor.max_rep_level == 0
        && column.descriptor.primitive_type.physical_type != PhysicalType::Int96
        && !matches!(
            column.descriptor.primitive_type.physical_type,
            PhysicalType::FixedLenByteArray(0)
        )
        && !matches!(
            column.descriptor.primitive_type.logical_type,
            Some(
                PrimitiveLogicalType::Unknown
                    | PrimitiveLogicalType::Uuid
                    | PrimitiveLogicalType::Time {
                        unit: TimeUnit::Nanoseconds,
                        ..
                    }
                    | PrimitiveLogicalType::Timestamp {
                        unit: TimeUnit::Nanoseconds,
                        ..
                    }
            )
        )
        && !matches!(
            column.descriptor.primitive_type.converted_type,
            Some(PrimitiveConvertedType::Interval)
        )
}

#[inline(never)]
fn column_info(column: &ColumnDescriptor) -> ColumnInfo {
    ColumnInfo {
        path: column.path_in_schema.join("."),
        physical_type: physical_type_name(column.descriptor.primitive_type.physical_type)
            .to_owned(),
        logical_type: column
            .descriptor
            .primitive_type
            .logical_type
            .as_ref()
            .map(logical_type_name),
        nullable: column.descriptor.max_def_level > 0,
        supported: is_supported(column),
    }
}

fn inspect(parsed: &Parsed) -> Result<FileInfo, Error> {
    let rows = u64::try_from(parsed.metadata.num_rows).map_err(|_error| invalid_format())?;
    let row_groups = u32::try_from(parsed.metadata.row_groups.len()).map_err(|_error| {
        error(
            ErrorClass::Limit,
            "Parquet row-group count is not representable",
        )
    })?;
    Ok(FileInfo {
        rows,
        row_groups,
        columns: parsed
            .metadata
            .schema()
            .columns()
            .iter()
            .map(column_info)
            .collect(),
    })
}

fn decode_error(source: parquet2::error::Error) -> Error {
    match source {
        parquet2::error::Error::FeatureNotActive(_, _)
        | parquet2::error::Error::FeatureNotSupported(_) => error(
            ErrorClass::Unsupported,
            "Parquet compression or encoding is not supported",
        ),
        parquet2::error::Error::WouldOverAllocate => error(
            ErrorClass::Limit,
            "Parquet page exceeds the decoding memory limit",
        ),
        _ => error(
            ErrorClass::Decode,
            "Parquet column data could not be decoded",
        ),
    }
}

#[inline(never)]
fn find_column(parsed: &Parsed, path: &str) -> Result<usize, Error> {
    if path.is_empty() || path.len() > MAX_COLUMN_PATH_BYTES {
        return Err(error(
            ErrorClass::InvalidRequest,
            "Parquet column path must be between 1 and 1024 bytes",
        ));
    }
    parsed
        .metadata
        .schema()
        .columns()
        .iter()
        .position(|column| column.path_in_schema.join(".") == path)
        .ok_or_else(|| error(ErrorClass::NotFound, "Parquet column was not found"))
}

#[inline(never)]
fn result_cell_bytes(cell: &Cell) -> Result<usize, Error> {
    let payload = match cell {
        Cell::Text(value) => value.len(),
        Cell::Bytes(value) => value.len(),
        Cell::Decimal(value) => value.unscaled.len(),
        Cell::Null
        | Cell::Boolean(_)
        | Cell::Signed(_)
        | Cell::Unsigned(_)
        | Cell::Floating(_)
        | Cell::Date(_)
        | Cell::Time(_)
        | Cell::Timestamp(_) => 0,
    };
    std::mem::size_of::<Cell>()
        .checked_add(payload)
        .ok_or_else(|| {
            error(
                ErrorClass::Limit,
                "Parquet column result exceeds the decoded-byte limit",
            )
        })
}

#[inline(never)]
fn validate_window(parsed: &Parsed, offset: u64, limit: u64) -> Result<(usize, usize), Error> {
    if limit > MAX_COLUMN_RESULT_CELLS as u64 {
        return Err(error(
            ErrorClass::Limit,
            "Parquet column result exceeds the 100000-cell limit",
        ));
    }
    let end = offset.checked_add(limit).ok_or_else(|| {
        error(
            ErrorClass::InvalidRequest,
            "Parquet column offset plus limit is not representable",
        )
    })?;
    let rows = u64::try_from(parsed.metadata.num_rows).map_err(|_error| invalid_format())?;
    if offset > rows || end > rows {
        return Err(error(
            ErrorClass::NotFound,
            "Parquet column window is out of bounds",
        ));
    }
    let offset = usize::try_from(offset).map_err(|_error| {
        error(
            ErrorClass::InvalidRequest,
            "Parquet column offset is not representable",
        )
    })?;
    let limit = usize::try_from(limit).map_err(|_error| {
        error(
            ErrorClass::InvalidRequest,
            "Parquet column limit is not representable",
        )
    })?;
    Ok((offset, limit))
}

#[inline(never)]
fn validate_chunk(
    chunk: &parquet2::metadata::ColumnChunkMetaData,
    row_group_rows: usize,
) -> Result<(), Error> {
    if chunk.file_path().is_some() {
        return Err(error(
            ErrorClass::Unsupported,
            "external Parquet column chunks are not supported",
        ));
    }
    let values = usize::try_from(chunk.num_values()).map_err(|_error| invalid_format())?;
    if values != row_group_rows {
        return Err(error(
            ErrorClass::Decode,
            "Parquet column value count differs from its row group",
        ));
    }
    let uncompressed_size = usize::try_from(chunk.uncompressed_size()).map_err(|_error| {
        error(
            ErrorClass::InvalidFormat,
            "Parquet column chunk has an invalid size",
        )
    })?;
    if uncompressed_size > MAX_COLUMN_CHUNK_BYTES {
        return Err(error(
            ErrorClass::Limit,
            "Parquet column chunk exceeds the 32 MiB limit",
        ));
    }
    Ok(())
}

#[inline(never)]
fn append_page_cells(
    output: &mut Vec<Cell>,
    decoded_bytes: &mut usize,
    raw_cells: Vec<decode::RawCell>,
    column: &ColumnDescriptor,
) -> Result<(), Error> {
    for raw in raw_cells {
        let cell = value::convert(raw, &column.descriptor.primitive_type)?;
        let cell_bytes = result_cell_bytes(&cell)?;
        *decoded_bytes = decoded_bytes.checked_add(cell_bytes).ok_or_else(|| {
            error(
                ErrorClass::Limit,
                "Parquet column result exceeds the decoded-byte limit",
            )
        })?;
        if *decoded_bytes > MAX_COLUMN_RESULT_BYTES {
            return Err(error(
                ErrorClass::Limit,
                "Parquet column result exceeds the 16 MiB decoded-byte limit",
            ));
        }
        output.push(cell);
    }
    Ok(())
}

struct ColumnOutput {
    cells: Vec<Cell>,
    decoded_bytes: usize,
}

#[inline(never)]
fn decode_dictionary_page(
    page: &DictPage,
    column: &ColumnDescriptor,
    saw_data: bool,
    dictionary: &mut Option<dictionary::DecodedDictionary>,
) -> Result<(), Error> {
    if saw_data || dictionary.is_some() {
        return Err(error(
            ErrorClass::Decode,
            "Parquet column contains an invalid dictionary page",
        ));
    }
    if page.num_values > MAX_ROWS {
        return Err(error(
            ErrorClass::Limit,
            "Parquet dictionary exceeds the value-count limit",
        ));
    }
    *dictionary = Some(
        dictionary::decode(page, column.descriptor.primitive_type.physical_type)
            .map_err(decode_error)?,
    );
    Ok(())
}

#[inline(never)]
fn validate_data_page(
    page: &DataPage,
    page_start: usize,
    row_group_rows: usize,
) -> Result<usize, Error> {
    let page_rows = page.num_values();
    let page_end = page_start.checked_add(page_rows).ok_or_else(|| {
        error(
            ErrorClass::Limit,
            "Parquet page value count exceeds the supported limit",
        )
    })?;
    if page_rows > MAX_ROWS || page_end > row_group_rows {
        return Err(error(
            ErrorClass::Decode,
            "Parquet data-page row count exceeds its row group",
        ));
    }
    Ok(page_end)
}

#[inline(never)]
fn selected_page_window(
    page_start: usize,
    page_end: usize,
    local_start: usize,
    local_end: usize,
) -> Result<Option<(usize, usize)>, Error> {
    if local_start >= page_end || local_end <= page_start {
        return Ok(None);
    }
    let page_offset = local_start.saturating_sub(page_start);
    let selected_end = local_end.min(page_end);
    let selected_start = local_start.max(page_start);
    let selected = selected_end
        .checked_sub(selected_start)
        .ok_or_else(invalid_format)?;
    Ok(Some((page_offset, selected)))
}

#[inline(never)]
fn decode_page_window(
    page: &DataPage,
    dictionary: Option<&dictionary::DecodedDictionary>,
    column: &ColumnDescriptor,
    page_offset: usize,
    selected: usize,
    output: &mut ColumnOutput,
) -> Result<(), Error> {
    let raw = decode::read_range(page, dictionary, page_offset, selected).map_err(decode_error)?;
    append_page_cells(&mut output.cells, &mut output.decoded_bytes, raw, column)
}

#[inline(never)]
fn read_row_group_window(
    parsed: &Parsed,
    row_group: &RowGroupMetaData,
    column_index: usize,
    column: &ColumnDescriptor,
    local_start: usize,
    local_end: usize,
    output: &mut ColumnOutput,
) -> Result<(), Error> {
    let expected = local_end
        .checked_sub(local_start)
        .ok_or_else(invalid_format)?;
    let chunk = row_group
        .columns()
        .get(column_index)
        .ok_or_else(invalid_format)?;
    validate_chunk(chunk, row_group.num_rows())?;

    let cursor = Cursor::new(parsed.input.as_slice());
    let pages =
        get_page_iterator(chunk, cursor, None, Vec::new(), MAX_PAGE_BYTES).map_err(decode_error)?;
    let mut pages = BasicDecompressor::new(pages, Vec::new());
    let mut dictionary = None;
    let mut saw_data = false;
    let mut page_start = 0usize;
    let before = output.cells.len();
    while let Some(page) = pages.next().map_err(decode_error)? {
        match page {
            Page::Dict(page) => {
                decode_dictionary_page(page, column, saw_data, &mut dictionary)?;
            }
            Page::Data(page) => {
                saw_data = true;
                let page_end = validate_data_page(page, page_start, row_group.num_rows())?;
                if let Some((page_offset, selected)) =
                    selected_page_window(page_start, page_end, local_start, local_end)?
                {
                    decode_page_window(
                        page,
                        dictionary.as_ref(),
                        column,
                        page_offset,
                        selected,
                        output,
                    )?;
                }
                page_start = page_end;
                if page_start >= local_end {
                    break;
                }
            }
        }
    }
    let produced = output
        .cells
        .len()
        .checked_sub(before)
        .ok_or_else(invalid_format)?;
    if produced != expected {
        return Err(error(
            ErrorClass::Decode,
            "Parquet column contains fewer rows than its metadata declares",
        ));
    }
    Ok(())
}

#[inline(never)]
fn read_column_window(
    parsed: &Parsed,
    column_index: usize,
    offset: usize,
    limit: usize,
) -> Result<Vec<Cell>, Error> {
    let column = &parsed.metadata.schema().columns()[column_index];
    if !is_supported(column) {
        return Err(error(
            ErrorClass::Unsupported,
            "Parquet column shape or logical type is not supported",
        ));
    }
    let end = offset.checked_add(limit).ok_or_else(|| {
        error(
            ErrorClass::InvalidRequest,
            "Parquet column offset plus limit is not representable",
        )
    })?;
    let mut output = ColumnOutput {
        cells: Vec::new(),
        decoded_bytes: 0,
    };
    output.cells.try_reserve_exact(limit).map_err(|_error| {
        error(
            ErrorClass::Limit,
            "Parquet column result cannot be allocated within the memory limit",
        )
    })?;
    if limit == 0 {
        return Ok(output.cells);
    }

    let mut group_start = 0usize;
    for row_group in &parsed.metadata.row_groups {
        let group_end = group_start
            .checked_add(row_group.num_rows())
            .ok_or_else(invalid_format)?;
        if offset < group_end && end > group_start {
            let local_start = offset.saturating_sub(group_start);
            let local_end = end
                .min(group_end)
                .checked_sub(group_start)
                .ok_or_else(invalid_format)?;
            read_row_group_window(
                parsed,
                row_group,
                column_index,
                column,
                local_start,
                local_end,
                &mut output,
            )?;
        }
        group_start = group_end;
        if group_start >= end {
            break;
        }
    }
    if output.cells.len() != limit {
        return Err(error(
            ErrorClass::Decode,
            "Parquet column contains fewer rows than its metadata declares",
        ));
    }
    Ok(output.cells)
}

#[inline(never)]
fn read_column(parsed: &Parsed, options: ColumnReadOptions) -> Result<Vec<Cell>, Error> {
    let column_index = find_column(parsed, &options.column)?;
    let column = &parsed.metadata.schema().columns()[column_index];
    if !is_supported(column) {
        return Err(error(
            ErrorClass::Unsupported,
            "Parquet column shape or logical type is not supported",
        ));
    }
    let (offset, limit) = validate_window(parsed, options.offset, options.limit)?;
    read_column_window(parsed, column_index, offset, limit)
}

#[inline(never)]
fn read_cell(parsed: &Parsed, options: ReadOptions) -> Result<Cell, Error> {
    if options.row >= u64::try_from(parsed.metadata.num_rows).map_err(|_error| invalid_format())? {
        return Err(error(
            ErrorClass::NotFound,
            "Parquet row index is out of bounds",
        ));
    }
    let mut cells = read_column(
        parsed,
        ColumnReadOptions {
            column: options.column,
            offset: options.row,
            limit: 1,
        },
    )?;
    cells.pop().ok_or_else(|| {
        error(
            ErrorClass::Decode,
            "Parquet column returned an empty scalar window",
        )
    })
}

impl Guest for Parquet {
    fn inspect(input: Vec<u8>) -> Result<FileInfo, Error> {
        let parsed = parse(input)?;
        inspect(&parsed)
    }

    fn read_cell(input: Vec<u8>, options: ReadOptions) -> Result<Cell, Error> {
        let parsed = parse(input)?;
        read_cell(&parsed, options)
    }

    fn read_column(input: Vec<u8>, options: ColumnReadOptions) -> Result<Vec<Cell>, Error> {
        let parsed = parse(input)?;
        read_column(&parsed, options)
    }
}

#[allow(unsafe_code, clippy::all, clippy::nursery, clippy::pedantic)]
#[cfg(target_arch = "wasm32")]
mod export {
    use super::Parquet;

    crate::bindings::export!(Parquet with_types_in crate::bindings);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::basic::Compression;
    use parquet::data_type::{
        BoolType, ByteArray, ByteArrayType, DoubleType, FixedLenByteArray, FixedLenByteArrayType,
        Int32Type, Int64Type,
    };
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use parquet2::metadata::Descriptor;
    use parquet2::page::Page;
    use parquet2::schema::Repetition;
    use parquet2::schema::types::{FieldInfo, IntegerType, ParquetType, PrimitiveType};

    use super::*;

    fn fixture(compression: Compression) -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type(
                "message schema {
                    REQUIRED INT64 order_id;
                    OPTIONAL BYTE_ARRAY status (UTF8);
                    REQUIRED DOUBLE total;
                }",
            )
            .expect("test schema should parse"),
        );
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(compression)
                .set_dictionary_enabled(true)
                .build(),
        );
        let mut writer = SerializedFileWriter::new(Vec::new(), schema, properties)
            .expect("fixture writer should open");
        let mut row_group = writer
            .next_row_group()
            .expect("fixture row group should open");

        let mut column = row_group
            .next_column()
            .expect("order_id column should open")
            .expect("order_id column should exist");
        column
            .typed::<Int64Type>()
            .write_batch(&[1001, 1002, 1003], None, None)
            .expect("order_id values should write");
        column.close().expect("order_id column should close");

        let mut column = row_group
            .next_column()
            .expect("status column should open")
            .expect("status column should exist");
        column
            .typed::<ByteArrayType>()
            .write_batch(
                &[ByteArray::from("pending"), ByteArray::from("paid")],
                Some(&[1, 0, 1]),
                None,
            )
            .expect("status values should write");
        column.close().expect("status column should close");

        let mut column = row_group
            .next_column()
            .expect("total column should open")
            .expect("total column should exist");
        column
            .typed::<DoubleType>()
            .write_batch(&[12.5, 27.75, 41.0], None, None)
            .expect("total values should write");
        column.close().expect("total column should close");

        assert!(
            row_group
                .next_column()
                .expect("column completion should succeed")
                .is_none()
        );
        row_group.close().expect("fixture row group should close");
        writer.into_inner().expect("fixture writer should close")
    }

    fn read(input: &[u8], column: &str, row: u64) -> Cell {
        <Parquet as Guest>::read_cell(
            input.to_vec(),
            ReadOptions {
                column: column.to_owned(),
                row,
            },
        )
        .expect("cell should decode")
    }

    fn read_column(input: &[u8], column: &str, offset: u64, limit: u64) -> Vec<Cell> {
        <Parquet as Guest>::read_column(
            input.to_vec(),
            ColumnReadOptions {
                column: column.to_owned(),
                offset,
                limit,
            },
        )
        .expect("column window should decode")
    }

    fn assert_same_cells(left: &[Cell], right: &[Cell]) {
        assert_eq!(format!("{left:?}"), format!("{right:?}"));
    }

    fn paged_row_group_fixture(rows: usize) -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type("message schema { REQUIRED INT64 value; }")
                .expect("paged schema should parse"),
        );
        let properties = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_data_page_row_count_limit(2)
                .build(),
        );
        let mut writer = SerializedFileWriter::new(Vec::new(), schema, properties)
            .expect("paged fixture writer should open");
        for group_start in (0..rows).step_by(4) {
            let group_end = group_start.saturating_add(4).min(rows);
            let values = (group_start..group_end)
                .map(|value| i64::try_from(value).expect("fixture value should fit"))
                .collect::<Vec<_>>();
            let mut row_group = writer
                .next_row_group()
                .expect("paged row group should open");
            let mut column = row_group
                .next_column()
                .expect("paged column should open")
                .expect("paged column should exist");
            for values in values.chunks(2) {
                column
                    .typed::<Int64Type>()
                    .write_batch(values, None, None)
                    .expect("paged values should write");
            }
            column.close().expect("paged column should close");
            row_group.close().expect("paged row group should close");
        }
        writer.into_inner().expect("paged fixture should close")
    }

    fn large_int_fixture(rows: usize) -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type("message schema { REQUIRED INT64 value; }")
                .expect("large schema should parse"),
        );
        let properties = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_data_page_row_count_limit(4_096)
                .build(),
        );
        let mut writer = SerializedFileWriter::new(Vec::new(), schema, properties)
            .expect("large fixture writer should open");
        let mut row_group = writer
            .next_row_group()
            .expect("large row group should open");
        let mut column = row_group
            .next_column()
            .expect("large column should open")
            .expect("large column should exist");
        let values = (0..rows)
            .map(|value| i64::try_from(value).expect("fixture value should fit"))
            .collect::<Vec<_>>();
        column
            .typed::<Int64Type>()
            .write_batch(&values, None, None)
            .expect("large values should write");
        column.close().expect("large column should close");
        row_group.close().expect("large row group should close");
        writer.into_inner().expect("large fixture should close")
    }

    fn typed_fidelity_fixture() -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type(
                "message schema {
                    OPTIONAL FIXED_LEN_BYTE_ARRAY(8) amount (DECIMAL(18,2));
                    REQUIRED INT64 observed_at (TIMESTAMP(MILLIS,false));
                }",
            )
            .expect("typed fidelity schema should parse"),
        );
        let properties = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .build(),
        );
        let mut writer = SerializedFileWriter::new(Vec::new(), schema, properties)
            .expect("typed fidelity writer should open");
        let mut row_group = writer
            .next_row_group()
            .expect("typed fidelity row group should open");

        let mut amount = row_group
            .next_column()
            .expect("amount column should open")
            .expect("amount column should exist");
        amount
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &[
                    FixedLenByteArray::from(1_234_i64.to_be_bytes().to_vec()),
                    FixedLenByteArray::from((-9_876_i64).to_be_bytes().to_vec()),
                ],
                Some(&[1, 0, 1]),
                None,
            )
            .expect("amount values should write");
        amount.close().expect("amount column should close");

        let mut observed_at = row_group
            .next_column()
            .expect("observed_at column should open")
            .expect("observed_at column should exist");
        observed_at
            .typed::<Int64Type>()
            .write_batch(
                &[1_700_000_000_001, 1_700_000_000_002, 1_700_000_000_003],
                None,
                None,
            )
            .expect("timestamp values should write");
        observed_at
            .close()
            .expect("observed_at column should close");
        row_group
            .close()
            .expect("typed fidelity row group should close");
        writer
            .into_inner()
            .expect("typed fidelity fixture should close")
    }

    fn data_page_count(input: &[u8], column_index: usize) -> usize {
        let mut cursor = Cursor::new(input);
        let metadata = read_metadata(&mut cursor).expect("fixture metadata should decode");
        metadata
            .row_groups
            .iter()
            .map(|row_group| {
                let chunk = &row_group.columns()[column_index];
                let pages =
                    get_page_iterator(chunk, Cursor::new(input), None, Vec::new(), MAX_PAGE_BYTES)
                        .expect("fixture page iterator should open");
                let mut pages = BasicDecompressor::new(pages, Vec::new());
                let mut count = 0;
                while let Some(page) = pages.next().expect("fixture page should decode") {
                    if matches!(page, Page::Data(_)) {
                        count += 1;
                    }
                }
                count
            })
            .sum()
    }

    fn repeated_fixture() -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type("message schema { REPEATED INT64 values; }")
                .expect("repeated schema should parse"),
        );
        let mut writer = SerializedFileWriter::new(
            Vec::new(),
            schema,
            Arc::new(WriterProperties::builder().build()),
        )
        .expect("repeated fixture writer should open");
        let mut row_group = writer
            .next_row_group()
            .expect("repeated row group should open");
        let mut column = row_group
            .next_column()
            .expect("repeated column should open")
            .expect("repeated column should exist");
        column
            .typed::<Int64Type>()
            .write_batch(&[1, 2], Some(&[1, 1]), Some(&[0, 1]))
            .expect("repeated values should write");
        column.close().expect("repeated column should close");
        row_group.close().expect("repeated row group should close");
        writer
            .into_inner()
            .expect("repeated fixture writer should close")
    }

    fn boolean_fixture() -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type("message schema { REQUIRED BOOLEAN enabled; }")
                .expect("boolean schema should parse"),
        );
        let mut writer = SerializedFileWriter::new(
            Vec::new(),
            schema,
            Arc::new(
                WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        )
        .expect("boolean fixture writer should open");
        let mut row_group = writer
            .next_row_group()
            .expect("boolean row group should open");
        let mut column = row_group
            .next_column()
            .expect("boolean column should open")
            .expect("boolean column should exist");
        column
            .typed::<BoolType>()
            .write_batch(&[true], None, None)
            .expect("boolean value should write");
        column.close().expect("boolean column should close");
        row_group.close().expect("boolean row group should close");
        writer.into_inner().expect("boolean fixture should close")
    }

    fn oversized_page_fixture() -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type("message schema { REQUIRED BYTE_ARRAY payload; }")
                .expect("oversized-page schema should parse"),
        );
        let properties = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_dictionary_enabled(false)
                .set_data_page_size_limit(4 * 1024 * 1024)
                .build(),
        );
        let mut writer = SerializedFileWriter::new(Vec::new(), schema, properties)
            .expect("oversized-page writer should open");
        let mut row_group = writer
            .next_row_group()
            .expect("oversized-page row group should open");
        let mut column = row_group
            .next_column()
            .expect("oversized-page column should open")
            .expect("oversized-page column should exist");
        let values = (0..2_500)
            .map(|_| ByteArray::from(vec![0x61; 512]))
            .collect::<Vec<_>>();
        column
            .typed::<ByteArrayType>()
            .write_batch(&values, None, None)
            .expect("oversized page values should write");
        column.close().expect("oversized-page column should close");
        row_group
            .close()
            .expect("oversized-page row group should close");
        writer
            .into_inner()
            .expect("oversized-page fixture should close")
    }

    fn plain_truncation_fixture() -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type(
                "message schema {
                    REQUIRED INT32 native_value;
                    REQUIRED BYTE_ARRAY binary_value;
                    REQUIRED FIXED_LEN_BYTE_ARRAY(4) fixed_value;
                    OPTIONAL INT32 optional_value;
                }",
            )
            .expect("plain truncation schema should parse"),
        );
        let mut writer = SerializedFileWriter::new(
            Vec::new(),
            schema,
            Arc::new(
                WriterProperties::builder()
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        )
        .expect("plain truncation writer should open");
        let mut row_group = writer
            .next_row_group()
            .expect("plain truncation row group should open");

        let mut column = row_group
            .next_column()
            .expect("native column should open")
            .expect("native column should exist");
        column
            .typed::<Int32Type>()
            .write_batch(&[7, 9], None, None)
            .expect("native values should write");
        column.close().expect("native column should close");

        let mut column = row_group
            .next_column()
            .expect("binary column should open")
            .expect("binary column should exist");
        column
            .typed::<ByteArrayType>()
            .write_batch(&[ByteArray::from("a"), ByteArray::from("bb")], None, None)
            .expect("binary values should write");
        column.close().expect("binary column should close");

        let mut column = row_group
            .next_column()
            .expect("fixed column should open")
            .expect("fixed column should exist");
        column
            .typed::<FixedLenByteArrayType>()
            .write_batch(
                &[
                    FixedLenByteArray::from(vec![1, 2, 3, 4]),
                    FixedLenByteArray::from(vec![5, 6, 7, 8]),
                ],
                None,
                None,
            )
            .expect("fixed values should write");
        column.close().expect("fixed column should close");

        let mut column = row_group
            .next_column()
            .expect("optional column should open")
            .expect("optional column should exist");
        column
            .typed::<Int32Type>()
            .write_batch(&[11, 13], Some(&[1, 1]), None)
            .expect("optional values should write");
        column.close().expect("optional column should close");

        row_group
            .close()
            .expect("plain truncation row group should close");
        writer
            .into_inner()
            .expect("plain truncation fixture should close")
    }

    fn first_data_page(input: &[u8], column_index: usize) -> parquet2::page::DataPage {
        let mut cursor = Cursor::new(input);
        let metadata = read_metadata(&mut cursor).expect("fixture metadata should decode");
        let chunk = &metadata.row_groups[0].columns()[column_index];
        let pages = get_page_iterator(chunk, Cursor::new(input), None, Vec::new(), MAX_PAGE_BYTES)
            .expect("fixture page iterator should open");
        let mut pages = BasicDecompressor::new(pages, Vec::new());
        while let Some(page) = pages.next().expect("fixture page should decode") {
            if let Page::Data(page) = page {
                return page.clone();
            }
        }
        panic!("fixture should contain a data page");
    }

    fn integer_type(integer_type: IntegerType) -> PrimitiveType {
        PrimitiveType {
            field_info: FieldInfo {
                name: "value".to_owned(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(PrimitiveLogicalType::Integer(integer_type)),
            converted_type: None,
            physical_type: PhysicalType::Int32,
        }
    }

    #[test]
    fn inspects_flat_file() {
        let info = <Parquet as Guest>::inspect(fixture(Compression::UNCOMPRESSED))
            .expect("fixture should inspect");
        assert_eq!(info.rows, 3);
        assert_eq!(info.row_groups, 1);
        assert_eq!(info.columns.len(), 3);
        assert_eq!(info.columns[1].path, "status");
        assert!(info.columns[1].nullable);
        assert!(info.columns[1].supported);
    }

    #[test]
    fn reads_uncompressed_typed_cells_and_null() {
        let input = fixture(Compression::UNCOMPRESSED);
        assert!(matches!(read(&input, "order_id", 1), Cell::Signed(1002)));
        assert!(matches!(read(&input, "status", 0), Cell::Text(value) if value == "pending"));
        assert!(matches!(read(&input, "status", 1), Cell::Null));
        assert!(matches!(read(&input, "total", 1), Cell::Floating(value) if value == 27.75));
    }

    #[test]
    fn reads_snappy_dictionary_cells() {
        let input = fixture(Compression::SNAPPY);
        assert!(matches!(read(&input, "status", 2), Cell::Text(value) if value == "paid"));
        assert!(matches!(read(&input, "order_id", 2), Cell::Signed(1003)));
    }

    #[test]
    fn column_windows_match_repeated_scalar_reads_for_every_fixture_window() {
        for compression in [Compression::UNCOMPRESSED, Compression::SNAPPY] {
            let input = fixture(compression);
            for column in ["order_id", "status", "total"] {
                for offset in 0..=3 {
                    for limit in 0..=3 - offset {
                        let batch = read_column(&input, column, offset, limit);
                        let scalar = (offset..offset + limit)
                            .map(|row| read(&input, column, row))
                            .collect::<Vec<_>>();
                        assert_same_cells(&batch, &scalar);
                    }
                }
            }
        }
    }

    #[test]
    fn column_window_crosses_page_and_row_group_boundaries_once() {
        let input = paged_row_group_fixture(11);
        let expected_pages = data_page_count(&input, 0);
        assert!(expected_pages > 3);
        decode::reset_range_decode_calls();
        let cells = read_column(&input, "value", 0, 11);
        assert_eq!(cells.len(), 11);
        for (index, cell) in cells.iter().enumerate() {
            assert!(matches!(cell, Cell::Signed(value) if *value == index as i64));
        }
        assert_eq!(decode::range_decode_calls(), expected_pages);

        let crossing = read_column(&input, "value", 3, 4);
        assert!(matches!(crossing[0], Cell::Signed(3)));
        assert!(matches!(crossing[3], Cell::Signed(6)));
    }

    #[test]
    fn column_window_preserves_decimal_null_and_timestamp_units() {
        let input = typed_fidelity_fixture();
        let amounts = read_column(&input, "amount", 0, 3);
        assert!(matches!(
            &amounts[0],
            Cell::Decimal(value)
                if value.unscaled == 1_234_i64.to_be_bytes()
                    && value.precision == 18
                    && value.scale == 2
        ));
        assert!(matches!(amounts[1], Cell::Null));
        assert!(matches!(
            &amounts[2],
            Cell::Decimal(value)
                if value.unscaled == (-9_876_i64).to_be_bytes()
                    && value.precision == 18
                    && value.scale == 2
        ));
        let timestamps = read_column(&input, "observed_at", 0, 3);
        assert!(matches!(
            &timestamps[0],
            Cell::Timestamp(value)
                if value.value == 1_700_000_000_001
                    && matches!(value.unit, bindings::exports::sigil::parquet::reader::TimeUnit::Milliseconds)
        ));
    }

    #[test]
    fn column_window_has_exact_zero_end_and_limit_semantics() {
        let input = fixture(Compression::UNCOMPRESSED);
        assert!(read_column(&input, "order_id", 0, 0).is_empty());
        assert!(read_column(&input, "order_id", 3, 0).is_empty());
        assert_eq!(read_column(&input, "order_id", 0, 3).len(), 3);

        let beyond_end = <Parquet as Guest>::read_column(
            input.clone(),
            ColumnReadOptions {
                column: "order_id".to_owned(),
                offset: 3,
                limit: 1,
            },
        )
        .expect_err("a nonempty end window should fail, not truncate");
        assert!(matches!(beyond_end.class, ErrorClass::NotFound));

        let overflowing = <Parquet as Guest>::read_column(
            input.clone(),
            ColumnReadOptions {
                column: "order_id".to_owned(),
                offset: u64::MAX,
                limit: 1,
            },
        )
        .expect_err("overflowing offset plus limit should fail");
        assert!(matches!(overflowing.class, ErrorClass::InvalidRequest));

        let over_limit = <Parquet as Guest>::read_column(
            input,
            ColumnReadOptions {
                column: "order_id".to_owned(),
                offset: 0,
                limit: (MAX_COLUMN_RESULT_CELLS as u64) + 1,
            },
        )
        .expect_err("an oversized result must fail before returning a short window");
        assert!(matches!(over_limit.class, ErrorClass::Limit));
    }

    #[test]
    fn maximum_cell_window_is_supported_and_max_plus_one_is_rejected() {
        let input = large_int_fixture(MAX_COLUMN_RESULT_CELLS);
        assert!(input.len() < MAX_INPUT_BYTES);
        let cells = read_column(&input, "value", 0, MAX_COLUMN_RESULT_CELLS as u64);
        assert_eq!(cells.len(), MAX_COLUMN_RESULT_CELLS);
        assert!(matches!(cells[0], Cell::Signed(0)));
        assert!(
            matches!(cells[MAX_COLUMN_RESULT_CELLS - 1], Cell::Signed(value) if value == (MAX_COLUMN_RESULT_CELLS - 1) as i64)
        );

        let failure = <Parquet as Guest>::read_column(
            input,
            ColumnReadOptions {
                column: "value".to_owned(),
                offset: 0,
                limit: (MAX_COLUMN_RESULT_CELLS as u64) + 1,
            },
        )
        .expect_err("max plus one cells should be a limit failure");
        assert!(matches!(failure.class, ErrorClass::Limit));
    }

    #[test]
    fn decoded_result_accounting_fails_before_exposing_partial_output() {
        let column = &parse(fixture(Compression::UNCOMPRESSED))
            .expect("fixture should parse")
            .metadata
            .schema()
            .columns()[0]
            .clone();
        let mut output = Vec::new();
        let mut at_limit = MAX_COLUMN_RESULT_BYTES;
        let failure = append_page_cells(
            &mut output,
            &mut at_limit,
            vec![decode::RawCell::Int64(1)],
            column,
        )
        .expect_err("one cell beyond the decoded-byte cap should fail");
        assert!(matches!(failure.class, ErrorClass::Limit));
        assert!(output.is_empty());

        let mut overflow = usize::MAX;
        let failure = append_page_cells(
            &mut output,
            &mut overflow,
            vec![decode::RawCell::Int64(1)],
            column,
        )
        .expect_err("decoded-byte arithmetic overflow should fail");
        assert!(matches!(failure.class, ErrorClass::Limit));
        assert!(output.is_empty());
    }

    #[test]
    fn rejects_invalid_requests_and_inputs() {
        let input = fixture(Compression::UNCOMPRESSED);
        let missing = <Parquet as Guest>::read_cell(
            input.clone(),
            ReadOptions {
                column: "missing".to_owned(),
                row: 0,
            },
        )
        .expect_err("unknown column should fail");
        assert!(matches!(missing.class, ErrorClass::NotFound));

        let outside = <Parquet as Guest>::read_cell(
            input,
            ReadOptions {
                column: "order_id".to_owned(),
                row: 3,
            },
        )
        .expect_err("out-of-bounds row should fail");
        assert!(matches!(outside.class, ErrorClass::NotFound));

        let malformed = <Parquet as Guest>::inspect(b"not parquet".to_vec())
            .expect_err("malformed file should fail");
        assert!(matches!(malformed.class, ErrorClass::InvalidFormat));

        let oversized = <Parquet as Guest>::inspect(vec![0; MAX_INPUT_BYTES + 1])
            .expect_err("oversized file should fail before parsing");
        assert!(matches!(oversized.class, ErrorClass::Limit));

        let truncated = <Parquet as Guest>::inspect(b"PAR1bad!PAR1".to_vec())
            .expect_err("truncated metadata should fail");
        assert!(matches!(truncated.class, ErrorClass::InvalidFormat));
    }

    #[test]
    fn reports_repeated_columns_as_unsupported() {
        let input = repeated_fixture();
        let info = <Parquet as Guest>::inspect(input.clone())
            .expect("repeated fixture metadata should inspect");
        assert!(!info.columns[0].supported);
        let failure = <Parquet as Guest>::read_cell(
            input,
            ReadOptions {
                column: "values".to_owned(),
                row: 0,
            },
        )
        .expect_err("repeated cell read should fail explicitly");
        assert!(matches!(failure.class, ErrorClass::Unsupported));

        let batch_failure = <Parquet as Guest>::read_column(
            repeated_fixture(),
            ColumnReadOptions {
                column: "values".to_owned(),
                offset: 0,
                limit: 0,
            },
        )
        .expect_err("an empty window must not hide an unsupported column");
        assert!(matches!(batch_failure.class, ErrorClass::Unsupported));
    }

    #[test]
    fn corrupt_required_boolean_page_fails_closed() {
        let input = boolean_fixture();
        let mut page = first_data_page(&input, 0);
        page.buffer_mut().clear();
        assert!(decode::read(&page, None, 0).is_err());
    }

    #[test]
    fn truncated_dictionary_indices_fail_closed() {
        let input = fixture(Compression::UNCOMPRESSED);
        let mut cursor = Cursor::new(input.as_slice());
        let metadata = read_metadata(&mut cursor).expect("fixture metadata should decode");
        let column = &metadata.row_groups[0].columns()[0];
        let pages = get_page_iterator(
            column,
            Cursor::new(input.as_slice()),
            None,
            Vec::new(),
            MAX_PAGE_BYTES,
        )
        .expect("fixture page iterator should open");
        let mut pages = BasicDecompressor::new(pages, Vec::new());
        let mut dictionary = None;
        let mut data_page = None;
        while let Some(page) = pages.next().expect("fixture page should decode") {
            match page {
                Page::Dict(page) => {
                    dictionary = Some(
                        dictionary::decode(page, PhysicalType::Int64)
                            .expect("fixture dictionary should decode"),
                    );
                }
                Page::Data(page) => data_page = Some(page.clone()),
            }
        }
        let dictionary = dictionary.expect("fixture should use dictionary encoding");
        let mut data_page = data_page.expect("fixture should contain a data page");
        data_page.buffer_mut().truncate(1);
        assert!(decode::read(&data_page, Some(&dictionary), 0).is_err());
    }

    #[test]
    fn truncated_plain_pages_fail_before_returning_an_early_value() {
        let input = plain_truncation_fixture();
        for (column_index, retained_bytes) in [(0, 4), (1, 5), (2, 4)] {
            let mut page = first_data_page(&input, column_index);
            page.buffer_mut().truncate(retained_bytes);
            assert!(
                decode::read(&page, None, 0).is_err(),
                "column {column_index} returned a value from a truncated page"
            );
            assert!(
                decode::read_range(&page, None, 0, 1).is_err(),
                "column {column_index} returned a window from a truncated page"
            );
        }

        let mut optional = first_data_page(&input, 3);
        *optional.buffer_mut() = vec![2, 0, 0, 0, 2, 1, 11, 0, 0, 0];
        assert!(
            decode::read(&optional, None, 0).is_err(),
            "truncated optional definition levels returned an early value"
        );
        assert!(
            decode::read_range(&optional, None, 0, 1).is_err(),
            "truncated optional definition levels returned an early window"
        );
    }

    #[test]
    fn malformed_v2_levels_and_hybrid_runs_return_errors() {
        assert!(parquet2::page::split_buffer_v2(&[], 1, 0).is_err());
        assert!(parquet2::encoding::hybrid_rle::HybridRleDecoder::try_new(&[], 1, 1).is_err());
        assert!(parquet2::encoding::hybrid_rle::HybridRleDecoder::try_new(&[2], 1, 1).is_err());
        let zeros = parquet2::encoding::hybrid_rle::HybridRleDecoder::try_new(&[], 0, 2)
            .expect("zero-bit dictionary indices are valid")
            .collect::<Result<Vec<_>, _>>()
            .expect("zero-bit indices should decode");
        assert_eq!(zeros, [0, 0]);
        let zeros_with_trailing_data =
            parquet2::encoding::hybrid_rle::HybridRleDecoder::try_new(&[1], 0, 2)
                .expect("zero-bit dictionary indices ignore their absent stream")
                .collect::<Result<Vec<_>, _>>()
                .expect("zero-bit indices should not trap");
        assert_eq!(zeros_with_trailing_data, [0, 0]);
    }

    #[test]
    fn rejects_uncompressed_pages_above_the_page_limit() {
        let input = oversized_page_fixture();
        assert!(input.len() < MAX_INPUT_BYTES);
        let failure = <Parquet as Guest>::read_cell(
            input,
            ReadOptions {
                column: "payload".to_owned(),
                row: 0,
            },
        )
        .expect_err("oversized decompressed page should fail before allocation");
        assert!(matches!(failure.class, ErrorClass::Limit));
    }

    #[test]
    fn narrow_integer_annotations_enforce_their_ranges() {
        for (value, kind) in [
            (128, IntegerType::Int8),
            (32_768, IntegerType::Int16),
            (256, IntegerType::UInt8),
            (65_536, IntegerType::UInt16),
        ] {
            assert!(value::convert(decode::RawCell::Int32(value), &integer_type(kind)).is_err());
        }
        assert!(matches!(
            value::convert(
                decode::RawCell::Int32(127),
                &integer_type(IntegerType::Int8)
            ),
            Ok(Cell::Signed(127))
        ));
        assert!(matches!(
            value::convert(
                decode::RawCell::Int32(255),
                &integer_type(IntegerType::UInt8)
            ),
            Ok(Cell::Unsigned(255))
        ));
    }

    #[test]
    fn zero_width_fixed_columns_are_unsupported() {
        let primitive = PrimitiveType {
            field_info: FieldInfo {
                name: "empty".to_owned(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            physical_type: PhysicalType::FixedLenByteArray(0),
        };
        let column = ColumnDescriptor::new(
            Descriptor {
                primitive_type: primitive.clone(),
                max_def_level: 0,
                max_rep_level: 0,
            },
            vec!["empty".to_owned()],
            ParquetType::PrimitiveType(primitive),
        );
        assert!(!is_supported(&column));
    }
}
