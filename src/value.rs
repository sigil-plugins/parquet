use crate::bindings::exports::sigil::parquet::reader::{
    Cell, DecimalValue, Error, ErrorClass, TemporalValue, TimeUnit as OutputTimeUnit,
};
use crate::decode::RawCell;
use crate::error;
use parquet2::schema::types::{
    IntegerType, PrimitiveConvertedType, PrimitiveLogicalType, PrimitiveType, TimeUnit,
};

const MAX_CELL_BYTES: usize = 1024 * 1024;

fn unsupported() -> Error {
    error(
        ErrorClass::Unsupported,
        "Parquet column logical type is not supported",
    )
}

fn mismatched() -> Error {
    error(
        ErrorClass::Decode,
        "Parquet cell does not match its declared column type",
    )
}

fn bytes(value: Vec<u8>) -> Result<Vec<u8>, Error> {
    if value.len() > MAX_CELL_BYTES {
        return Err(error(
            ErrorClass::Limit,
            "Parquet cell exceeds the 1 MiB decoded-value limit",
        ));
    }
    Ok(value)
}

fn text(value: Vec<u8>) -> Result<Cell, Error> {
    let value = bytes(value)?;
    String::from_utf8(value)
        .map(Cell::Text)
        .map_err(|_error| error(ErrorClass::Decode, "Parquet text cell is not valid UTF-8"))
}

fn decimal(value: RawCell, precision: usize, scale: usize) -> Result<Cell, Error> {
    let unscaled = match value {
        RawCell::Int32(value) => value.to_be_bytes().to_vec(),
        RawCell::Int64(value) => value.to_be_bytes().to_vec(),
        RawCell::Bytes(value) => bytes(value)?,
        _ => return Err(mismatched()),
    };
    Ok(Cell::Decimal(DecimalValue {
        unscaled,
        precision: u32::try_from(precision).map_err(|_error| unsupported())?,
        scale: i32::try_from(scale).map_err(|_error| unsupported())?,
    }))
}

fn temporal(value: RawCell, unit: TimeUnit, timestamp: bool) -> Result<Cell, Error> {
    let value = match value {
        RawCell::Int32(value) => i64::from(value),
        RawCell::Int64(value) => value,
        _ => return Err(mismatched()),
    };
    let unit = match unit {
        TimeUnit::Milliseconds => OutputTimeUnit::Milliseconds,
        TimeUnit::Microseconds => OutputTimeUnit::Microseconds,
        TimeUnit::Nanoseconds => return Err(unsupported()),
    };
    let value = TemporalValue { value, unit };
    if timestamp {
        Ok(Cell::Timestamp(value))
    } else {
        Ok(Cell::Time(value))
    }
}

fn integer(value: RawCell, integer_type: IntegerType) -> Result<Cell, Error> {
    match (value, integer_type) {
        (RawCell::Int32(value), IntegerType::Int8 | IntegerType::Int16 | IntegerType::Int32) => {
            Ok(Cell::Signed(i64::from(value)))
        }
        (RawCell::Int64(value), IntegerType::Int64) => Ok(Cell::Signed(value)),
        (RawCell::Int32(value), IntegerType::UInt8 | IntegerType::UInt16) => u64::try_from(value)
            .map(Cell::Unsigned)
            .map_err(|_error| mismatched()),
        (RawCell::Int32(value), IntegerType::UInt32) => Ok(Cell::Unsigned(u64::from(value as u32))),
        (RawCell::Int64(value), IntegerType::UInt64) => Ok(Cell::Unsigned(value as u64)),
        _ => Err(mismatched()),
    }
}

fn logical(value: RawCell, logical_type: PrimitiveLogicalType) -> Result<Cell, Error> {
    match logical_type {
        PrimitiveLogicalType::String | PrimitiveLogicalType::Enum | PrimitiveLogicalType::Json => {
            match value {
                RawCell::Bytes(value) => text(value),
                _ => Err(mismatched()),
            }
        }
        PrimitiveLogicalType::Bson => match value {
            RawCell::Bytes(value) => bytes(value).map(Cell::Bytes),
            _ => Err(mismatched()),
        },
        PrimitiveLogicalType::Decimal(precision, scale) => decimal(value, precision, scale),
        PrimitiveLogicalType::Date => match value {
            RawCell::Int32(value) => Ok(Cell::Date(value)),
            _ => Err(mismatched()),
        },
        PrimitiveLogicalType::Time { unit, .. } => temporal(value, unit, false),
        PrimitiveLogicalType::Timestamp { unit, .. } => temporal(value, unit, true),
        PrimitiveLogicalType::Integer(integer_type) => integer(value, integer_type),
        PrimitiveLogicalType::Unknown | PrimitiveLogicalType::Uuid => Err(unsupported()),
    }
}

fn converted(value: RawCell, converted_type: PrimitiveConvertedType) -> Result<Cell, Error> {
    match converted_type {
        PrimitiveConvertedType::Utf8
        | PrimitiveConvertedType::Enum
        | PrimitiveConvertedType::Json => match value {
            RawCell::Bytes(value) => text(value),
            _ => Err(mismatched()),
        },
        PrimitiveConvertedType::Bson => match value {
            RawCell::Bytes(value) => bytes(value).map(Cell::Bytes),
            _ => Err(mismatched()),
        },
        PrimitiveConvertedType::Decimal(precision, scale) => decimal(value, precision, scale),
        PrimitiveConvertedType::Date => match value {
            RawCell::Int32(value) => Ok(Cell::Date(value)),
            _ => Err(mismatched()),
        },
        PrimitiveConvertedType::TimeMillis => temporal(value, TimeUnit::Milliseconds, false),
        PrimitiveConvertedType::TimeMicros => temporal(value, TimeUnit::Microseconds, false),
        PrimitiveConvertedType::TimestampMillis => temporal(value, TimeUnit::Milliseconds, true),
        PrimitiveConvertedType::TimestampMicros => temporal(value, TimeUnit::Microseconds, true),
        PrimitiveConvertedType::Uint8 => integer(value, IntegerType::UInt8),
        PrimitiveConvertedType::Uint16 => integer(value, IntegerType::UInt16),
        PrimitiveConvertedType::Uint32 => integer(value, IntegerType::UInt32),
        PrimitiveConvertedType::Uint64 => integer(value, IntegerType::UInt64),
        PrimitiveConvertedType::Int8 => integer(value, IntegerType::Int8),
        PrimitiveConvertedType::Int16 => integer(value, IntegerType::Int16),
        PrimitiveConvertedType::Int32 => integer(value, IntegerType::Int32),
        PrimitiveConvertedType::Int64 => integer(value, IntegerType::Int64),
        PrimitiveConvertedType::Interval => Err(unsupported()),
    }
}

pub(crate) fn convert(value: RawCell, primitive_type: &PrimitiveType) -> Result<Cell, Error> {
    if matches!(value, RawCell::Null) {
        return Ok(Cell::Null);
    }
    if let Some(logical_type) = primitive_type.logical_type {
        return logical(value, logical_type);
    }
    if let Some(converted_type) = primitive_type.converted_type {
        return converted(value, converted_type);
    }
    match value {
        RawCell::Null => Ok(Cell::Null),
        RawCell::Boolean(value) => Ok(Cell::Boolean(value)),
        RawCell::Int32(value) => Ok(Cell::Signed(i64::from(value))),
        RawCell::Int64(value) => Ok(Cell::Signed(value)),
        RawCell::Float(value) => Ok(Cell::Floating(f64::from(value))),
        RawCell::Double(value) => Ok(Cell::Floating(value)),
        RawCell::Bytes(value) => bytes(value).map(Cell::Bytes),
        RawCell::Int96 => Err(unsupported()),
    }
}
