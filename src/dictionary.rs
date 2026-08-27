use parquet2::encoding::get_length;
use parquet2::error::Error;
use parquet2::page::DictPage;
use parquet2::schema::types::PhysicalType;
use parquet2::types::{NativeType, decode as decode_native};

#[derive(Debug)]
pub(crate) struct PrimitiveDictionary<T: NativeType> {
    values: Vec<T>,
}

impl<T: NativeType> PrimitiveDictionary<T> {
    pub(crate) fn value(&self, index: usize) -> Result<&T, Error> {
        self.values.get(index).ok_or_else(|| {
            Error::OutOfSpec("dictionary index exceeds the dictionary page".to_owned())
        })
    }
}

#[derive(Debug)]
pub(crate) struct BinaryDictionary {
    values: Vec<Vec<u8>>,
}

impl BinaryDictionary {
    pub(crate) fn value(&self, index: usize) -> Result<&[u8], Error> {
        self.values.get(index).map(Vec::as_slice).ok_or_else(|| {
            Error::OutOfSpec("dictionary index exceeds the dictionary page".to_owned())
        })
    }
}

#[derive(Debug)]
pub(crate) struct FixedDictionary {
    values: Vec<u8>,
    size: usize,
}

impl FixedDictionary {
    pub(crate) fn value(&self, index: usize) -> Result<&[u8], Error> {
        let start = index
            .checked_mul(self.size)
            .ok_or(Error::WouldOverAllocate)?;
        let end = start
            .checked_add(self.size)
            .ok_or(Error::WouldOverAllocate)?;
        self.values.get(start..end).ok_or_else(|| {
            Error::OutOfSpec("dictionary index exceeds the dictionary page".to_owned())
        })
    }
}

#[derive(Debug)]
pub(crate) enum DecodedDictionary {
    Int32(PrimitiveDictionary<i32>),
    Int64(PrimitiveDictionary<i64>),
    Int96(PrimitiveDictionary<[u32; 3]>),
    Float(PrimitiveDictionary<f32>),
    Double(PrimitiveDictionary<f64>),
    Binary(BinaryDictionary),
    Fixed(FixedDictionary),
}

fn read_native<T: NativeType>(page: &DictPage) -> Result<PrimitiveDictionary<T>, Error> {
    let size = std::mem::size_of::<T>();
    let byte_count = page
        .num_values
        .checked_mul(size)
        .ok_or(Error::WouldOverAllocate)?;
    let values = page.buffer.get(..byte_count).ok_or_else(|| {
        Error::OutOfSpec("dictionary page is shorter than its declared value count".to_owned())
    })?;
    Ok(PrimitiveDictionary {
        values: values.chunks_exact(size).map(decode_native::<T>).collect(),
    })
}

fn read_binary(page: &DictPage) -> Result<BinaryDictionary, Error> {
    let mut remaining = page.buffer.as_slice();
    let mut values = Vec::with_capacity(page.num_values);
    for _ in 0..page.num_values {
        let length = get_length(remaining).ok_or_else(|| {
            Error::OutOfSpec("dictionary byte-array length is missing".to_owned())
        })?;
        remaining = remaining.get(4..).ok_or_else(|| {
            Error::OutOfSpec("dictionary byte-array length is truncated".to_owned())
        })?;
        let value = remaining.get(..length).ok_or_else(|| {
            Error::OutOfSpec("dictionary byte-array value is truncated".to_owned())
        })?;
        values.push(value.to_vec());
        remaining = remaining.get(length..).ok_or_else(|| {
            Error::OutOfSpec("dictionary byte-array value is truncated".to_owned())
        })?;
    }
    Ok(BinaryDictionary { values })
}

fn read_fixed(page: &DictPage, size: usize) -> Result<FixedDictionary, Error> {
    let byte_count = page
        .num_values
        .checked_mul(size)
        .ok_or(Error::WouldOverAllocate)?;
    let values = page.buffer.get(..byte_count).ok_or_else(|| {
        Error::OutOfSpec("fixed dictionary page is shorter than declared".to_owned())
    })?;
    Ok(FixedDictionary {
        values: values.to_vec(),
        size,
    })
}

pub(crate) fn decode(
    page: &DictPage,
    physical_type: PhysicalType,
) -> Result<DecodedDictionary, Error> {
    match physical_type {
        PhysicalType::Boolean => Err(Error::OutOfSpec(
            "boolean columns cannot be dictionary encoded".to_owned(),
        )),
        PhysicalType::Int32 => read_native(page).map(DecodedDictionary::Int32),
        PhysicalType::Int64 => read_native(page).map(DecodedDictionary::Int64),
        PhysicalType::Int96 => read_native(page).map(DecodedDictionary::Int96),
        PhysicalType::Float => read_native(page).map(DecodedDictionary::Float),
        PhysicalType::Double => read_native(page).map(DecodedDictionary::Double),
        PhysicalType::ByteArray => read_binary(page).map(DecodedDictionary::Binary),
        PhysicalType::FixedLenByteArray(size) => {
            read_fixed(page, size).map(DecodedDictionary::Fixed)
        }
    }
}
