use parquet2::deserialize::{
    BinaryPageState, BooleanPageState, DefLevelsDecoder, FixedLenBinaryPageState, HybridEncoded,
    NativePageState,
};
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::error::Error;
use parquet2::page::DataPage;
use parquet2::schema::types::PhysicalType;
use parquet2::types::NativeType;

use crate::dictionary::{
    BinaryDictionary, DecodedDictionary, FixedDictionary, PrimitiveDictionary,
};

#[cfg(test)]
thread_local! {
    static RANGE_DECODE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_range_decode_calls() {
    RANGE_DECODE_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn range_decode_calls() -> usize {
    RANGE_DECODE_CALLS.get()
}

#[derive(Debug)]
pub(crate) enum RawCell {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Int96,
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
}

#[inline(never)]
fn deserialize_optional<C, I>(
    validity: DefLevelsDecoder<'_>,
    mut values: I,
) -> Result<Vec<Option<C>>, Error>
where
    C: Clone,
    I: Iterator<Item = Result<C, Error>>,
{
    match validity {
        DefLevelsDecoder::Bitmap(bitmap) => deserialize_bitmap(bitmap, values),
        DefLevelsDecoder::Levels(levels, max_level) => {
            let mut decoded = Vec::with_capacity(levels.size_hint().0);
            for level in levels {
                if level? == max_level {
                    decoded.push(Some(next_value(&mut values)?));
                } else {
                    decoded.push(None);
                }
            }
            Ok(decoded)
        }
    }
}

#[inline(never)]
fn next_value<C, I>(values: &mut I) -> Result<C, Error>
where
    I: Iterator<Item = Result<C, Error>>,
{
    values.next().ok_or_else(|| {
        Error::OutOfSpec("definition levels contain more values than the data page".to_owned())
    })?
}

#[inline(never)]
fn deserialize_bitmap<C, I>(
    validity: parquet2::deserialize::HybridDecoderBitmapIter<'_>,
    mut values: I,
) -> Result<Vec<Option<C>>, Error>
where
    C: Clone,
    I: Iterator<Item = Result<C, Error>>,
{
    let mut decoded = Vec::with_capacity(validity.len());
    for run in validity {
        match run? {
            HybridEncoded::Bitmap(bitmap, length) => {
                for is_set in BitmapIter::new(bitmap, 0, length) {
                    if is_set {
                        decoded.push(Some(next_value(&mut values)?));
                    } else {
                        decoded.push(None);
                    }
                }
            }
            HybridEncoded::Repeated(is_set, length) => {
                if is_set {
                    for _ in 0..length {
                        decoded.push(Some(next_value(&mut values)?));
                    }
                } else {
                    decoded.extend(std::iter::repeat_n(None, length));
                }
            }
        }
    }
    Ok(decoded)
}

#[inline(never)]
fn native_values<T>(
    page: &DataPage,
    dictionary: Option<&PrimitiveDictionary<T>>,
) -> Result<Vec<Option<T>>, Error>
where
    T: NativeType + Copy,
{
    match NativePageState::<T, _>::try_new(page, dictionary)? {
        NativePageState::Optional(validity, mut values) => {
            deserialize_optional(validity, values.by_ref().map(Ok))
        }
        NativePageState::Required(values) => Ok(values.map(Some).collect()),
        NativePageState::RequiredDictionary(dictionary) => dictionary
            .indexes
            .map(|index| {
                index.and_then(|index| dictionary.dict.value(index as usize).copied().map(Some))
            })
            .collect(),
        NativePageState::OptionalDictionary(validity, dictionary) => {
            let values = dictionary.indexes.map(|index| {
                index.and_then(|index| dictionary.dict.value(index as usize).copied())
            });
            deserialize_optional(validity, values)
        }
    }
}

#[inline(never)]
fn binary_values(
    page: &DataPage,
    dictionary: Option<&BinaryDictionary>,
) -> Result<Vec<Option<Vec<u8>>>, Error> {
    match BinaryPageState::try_new(page, dictionary)? {
        BinaryPageState::Optional(validity, values) => {
            deserialize_optional(validity, values.map(|value| value.map(<[u8]>::to_vec)))
        }
        BinaryPageState::Required(values) => values
            .map(|value| value.map(<[u8]>::to_vec))
            .map(|value| value.map(Some))
            .collect(),
        BinaryPageState::RequiredDictionary(dictionary) => dictionary
            .indexes
            .map(|index| {
                index.and_then(|index| {
                    dictionary
                        .dict
                        .value(index as usize)
                        .map(<[u8]>::to_vec)
                        .map(Some)
                })
            })
            .collect(),
        BinaryPageState::OptionalDictionary(validity, dictionary) => {
            let values = dictionary.indexes.map(|index| {
                index.and_then(|index| dictionary.dict.value(index as usize).map(<[u8]>::to_vec))
            });
            deserialize_optional(validity, values)
        }
    }
}

#[inline(never)]
fn fixed_values(
    page: &DataPage,
    dictionary: Option<&FixedDictionary>,
) -> Result<Vec<Option<Vec<u8>>>, Error> {
    match FixedLenBinaryPageState::try_new(page, dictionary)? {
        FixedLenBinaryPageState::Optional(validity, values) => {
            deserialize_optional(validity, values.map(|value| Ok(value.to_vec())))
        }
        FixedLenBinaryPageState::Required(values) => {
            Ok(values.map(|value| Some(value.to_vec())).collect())
        }
        FixedLenBinaryPageState::RequiredDictionary(dictionary) => dictionary
            .indexes
            .map(|index| {
                index.and_then(|index| {
                    dictionary
                        .dict
                        .value(index as usize)
                        .map(<[u8]>::to_vec)
                        .map(Some)
                })
            })
            .collect(),
        FixedLenBinaryPageState::OptionalDictionary(validity, dictionary) => {
            let values = dictionary.indexes.map(|index| {
                index.and_then(|index| dictionary.dict.value(index as usize).map(<[u8]>::to_vec))
            });
            deserialize_optional(validity, values)
        }
    }
}

#[inline(never)]
fn boolean_values(page: &DataPage) -> Result<Vec<Option<bool>>, Error> {
    match BooleanPageState::try_new(page)? {
        BooleanPageState::Optional(validity, mut values) => {
            deserialize_optional(validity, values.by_ref().map(Ok))
        }
        BooleanPageState::Required(bitmap, length) => {
            if bitmap
                .len()
                .checked_mul(8)
                .ok_or(Error::WouldOverAllocate)?
                < length
            {
                return Err(Error::OutOfSpec(
                    "boolean data page is shorter than its declared value count".to_owned(),
                ));
            }
            Ok(BitmapIter::new(bitmap, 0, length).map(Some).collect())
        }
    }
}

fn dictionary_for_i32(
    dictionary: Option<&DecodedDictionary>,
) -> Result<Option<&PrimitiveDictionary<i32>>, Error> {
    match dictionary {
        Some(DecodedDictionary::Int32(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(Error::OutOfSpec(
            "dictionary physical type mismatch".to_owned(),
        )),
    }
}

fn dictionary_for_i64(
    dictionary: Option<&DecodedDictionary>,
) -> Result<Option<&PrimitiveDictionary<i64>>, Error> {
    match dictionary {
        Some(DecodedDictionary::Int64(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(Error::OutOfSpec(
            "dictionary physical type mismatch".to_owned(),
        )),
    }
}

fn dictionary_for_i96(
    dictionary: Option<&DecodedDictionary>,
) -> Result<Option<&PrimitiveDictionary<[u32; 3]>>, Error> {
    match dictionary {
        Some(DecodedDictionary::Int96(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(Error::OutOfSpec(
            "dictionary physical type mismatch".to_owned(),
        )),
    }
}

fn dictionary_for_f32(
    dictionary: Option<&DecodedDictionary>,
) -> Result<Option<&PrimitiveDictionary<f32>>, Error> {
    match dictionary {
        Some(DecodedDictionary::Float(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(Error::OutOfSpec(
            "dictionary physical type mismatch".to_owned(),
        )),
    }
}

fn dictionary_for_f64(
    dictionary: Option<&DecodedDictionary>,
) -> Result<Option<&PrimitiveDictionary<f64>>, Error> {
    match dictionary {
        Some(DecodedDictionary::Double(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(Error::OutOfSpec(
            "dictionary physical type mismatch".to_owned(),
        )),
    }
}

fn dictionary_for_binary(
    dictionary: Option<&DecodedDictionary>,
) -> Result<Option<&BinaryDictionary>, Error> {
    match dictionary {
        Some(DecodedDictionary::Binary(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(Error::OutOfSpec(
            "dictionary physical type mismatch".to_owned(),
        )),
    }
}

fn dictionary_for_fixed(
    dictionary: Option<&DecodedDictionary>,
) -> Result<Option<&FixedDictionary>, Error> {
    match dictionary {
        Some(DecodedDictionary::Fixed(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(Error::OutOfSpec(
            "dictionary physical type mismatch".to_owned(),
        )),
    }
}

fn window<T>(
    values: Vec<Option<T>>,
    offset: usize,
    limit: usize,
    expected_values: usize,
) -> Result<Vec<Option<T>>, Error> {
    if values.len() != expected_values {
        return Err(Error::OutOfSpec(
            "decoded data-page value count differs from its header".to_owned(),
        ));
    }
    let end = offset.checked_add(limit).ok_or(Error::WouldOverAllocate)?;
    if end > expected_values {
        return Err(Error::OutOfSpec(
            "data-page window exceeds its declared value count".to_owned(),
        ));
    }
    let selected = values
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    if selected.len() != limit {
        return Err(Error::OutOfSpec(
            "data page contains fewer rows than declared".to_owned(),
        ));
    }
    Ok(selected)
}

#[inline(never)]
fn read_i32_range(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Option<RawCell>>, Error> {
    Ok(window(
        native_values(page, dictionary_for_i32(dictionary)?)?,
        offset,
        limit,
        page.num_values(),
    )?
    .into_iter()
    .map(|value| value.map(RawCell::Int32))
    .collect())
}

#[inline(never)]
fn read_i64_range(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Option<RawCell>>, Error> {
    Ok(window(
        native_values(page, dictionary_for_i64(dictionary)?)?,
        offset,
        limit,
        page.num_values(),
    )?
    .into_iter()
    .map(|value| value.map(RawCell::Int64))
    .collect())
}

#[inline(never)]
fn read_i96_range(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Option<RawCell>>, Error> {
    Ok(window(
        native_values(page, dictionary_for_i96(dictionary)?)?,
        offset,
        limit,
        page.num_values(),
    )?
    .into_iter()
    .map(|value| value.map(|_value| RawCell::Int96))
    .collect())
}

#[inline(never)]
fn read_f32_range(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Option<RawCell>>, Error> {
    Ok(window(
        native_values(page, dictionary_for_f32(dictionary)?)?,
        offset,
        limit,
        page.num_values(),
    )?
    .into_iter()
    .map(|value| value.map(RawCell::Float))
    .collect())
}

#[inline(never)]
fn read_f64_range(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Option<RawCell>>, Error> {
    Ok(window(
        native_values(page, dictionary_for_f64(dictionary)?)?,
        offset,
        limit,
        page.num_values(),
    )?
    .into_iter()
    .map(|value| value.map(RawCell::Double))
    .collect())
}

#[inline(never)]
fn read_binary_range(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Option<RawCell>>, Error> {
    Ok(window(
        binary_values(page, dictionary_for_binary(dictionary)?)?,
        offset,
        limit,
        page.num_values(),
    )?
    .into_iter()
    .map(|value| value.map(RawCell::Bytes))
    .collect())
}

#[inline(never)]
fn read_fixed_range(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Option<RawCell>>, Error> {
    Ok(window(
        fixed_values(page, dictionary_for_fixed(dictionary)?)?,
        offset,
        limit,
        page.num_values(),
    )?
    .into_iter()
    .map(|value| value.map(RawCell::Bytes))
    .collect())
}

#[inline(never)]
pub(crate) fn read_range(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    offset: usize,
    limit: usize,
) -> Result<Vec<RawCell>, Error> {
    #[cfg(test)]
    RANGE_DECODE_CALLS.set(RANGE_DECODE_CALLS.get() + 1);

    let values = match page.descriptor.primitive_type.physical_type {
        PhysicalType::Boolean => window(boolean_values(page)?, offset, limit, page.num_values())?
            .into_iter()
            .map(|value| value.map(RawCell::Boolean))
            .collect(),
        PhysicalType::Int32 => read_i32_range(page, dictionary, offset, limit)?,
        PhysicalType::Int64 => read_i64_range(page, dictionary, offset, limit)?,
        PhysicalType::Int96 => read_i96_range(page, dictionary, offset, limit)?,
        PhysicalType::Float => read_f32_range(page, dictionary, offset, limit)?,
        PhysicalType::Double => read_f64_range(page, dictionary, offset, limit)?,
        PhysicalType::ByteArray => read_binary_range(page, dictionary, offset, limit)?,
        PhysicalType::FixedLenByteArray(_) => read_fixed_range(page, dictionary, offset, limit)?,
    };
    Ok(values
        .into_iter()
        .map(|value| value.unwrap_or(RawCell::Null))
        .collect())
}

#[inline(never)]
#[cfg(test)]
pub(crate) fn read(
    page: &DataPage,
    dictionary: Option<&DecodedDictionary>,
    index: usize,
) -> Result<RawCell, Error> {
    read_range(page, dictionary, index, 1)?
        .pop()
        .ok_or_else(|| Error::OutOfSpec("data-page cell window is empty".to_owned()))
}
