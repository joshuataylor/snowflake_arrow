use crate::serialize::iterator::ReturnTypeStreamingIterator;
use crate::SnowflakeReturnType;
use crate::SnowflakeReturnType::Int32;
use arrow2::array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, PrimitiveArray,
    StructArray, Utf8Array,
};
use arrow2::datatypes::{DataType, Metadata};
use arrow2::error::Result;
use arrow2::scalar::PrimitiveScalar;
use arrow2::temporal_conversions::date32_to_date;
use chrono::NaiveDateTime;
use std::any::Any;
use streaming_iterator::StreamingIterator;

/// Returns a [`StreamingIterator`] that yields `ReturnType` serialized from `array`
pub fn new_serializer<'a>(
    field_metadata: &Metadata,
    array: &'a dyn Array,
    cast_elixir_types: bool,
) -> Result<Box<dyn StreamingIterator<Item = Vec<SnowflakeReturnType>> + 'a>> {
    Ok(match array.data_type() {
        DataType::Boolean => {
            let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Box::new(ReturnTypeStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    buf.push(SnowflakeReturnType::Boolean(x));
                },
                vec![],
            ))
        }
        DataType::Int32 => {
            let values = array.as_any().downcast_ref::<Int32Array>().unwrap();

            Box::new(ReturnTypeStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    let i = match x {
                        Some(&ss) => SnowflakeReturnType::Int32(Some(ss)),
                        None => SnowflakeReturnType::Int32(None),
                    };

                    buf.push(i)
                },
                vec![],
            ))
        }
        DataType::Int64 => {
            let values = array.as_any().downcast_ref::<Int64Array>().unwrap();

            Box::new(ReturnTypeStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    let i = match x {
                        Some(&ss) => SnowflakeReturnType::Int64(Some(ss)),
                        None => SnowflakeReturnType::Int64(None),
                    };

                    buf.push(i)
                },
                vec![],
            ))
        }
        DataType::Float64 => {
            let values = array.as_any().downcast_ref::<Float64Array>().unwrap();

            Box::new(ReturnTypeStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    let i = match x {
                        Some(&ss) => SnowflakeReturnType::Float64(Some(ss)),
                        None => SnowflakeReturnType::Float64(None),
                    };

                    buf.push(i)
                },
                vec![],
            ))
        }
        DataType::Utf8 => {
            let values = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

            Box::new(ReturnTypeStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    let value = SnowflakeReturnType::String(x.map(|t| t.to_string()));

                    buf.push(value)
                },
                vec![],
            ))
        }
        DataType::Struct(_fields) => {
            let values = array.as_any().downcast_ref::<StructArray>().unwrap();

            Box::new(ReturnTypeStreamingIterator::new(
                values.iter(),
                move |x, buf| {
                    let value = match x {
                        Some(v) => {
                            // probably a much better way to do this
                            let epoch = v
                                .get(0)
                                .unwrap()
                                .as_any()
                                .downcast_ref::<PrimitiveScalar<i64>>()
                                .unwrap()
                                .value()
                                .unwrap();
                            let fraction = v
                                .get(1)
                                .unwrap()
                                .as_any()
                                .downcast_ref::<PrimitiveScalar<i32>>()
                                .unwrap()
                                .value()
                                .unwrap();
                            let timestamp = NaiveDateTime::from_timestamp(epoch, fraction as u32);
                            if cast_elixir_types {
                                SnowflakeReturnType::DateTime(Some(timestamp.into()))
                            } else {
                                SnowflakeReturnType::String(Some(timestamp.to_string()))
                            }
                        }
                        None => {
                            if cast_elixir_types {
                                SnowflakeReturnType::DateTime(None)
                            } else {
                                SnowflakeReturnType::String(None)
                            }
                        }
                    };

                    buf.push(value)
                },
                vec![],
            ))
        }
        DataType::Binary => {
            let values = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            Box::new(ReturnTypeStreamingIterator::new(
                values.iter(),
                |x, buf| {
                    let value = SnowflakeReturnType::Binary(x.map(|t| t.to_vec()));

                    buf.push(value)
                },
                vec![],
            ))
        }

        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            Box::new(ReturnTypeStreamingIterator::new(
                values.iter(),
                move |x, buf| {
                    // Probably a better way to do this, but :shrug:
                    if cast_elixir_types {
                        let date = match x {
                            None => SnowflakeReturnType::Date(None),
                            Some(date) => {
                                SnowflakeReturnType::Date(Some(date32_to_date(*date).into()))
                            }
                        };
                        buf.push(date);
                    } else {
                        let date = match x {
                            None => SnowflakeReturnType::String(None),
                            Some(date) => {
                                SnowflakeReturnType::String(Some(date32_to_date(*date).to_string()))
                            }
                        };
                        buf.push(date);
                    }
                },
                vec![],
            ))
        }
        dt => panic!("data type: {:?} not supported by Snowflake", dt),
    })
}

#[inline]
pub fn convert_timestamps(column: &dyn Any, cast_elixir: bool) -> Vec<SnowflakeReturnType> {
    let (_fields, arrays, _bitmap) = column
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone()
        .into_data();

    // Get the fractional parts from the second array
    let fractions = arrays[1].as_any().downcast_ref::<Int32Array>().unwrap();

    // Loop the epochs and get the fraction to get the final timestamp
    arrays[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, epoch)| match epoch {
            Some(epoch) => {
                let value1 = *epoch as i64;
                let value2 = fractions.value(i);
                let timestamp = NaiveDateTime::from_timestamp(value1, value2 as u32);
                if cast_elixir {
                    SnowflakeReturnType::DateTime(Some(timestamp.into()))
                } else {
                    SnowflakeReturnType::String(Some(timestamp.to_string()))
                }
            }
            None => {
                if cast_elixir {
                    SnowflakeReturnType::DateTime(None)
                } else {
                    SnowflakeReturnType::String(None)
                }
            }
        })
        .collect::<Vec<SnowflakeReturnType>>()
}
