use crate::ReturnType;

use arrow2::array::*;
use arrow2::datatypes::{DataType, Metadata};
use arrow2::temporal_conversions::date32_to_date;
use chrono::NaiveDateTime;
use std::any::Any;
use std::sync::Arc;

#[inline]
pub fn new_serializer<'a>(
    field_metadata: &Metadata,
    array: &'a Arc<dyn Array>,
    cast_elixir_types: bool,
) -> Vec<ReturnType> {
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<i8>>()
            .unwrap()
            .iter()
            .map(|x| match x {
                Some(&x) => ReturnType::Int8(Some(x)),
                None => ReturnType::Int8(None),
            })
            .collect::<Vec<ReturnType>>(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<i16>>()
            .unwrap()
            .iter()
            // .map(|x| ReturnType::Int16(x))
            .map(|x| match x {
                Some(&x) => ReturnType::Int16(Some(x)),
                None => ReturnType::Int16(None),
            })
            .collect::<Vec<ReturnType>>(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap()
            .iter()
            .map(|x| match x {
                Some(&x) => ReturnType::Int64(Some(x)),
                None => ReturnType::Int64(None),
            })
            // .map(|x| ReturnType::Int64(x))
            .collect::<Vec<ReturnType>>(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap()
            .iter()
            .map(|x| match x {
                Some(&x) => ReturnType::Float64(Some(x)),
                None => ReturnType::Float64(None),
            })
            .collect::<Vec<ReturnType>>(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .map(|x| ReturnType::Boolean(x))
            .collect(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap()
            .iter()
            .map(|x| ReturnType::String(x.map(|t| t.to_string())))
            .collect(),
        DataType::Date32 => date32_to_dates(array, cast_elixir_types),
        // Snowflake sends back floats in integers, I think it's because they
        // were super early adopters of Arrow so they had to use what they could.
        DataType::Int32 => {
            if field_metadata.get("scale").unwrap() == "0" {
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|x| match x {
                        Some(&x) => ReturnType::Int32(Some(x)),
                        None => ReturnType::Int32(None),
                    })
                    .collect::<Vec<ReturnType>>()
            } else {
                let scale = field_metadata.get("scale").unwrap().parse::<i32>().unwrap();
                float_to_vecs(array, scale)
            }
        }
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray<i32>>()
            .unwrap()
            .iter()
            .map(|x| ReturnType::Binary(x.map(|t| t.to_vec())))
            .collect::<Vec<ReturnType>>(),
        DataType::Struct(_f) => {
            let logical_type = field_metadata.get("logicalType").unwrap().as_str();
            match logical_type {
                "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                    convert_timestamps(array.as_any(), cast_elixir_types)
                }
                _ => unreachable!(),
            }
        }
        _x => {
            // println!("{}", x.to_string());
            unreachable!()
        }
    }
}

#[inline]
pub fn float_to_vecs(array: &Arc<dyn Array>, scale: i32) -> Vec<ReturnType> {
    array
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .map(|t| match t {
            Some(&v) => {
                let float = v as f64 / 10f64.powi(scale);
                ReturnType::Float642(Some(float))
            }
            None => ReturnType::Float64(None),
        })
        .collect::<Vec<ReturnType>>()
}

#[inline]
pub fn convert_timestamps(column: &dyn Any, cast_elixir: bool) -> Vec<ReturnType> {
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
                    ReturnType::DateTime(Some(timestamp.into()))
                } else {
                    ReturnType::String(Some(timestamp.to_string()))
                }
            }
            None => {
                if cast_elixir {
                    ReturnType::DateTime(None)
                } else {
                    ReturnType::String(None)
                }
            }
        })
        .collect::<Vec<ReturnType>>()
}

#[inline]
pub fn date32_to_dates(array: &Arc<dyn Array>, cast_elixir: bool) -> Vec<ReturnType> {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<i32>>()
        .unwrap()
        .iter()
        .map(|t| match t {
            Some(t) => {
                if cast_elixir {
                    ReturnType::Date(Some(date32_to_date(*t).into()))
                } else {
                    ReturnType::String(Some(date32_to_date(*t).to_string()))
                }
            }
            None => {
                if cast_elixir {
                    ReturnType::Date(None)
                } else {
                    ReturnType::String(None)
                }
            }
        })
        .collect::<Vec<ReturnType>>()
}
