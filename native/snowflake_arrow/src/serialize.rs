use crate::ReturnType;
use arrow2::array::PrimitiveArray;
use arrow2::array::*;
use arrow2::datatypes::{DataType, Metadata};
use arrow2::temporal_conversions::date32_to_date;
use chrono::NaiveDateTime;
use std::any::Any;
use std::sync::Arc;

#[inline]
pub fn new_serializer<'a>(field_metadata: &Metadata, array: &'a Arc<dyn Array>) -> ReturnType<'a> {
    match array.data_type() {
        DataType::Int64 => ReturnType::Int64(
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&i64>>>(),
        ),
        DataType::Float64 => ReturnType::Float64(
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<f64>>()
                .unwrap()
                .iter()
                .map(|x| match x {
                    Some(x) => Some(*x),
                    None => None,
                })
                .collect::<Vec<Option<f64>>>(),
        ),
        DataType::Boolean => ReturnType::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<bool>>>(),
        ),
        DataType::Utf8 => ReturnType::Utf8(
            array
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .unwrap()
                .iter()
                .collect::<Vec<Option<&str>>>(),
        ),
        DataType::Date32 => ReturnType::String(date32_to_dates(array)),

        // Snowflake is goofy and sends back floats in integers, I think it's because they
        // were super early adopters of Arrow so they had to use what they could.
        DataType::Int32 => {
            if field_metadata.get("scale").unwrap() == "0" {
                ReturnType::Int32(
                    array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<Option<&i32>>>(),
                )
            } else {
                let scale = field_metadata.get("scale").unwrap().parse::<i32>().unwrap();
                ReturnType::Float64(float_to_vecs(array, scale))
            }
        }
        DataType::Binary => ReturnType::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray<i32>>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
        ),

        DataType::Struct(_f) => {
            let logical_type = field_metadata.get("logicalType").unwrap().as_str();
            match logical_type {
                "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                    ReturnType::String(convert_timestamps(array.as_any()))
                }
                _a => ReturnType::Missing(vec![Some("".to_string())]),
            }
        }

        _ => ReturnType::Missing(vec![Some("missing value".to_string())]),
    }
}

#[inline]
pub fn float_to_vecs(array: &Arc<dyn Array>, scale: i32) -> Vec<Option<f64>> {
    array
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .map(|t| match t {
            Some(v) => Some(*v as f64 / 10f64.powi(scale)),
            None => None,
        })
        .collect::<Vec<Option<f64>>>()
}

#[inline]
pub fn convert_timestamps(column: &dyn Any) -> Vec<Option<String>> {
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
                Some(NaiveDateTime::from_timestamp(value1, value2 as u32).to_string())
            }
            None => None,
        })
        .collect::<Vec<Option<String>>>()
}

#[inline]
pub fn date32_to_dates(array: &Arc<dyn Array>) -> Vec<Option<String>> {
    array
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .map(|t| match t {
            Some(v) => Some(date32_to_date(*v).to_string()),
            None => None,
        })
        .collect::<Vec<Option<String>>>()
}
