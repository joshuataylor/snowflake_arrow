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
) -> Vec<ReturnType<'a>> {
    match array.data_type() {
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap()
            .iter()
            .map(|x| ReturnType::Int64(x))
            .collect::<Vec<ReturnType>>(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap()
            .iter()
            .map(|x| ReturnType::Float64(x))
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
            .map(|x| ReturnType::Utf8(x))
            .collect(),
        DataType::Date32 => date32_to_dates(array),
        // Snowflake sends back floats in integers, I think it's because they
        // were super early adopters of Arrow so they had to use what they could.
        DataType::Int32 => {
            if field_metadata.get("scale").unwrap() == "0" {
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|x| ReturnType::Int32(x))
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
            .map(|x| match &x {
                Some(x) => ReturnType::Binary(Some(x)),
                None => ReturnType::Binary(None),
            })
            .collect::<Vec<ReturnType>>(),
        DataType::Struct(_f) => {
            let logical_type = field_metadata.get("logicalType").unwrap().as_str();
            match logical_type {
                "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                    convert_timestamps(array.as_any())
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
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
pub fn convert_timestamps(column: &dyn Any) -> Vec<ReturnType> {
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
                ReturnType::String(Some(
                    NaiveDateTime::from_timestamp(value1, value2 as u32).to_string(),
                ))
            }
            None => ReturnType::String(None),
        })
        .collect::<Vec<ReturnType>>()
}

#[inline]
pub fn date32_to_dates(array: &Arc<dyn Array>) -> Vec<ReturnType> {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<i32>>()
        .unwrap()
        .iter()
        .map(|t| match t {
            Some(t) => ReturnType::String(Some(date32_to_date(*t).to_string())),
            None => ReturnType::String(None),
        })
        .collect::<Vec<ReturnType>>()
}
