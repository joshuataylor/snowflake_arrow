use crate::helpers::get_chunks_for_data;
use crate::rows::atoms::elixir_calendar_iso;
use arrow2::array::{Array, BinaryArray, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StructArray, Utf8Array};
use arrow2::datatypes::DataType;
use arrow2::temporal_conversions::date32_to_date;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use rustler::NifStruct;
use rustler::NifMap;
use rustler::{Binary, Encoder, Env, NewBinary, Term};

mod atoms {
    rustler::atoms! {
      elixir_calendar_iso = "Elixir.Calendar.ISO"
    }
}

#[derive(NifStruct)]
#[module = "Date"]
pub struct ElixirDate {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub calendar: rustler::Atom,
}

#[derive(NifMap)]
pub struct ElixirDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: (u32, usize),
    pub calendar: rustler::Atom,
    pub hour: u32,
}

impl From<NaiveDate> for ElixirDate {
    fn from(d: NaiveDate) -> Self {
        ElixirDate {
            year: d.year(),
            month: d.month(),
            day: d.day(),
            calendar: elixir_calendar_iso(),
        }
    }
}

impl From<NaiveDateTime> for ElixirDateTime {
    fn from(d: NaiveDateTime) -> Self {
        ElixirDateTime {
            year: d.year(),
            month: d.month(),
            day: d.day(),
            hour: d.hour(),
            minute: d.minute(),
            second: d.second(),
            microsecond: (d.timestamp_subsec_micros(), 6),
            calendar: elixir_calendar_iso(),
        }
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn convert_arrow_stream_to_columns<'a>(
    env: Env<'a>,
    arrow_stream_data: Binary,
    _cast_elixir_types: bool,
) -> Term<'a> {
    let (chunks, field_metadata, stream_metadata, _total_rows) =
        get_chunks_for_data(&mut arrow_stream_data.as_ref());
    let length = stream_metadata.schema.fields.len();
    let mut grouped_arrays: Vec<(DataType, Vec<Box<dyn Array>>)> = Vec::with_capacity(length);
    let chunk_length = chunks.len();

    // group the chunks together, it's faster to encode the binaries as a single binary
    for chunk in chunks {
        for (index, array) in chunk.arrays().iter().enumerate() {
            // does this vec exist, if not push it
            if grouped_arrays.len() < index + 1 {
                grouped_arrays.push((array.data_type().clone(), Vec::with_capacity(chunk_length)));
            }
            let (_dt, gr) = grouped_arrays.get_mut(index).unwrap();
            // is a clone really necessary?
            gr.push(array.clone());
        }
    }

    let mut returned_arrays: Vec<Term> = Vec::with_capacity(length);

    for (array_index, (data_type, grp_a)) in grouped_arrays.iter().enumerate() {
        let fm = field_metadata.get(&array_index).unwrap();

        match data_type {
            DataType::Utf8 => {
                let mut values: Vec<u8> = Vec::new();
                let mut last_offset: usize = 0;
                let mut offsets: Vec<usize> = Vec::new();

                // @todo move this to macro as binary is pretty much the same
                for array in grp_a {
                    let utf8_array = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

                    let mut offset_loop: usize = 0;
                    for offset in utf8_array.offsets().iter().skip(1) {
                        offset_loop = *offset as usize;
                        offsets.push(last_offset + offset_loop);
                    }

                    values.extend(utf8_array.values().as_slice());

                    last_offset += offset_loop;
                }

                returned_arrays.push(make_sub_binaries(env, &values, &offsets).encode(env));
            }
            DataType::Boolean => {
                returned_arrays.push(
                    grp_a
                        .iter()
                        .flat_map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap())
                        .collect::<Vec<Option<bool>>>()
                        .encode(env),
                );
            }
            DataType::Int32 => {
                if fm.get("scale").unwrap() == "0" {
                    returned_arrays.push(
                        grp_a
                            .iter()
                            .flat_map(|array| array.as_any().downcast_ref::<Int32Array>().unwrap())
                            .collect::<Vec<Option<&i32>>>()
                            .encode(env),
                    );
                } else {
                    let scale = fm.get("scale").unwrap().parse::<i32>().unwrap();

                    returned_arrays.push(
                        grp_a
                            .iter()
                            .flat_map(|a| {
                                a.as_any()
                                    .downcast_ref::<Int32Array>()
                                    .unwrap()
                                    .iter()
                                    .map(|v| v.map(|x| *x as f64 / 10f64.powi(scale) as f64))
                            })
                            .collect::<Vec<Option<f64>>>()
                            .encode(env),
                    );
                }
            }
            DataType::Int16 => {
                returned_arrays.push(
                    grp_a
                        .iter()
                        .flat_map(|array| array.as_any().downcast_ref::<Int16Array>().unwrap())
                        .collect::<Vec<Option<&i16>>>()
                        .encode(env),
                );
            }
            DataType::Int8 => {
                returned_arrays.push(
                    grp_a
                        .iter()
                        .flat_map(|array| array.as_any().downcast_ref::<Int8Array>().unwrap())
                        .collect::<Vec<Option<&i8>>>()
                        .encode(env),
                );
            }
            DataType::Int64 => {
                returned_arrays.push(
                    grp_a
                        .iter()
                        .flat_map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap())
                        .collect::<Vec<Option<&i64>>>()
                        .encode(env),
                );
            }
            DataType::Float64 => {
                returned_arrays.push(
                    grp_a
                        .iter()
                        .flat_map(|array| array.as_any().downcast_ref::<Float64Array>().unwrap())
                        .collect::<Vec<Option<&f64>>>()
                        .encode(env),
                );
            }
            DataType::Date32 => {
                returned_arrays.push(
                    grp_a
                        .iter()
                        .flat_map(|array| array.as_any().downcast_ref::<Int32Array>().unwrap())
                        .map(|x| x.map(|date| date32_to_date(*date).into()))
                        .collect::<Vec<Option<ElixirDate>>>()
                        .encode(env)
                );
            }
            // DataType::Date32 => {
            //     returned_arrays.push(
            //         grp_a
            //             .iter()
            //             .flat_map(|array| array.as_any().downcast_ref::<Int32Array>().unwrap())
            //             .map(|x| x.map(|date| {
            //                 let d = date32_to_date(*date);
            //                 (d.year(), d.month(), d.day())
            //             }))
            //             .collect::<Vec<Option<(i32, u32, u32)>>>()
            //             .encode(env)
            //     );
            // }
            DataType::Struct(_f) => {
                let logical_type = fm.get("logicalType").unwrap().as_str();
                match logical_type {
                    "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
                        // Returning these as tuples is faster than returning a struct
                        // This is because the struct has to be encoded by Rustler, and bulk encoding 130k+ structs is slow
                        // @todo figure out a better way to do this
                        let mut datetimes: Vec<Option<(i32, u32, u32, u32, u32, u32, u32)>> = vec![];
                        for array in grp_a {
                            let (_fields, struct_arrays, _bitmap) = array
                                .as_any()
                                .downcast_ref::<StructArray>()
                                .unwrap()
                                .clone()
                                .into_data();

                            // Get the fractional parts from the second array
                            let fractions = struct_arrays[1]
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap();

                            datetimes.extend(struct_arrays[0]
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .iter()
                                .enumerate()
                                .map(|(i, x)| x.map(|&epoch| {
                                    let ts = NaiveDateTime::from_timestamp(epoch, fractions.value(i) as u32);

                                    (ts.year(), ts.month(), ts.day(), ts.hour(), ts.minute(), ts.second(), ts.timestamp_subsec_micros())
                                })));
                        }

                        returned_arrays.push(datetimes.encode(env));
                    }
                    _ => unreachable!(),
                }
            }
            DataType::Binary => {
                let mut values: Vec<u8> = Vec::new();
                let mut last_offset: usize = 0;
                let mut offsets: Vec<usize> = Vec::new();

                for array in grp_a {
                    let utf8_array = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();

                    let mut offset_loop: usize = 0;
                    for offset in utf8_array.offsets().iter().skip(1) {
                        offset_loop = *offset as usize;
                        offsets.push(last_offset + offset_loop);
                    }

                    values.extend(utf8_array.values().as_slice());

                    last_offset += offset_loop;
                }

                returned_arrays.push(make_sub_binaries(env, &values, &offsets).encode(env));
            }
            _dt => {
                // unreachable!("Unsupported data type {:?}", dt);
            }
        }
    }
    returned_arrays.encode(env)
}

#[inline]
fn make_sub_binaries<'a>(env: Env<'a>, values: &Vec<u8>, offsets: &Vec<usize>) -> Vec<Option<Binary<'a>>> {
    let mut values_binary = NewBinary::new(env, values.len());
    values_binary
        .as_mut_slice()
        .copy_from_slice(values.as_slice());

    let binary: Binary = values_binary.into();

    let mut last_offset: usize = 0;
    let mut binaries: Vec<Option<Binary>> = Vec::with_capacity(offsets.len());
    for &offset in offsets {
        if last_offset == offset {
            last_offset = offset;
            binaries.push(None);
        } else {
            let slice = binary
                .make_subbinary(last_offset, offset - last_offset)
                .unwrap();
            binaries.push(Some(slice));
            last_offset = offset;
        }
    }

    binaries
}