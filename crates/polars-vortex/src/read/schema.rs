//! Vortex `DType` → polars-arrow `ArrowSchema` translation.
//!
//! Polars maintains its own internal fork of `arrow` (called `polars-arrow` and re-exported
//! as `arrow` within the workspace), so we cannot reuse Vortex's `DType::to_arrow_schema()`
//! (which emits upstream `arrow_schema` types). This module is a parallel implementation
//! that mirrors [`vortex::dtype::DType::to_arrow_dtype`] case-for-case but emits the polars
//! variants.

use std::sync::Arc;

use arrow::datatypes::{
    ArrowDataType, ArrowSchema, Field as ArrowField, TimeUnit as ArrowTimeUnit,
};
use polars_core::prelude::Schema;
use polars_core::schema::{SchemaExt, SchemaRef};
use polars_error::{PolarsResult, polars_bail, polars_err};
use polars_utils::pl_str::PlSmallStr;
use vortex::array::extension::datetime::AnyTemporal;
use vortex::array::extension::datetime::TemporalMetadata;
use vortex::array::extension::datetime::TimeUnit as VortexTimeUnit;
use vortex::dtype::{DType, Nullability, PType};

/// Translate a top-level Vortex [`DType::Struct`] (the shape every Vortex file's schema takes)
/// into both a polars-arrow [`ArrowSchema`] and a Polars [`Schema`].
///
/// We keep both since the streaming reader uses Arrow for `RecordBatch` decoding while the
/// rest of the Polars stack wants the polars [`Schema`] view.
pub fn vortex_dtype_to_schema(dt: &DType) -> PolarsResult<(SchemaRef, Arc<ArrowSchema>)> {
    let DType::Struct(struct_fields, nullable) = dt else {
        polars_bail!(ComputeError:
            "only DType::Struct can be the top-level Vortex file schema, got {:?}", dt);
    };
    if *nullable != Nullability::NonNullable {
        polars_bail!(ComputeError:
            "top-level struct in a Vortex schema must be NonNullable");
    }

    let mut arrow = ArrowSchema::with_capacity(struct_fields.names().len());
    for (name, field_dtype) in struct_fields.names().iter().zip(struct_fields.fields()) {
        let field = ArrowField::new(
            PlSmallStr::from(name.as_ref()),
            vortex_dtype_to_arrow_dtype(&field_dtype)?,
            field_dtype.is_nullable(),
        );
        arrow.insert(field.name.clone(), field);
    }
    let arrow = Arc::new(arrow);
    let pl = Arc::new(<Schema as SchemaExt>::from_arrow_schema(arrow.as_ref()));
    Ok((pl, arrow))
}

/// Recursive: translate a Vortex `DType` into a polars-arrow `ArrowDataType`.
pub fn vortex_dtype_to_arrow_dtype(dt: &DType) -> PolarsResult<ArrowDataType> {
    Ok(match dt {
        DType::Null => ArrowDataType::Null,
        DType::Bool(_) => ArrowDataType::Boolean,
        DType::Primitive(ptype, _) => match ptype {
            PType::U8 => ArrowDataType::UInt8,
            PType::U16 => ArrowDataType::UInt16,
            PType::U32 => ArrowDataType::UInt32,
            PType::U64 => ArrowDataType::UInt64,
            PType::I8 => ArrowDataType::Int8,
            PType::I16 => ArrowDataType::Int16,
            PType::I32 => ArrowDataType::Int32,
            PType::I64 => ArrowDataType::Int64,
            PType::F16 => ArrowDataType::Float16,
            PType::F32 => ArrowDataType::Float32,
            PType::F64 => ArrowDataType::Float64,
        },
        DType::Decimal(dec, _) => {
            let precision = dec.precision() as usize;
            let scale = dec.scale() as usize;
            // Mirrors vortex-array's DataType selection: <=38 → 128-bit, >=39 → 256-bit.
            // polars-arrow's `Decimal` variant is 128-bit; `Decimal256` is the 256-bit.
            if precision <= 38 {
                ArrowDataType::Decimal(precision, scale)
            } else {
                ArrowDataType::Decimal256(precision, scale)
            }
        }
        // Vortex's Utf8/Binary map to polars-arrow's view variants — matches what
        // Vortex's own to_arrow_dtype emits and what the file format produces at decode.
        DType::Utf8(_) => ArrowDataType::Utf8View,
        DType::Binary(_) => ArrowDataType::BinaryView,
        DType::List(elem_dtype, _) => {
            let inner = ArrowField::new(
                PlSmallStr::from("item"),
                vortex_dtype_to_arrow_dtype(elem_dtype)?,
                elem_dtype.is_nullable(),
            );
            ArrowDataType::List(Box::new(inner))
        }
        DType::FixedSizeList(elem_dtype, size, _) => {
            let inner = ArrowField::new(
                PlSmallStr::from("item"),
                vortex_dtype_to_arrow_dtype(elem_dtype)?,
                elem_dtype.is_nullable(),
            );
            ArrowDataType::FixedSizeList(Box::new(inner), *size as usize)
        }
        DType::Struct(fields, _) => {
            let mut out = Vec::with_capacity(fields.names().len());
            for (name, field_dt) in fields.names().iter().zip(fields.fields()) {
                out.push(ArrowField::new(
                    PlSmallStr::from(name.as_ref()),
                    vortex_dtype_to_arrow_dtype(&field_dt)?,
                    field_dt.is_nullable(),
                ));
            }
            ArrowDataType::Struct(out)
        }
        DType::Union(_) => {
            polars_bail!(ComputeError: "Vortex Union types are not yet supported in Polars")
        }
        DType::Variant(_) => {
            polars_bail!(ComputeError: "Vortex Variant types are not yet supported in Polars")
        }
        DType::Extension(ext) => {
            // Temporal extensions are the only common case; everything else is bailed for now.
            if let Some(temporal) = ext.metadata_opt::<AnyTemporal>() {
                return Ok(match temporal {
                    TemporalMetadata::Timestamp(unit, tz) => ArrowDataType::Timestamp(
                        vortex_time_unit_to_arrow(*unit, "Timestamp")?,
                        tz.as_ref().map(|s| PlSmallStr::from(s.as_ref())),
                    ),
                    TemporalMetadata::Date(unit) => match unit {
                        VortexTimeUnit::Days => ArrowDataType::Date32,
                        VortexTimeUnit::Milliseconds => ArrowDataType::Date64,
                        other => polars_bail!(ComputeError:
                            "invalid Vortex time unit {:?} for Date extension", other),
                    },
                    TemporalMetadata::Time(unit) => match unit {
                        VortexTimeUnit::Seconds => ArrowDataType::Time32(ArrowTimeUnit::Second),
                        VortexTimeUnit::Milliseconds => {
                            ArrowDataType::Time32(ArrowTimeUnit::Millisecond)
                        }
                        VortexTimeUnit::Microseconds => {
                            ArrowDataType::Time64(ArrowTimeUnit::Microsecond)
                        }
                        VortexTimeUnit::Nanoseconds => {
                            ArrowDataType::Time64(ArrowTimeUnit::Nanosecond)
                        }
                        VortexTimeUnit::Days => polars_bail!(ComputeError:
                            "invalid Vortex time unit Days for Time extension"),
                    },
                });
            }
            polars_bail!(ComputeError:
                "unsupported Vortex extension type \"{}\"", ext.id())
        }
    })
}

fn vortex_time_unit_to_arrow(
    unit: VortexTimeUnit,
    context: &'static str,
) -> PolarsResult<ArrowTimeUnit> {
    Ok(match unit {
        VortexTimeUnit::Seconds => ArrowTimeUnit::Second,
        VortexTimeUnit::Milliseconds => ArrowTimeUnit::Millisecond,
        VortexTimeUnit::Microseconds => ArrowTimeUnit::Microsecond,
        VortexTimeUnit::Nanoseconds => ArrowTimeUnit::Nanosecond,
        VortexTimeUnit::Days => {
            return Err(polars_err!(ComputeError:
                "invalid Vortex time unit Days for {}", context));
        }
    })
}
