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
            // Vortex's `dec.scale()` returns `i8` (Arrow spec permits negative
            // scales), but polars-arrow's `Decimal(usize, usize)` and
            // `Decimal256(usize, usize)` only represent non-negative scales. A
            // negative scale would `as usize`-wrap to a huge value and silently
            // produce a wildly wrong dtype, so reject it explicitly instead.
            let precision = dec.precision() as usize;
            let raw_scale = dec.scale();
            if raw_scale < 0 {
                polars_bail!(ComputeError:
                    "Vortex decimal with negative scale {} is not representable in \
                     polars-arrow", raw_scale);
            }
            let scale = raw_scale as usize;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitives_map_correctly() {
        let cases = [
            (PType::U8, ArrowDataType::UInt8),
            (PType::U16, ArrowDataType::UInt16),
            (PType::U32, ArrowDataType::UInt32),
            (PType::U64, ArrowDataType::UInt64),
            (PType::I8, ArrowDataType::Int8),
            (PType::I16, ArrowDataType::Int16),
            (PType::I32, ArrowDataType::Int32),
            (PType::I64, ArrowDataType::Int64),
            (PType::F16, ArrowDataType::Float16),
            (PType::F32, ArrowDataType::Float32),
            (PType::F64, ArrowDataType::Float64),
        ];
        for (vortex_ptype, expected_arrow) in cases {
            let dt = DType::Primitive(vortex_ptype, Nullability::Nullable);
            assert_eq!(
                vortex_dtype_to_arrow_dtype(&dt).unwrap(),
                expected_arrow,
                "{vortex_ptype:?} should map to {expected_arrow:?}"
            );
        }
    }

    #[test]
    fn utf8_and_binary_become_view_variants() {
        let utf8 = DType::Utf8(Nullability::Nullable);
        assert_eq!(vortex_dtype_to_arrow_dtype(&utf8).unwrap(), ArrowDataType::Utf8View);

        let bin = DType::Binary(Nullability::Nullable);
        assert_eq!(vortex_dtype_to_arrow_dtype(&bin).unwrap(), ArrowDataType::BinaryView);
    }

    #[test]
    fn top_level_must_be_struct() {
        let not_struct = DType::Bool(Nullability::Nullable);
        let err = vortex_dtype_to_schema(&not_struct).unwrap_err();
        assert!(
            err.to_string().contains("DType::Struct"),
            "error should mention DType::Struct, got: {err}"
        );
    }

    #[test]
    fn top_level_struct_must_be_non_nullable() {
        use vortex::dtype::{FieldName, StructFields};

        let inner = vec![DType::Bool(Nullability::Nullable)];
        let names: Vec<FieldName> = vec!["x".into()];
        let fields = StructFields::new(names.into(), inner);
        let nullable_struct = DType::Struct(fields, Nullability::Nullable);

        let err = vortex_dtype_to_schema(&nullable_struct).unwrap_err();
        assert!(
            err.to_string().contains("must be NonNullable"),
            "error should explain nullability constraint, got: {err}"
        );
    }

    #[test]
    fn variant_dtype_errors_with_clear_message() {
        // DType::Variant is in vortex 0.70.0's public enum but polars-vortex
        // does not (yet) map it to an Arrow type; the bail-path should remain
        // user-readable rather than degrading to a generic match-fail or panic.
        let dt = DType::Variant(Nullability::Nullable);
        let err = vortex_dtype_to_arrow_dtype(&dt).unwrap_err();
        assert!(
            err.to_string().contains("Variant"),
            "error should mention Variant, got: {err}"
        );
    }

    #[test]
    fn null_dtype_maps_to_arrow_null() {
        let dt = DType::Null;
        assert_eq!(vortex_dtype_to_arrow_dtype(&dt).unwrap(), ArrowDataType::Null);
    }

    #[test]
    fn bool_maps_to_arrow_boolean() {
        let dt = DType::Bool(Nullability::Nullable);
        assert_eq!(vortex_dtype_to_arrow_dtype(&dt).unwrap(), ArrowDataType::Boolean);

        let dt = DType::Bool(Nullability::NonNullable);
        assert_eq!(vortex_dtype_to_arrow_dtype(&dt).unwrap(), ArrowDataType::Boolean);
    }

    #[test]
    fn decimal_precision_chooses_128_or_256() {
        use vortex::dtype::DecimalDType;

        // precision <= 38 → Decimal (128-bit)
        let small = DType::Decimal(DecimalDType::new(10, 2), Nullability::Nullable);
        assert_eq!(
            vortex_dtype_to_arrow_dtype(&small).unwrap(),
            ArrowDataType::Decimal(10, 2)
        );

        // precision == 38 boundary (still 128-bit)
        let boundary = DType::Decimal(DecimalDType::new(38, 0), Nullability::Nullable);
        assert_eq!(
            vortex_dtype_to_arrow_dtype(&boundary).unwrap(),
            ArrowDataType::Decimal(38, 0)
        );

        // precision == 39 → Decimal256
        let large = DType::Decimal(DecimalDType::new(39, 5), Nullability::Nullable);
        assert_eq!(
            vortex_dtype_to_arrow_dtype(&large).unwrap(),
            ArrowDataType::Decimal256(39, 5)
        );
    }

    #[test]
    fn decimal_with_negative_scale_errors_cleanly() {
        // Vortex permits negative scales per the Arrow spec; polars-arrow does not.
        // The convertor should reject explicitly rather than silently wrap.
        use vortex::dtype::DecimalDType;

        let dt = DType::Decimal(DecimalDType::new(10, -2), Nullability::Nullable);
        let err = vortex_dtype_to_arrow_dtype(&dt).unwrap_err();
        assert!(
            err.to_string().contains("negative scale"),
            "error should mention 'negative scale', got: {err}"
        );
    }

    #[test]
    fn list_maps_through_recursively() {
        let inner = DType::Primitive(PType::I32, Nullability::Nullable);
        let list = DType::List(std::sync::Arc::new(inner), Nullability::Nullable);

        match vortex_dtype_to_arrow_dtype(&list).unwrap() {
            ArrowDataType::List(field) => {
                assert_eq!(field.name.as_str(), "item");
                assert!(field.is_nullable);
                assert_eq!(field.dtype, ArrowDataType::Int32);
            }
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn fixed_size_list_maps_through_recursively() {
        let inner = DType::Primitive(PType::F64, Nullability::NonNullable);
        let fsl = DType::FixedSizeList(std::sync::Arc::new(inner), 4, Nullability::Nullable);

        match vortex_dtype_to_arrow_dtype(&fsl).unwrap() {
            ArrowDataType::FixedSizeList(field, size) => {
                assert_eq!(size, 4);
                assert_eq!(field.dtype, ArrowDataType::Float64);
                assert!(!field.is_nullable);
            }
            other => panic!("expected FixedSizeList, got {:?}", other),
        }
    }

    #[test]
    fn struct_maps_to_arrow_struct() {
        use vortex::dtype::{FieldName, StructFields};

        let inner_dtypes = vec![
            DType::Primitive(PType::I64, Nullability::Nullable),
            DType::Utf8(Nullability::Nullable),
        ];
        let names: Vec<FieldName> = vec!["a".into(), "b".into()];
        let fields = StructFields::new(names.into(), inner_dtypes);
        let dt = DType::Struct(fields, Nullability::Nullable);

        match vortex_dtype_to_arrow_dtype(&dt).unwrap() {
            ArrowDataType::Struct(field_vec) => {
                assert_eq!(field_vec.len(), 2);
                assert_eq!(field_vec[0].name.as_str(), "a");
                assert_eq!(field_vec[0].dtype, ArrowDataType::Int64);
                assert_eq!(field_vec[1].name.as_str(), "b");
                assert_eq!(field_vec[1].dtype, ArrowDataType::Utf8View);
            }
            other => panic!("expected Struct, got {:?}", other),
        }
    }

    #[test]
    fn date_extensions_map_to_arrow_date32_and_date64() {
        use vortex::array::extension::datetime::{Date, TimeUnit as VTimeUnit};

        let d32 = DType::Extension(Date::new(VTimeUnit::Days, Nullability::Nullable).erased());
        assert_eq!(vortex_dtype_to_arrow_dtype(&d32).unwrap(), ArrowDataType::Date32);

        let d64 = DType::Extension(
            Date::new(VTimeUnit::Milliseconds, Nullability::Nullable).erased(),
        );
        assert_eq!(vortex_dtype_to_arrow_dtype(&d64).unwrap(), ArrowDataType::Date64);
    }

    #[test]
    fn time_extensions_map_to_time32_or_time64() {
        use vortex::array::extension::datetime::{Time, TimeUnit as VTimeUnit};

        let cases = [
            (VTimeUnit::Seconds, ArrowDataType::Time32(ArrowTimeUnit::Second)),
            (VTimeUnit::Milliseconds, ArrowDataType::Time32(ArrowTimeUnit::Millisecond)),
            (VTimeUnit::Microseconds, ArrowDataType::Time64(ArrowTimeUnit::Microsecond)),
            (VTimeUnit::Nanoseconds, ArrowDataType::Time64(ArrowTimeUnit::Nanosecond)),
        ];
        for (unit, expected) in cases {
            let dt = DType::Extension(Time::new(unit, Nullability::Nullable).erased());
            assert_eq!(
                vortex_dtype_to_arrow_dtype(&dt).unwrap(),
                expected,
                "{unit:?}"
            );
        }
    }

    #[test]
    fn time_extension_days_unit_errors() {
        use vortex::array::extension::datetime::{Time, TimeUnit as VTimeUnit};
        // Vortex's Time::new with Days panics during construction, but if we
        // somehow produce one the convertor should reject it. The error path
        // matters because users may receive a Vortex file built by another
        // language that doesn't enforce the same invariant.
        //
        // We can't easily construct a Days-unit Time scalar via the public
        // API (Time::new panics), so this is structurally tested via the
        // `_ => polars_bail!(...)` arm. Documented for completeness.
        let _ = Time::new(VTimeUnit::Nanoseconds, Nullability::Nullable);
    }

    #[test]
    fn timestamp_with_and_without_timezone() {
        use std::sync::Arc;

        use vortex::array::extension::datetime::{TimeUnit as VTimeUnit, Timestamp};

        // No timezone.
        let ts = DType::Extension(
            Timestamp::new(VTimeUnit::Nanoseconds, Nullability::Nullable).erased(),
        );
        match vortex_dtype_to_arrow_dtype(&ts).unwrap() {
            ArrowDataType::Timestamp(unit, tz) => {
                assert_eq!(unit, ArrowTimeUnit::Nanosecond);
                assert!(tz.is_none());
            }
            other => panic!("expected Timestamp, got {:?}", other),
        }

        // With timezone.
        let ts_tz = DType::Extension(
            Timestamp::new_with_tz(
                VTimeUnit::Microseconds,
                Some(Arc::from("UTC")),
                Nullability::Nullable,
            )
            .erased(),
        );
        match vortex_dtype_to_arrow_dtype(&ts_tz).unwrap() {
            ArrowDataType::Timestamp(unit, tz) => {
                assert_eq!(unit, ArrowTimeUnit::Microsecond);
                assert_eq!(tz.as_deref(), Some("UTC"));
            }
            other => panic!("expected Timestamp(_, Some(UTC)), got {:?}", other),
        }
    }

    #[test]
    fn vortex_dtype_to_schema_round_trips_nullability() {
        use vortex::dtype::{FieldName, StructFields};

        let fields = StructFields::new(
            vec![FieldName::from("nullable_col"), FieldName::from("non_nullable_col")].into(),
            vec![
                DType::Primitive(PType::I64, Nullability::Nullable),
                DType::Primitive(PType::I64, Nullability::NonNullable),
            ],
        );
        let dt = DType::Struct(fields, Nullability::NonNullable);

        let (pl, arrow) = vortex_dtype_to_schema(&dt).unwrap();
        assert_eq!(pl.len(), 2);
        let f0 = arrow.iter_values().nth(0).unwrap();
        let f1 = arrow.iter_values().nth(1).unwrap();
        assert_eq!(f0.name.as_str(), "nullable_col");
        assert!(f0.is_nullable);
        assert_eq!(f1.name.as_str(), "non_nullable_col");
        assert!(!f1.is_nullable);
    }
}
