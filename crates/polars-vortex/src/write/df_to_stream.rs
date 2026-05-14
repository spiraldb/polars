//! Convert a Polars `DataFrame` into a Vector of struct-typed Vortex arrays — the
//! input shape that `VortexWriteOptions::write` expects after wrapping in an
//! [`ArrayStream`](vortex::array::stream::ArrayStream).
//!
//! Each Polars chunk becomes one upstream `arrow_array::RecordBatch`, which Vortex's
//! `ArrayRef::from_arrow(&StructArray, …)` ingests directly. The C-ABI bridge in
//! [`crate::write::array_bridge`] moves the per-column buffers zero-copy.

use std::mem;
use std::sync::Arc;

use arrow::array::Array as PolarsArray;
use arrow::datatypes::ArrowSchema as PolarsArrowSchema;
use arrow::ffi::{ArrowSchema as PolarsFfiSchema, export_field_to_c};
use arrow_array::StructArray;
use arrow_array::ffi::FFI_ArrowSchema;
use arrow_schema::{Field as UpstreamField, Schema as UpstreamSchema};
use polars_core::frame::DataFrame;
use polars_core::prelude::CompatLevel;
use polars_core::schema::{Schema as PolarsSchema, SchemaExt};
use polars_error::{PolarsResult, polars_err};
use vortex::array::ArrayRef as VortexArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::dtype::{DType, Nullability};
use vortex::dtype::arrow::FromArrowType;

use crate::write::array_bridge::polars_chunk_to_upstream_record_batch;

/// Convert a [`DataFrame`] into struct-typed Vortex arrays (one per row chunk) plus
/// the top-level Vortex [`DType`] derived from the upstream Arrow schema produced
/// by the bridge. All buffer moves are zero-copy via the C ABI.
///
/// All columns must share the same chunk count — call `df.rechunk()` first if they
/// don't. Returns `(top_dtype, chunks)`.
pub fn dataframe_to_vortex_chunks(
    df: &DataFrame,
) -> PolarsResult<(DType, Vec<VortexArrayRef>)> {
    // Build the polars-arrow schema once.
    let pl_schema = df.schema();
    let pl_arrow_schema: PolarsArrowSchema = pl_schema.to_arrow(CompatLevel::newest());

    let columns = df.columns();
    if columns.is_empty() {
        // Empty schema → empty struct; the writer will emit a header-only file.
        return Ok((
            DType::Struct(
                vortex::dtype::StructFields::new(
                    vortex::dtype::FieldNames::default(),
                    vec![],
                ),
                Nullability::NonNullable,
            ),
            Vec::new(),
        ));
    }

    let n_chunks = columns[0].as_materialized_series().chunks().len();
    for c in &columns[1..] {
        let nc = c.as_materialized_series().chunks().len();
        if nc != n_chunks {
            return Err(polars_err!(ComputeError:
                "vortex write: misaligned chunks ({n_chunks} vs {nc}); call `df.rechunk()` first"));
        }
    }

    let mut chunks = Vec::with_capacity(n_chunks);
    let mut top_dtype: Option<DType> = None;
    for chunk_idx in 0..n_chunks {
        // polars-arrow stores chunks as `Box<dyn Array>`; clone each via the trait's
        // `to_boxed` (cheap — buffers stay shared).
        let column_boxes: Vec<Box<dyn PolarsArray>> = columns
            .iter()
            .map(|c| {
                c.as_materialized_series()
                    .chunks()
                    .get(chunk_idx)
                    .expect("chunk index in range")
                    .to_boxed()
            })
            .collect();

        // Bridge polars columns → upstream `RecordBatch` via the C ABI.
        let rb = polars_chunk_to_upstream_record_batch(column_boxes, &pl_arrow_schema)?;

        // Vortex doesn't have a direct `RecordBatch -> ArrayRef`, but does have
        // `FromArrowArray<&StructArray>`. RecordBatch -> StructArray is a no-op
        // wrap (same schema, same columns).
        let schema: &Arc<UpstreamSchema> = rb.schema_ref();
        if top_dtype.is_none() {
            // Top-level struct DType, derived from the upstream schema.
            top_dtype = Some(<DType as FromArrowType<&UpstreamSchema>>::from_arrow(
                schema.as_ref(),
            ));
        }
        let struct_array: StructArray = rb.into();
        let arr = <VortexArrayRef as FromArrowArray<&StructArray>>::from_arrow(
            &struct_array,
            false,
        )
        .map_err(|e| polars_err!(ComputeError: "vortex write: from_arrow StructArray: {e}"))?;
        chunks.push(arr);
    }

    let top_dtype = top_dtype.expect("at least one chunk");
    Ok((top_dtype, chunks))
}

/// Derive a top-level Vortex `DType::Struct` from a Polars `Schema` without needing
/// any data. Used by the streaming sink to know the stream's dtype before the first
/// morsel arrives.
///
/// Goes polars `Schema` → polars-arrow `ArrowSchema` → upstream `arrow_schema::Schema`
/// (field-by-field via the C-ABI struct transmute) → Vortex `DType` via
/// `FromArrowType<&Schema>`.
pub fn polars_schema_to_vortex_dtype(pl_schema: &PolarsSchema) -> PolarsResult<DType> {
    let pl_arrow_schema = pl_schema.to_arrow(CompatLevel::newest());
    let mut up_fields: Vec<UpstreamField> = Vec::with_capacity(pl_arrow_schema.len());
    for (_, pl_field) in pl_arrow_schema.iter() {
        let pl_ffi: PolarsFfiSchema = export_field_to_c(pl_field);
        const _: () = assert!(mem::size_of::<PolarsFfiSchema>() == mem::size_of::<FFI_ArrowSchema>());
        // SAFETY: both structs are `#[repr(C)]` with the Arrow C Data Interface layout.
        let up_ffi: FFI_ArrowSchema = unsafe { mem::transmute(pl_ffi) };
        let up_field = UpstreamField::try_from(&up_ffi).map_err(|e| {
            polars_err!(ComputeError: "vortex write: schema FFI Field conversion: {e}")
        })?;
        up_fields.push(up_field);
    }
    let up_schema = UpstreamSchema::new(up_fields);
    Ok(<DType as FromArrowType<&UpstreamSchema>>::from_arrow(&up_schema))
}
