//! Export logical Polars columns before Vortex conversion.

use std::borrow::Cow;
use std::mem;

use arrow_array::ffi::FFI_ArrowSchema;
use arrow_schema::{Field, Schema as UpstreamSchema};
use polars_arrow::datatypes::ArrowSchema;
use polars_arrow::ffi::{ArrowSchema as CArrowSchema, export_field_to_c};
use polars_core::prelude::{CompatLevel, DataFrame};
use polars_core::schema::{Schema, SchemaExt};
use polars_error::{PolarsResult, polars_ensure, to_compute_err};
use vortex::array::ArrayRef;
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::DType;

use super::array_bridge::polars_chunk_to_upstream_record_batch;
use crate::session::session;

pub fn dataframe_to_vortex_chunks(df: &DataFrame) -> PolarsResult<(DType, Vec<ArrayRef>)> {
    validate_schema(df.schema().as_ref())?;
    let arrow_schema = df.schema().to_arrow(CompatLevel::newest());
    let top_dtype = polars_arrow_schema_to_vortex_dtype(&arrow_schema)?;
    let mut aligned = df.clone();
    let mut chunks = Vec::new();
    // split_chunks aligns boundaries and slices scalar columns to each batch.
    for frame in aligned.split_chunks() {
        let arrays = frame
            .columns()
            .iter()
            .zip(arrow_schema.iter_values())
            .map(|(column, field)| {
                column
                    .as_materialized_series()
                    .to_arrow_with_field(0, Cow::Borrowed(field), true)
            })
            .collect::<PolarsResult<Vec<_>>>()?;
        let batch = polars_chunk_to_upstream_record_batch(arrays, &arrow_schema, frame.height())?;
        let schema = batch.schema();
        chunks.push(
            session()
                .arrow()
                .from_arrow_record_batch(batch, &schema)
                .map_err(to_compute_err)?,
        );
    }
    // A zero-column frame still has a row count. split_chunks can emit no batches.
    if df.width() == 0 && df.height() != 0 && chunks.is_empty() {
        let batch = polars_chunk_to_upstream_record_batch(Vec::new(), &arrow_schema, df.height())?;
        let schema = batch.schema();
        chunks.push(
            session()
                .arrow()
                .from_arrow_record_batch(batch, &schema)
                .map_err(to_compute_err)?,
        );
    }
    Ok((top_dtype, chunks))
}

pub fn polars_schema_to_vortex_dtype(schema: &Schema) -> PolarsResult<DType> {
    validate_schema(schema)?;
    polars_arrow_schema_to_vortex_dtype(&schema.to_arrow(CompatLevel::newest()))
}

pub fn polars_arrow_schema_to_vortex_dtype(schema: &ArrowSchema) -> PolarsResult<DType> {
    const _: () = assert!(mem::size_of::<CArrowSchema>() == mem::size_of::<FFI_ArrowSchema>());
    const _: () = assert!(mem::align_of::<CArrowSchema>() == mem::align_of::<FFI_ArrowSchema>());
    let fields = schema
        .iter_values()
        .map(|field| {
            // SAFETY: both repr(C) structs implement the Arrow C schema ABI.
            let ffi: FFI_ArrowSchema = unsafe { mem::transmute(export_field_to_c(field)) };
            Field::try_from(&ffi).map_err(to_compute_err)
        })
        .collect::<PolarsResult<Vec<_>>>()?;
    session()
        .arrow()
        .from_arrow_schema(&UpstreamSchema::new(fields))
        .map_err(to_compute_err)
}

fn validate_schema(schema: &Schema) -> PolarsResult<()> {
    for (name, dtype) in schema.iter() {
        polars_ensure!(!dtype.contains_objects(), InvalidOperation:
            "Vortex does not support Object values in column {name}");
        dtype.try_visit_with(|nested| {
            polars_ensure!(!nested.is_extension(), InvalidOperation:
                "Vortex does not preserve custom extension types in column {name}");
            Ok(())
        })?;
        polars_ensure!(!dtype.contains_categoricals() && !dtype.contains_enums(), InvalidOperation:
            "Vortex does not preserve Categorical or Enum metadata in column {name}");
    }
    Ok(())
}
