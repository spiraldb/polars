//! Reverse C-ABI bridge: polars-arrow `Array` -> upstream `arrow_array::ArrayRef`.
//!
//! Mirror of [`crate::read::array_bridge`] in the opposite direction. Same C Data
//! Interface trick: both polars-arrow's `ArrowArray` / `ArrowSchema` and upstream
//! `FFI_ArrowArray` / `FFI_ArrowSchema` are `#[repr(C)]` with identical 9-field /
//! 8-field layouts (per the Arrow C ABI spec), so `mem::transmute` moves the
//! ~80-byte struct without touching the buffers. The buffers themselves stay
//! reference-counted on both sides.
//!
//! Used by the writer to turn a polars-arrow column into an upstream
//! `arrow_array::ArrayRef`, which Vortex's `ArrayRef::from_arrow(&dyn ArrowArray, …)`
//! can then ingest.

use std::mem;
use std::sync::Arc;

use arrow::array::{Array as PolarsArray, ArrayRef as PolarsArrayRef};
use arrow::datatypes::Field as PolarsField;
use arrow::ffi::{
    ArrowArray as PolarsFfiArray, ArrowSchema as PolarsFfiSchema, export_array_to_c,
    export_field_to_c,
};
use arrow_array::ArrayRef as UpstreamArrayRef;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::make_array;
use polars_error::{PolarsResult, polars_err};

/// Move a polars-arrow column into an upstream `arrow_array::ArrayRef` via the C
/// Data Interface. The polars-arrow buffers stay where they are — the export
/// publishes pointers, the import attaches them to upstream array structs.
pub fn polars_array_to_upstream(
    array: Box<dyn PolarsArray>,
    field: &PolarsField,
) -> PolarsResult<UpstreamArrayRef> {
    // Step 1: polars-arrow side -> polars-arrow's C-ABI structs.
    let polars_array_ffi: PolarsFfiArray = export_array_to_c(array);
    let polars_schema_ffi: PolarsFfiSchema = export_field_to_c(field);

    // Step 2: re-interpret the same bytes as upstream's C-ABI structs.
    const _: () = assert!(mem::size_of::<PolarsFfiArray>() == mem::size_of::<FFI_ArrowArray>());
    const _: () = assert!(mem::size_of::<PolarsFfiSchema>() == mem::size_of::<FFI_ArrowSchema>());

    // SAFETY: same `#[repr(C)]` layout per the Arrow C Data Interface spec. The
    // release callbacks in the polars-arrow structs correctly free the polars
    // buffers when upstream drops the import.
    let up_array: FFI_ArrowArray = unsafe { mem::transmute(polars_array_ffi) };
    let up_schema: FFI_ArrowSchema = unsafe { mem::transmute(polars_schema_ffi) };

    // Step 3: upstream import.
    // SAFETY: structs came from a valid polars-arrow export, satisfying C ABI invariants.
    let array_data = unsafe { arrow_array::ffi::from_ffi(up_array, &up_schema) }
        .map_err(|e| polars_err!(ComputeError: "vortex write FFI import: {e}"))?;
    Ok(make_array(array_data))
}

/// Convert a slice of polars-arrow column arrays + a polars-arrow schema into an
/// upstream `arrow_array::RecordBatch`. The number of columns must match the schema.
pub fn polars_chunk_to_upstream_record_batch(
    columns: Vec<Box<dyn PolarsArray>>,
    polars_schema: &arrow::datatypes::ArrowSchema,
) -> PolarsResult<arrow_array::RecordBatch> {
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};

    if columns.len() != polars_schema.len() {
        return Err(polars_err!(ComputeError:
            "vortex write: column count {} does not match schema length {}",
            columns.len(), polars_schema.len()));
    }

    let mut upstream_fields = Vec::with_capacity(columns.len());
    let mut upstream_arrays = Vec::with_capacity(columns.len());
    for (col_idx, polars_array) in columns.into_iter().enumerate() {
        let (polars_name, polars_field) = polars_schema
            .get_at_index(col_idx)
            .ok_or_else(|| polars_err!(ComputeError:
                "vortex write: column index {} outside schema (len {})",
                col_idx, polars_schema.len()))?;

        let arrow_array = polars_array_to_upstream(polars_array, polars_field)?;
        let dt: DataType = arrow_array.data_type().clone();
        let is_nullable = polars_field.is_nullable;
        upstream_fields.push(Field::new(polars_name.as_str(), dt, is_nullable));
        upstream_arrays.push(arrow_array);
    }
    let schema = Arc::new(Schema::new(upstream_fields));
    RecordBatch::try_new(schema, upstream_arrays)
        .map_err(|e| polars_err!(ComputeError: "vortex write RecordBatch::try_new: {e}"))
}
