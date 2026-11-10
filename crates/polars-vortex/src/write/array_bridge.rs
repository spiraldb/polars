//! Arrow C Data Interface export for logical Polars arrays.

use std::mem;
use std::sync::Arc;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{ArrayRef as UpstreamArrayRef, make_array};
use polars_arrow::array::Array as PolarsArray;
use polars_arrow::datatypes::Field as PolarsField;
use polars_arrow::ffi::{
    ArrowArray as PolarsFfiArray, ArrowSchema as PolarsFfiSchema, export_array_to_c,
    export_field_to_c,
};
use polars_error::{PolarsResult, polars_ensure, polars_err};

pub fn polars_array_to_upstream(
    array: Box<dyn PolarsArray>,
    field: &PolarsField,
) -> PolarsResult<UpstreamArrayRef> {
    const _: () = assert!(mem::size_of::<PolarsFfiArray>() == mem::size_of::<FFI_ArrowArray>());
    const _: () = assert!(mem::align_of::<PolarsFfiArray>() == mem::align_of::<FFI_ArrowArray>());
    const _: () = assert!(mem::size_of::<PolarsFfiSchema>() == mem::size_of::<FFI_ArrowSchema>());
    const _: () = assert!(mem::align_of::<PolarsFfiSchema>() == mem::align_of::<FFI_ArrowSchema>());

    polars_ensure!(array.dtype() == &field.dtype, SchemaMismatch:
        "Vortex Arrow export dtype {:?} does not match field {:?}", array.dtype(), field.dtype);
    let expected_len = array.len() as i64;

    let polars_array_ffi: PolarsFfiArray = export_array_to_c(array);
    let polars_schema_ffi: PolarsFfiSchema = export_field_to_c(field);

    // SAFETY: same `#[repr(C)]` layout per the Arrow C Data Interface spec —
    let up_array: FFI_ArrowArray = unsafe { mem::transmute(polars_array_ffi) };
    let up_schema: FFI_ArrowSchema = unsafe { mem::transmute(polars_schema_ffi) };

    // SAFETY: structs came from a valid polars-arrow export, satisfying C ABI invariants.
    let array_data = unsafe { arrow_array::ffi::from_ffi(up_array, &up_schema) }
        .map_err(|e| polars_err!(ComputeError: "vortex write FFI import: {e}"))?;
    let imported = make_array(array_data);

    if imported.len() as i64 != expected_len {
        return Err(polars_err!(ComputeError:
            "vortex bridge: FFI round-trip length mismatch (polars={expected_len}, \
             upstream={}). This is a polars-arrow / arrow-rs layout incompatibility — \
             please file an issue.", imported.len()));
    }

    Ok(imported)
}

#[allow(clippy::disallowed_types)]
pub fn polars_chunk_to_upstream_record_batch(
    columns: Vec<Box<dyn PolarsArray>>,
    polars_schema: &polars_arrow::datatypes::ArrowSchema,
    num_rows: usize,
) -> PolarsResult<arrow_array::RecordBatch> {
    use std::collections::HashMap;

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
        let (polars_name, polars_field) = polars_schema.get_at_index(col_idx).ok_or_else(|| {
            polars_err!(ComputeError:
                "vortex write: column index {} outside schema (len {})",
                col_idx, polars_schema.len())
        })?;

        let arrow_array = polars_array_to_upstream(polars_array, polars_field)?;
        let dt: DataType = arrow_array.data_type().clone();
        let is_nullable = polars_field.is_nullable;
        let mut field = Field::new(polars_name.as_str(), dt, is_nullable);
        if let Some(md) = polars_field.metadata.as_ref()
            && !md.is_empty()
        {
            let upstream_md: HashMap<String, String> = md
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect();
            field = field.with_metadata(upstream_md);
        }
        upstream_fields.push(field);
        upstream_arrays.push(arrow_array);
    }
    let schema = Arc::new(Schema::new(upstream_fields));
    RecordBatch::try_new_with_options(
        schema,
        upstream_arrays,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .map_err(|e| polars_err!(ComputeError: "vortex write RecordBatch::try_new: {e}"))
}
