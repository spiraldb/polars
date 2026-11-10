//! Schema conversion through Vortex's Arrow exporter.

use std::sync::Arc;

use polars_arrow::datatypes::ArrowSchema;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_ensure, to_compute_err};
use vortex::dtype::{DType, Nullability};

use super::array_bridge::{import_upstream_field, polars_export_field, record_batch_to_dataframe};

pub fn vortex_dtype_to_schema(dtype: &DType) -> PolarsResult<(SchemaRef, Arc<ArrowSchema>)> {
    polars_ensure!(matches!(dtype, DType::Struct(_, Nullability::NonNullable)),
        ComputeError: "Vortex tables require a non-nullable struct schema");
    let field = polars_export_field("", dtype)?;
    let arrow_schema::DataType::Struct(fields) = field.data_type() else {
        unreachable!()
    };
    let upstream = arrow_schema::Schema::new(fields.clone());
    let mut arrow = ArrowSchema::with_capacity(upstream.fields().len());
    for field in upstream.fields() {
        let field = import_upstream_field(field)?;
        arrow.insert(field.name.clone(), field);
    }
    // Use the fallible Series importer so unsupported logical types return errors.
    let arrays = upstream
        .fields()
        .iter()
        .map(|field| arrow_array::new_empty_array(field.data_type()))
        .collect();
    let batch = arrow_array::RecordBatch::try_new_with_options(
        Arc::new(upstream),
        arrays,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(0)),
    )
    .map_err(to_compute_err)?;
    let schema = record_batch_to_dataframe(batch)?.schema().clone();
    Ok((schema, Arc::new(arrow)))
}
