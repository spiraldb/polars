//! Zero-copy bridge from Vortex's upstream-arrow `ArrayRef` / `RecordBatch` to Polars'
//! internal-fork polars-arrow types via the Arrow C Data Interface.
//!
//! ## Why this exists
//!
//! Vortex's `ArrowArrayExecutor::execute_record_batch` produces a
//! [`arrow_array::RecordBatch`] (upstream Arrow). Polars uses its own
//! [`polars-arrow`](arrow) fork, which is nominally a distinct crate with distinct
//! `ArrowDataType` / `Array` enums. We can't directly hand an upstream array to
//! `Series::from_arrow` — but the two crates share the *exact* same Arrow C Data
//! Interface struct layout (both `#[repr(C)]`, both 9 fields in the same order, per the
//! [C Data Interface spec](https://arrow.apache.org/docs/format/CDataInterface.html)).
//! So we go through the C ABI: upstream → `FFI_ArrowArray` → `transmute` → polars-arrow
//! `ArrowArray` → `import_array_from_c`. Zero buffer copy — only the 9-field C-ABI struct
//! is moved (~80 bytes).
//!
//! Polars-arrow's own `import_array_from_c` doesn't take a `Field`/`Schema`; it takes a
//! `Box<dyn Array>` + a known polars-arrow `ArrowDataType`. We already have the dtype
//! from [`super::schema::vortex_dtype_to_arrow_dtype`], so we plumb that through column
//! by column.

use std::mem;

use arrow::ffi::ArrowArray as PolarsFfiArray;
use arrow::array::ArrayRef as PolarsArrayRef;
use arrow::datatypes::ArrowDataType as PolarsArrowDataType;
use arrow_array::RecordBatch as UpstreamRecordBatch;
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::Schema as UpstreamSchema;
use polars_core::frame::DataFrame;
use polars_core::frame::column::IntoColumn;
use polars_core::prelude::Schema as PolarsSchema;
use polars_core::series::Series;
use polars_error::{PolarsResult, polars_err};
use polars_utils::pl_str::PlSmallStr;

/// Convert an upstream `RecordBatch` produced by Vortex into a Polars `DataFrame`,
/// reusing the columnar buffers via the C Data Interface.
///
/// `polars_schema` and `polars_dtypes` must be aligned with `record_batch.columns()` —
/// they come from [`super::schema::vortex_dtype_to_schema`] at file-open time, so this
/// alignment is naturally maintained by the streaming reader. The `polars_schema` is
/// used only for column names; per-column dtypes drive the Arrow import.
pub fn record_batch_to_dataframe(
    record_batch: UpstreamRecordBatch,
    polars_schema: &PolarsSchema,
    polars_dtypes: &[PolarsArrowDataType],
) -> PolarsResult<DataFrame> {
    debug_assert_eq!(record_batch.num_columns(), polars_dtypes.len());
    debug_assert_eq!(record_batch.num_columns(), polars_schema.len());

    let num_rows = record_batch.num_rows();
    let mut columns = Vec::with_capacity(record_batch.num_columns());

    for ((col_idx, upstream_array), polars_dtype) in record_batch
        .columns()
        .iter()
        .enumerate()
        .zip(polars_dtypes.iter())
    {
        let polars_array = ffi_transfer_array(upstream_array.as_ref(), polars_dtype.clone())?;
        let name = polars_schema
            .get_at_index(col_idx)
            .ok_or_else(|| {
                polars_err!(ComputeError:
                    "vortex bridge: column index {} outside Polars schema (len {})",
                    col_idx, polars_schema.len())
            })?
            .0
            .clone();
        let series = Series::from_arrow(name, polars_array)?;
        columns.push(series.into_column());
    }

    DataFrame::new(num_rows, columns)
}

/// Move one upstream `arrow_array::Array` into polars-arrow via the C Data Interface.
///
/// The upstream array's backing buffers stay where they are — the C ABI export just
/// publishes pointers to them, and the polars-arrow import reattaches those pointers to
/// its own array types. The buffers are reference-counted on both sides, so dropping the
/// upstream array does not free anything that polars-arrow still holds.
fn ffi_transfer_array(
    upstream: &dyn arrow_array::Array,
    polars_dtype: PolarsArrowDataType,
) -> PolarsResult<PolarsArrayRef> {
    // Export the upstream array to an `FFI_ArrowArray` (its C ABI representation).
    // We don't need the upstream schema — we already have our own `polars_dtype`.
    let array_data = upstream.to_data();
    let (upstream_ffi, _upstream_schema_ffi) =
        arrow_array::ffi::to_ffi(&array_data).map_err(|e| {
            polars_err!(ComputeError:
                "vortex bridge: failed to export upstream Arrow array to FFI: {e}")
        })?;

    // Re-interpret the 9-field C-ABI struct as polars-arrow's `ArrowArray`. The two
    // structs have identical `#[repr(C)]` layout (this is exactly the property the C
    // Data Interface guarantees), so this transmute is safe — only the Rust-level type
    // changes, the bytes do not. Size assertion at compile time:
    const _: () = assert!(mem::size_of::<FFI_ArrowArray>() == mem::size_of::<PolarsFfiArray>());

    // SAFETY: both structs are `#[repr(C)]` with the layout mandated by the Arrow C Data
    // Interface (length, null_count, offset, n_buffers, n_children, buffers, children,
    // dictionary, release, private_data). The release callback in `FFI_ArrowArray` will
    // correctly free the upstream-array's private_data when polars-arrow drops it.
    let polars_ffi: PolarsFfiArray = unsafe { mem::transmute(upstream_ffi) };

    // SAFETY: `polars_ffi` came from a valid `to_ffi` export, satisfying the
    // C Data Interface invariants.
    unsafe { arrow::ffi::import_array_from_c(polars_ffi, polars_dtype) }
}

/// Capture each column's polars-arrow `ArrowDataType` from a polars-arrow schema, in the
/// same field order. This is the input expected by [`record_batch_to_dataframe`].
pub fn arrow_dtypes_from_schema(
    arrow_schema: &arrow::datatypes::ArrowSchema,
) -> Vec<PolarsArrowDataType> {
    arrow_schema
        .iter_values()
        .map(|f| f.dtype.clone())
        .collect()
}

// Re-export the upstream types we surface in the public API so callers don't need to add
// a direct dependency on arrow-array / arrow-schema for type names.
pub use arrow_array::RecordBatch as ArrowRecordBatch;
pub use arrow_schema::Schema as ArrowUpstreamSchema;

// Allow the `_upstream_schema: UpstreamSchema` symbol to be referenced for trait
// resolution clarity in future expansions.
#[allow(unused_imports)]
use UpstreamSchema as _;
