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
//! Polars-arrow's own `import_array_from_c` doesn't take a `Field`/`Schema`; it takes
//! the FFI [`ArrowArray`](arrow::ffi::ArrowArray) struct plus a known polars-arrow
//! [`ArrowDataType`]. We already have the dtype from
//! [`super::schema::vortex_dtype_to_arrow_dtype`], so we plumb that through column by
//! column.

use std::mem;

use arrow::array::ArrayRef as PolarsArrayRef;
use arrow::datatypes::ArrowDataType as PolarsArrowDataType;
use arrow::ffi::ArrowArray as PolarsFfiArray;
use arrow_array::RecordBatch as UpstreamRecordBatch;
use arrow_array::ffi::FFI_ArrowArray;
use polars_core::frame::DataFrame;
use polars_core::frame::column::IntoColumn;
use polars_core::prelude::Schema as PolarsSchema;
use polars_core::series::Series;
use polars_error::{PolarsResult, polars_err};

/// Convert an upstream `RecordBatch` produced by Vortex into a Polars `DataFrame`,
/// reusing the columnar buffers via the C Data Interface.
///
/// `polars_schema` and `polars_dtypes` must be aligned with `record_batch.columns()` —
/// they come from [`super::schema::vortex_dtype_to_schema`] at file-open time, so this
/// alignment is naturally maintained by the streaming reader. We validate the
/// column count and (in debug builds) each column's name to catch a regression
/// that misaligns the three early.
pub fn record_batch_to_dataframe(
    record_batch: UpstreamRecordBatch,
    polars_schema: &PolarsSchema,
    polars_dtypes: &[PolarsArrowDataType],
) -> PolarsResult<DataFrame> {
    let n_cols = record_batch.num_columns();
    if n_cols != polars_dtypes.len() {
        return Err(polars_err!(ComputeError:
            "vortex bridge: record_batch has {n_cols} columns but {} polars-arrow dtypes",
            polars_dtypes.len()));
    }
    if n_cols != polars_schema.len() {
        return Err(polars_err!(ComputeError:
            "vortex bridge: record_batch has {n_cols} columns but polars_schema has {}",
            polars_schema.len()));
    }
    // Field-name parity catches the case where the upstream record_batch was
    // reordered or relabeled relative to what `vortex_dtype_to_schema` returned.
    // Gated to debug builds because it's a per-batch O(columns) string compare;
    // a regression here would surface as a `Series::from_arrow` dtype mismatch
    // anyway, but a clearer error up front is worth the debug cost.
    if cfg!(debug_assertions) {
        for (col_idx, upstream_field) in record_batch.schema_ref().fields().iter().enumerate() {
            let polars_name = &polars_schema.get_at_index(col_idx).unwrap().0;
            debug_assert_eq!(
                upstream_field.name(),
                polars_name.as_str(),
                "vortex bridge: column {col_idx} name mismatch \
                 (upstream={:?}, polars={:?})",
                upstream_field.name(),
                polars_name.as_str(),
            );
        }
    }

    let num_rows = record_batch.num_rows();
    let mut columns = Vec::with_capacity(n_cols);

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
/// ## Why a transmute
///
/// The two crates (`arrow-array`/`arrow-data` upstream and Polars' internal-fork
/// `polars-arrow`) each implement the Arrow C Data Interface independently. Both
/// `FFI_ArrowArray` (upstream) and [`PolarsFfiArray`] (polars-arrow) are
/// `#[repr(C)]` with the same 9-field layout defined by the spec:
///
/// > length: i64, null_count: i64, offset: i64, n_buffers: i64, n_children: i64,
/// > buffers: *mut *const c_void, children: *mut *mut Self,
/// > dictionary: *mut Self, release: Option<unsafe extern "C" fn(*mut Self)>,
/// > private_data: *mut c_void
///
/// The fields of polars-arrow's struct are `pub(super)`, so we can't construct
/// one field-by-field from outside the polars-arrow module — the only
/// bytes-preserving bridge is a transmute. We strengthen the compile-time check
/// with size + alignment asserts and verify the imported array's length at
/// runtime against the upstream's value (which IS accessible via pub fields).
///
/// The release callback baked into the FFI struct continues to point at
/// upstream's `*mut FFI_ArrowArray` cleanup function. polars-arrow's drop calls
/// it with `*mut PolarsFfiArray`; this is sound because the structs are
/// byte-identical (so the function reads the same memory regardless of which
/// type we cast through).
///
/// ## Buffer ownership
///
/// The upstream array's backing buffers stay where they are — the C ABI export
/// just publishes pointers to them, and the polars-arrow import reattaches those
/// pointers to its own array types. Reference counting is preserved through
/// `private_data` + `release` on both sides.
fn ffi_transfer_array(
    upstream: &dyn arrow_array::Array,
    polars_dtype: PolarsArrowDataType,
) -> PolarsResult<PolarsArrayRef> {
    // Compile-time: matching struct size AND alignment (size alone wouldn't catch
    // a hypothetical alignment divergence between the two `#[repr(C)]` definitions).
    const _: () = assert!(mem::size_of::<FFI_ArrowArray>() == mem::size_of::<PolarsFfiArray>());
    const _: () = assert!(mem::align_of::<FFI_ArrowArray>() == mem::align_of::<PolarsFfiArray>());

    // Export the upstream array to an `FFI_ArrowArray` (its C ABI representation).
    // We don't need the upstream schema — we already have our own `polars_dtype`.
    let array_data = upstream.to_data();
    let (upstream_ffi, _upstream_schema_ffi) =
        arrow_array::ffi::to_ffi(&array_data).map_err(|e| {
            polars_err!(ComputeError:
                "vortex bridge: failed to export upstream Arrow array to FFI: {e}")
        })?;

    // Snapshot the upstream-FFI length BEFORE transmuting so we have a reference
    // value to validate the import against. The `length` field is `pub` on
    // upstream's struct, so we can read it directly.
    let expected_len = upstream_ffi.length;

    // SAFETY: both structs are `#[repr(C)]` with the layout mandated by the Arrow C Data
    // Interface (length, null_count, offset, n_buffers, n_children, buffers, children,
    // dictionary, release, private_data) — verified at compile time above. The release
    // callback in `FFI_ArrowArray` is preserved through the cast and will correctly free
    // the upstream-array's private_data when polars-arrow drops it.
    let polars_ffi: PolarsFfiArray = unsafe { mem::transmute(upstream_ffi) };

    // SAFETY: `polars_ffi` came from a valid `to_ffi` export, satisfying the
    // C Data Interface invariants.
    let imported = unsafe { arrow::ffi::import_array_from_c(polars_ffi, polars_dtype) }?;

    // Runtime sanity check: the imported array's length must equal the upstream's.
    // A mismatch would indicate the transmute landed on a different `length` field —
    // i.e., the two struct layouts diverged. Belt-and-suspenders alongside the
    // compile-time size/alignment asserts.
    if imported.len() as i64 != expected_len {
        return Err(polars_err!(ComputeError:
            "vortex bridge: FFI round-trip length mismatch (upstream={expected_len}, \
             imported={}). This is a polars-arrow / arrow-rs layout incompatibility — \
             please file an issue.", imported.len()));
    }

    Ok(imported)
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
