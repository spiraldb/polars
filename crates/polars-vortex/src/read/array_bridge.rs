//! Arrow C Data Interface bridge for Vortex batches.

use std::mem;

use arrow_array::RecordBatch;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use polars_arrow::array::{
    ArrayRef, BinaryViewArray, BooleanArray, FixedSizeListArray, ListArray, PrimitiveArray,
    StructArray, Utf8ViewArray, View,
};
use polars_arrow::bitmap::Bitmap;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_arrow::ffi::{ArrowArray, ArrowSchema as CArrowSchema};
use polars_buffer::Buffer;
use polars_core::prelude::{DataFrame, IntoColumn, Series};
use polars_error::{PolarsResult, polars_err, to_compute_err};

/// Import the actual exported field, including nested layout and metadata.
pub fn import_upstream_field(field: &arrow_schema::Field) -> PolarsResult<Field> {
    const _: () = assert!(mem::size_of::<FFI_ArrowSchema>() == mem::size_of::<CArrowSchema>());
    const _: () = assert!(mem::align_of::<FFI_ArrowSchema>() == mem::align_of::<CArrowSchema>());
    let schema = FFI_ArrowSchema::try_from(field).map_err(to_compute_err)?;
    // SAFETY: both repr(C) types implement the Arrow C schema ABI. Ownership moves
    // with the release callback, which releases the original exporter state.
    let schema: CArrowSchema = unsafe { mem::transmute(schema) };
    unsafe { polars_arrow::ffi::import_field_from_c(&schema) }
}

pub fn record_batch_to_dataframe(batch: RecordBatch) -> PolarsResult<DataFrame> {
    const _: () = assert!(mem::size_of::<FFI_ArrowArray>() == mem::size_of::<ArrowArray>());
    const _: () = assert!(mem::align_of::<FFI_ArrowArray>() == mem::align_of::<ArrowArray>());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (upstream_field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let field = import_upstream_field(upstream_field)?;
        let array = import_upstream_array(array, &field.dtype)?;
        columns.push(Series::try_from((&field, array))?.into_column());
    }
    DataFrame::new(batch.num_rows(), columns)
}

/// Decode with the registered Vortex Arrow exporter, then import its actual schema.
pub fn vortex_array_to_dataframe(array: vortex::array::ArrayRef) -> PolarsResult<DataFrame> {
    use vortex::array::VortexSessionExecute;
    use vortex::arrow::ArrowSessionExt;
    let mut ctx = crate::session::session().create_execution_ctx();
    let target = polars_export_field("", array.dtype())?;
    // Compaction copies backing buffers and is unnecessary for an owned handoff.
    let options =
        vortex::arrow::ArrowExportOptions::default().with(vortex::arrow::CompactBuffers(false));
    let exported = crate::session::session()
        .arrow()
        .exporter(&options)
        .execute_arrow(array, Some(&target), &mut ctx)
        .map_err(to_compute_err)?;
    let structure = exported
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .ok_or_else(
            || polars_error::polars_err!(ComputeError: "Vortex tables require a struct array"),
        )?;
    record_batch_to_dataframe(RecordBatch::from(structure))
}

/// Select Polars list offsets without changing byte views or extension metadata.
pub fn polars_export_field(
    name: &str,
    dtype: &vortex::dtype::DType,
) -> PolarsResult<arrow_schema::Field> {
    use vortex::arrow::ArrowSessionExt;
    let field = crate::session::session()
        .arrow()
        .to_arrow_field(name, dtype)
        .map_err(to_compute_err)?;
    Ok(compatible_field(&field))
}

fn compatible_field(field: &arrow_schema::Field) -> arrow_schema::Field {
    use arrow_schema::DataType as A;
    let dtype = match field.data_type() {
        A::List(child) | A::LargeList(child) | A::ListView(child) | A::LargeListView(child) => {
            A::LargeList(std::sync::Arc::new(compatible_field(child)))
        },
        A::FixedSizeList(child, size) => {
            A::FixedSizeList(std::sync::Arc::new(compatible_field(child)), *size)
        },
        A::Struct(fields) => {
            A::Struct(fields.iter().map(|field| compatible_field(field)).collect())
        },
        A::Map(entries, sorted) => A::Map(std::sync::Arc::new(compatible_field(entries)), *sorted),
        dtype => dtype.clone(),
    };
    field.clone().with_data_type(dtype)
}

// Arrow Buffer implements Deref, while the owner API requires AsRef.
struct BytesOwner<B>(B);
impl<B: std::ops::Deref<Target = [u8]>> AsRef<[u8]> for BytesOwner<B> {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Import shared owners directly where C export would align and copy sliced bitmaps.
fn import_upstream_array(
    array: &arrow_array::ArrayRef,
    dtype: &ArrowDataType,
) -> PolarsResult<ArrayRef> {
    use arrow_array::Array;
    let validity = array.nulls().map(|nulls| {
        let storage = Buffer::from_owner(BytesOwner(nulls.buffer().clone())).into_storage();
        // SAFETY: Arrow validates bitmap bounds and carries the matching null count.
        unsafe {
            Bitmap::from_inner_unchecked(
                storage,
                nulls.offset(),
                nulls.len(),
                Some(nulls.null_count()),
            )
        }
    });
    macro_rules! primitive {
        ($native:ty) => {{
            let data = array.to_data();
            let start = data.offset() * size_of::<$native>();
            let end = start + array.len() * size_of::<$native>();
            let values = Buffer::from_owner(BytesOwner(data.buffers()[0].clone())).sliced(start..end)
                .try_transmute::<$native>()
                .map_err(|_| polars_err!(ComputeError: "Vortex Arrow buffer has invalid native alignment"))?;
            return Ok(Box::new(PrimitiveArray::<$native>::try_new(dtype.clone(), values, validity)?));
        }};
    }
    match dtype {
        ArrowDataType::Int8 => primitive!(i8),
        ArrowDataType::Int16 => primitive!(i16),
        ArrowDataType::Int32 | ArrowDataType::Date32 | ArrowDataType::Time32(_) => primitive!(i32),
        ArrowDataType::Int64
        | ArrowDataType::Date64
        | ArrowDataType::Time64(_)
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Duration(_) => primitive!(i64),
        ArrowDataType::UInt8 => primitive!(u8),
        ArrowDataType::UInt16 => primitive!(u16),
        ArrowDataType::UInt32 => primitive!(u32),
        ArrowDataType::UInt64 => primitive!(u64),
        ArrowDataType::Float16 => primitive!(polars_utils::float16::pf16),
        ArrowDataType::Float32 => primitive!(f32),
        ArrowDataType::Float64 => primitive!(f64),
        ArrowDataType::Decimal(_, _) => primitive!(i128),
        ArrowDataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap()
                .values();
            let storage = Buffer::from_owner(BytesOwner(values.inner().clone())).into_storage();
            // SAFETY: Arrow validates the independent value bitmap bounds.
            let values = unsafe {
                Bitmap::from_inner_unchecked(storage, values.offset(), values.len(), None)
            };
            Ok(Box::new(BooleanArray::new(dtype.clone(), values, validity)))
        },
        ArrowDataType::Utf8View | ArrowDataType::BinaryView => {
            macro_rules! views {
                ($array:ty, $output:ty) => {{
                    const _: () = assert!(size_of::<View>() == size_of::<u128>());
                    const _: () = assert!(std::mem::align_of::<View>() <= std::mem::align_of::<u128>());
                    let array = array.as_any().downcast_ref::<$array>().unwrap();
                    let views = Buffer::from_owner(array.views().clone()).try_transmute::<View>()
                        .map_err(|_| polars_err!(ComputeError: "Vortex Arrow views have invalid alignment"))?;
                    let buffers: Buffer<Buffer<u8>> = array.data_buffers().iter().cloned()
                        .map(|buffer| Buffer::from_owner(BytesOwner(buffer))).collect();
                    // SAFETY: Arrow byte views use the same C layout and validate their payloads.
                    Ok(Box::new(unsafe { <$output>::new_unchecked_unknown_md(dtype.clone(), views, buffers, validity, None) }) as ArrayRef)
                }};
            }
            if matches!(dtype, ArrowDataType::Utf8View) {
                views!(arrow_array::StringViewArray, Utf8ViewArray)
            } else {
                views!(arrow_array::BinaryViewArray, BinaryViewArray)
            }
        },
        ArrowDataType::LargeList(child) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::LargeListArray>()
                .unwrap();
            let offsets = Buffer::from_owner(array.offsets().inner().clone()).try_into()?;
            let values = import_upstream_array(array.values(), &child.dtype)?;
            Ok(Box::new(ListArray::<i64>::try_new(
                dtype.clone(),
                offsets,
                values,
                validity,
            )?))
        },
        ArrowDataType::FixedSizeList(child, _) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::FixedSizeListArray>()
                .unwrap();
            let values = import_upstream_array(array.values(), &child.dtype)?;
            Ok(Box::new(FixedSizeListArray::try_new(
                dtype.clone(),
                array.len(),
                values,
                validity,
            )?))
        },
        ArrowDataType::Map(entries, _) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::MapArray>()
                .unwrap();
            let offsets = Buffer::from_owner(array.offsets().inner().clone()).try_into()?;
            let source: arrow_array::ArrayRef = std::sync::Arc::new(array.entries().clone());
            let values = import_upstream_array(&source, &entries.dtype)?;
            Ok(Box::new(polars_arrow::array::MapArray::try_new(
                dtype.clone(),
                offsets,
                values,
                validity,
            )?))
        },
        ArrowDataType::Struct(fields) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .unwrap();
            let values = array
                .columns()
                .iter()
                .zip(fields)
                .map(|(array, field)| import_upstream_array(array, &field.dtype))
                .collect::<PolarsResult<Vec<_>>>()?;
            Ok(Box::new(StructArray::try_new(
                dtype.clone(),
                array.len(),
                values,
                validity,
            )?))
        },
        _ => {
            let (array, _) = arrow_array::ffi::to_ffi(&array.to_data()).map_err(to_compute_err)?;
            // SAFETY: both repr(C) types implement the Arrow C array ABI.
            let array: ArrowArray = unsafe { mem::transmute(array) };
            unsafe { polars_arrow::ffi::import_array_from_c(array, dtype.clone()) }
        },
    }
}
