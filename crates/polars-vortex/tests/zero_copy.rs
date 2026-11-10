//! These tests verify shared data buffers and independent bitmap offsets.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, Int64Array, RecordBatch, StringViewArray,
};
use arrow_schema::{Field, Schema};
use polars_arrow::array::{
    Array as _, BinaryViewArray as PlBinaryView, BooleanArray as PlBoolean, ListArray as PlList,
    PrimitiveArray, StructArray as PlStruct, Utf8ViewArray,
};
use polars_vortex::read::array_bridge::{record_batch_to_dataframe, vortex_array_to_dataframe};
use vortex::arrow::ArrowSessionExt;

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let fields: Vec<_> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap()
}

#[test]
fn sliced_primitive_boolean_and_views_retain_buffers_after_arrow_owners_drop() {
    let integers = Int64Array::from(vec![Some(0), Some(1), None, Some(3), Some(4)]).slice(1, 3);
    let boolean = BooleanArray::from(vec![Some(false), Some(true), None, Some(false), Some(true)])
        .slice(1, 3);
    let strings = StringViewArray::from_iter([
        Some("padding"),
        Some("first string exceeds inline capacity"),
        None,
        Some("last string exceeds inline capacity"),
        Some("padding"),
    ])
    .slice(1, 3);
    let binary = BinaryViewArray::from_iter([
        Some(b"padding".as_slice()),
        Some(b"first binary exceeds inline capacity".as_slice()),
        None,
        Some(b"last binary exceeds inline capacity".as_slice()),
        Some(b"padding".as_slice()),
    ])
    .slice(1, 3);
    let int_values = integers.values().as_ptr();
    let int_validity = integers.nulls().unwrap().buffer().as_ptr();
    let bool_values = boolean.values().inner().as_ptr();
    let bool_validity = boolean.nulls().unwrap().buffer().as_ptr();
    let string_views = strings.views().as_ptr().cast::<u8>();
    let string_data: Vec<_> = strings.data_buffers().iter().map(|b| b.as_ptr()).collect();
    let binary_views = binary.views().as_ptr().cast::<u8>();
    let binary_data: Vec<_> = binary.data_buffers().iter().map(|b| b.as_ptr()).collect();
    // This call consumes the batch. The frame must retain all foreign owners itself.
    let frame = record_batch_to_dataframe(batch(vec![
        ("integer", Arc::new(integers)),
        ("boolean", Arc::new(boolean)),
        ("string", Arc::new(strings)),
        ("binary", Arc::new(binary)),
    ]))
    .unwrap();
    let integer = frame
        .column("integer")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    assert_eq!(integer.values().as_ptr(), int_values);
    let (bytes, offset, _) = integer.validity().unwrap().as_slice();
    assert_eq!(bytes.as_ptr(), int_validity);
    assert_eq!(offset, 1);
    let boolean = frame
        .column("boolean")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<PlBoolean>()
        .unwrap();
    let (bytes, offset, _) = boolean.values().as_slice();
    assert_eq!(bytes.as_ptr(), bool_values);
    assert_eq!(offset, 1);
    assert_eq!(
        boolean.validity().unwrap().as_slice().0.as_ptr(),
        bool_validity
    );
    let strings = frame
        .column("string")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<Utf8ViewArray>()
        .unwrap();
    assert_eq!(strings.views().as_ptr().cast::<u8>(), string_views);
    assert_eq!(
        strings
            .data_buffers()
            .iter()
            .map(|b| b.as_ptr())
            .collect::<Vec<_>>(),
        string_data
    );
    let binary = frame
        .column("binary")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<PlBinaryView>()
        .unwrap();
    assert_eq!(binary.views().as_ptr().cast::<u8>(), binary_views);
    assert_eq!(
        binary
            .data_buffers()
            .iter()
            .map(|b| b.as_ptr())
            .collect::<Vec<_>>(),
        binary_data
    );
    assert_eq!(
        frame.column("integer").unwrap().i64().unwrap().get(2),
        Some(3)
    );
    assert_eq!(
        frame.column("boolean").unwrap().bool().unwrap().get(0),
        Some(true)
    );
    assert_eq!(
        frame.column("string").unwrap().str().unwrap().get(0),
        Some("first string exceeds inline capacity")
    );
    assert_eq!(
        frame.column("binary").unwrap().binary().unwrap().get(2),
        Some(b"last binary exceeds inline capacity".as_slice())
    );
}

#[test]
fn vortex_export_preserves_sparse_string_backing_buffers() {
    let strings = StringViewArray::from_iter_values(
        (0..1000).map(|i| format!("{i:04}: {}", "long backing string data ".repeat(10))),
    )
    .slice(500, 1);
    let expected = strings.value(0).to_string();
    let views = strings.views().as_ptr().cast::<u8>();
    let backing: Vec<_> = strings.data_buffers().iter().map(|b| b.as_ptr()).collect();
    let batch = batch(vec![("value", Arc::new(strings))]);
    let schema = batch.schema();
    let input = polars_vortex::session()
        .arrow()
        .from_arrow_record_batch(batch, &schema)
        .unwrap();
    let frame = vortex_array_to_dataframe(input).unwrap();
    let array = frame
        .column("value")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<Utf8ViewArray>()
        .unwrap();
    assert_eq!(array.views().as_ptr().cast::<u8>(), views);
    assert_eq!(
        array
            .data_buffers()
            .iter()
            .map(|b| b.as_ptr())
            .collect::<Vec<_>>(),
        backing
    );
    assert_eq!(
        frame.column("value").unwrap().str().unwrap().get(0),
        Some(expected.as_str())
    );
}

#[cfg(feature = "dtype-struct")]
#[test]
fn vortex_nested_large_list_retains_offsets_and_string_views() {
    use arrow_array::StructArray;
    use arrow_array::builder::{LargeListBuilder, StringViewBuilder};
    let mut builder = LargeListBuilder::new(StringViewBuilder::new());
    builder
        .values()
        .append_value("long string inside the first list");
    builder.append(true);
    builder.append(false);
    builder
        .values()
        .append_value("long string inside the last list");
    builder.values().append_null();
    builder.append(true);
    let lists = builder.finish();
    let offsets = lists.offsets().as_ptr();
    let strings = lists
        .values()
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    let views = strings.views().as_ptr().cast::<u8>();
    let backing: Vec<_> = strings.data_buffers().iter().map(|b| b.as_ptr()).collect();
    let structure = StructArray::from(vec![(
        Arc::new(Field::new("list", lists.data_type().clone(), true)),
        Arc::new(lists) as ArrayRef,
    )]);
    let batch = batch(vec![("outer", Arc::new(structure))]);
    let schema = batch.schema();
    let input = polars_vortex::session()
        .arrow()
        .from_arrow_record_batch(batch, &schema)
        .unwrap();
    let frame = vortex_array_to_dataframe(input).unwrap();
    let array = frame
        .column("outer")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<PlStruct>()
        .unwrap();
    let list = array.values()[0]
        .as_any()
        .downcast_ref::<PlList<i64>>()
        .unwrap();
    assert_eq!(list.offsets().as_ptr(), offsets);
    let strings = list
        .values()
        .as_any()
        .downcast_ref::<Utf8ViewArray>()
        .unwrap();
    assert_eq!(strings.views().as_ptr().cast::<u8>(), views);
    assert_eq!(
        strings
            .data_buffers()
            .iter()
            .map(|b| b.as_ptr())
            .collect::<Vec<_>>(),
        backing
    );
    assert_eq!(strings.value(0), "long string inside the first list");
    assert_eq!(list.len(), 3);
}

#[cfg(feature = "dtype-f16")]
#[test]
fn sliced_float16_retains_values_and_validity() {
    type Half = <arrow_array::types::Float16Type as arrow_array::types::ArrowPrimitiveType>::Native;
    let array = arrow_array::Float16Array::from(vec![
        Some(Half::from_f32(0.0)),
        Some(Half::from_f32(1.5)),
        None,
        Some(Half::from_f32(-2.0)),
    ])
    .slice(1, 3);
    let values = array.values().as_ptr().cast::<u8>();
    let validity = array.nulls().unwrap().buffer().as_ptr();
    let frame = record_batch_to_dataframe(batch(vec![("value", Arc::new(array))])).unwrap();
    let array = frame
        .column("value")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<polars_utils::float16::pf16>>()
        .unwrap();
    assert_eq!(array.values().as_ptr().cast::<u8>(), values);
    let (bytes, offset, _) = array.validity().unwrap().as_slice();
    assert_eq!(bytes.as_ptr(), validity);
    assert_eq!(offset, 1);
    assert_eq!(array.value(0).0.to_f32(), 1.5);
}

#[cfg(feature = "dtype-map")]
#[test]
fn sliced_map_retains_bitmap_and_children_while_offsets_widen_once() {
    use arrow_array::builder::{Int64Builder, MapBuilder, StringViewBuilder};
    let mut builder = MapBuilder::new(None, StringViewBuilder::new(), Int64Builder::new());
    builder
        .keys()
        .append_value("padding key exceeds inline capacity");
    builder.values().append_value(1);
    builder.append(true).unwrap();
    builder.append(false).unwrap();
    builder
        .keys()
        .append_value("first key exceeds inline capacity");
    builder.values().append_value(2);
    builder.append(true).unwrap();
    builder
        .keys()
        .append_value("last key exceeds inline capacity");
    builder.values().append_null();
    builder.append(true).unwrap();
    let map = builder.finish().slice(1, 3);
    let validity = map.nulls().unwrap().buffer().as_ptr();
    let keys = map
        .keys()
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    let key_views = keys.views().as_ptr().cast::<u8>();
    let key_data: Vec<_> = keys.data_buffers().iter().map(|b| b.as_ptr()).collect();
    let values = map.values().as_any().downcast_ref::<Int64Array>().unwrap();
    let value_data = values.values().as_ptr();
    let value_validity = values.nulls().unwrap().buffer().as_ptr();
    let frame = record_batch_to_dataframe(batch(vec![("map", Arc::new(map))])).unwrap();
    assert!(frame.column("map").unwrap().dtype().is_map());
    let list = frame
        .column("map")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<PlList<i64>>()
        .unwrap();
    let (bytes, offset, _) = list.validity().unwrap().as_slice();
    assert_eq!(bytes.as_ptr(), validity);
    assert_eq!(offset, 1);
    assert_eq!(list.offsets().as_slice(), &[1i64, 1, 2, 3]);
    let entries = list.values().as_any().downcast_ref::<PlStruct>().unwrap();
    let keys = entries.values()[0]
        .as_any()
        .downcast_ref::<Utf8ViewArray>()
        .unwrap();
    assert_eq!(keys.views().as_ptr().cast::<u8>(), key_views);
    assert_eq!(
        keys.data_buffers()
            .iter()
            .map(|b| b.as_ptr())
            .collect::<Vec<_>>(),
        key_data
    );
    let values = entries.values()[1]
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    assert_eq!(values.values().as_ptr(), value_data);
    assert_eq!(
        values.validity().unwrap().as_slice().0.as_ptr(),
        value_validity
    );
    assert_eq!(values.value(1), 2);
    assert_eq!(keys.value(2), "last key exceeds inline capacity");
}

#[cfg(feature = "dtype-struct")]
#[test]
fn nested_list_retains_sliced_validity_bitmap() {
    use arrow_array::builder::{Int64Builder, LargeListBuilder};
    let mut builder = LargeListBuilder::new(Int64Builder::new());
    builder.values().append_value(0);
    builder.append(true);
    builder.append(false);
    builder.values().append_value(2);
    builder.append(true);
    builder.values().append_value(3);
    builder.append(true);
    let list = builder.finish().slice(1, 3);
    let bitmap = list.nulls().unwrap().buffer().as_ptr();
    let offsets = list.offsets().as_ptr();
    let structure = arrow_array::StructArray::from(vec![(
        Arc::new(Field::new("list", list.data_type().clone(), true)),
        Arc::new(list) as ArrayRef,
    )]);
    let frame = record_batch_to_dataframe(batch(vec![("outer", Arc::new(structure))])).unwrap();
    let structure = frame
        .column("outer")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<PlStruct>()
        .unwrap();
    let list = structure.values()[0]
        .as_any()
        .downcast_ref::<PlList<i64>>()
        .unwrap();
    assert_eq!(list.offsets().as_ptr(), offsets);
    let (bytes, offset, _) = list.validity().unwrap().as_slice();
    assert_eq!(bytes.as_ptr(), bitmap);
    assert_eq!(offset, 1);
    assert_eq!(list.len(), 3);
}

#[cfg(feature = "dtype-struct")]
#[test]
fn propagated_nullable_struct_retains_first_child_bitmap() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(0), None, Some(2), Some(3)]));
    let structure = arrow_array::StructArray::try_new(
        vec![Arc::new(Field::new(
            "value",
            arrow_schema::DataType::Int64,
            true,
        ))]
        .into(),
        vec![values],
        Some([true, false, true, true].into_iter().collect()),
    )
    .unwrap()
    .slice(1, 3);
    let parent_bitmap = structure.nulls().unwrap().buffer().as_ptr();
    let child = structure
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let child_bitmap = child.nulls().unwrap().buffer().as_ptr();
    let child_values = child.values().as_ptr();
    let frame = record_batch_to_dataframe(batch(vec![("outer", Arc::new(structure))])).unwrap();
    let structure = frame
        .column("outer")
        .unwrap()
        .as_materialized_series()
        .chunks()[0]
        .as_any()
        .downcast_ref::<PlStruct>()
        .unwrap();
    assert_eq!(
        structure.validity().unwrap().as_slice().0.as_ptr(),
        parent_bitmap
    );
    let child = structure.values()[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    assert_eq!(child.values().as_ptr(), child_values);
    let (bytes, offset, _) = child.validity().unwrap().as_slice();
    assert_eq!(bytes.as_ptr(), child_bitmap);
    assert_eq!(offset, 1);
    assert_eq!(child.value(2), 3);
}
