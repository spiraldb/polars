//! Charges for compressed prefetch from footer metadata.
//!
//! A charge includes referenced segment bytes and an allowance for coalesced gaps.
//! It does not bound decoded arrays, output batches, or the memory of the whole query.

use std::ops::Range;

use polars_error::{PolarsResult, polars_ensure, polars_err};
use polars_vortex::vortex::file::Footer;
use polars_vortex::vortex::io::CoalesceConfig;
use polars_vortex::vortex::layout::LayoutChildType;
use polars_vortex::vortex::layout::segments::SegmentId;

/// Compute a conservative compressed-prefetch charge for each sorted row range.
///
/// A shared segment receives one charge per split that can access it.
/// Auxiliary subtrees apply to their parent's active interval, regardless of their own row counts.
pub(super) fn split_costs(
    footer: &Footer,
    columns: &[String],
    ranges: &[Range<u64>],
    coalesce: CoalesceConfig,
) -> PolarsResult<Vec<usize>> {
    for (index, range) in ranges.iter().enumerate() {
        polars_ensure!(range.start <= range.end, ComputeError: "invalid Vortex split range");
        if index != 0 {
            polars_ensure!(ranges[index - 1].end <= range.start, ComputeError:
                "Vortex split ranges must be sorted and disjoint");
        }
    }
    let mut intervals = vec![Vec::new(); ranges.len()];
    let mut pending = vec![(
        footer.layout().clone(),
        0_u64,
        0..footer.layout().row_count(),
        true,
        false,
    )];
    while let Some((layout, origin, active, project, auxiliary)) = pending.pop() {
        if active.is_empty() {
            continue;
        }
        let overlaps = overlapping(ranges, &active);
        if overlaps.is_empty() {
            continue;
        }
        for id in layout.segment_ids() {
            polars_ensure!((*id as usize) < footer.segment_map().len(), ComputeError:
                "Vortex layout references an invalid segment");
            intervals[overlaps.start].push((id, overlaps.end));
        }
        for slot in 0..layout.nslots() {
            let Some(kind) = layout.slot_type(slot) else {
                continue;
            };
            let mut child_origin = origin;
            let mut child_active = active.clone();
            let mut child_project = project;
            let mut child_auxiliary = auxiliary;
            match &kind {
                LayoutChildType::Field(name) => {
                    if project
                        && !columns
                            .iter()
                            .any(|column| column.as_str() == name.as_ref())
                    {
                        continue;
                    }
                    child_project = false;
                },
                LayoutChildType::Auxiliary(_) => {
                    child_project = false;
                    child_auxiliary = true;
                },
                LayoutChildType::Chunk((_, offset)) if !auxiliary => {
                    child_origin = origin.checked_add(*offset).ok_or_else(overflow)?;
                },
                _ => {},
            }
            let child = layout
                .slot(slot)
                .map_err(|error| polars_err!(ComputeError: "Vortex layout child: {error}"))?
                .ok_or_else(|| polars_err!(ComputeError: "missing Vortex layout child"))?;
            if matches!(kind, LayoutChildType::Chunk(_)) && !auxiliary {
                let end = child_origin
                    .checked_add(child.row_count())
                    .ok_or_else(overflow)?;
                child_active = active.start.max(child_origin)..active.end.min(end);
            }
            pending.push((
                child,
                child_origin,
                child_active,
                child_project,
                child_auxiliary,
            ));
        }
    }
    let alignment = footer
        .segment_map()
        .iter()
        .chain(footer.metadata_segments().map(|(_, segment)| segment))
        .map(|segment| segment.alignment.as_usize() as u64)
        .max()
        .unwrap_or(1);
    let mut starts = vec![0_usize; ranges.len() + 1];
    let mut ends = vec![0_usize; ranges.len() + 1];
    // Bucket by interval start, then merge each segment's intervals in one sweep.
    // Memory and work follow layout references plus segments and split boundaries.
    let mut merged: Vec<Option<Range<usize>>> = vec![None; footer.segment_map().len()];
    let charges = (0..footer.segment_map().len())
        .map(|index| {
            segment_cost(
                footer,
                &[SegmentId::try_from(index).map_err(|_| overflow())?],
                coalesce.distance,
                alignment,
            )
        })
        .collect::<PolarsResult<Vec<_>>>()?;
    for (start, references) in intervals.into_iter().enumerate() {
        for (id, end) in references {
            let index = *id as usize;
            match &mut merged[index] {
                Some(range) if start <= range.end => range.end = range.end.max(end),
                previous => {
                    if let Some(range) = previous.take() {
                        add_interval(&mut starts, &mut ends, &range, charges[index])?;
                    }
                    *previous = Some(start..end);
                },
            }
        }
    }
    for (index, range) in merged.into_iter().enumerate() {
        if let Some(range) = range {
            add_interval(&mut starts, &mut ends, &range, charges[index])?;
        }
    }
    let mut active = 0_usize;
    ranges
        .iter()
        .enumerate()
        .map(|(index, range)| {
            active = active.checked_sub(ends[index]).ok_or_else(overflow)?;
            active = active.checked_add(starts[index]).ok_or_else(overflow)?;
            Ok(if range.is_empty() { 0 } else { active })
        })
        .collect()
}

fn add_interval(
    starts: &mut [usize],
    ends: &mut [usize],
    range: &Range<usize>,
    charge: usize,
) -> PolarsResult<()> {
    starts[range.start] = starts[range.start]
        .checked_add(charge)
        .ok_or_else(overflow)?;
    ends[range.end] = ends[range.end].checked_add(charge).ok_or_else(overflow)?;
    Ok(())
}

fn overflow() -> polars_error::PolarsError {
    polars_err!(ComputeError: "Vortex prefetch charge overflow")
}

fn overlapping(ranges: &[Range<u64>], active: &Range<u64>) -> Range<usize> {
    let start = ranges.partition_point(|range| range.end <= active.start);
    let end = ranges.partition_point(|range| range.start < active.end);
    start..end.max(start)
}

fn segment_cost(
    footer: &Footer,
    ids: &[SegmentId],
    distance: u64,
    alignment: u64,
) -> PolarsResult<usize> {
    let mut cost = 0_u64;
    for id in ids {
        let segment = footer.segment_map().get(**id as usize).ok_or_else(
            || polars_err!(ComputeError: "Vortex layout references an invalid segment"),
        )?;
        segment
            .offset
            .checked_add(u64::from(segment.length))
            .ok_or_else(overflow)?;
        cost = cost
            .checked_add(u64::from(segment.length))
            .ok_or_else(overflow)?;
        // Different admitted splits can share a coalesced read and its leading padding.
        cost = cost
            .checked_add(distance)
            .and_then(|cost| cost.checked_add(segment.offset % alignment))
            .ok_or_else(overflow)?;
    }
    usize::try_from(cost).map_err(|_| overflow())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use polars_vortex::vortex::array::dtype::{DType, Nullability, PType, StructFields};
    use polars_vortex::vortex::buffer::Alignment;
    use polars_vortex::vortex::file::{OpenOptionsSessionExt, SegmentSpec};
    use polars_vortex::vortex::layout::layouts::chunked::ChunkedLayout;
    use polars_vortex::vortex::layout::layouts::flat::FlatLayout;
    use polars_vortex::vortex::layout::layouts::struct_::StructLayout;
    use polars_vortex::vortex::layout::layouts::zoned::ZonedLayout;
    use polars_vortex::vortex::layout::{LayoutRef, layout_children};
    use polars_vortex::vortex::session::registry::ReadContext;

    use super::*;

    fn integer() -> DType {
        DType::Primitive(PType::I64, Nullability::NonNullable)
    }

    fn flat(rows: u64, dtype: DType, segment: u32) -> LayoutRef {
        FlatLayout::new(rows, dtype, segment.into(), ReadContext::new(Vec::new())).into_layout()
    }

    fn make_footer(layout: LayoutRef, lengths: &[u32]) -> Footer {
        let mut offset = 0;
        let segments = lengths
            .iter()
            .map(|&length| {
                let segment = SegmentSpec {
                    offset,
                    length,
                    alignment: Alignment::none(),
                };
                offset += u64::from(length);
                segment
            })
            .collect::<Vec<_>>();
        Footer::new(layout, segments.into(), None, ReadContext::new(Vec::new()))
    }

    fn config(distance: u64) -> CoalesceConfig {
        CoalesceConfig::new(distance, u64::MAX)
    }

    #[test]
    fn projection_chunks_clipped_ranges_and_shared_segments() {
        let chunks = ChunkedLayout::new(
            10,
            integer(),
            layout_children(vec![flat(5, integer(), 0), flat(5, integer(), 1)]),
        )
        .into_layout();
        let dtype = DType::Struct(
            StructFields::from_iter([("selected", integer()), ("other", integer())]),
            Nullability::NonNullable,
        );
        let root = StructLayout::new(10, dtype, vec![chunks, flat(10, integer(), 2)]).into_layout();
        let footer = make_footer(root, &[10, 20, 100]);
        let ranges = [2..5, 5..5, 5..8];
        assert_eq!(
            split_costs(&footer, &["selected".into()], &ranges, config(0)).unwrap(),
            [10, 0, 20]
        );
        assert_eq!(
            split_costs(&footer, &["other".into()], &ranges, config(0)).unwrap(),
            [100, 0, 100]
        );
        assert_eq!(
            split_costs(&footer, &[], &ranges, config(0)).unwrap(),
            [0, 0, 0]
        );

        let shared = ChunkedLayout::new(
            10,
            integer(),
            layout_children(vec![flat(5, integer(), 0), flat(5, integer(), 0)]),
        )
        .into_layout();
        let footer = make_footer(shared, &[11]);
        assert_eq!(
            split_costs(&footer, &[], &[0..10], config(123)).unwrap(),
            [134]
        );
    }

    #[test]
    fn auxiliary_rows_apply_to_the_whole_parent_interval() {
        let empty_struct = DType::Struct(
            StructFields::from_iter(Vec::<(&str, DType)>::new()),
            Nullability::NonNullable,
        );
        // The auxiliary subtree has two rows, but both segments apply to every data split.
        let zones = ChunkedLayout::new(
            2,
            empty_struct.clone(),
            layout_children(vec![
                flat(1, empty_struct.clone(), 1),
                flat(1, empty_struct, 2),
            ]),
        )
        .into_layout();
        let zoned = ZonedLayout::try_new(
            flat(10, integer(), 0),
            zones,
            NonZeroUsize::new(5).unwrap(),
            Arc::from([]),
        )
        .unwrap()
        .into_layout();
        let footer = make_footer(zoned, &[10, 3, 4]);
        assert_eq!(
            split_costs(&footer, &[], &[2..5, 5..8], config(2)).unwrap(),
            [23, 23]
        );
    }

    #[test]
    fn many_auxiliary_segments_do_not_expand_per_split() {
        const K: usize = 65_536;
        let empty = DType::Struct(
            StructFields::from_iter(Vec::<(&str, DType)>::new()),
            Nullability::NonNullable,
        );
        let data = ChunkedLayout::new(
            K as u64,
            integer(),
            layout_children(
                (0..K)
                    .map(|index| flat(1, integer(), index as u32))
                    .collect(),
            ),
        )
        .into_layout();
        let zones = ChunkedLayout::new(
            K as u64,
            empty.clone(),
            layout_children(
                (0..K)
                    .map(|index| flat(1, empty.clone(), (K + index) as u32))
                    .collect(),
            ),
        )
        .into_layout();
        let root = ZonedLayout::try_new(data, zones, NonZeroUsize::new(1).unwrap(), Arc::from([]))
            .unwrap()
            .into_layout();
        let dtype = DType::Struct(
            StructFields::from_iter([("selected", integer())]),
            Nullability::NonNullable,
        );
        let root = StructLayout::new(K as u64, dtype, vec![root]).into_layout();
        let footer = make_footer(root, &vec![1; 2 * K]);
        let ranges = (0..K as u64).map(|row| row..row + 1).collect::<Vec<_>>();
        let costs = split_costs(&footer, &["selected".into()], &ranges, config(0)).unwrap();
        assert_eq!(costs, vec![K + 1; K]);
    }

    #[test]
    fn rejects_invalid_ranges_and_overflow() {
        let mut footer = make_footer(flat(10, integer(), 0), &[1]);
        assert!(split_costs(&footer, &[], &[5..4], config(0)).is_err());
        assert!(split_costs(&footer, &[], &[0..6, 5..10], config(0)).is_err());
        assert_eq!(overlapping(&[0..5, 5..10, 10..15], &(5..10)), 1..2);
        assert_eq!(overlapping(&[0..5, 5..10], &(10..12)), 2..2);
        let invalid = SegmentSpec {
            offset: u64::MAX,
            length: 1,
            alignment: Alignment::none(),
        };
        footer = Footer::new(
            footer.layout().clone(),
            Arc::from([invalid]),
            None,
            ReadContext::new(Vec::new()),
        );
        assert!(split_costs(&footer, &[], &[0..10], config(0)).is_err());
        let footer = make_footer(flat(10, integer(), 0), &[1, 1]);
        assert!(segment_cost(&footer, &[0.into(), 1.into()], u64::MAX, 1).is_err());
    }

    #[test]
    fn written_footer_projection_costs_are_positive_and_monotone() {
        use polars_core::prelude::{Column, DataFrame};
        use polars_core::runtime::ASYNC;
        use polars_vortex::{VortexCompression, VortexWriteOptions};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("budget.vortex");
        let values: Vec<i64> = (0..2048).collect();
        let df = DataFrame::new(
            values.len(),
            vec![
                Column::new("a".into(), values.as_slice()),
                Column::new("b".into(), values.as_slice()),
            ],
        )
        .unwrap();
        let options = VortexWriteOptions {
            compression: VortexCompression::Uncompressed,
            row_block_size: Some(512),
            ..Default::default()
        };
        polars_vortex::write::write_vortex(&df, &path, &options).unwrap();
        ASYNC.block_on(async {
            let file = polars_vortex::session::session()
                .open_options()
                .open_path(&path)
                .await
                .unwrap();
            let ranges = [17..512, 512..1024, 1024..1536, 1536..2000];
            let narrow = split_costs(file.footer(), &["a".into()], &ranges, config(0)).unwrap();
            let wide =
                split_costs(file.footer(), &["a".into(), "b".into()], &ranges, config(0)).unwrap();
            assert!(narrow.iter().all(|&bytes| bytes > 0));
            assert!(narrow.iter().zip(wide).all(|(&a, b)| a < b));
        });
    }
}
