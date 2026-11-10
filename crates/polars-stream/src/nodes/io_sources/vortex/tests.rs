//! Admission must precede the registration of data requests.
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{FutureExt, stream};
use polars_core::prelude::{Column, DataFrame};
use polars_core::runtime::ASYNC;
use polars_vortex::session::ExecutionContext;
use polars_vortex::vortex::array::buffer::BufferHandle;
use polars_vortex::vortex::array::stream::ArrayStreamAdapter;
use polars_vortex::vortex::buffer::{Alignment, ByteBuffer};
use polars_vortex::vortex::error::VortexResult;
use polars_vortex::vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use polars_vortex::vortex::io::{CoalesceConfig, VortexReadAt};
use polars_vortex::vortex::layout::layouts::chunked::writer::ChunkedLayoutStrategy;
use polars_vortex::vortex::layout::layouts::flat::writer::FlatLayoutStrategy;
use tokio::sync::oneshot;

use super::*;

#[derive(Default)]
struct Probe {
    first: Option<oneshot::Sender<Range<u64>>>,
    release: Option<oneshot::Receiver<()>>,
    gate_from: u64,
}
struct RecordingReadAt {
    bytes: ByteBuffer,
    probe: Mutex<Probe>,
}
impl RecordingReadAt {
    fn arm(&self) -> (oneshot::Receiver<Range<u64>>, oneshot::Sender<()>) {
        self.arm_from(0)
    }
    fn arm_from(&self, gate_from: u64) -> (oneshot::Receiver<Range<u64>>, oneshot::Sender<()>) {
        let (first_tx, first_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self.probe.lock().unwrap() = Probe {
            first: Some(first_tx),
            release: Some(release_rx),
            gate_from,
        };
        (first_rx, release_tx)
    }
}
impl VortexReadAt for RecordingReadAt {
    fn concurrency(&self) -> usize {
        2
    }
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        Some(CoalesceConfig::file())
    }
    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        futures::future::ready(Ok(self.bytes.len() as u64)).boxed()
    }
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let mut probe = self.probe.lock().unwrap();
        let eligible = offset + length as u64 > probe.gate_from;
        let release = if eligible && probe.first.is_some() {
            let first = probe.first.take().unwrap();
            first.send(offset..offset + length as u64).unwrap();
            probe.release.take()
        } else {
            None
        };
        let bytes = self
            .bytes
            .slice_unaligned(offset as usize..offset as usize + length)
            .aligned(alignment);
        async move {
            if let Some(release) = release {
                release.await.unwrap();
            }
            Ok(BufferHandle::new_host(bytes))
        }
        .boxed()
    }
}

async fn fixture(
    context: &ExecutionContext,
) -> (
    Arc<RecordingReadAt>,
    Arc<RepeatedScan<ArrayRef>>,
    Vec<Range<u64>>,
) {
    fixture_filtered(context, false).await
}

async fn fixture_filtered(
    context: &ExecutionContext,
    filtered: bool,
) -> (
    Arc<RecordingReadAt>,
    Arc<RepeatedScan<ArrayRef>>,
    Vec<Range<u64>>,
) {
    let (spy, prepared, segments, _) = fixture_with_reader(context, filtered).await;
    (spy, prepared, segments)
}

async fn fixture_with_reader(
    context: &ExecutionContext,
    filtered: bool,
) -> (
    Arc<RecordingReadAt>,
    Arc<RepeatedScan<ArrayRef>>,
    Vec<Range<u64>>,
    Arc<dyn LayoutReader>,
) {
    let mut arrays = Vec::new();
    let mut dtype = None;
    for start in [0_i64, 5_000] {
        let values = (start..start + 5_000).collect::<Vec<_>>();
        let df =
            DataFrame::new(5_000, vec![Column::new("value".into(), values.as_slice())]).unwrap();
        let (next_dtype, chunks) =
            polars_vortex::write::df_to_stream::dataframe_to_vortex_chunks(&df).unwrap();
        dtype = Some(next_dtype);
        arrays.extend(chunks);
    }
    let mut bytes = Vec::new();
    context
        .session()
        .write_options()
        .with_strategy(Arc::new(ChunkedLayoutStrategy::new(
            FlatLayoutStrategy::default(),
        )))
        .with_file_statistics(Vec::new())
        .write(
            &mut bytes,
            ArrayStreamAdapter::new(dtype.unwrap(), stream::iter(arrays.into_iter().map(Ok))),
        )
        .await
        .unwrap();
    let spy = Arc::new(RecordingReadAt {
        bytes: ByteBuffer::from(bytes),
        probe: Mutex::new(Probe::default()),
    });
    // Supplied metadata avoids the mandatory 65,535-byte tail preload.
    // That preload otherwise caches the second 40,000-byte segment.
    let footer = context
        .session()
        .open_options()
        .open_buffer(spy.bytes.clone())
        .unwrap()
        .footer()
        .clone();
    let file = context
        .session()
        .open_options()
        .with_initial_read_size(1_024)
        .with_footer(footer)
        .without_segment_cache()
        .open_read(spy.clone())
        .await
        .unwrap();
    let reader = file.layout_reader().unwrap();
    let mut scan = ScanBuilder::new(file.session().clone(), reader.clone());
    if filtered {
        use polars_vortex::vortex::expr::{get_item, lit, lt, root};
        scan = scan.with_filter(
            lt(get_item("value", root()), lit(5_000_i64))
                .bind(file.dtype())
                .unwrap(),
        );
    }
    let boundaries = scan.full_file_splits().unwrap();
    assert_eq!(boundaries.as_slice(), &[0, 5_000, 10_000]);
    let ranges = file
        .footer()
        .segment_map()
        .iter()
        .map(|segment| segment.byte_range())
        .collect::<Vec<_>>();
    assert_eq!(ranges.len(), 2);
    assert!(ranges[1].end - ranges[0].start < CoalesceConfig::file().max_size);
    let prepared = Arc::new(
        scan.with_natural_splits(boundaries.into())
            .prepare()
            .unwrap(),
    );
    (spy, prepared, ranges, reader)
}

fn covers(request: &Range<u64>, segment: &Range<u64>) -> bool {
    request.start <= segment.start && request.end >= segment.end
}

#[test]
fn eager_registration_coalesces_unadmitted_data_but_lazy_admission_does_not() {
    ASYNC.block_on(async {
        tokio::time::timeout(Duration::from_secs(20), async {
            let context = ExecutionContext::new(None);
            // This control detects the eager-registration regression, rather than only a semaphore change.
            let (spy, prepared, segments) = fixture(&context).await;
            let (first_read, release) = spy.arm();
            let mut eager = prepared.execute(None).unwrap();
            let first = context.handle().spawn(eager.remove(0));
            let request = first_read.await.unwrap();
            assert!(
                covers(&request, &segments[0]),
                "request={request:?}, segments={segments:?}"
            );
            assert!(
                covers(&request, &segments[1]),
                "request={request:?}, segments={segments:?}"
            );
            release.send(()).unwrap();
            assert_eq!(first.await.unwrap().unwrap().len(), 5_000);
            drop(eager);

            let (spy, prepared, segments) = fixture(&context).await;
            let budget = PipelineBudget::new(Some(1), Some(1), 1);
            let permit = budget.acquire(40_000, WindowGrowth::Fixed, 1).await;
            let (first_read, release) = spy.arm();
            let first = context.handle().spawn(export_budgeted_split(
                prepared.clone(),
                0..5_000,
                false,
                context.handle(),
                permit,
                None,
            ));
            let request = first_read.await.unwrap();
            assert!(
                covers(&request, &segments[0]),
                "request={request:?}, segments={segments:?}"
            );
            assert!(
                request.end <= segments[1].start,
                "request={request:?}, segments={segments:?}"
            );
            let second_permit = budget.acquire(40_000, WindowGrowth::Fixed, 1);
            tokio::pin!(second_permit);
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut second_permit)
                    .await
                    .is_err()
            );
            release.send(()).unwrap();
            let (result, _, permit) = first.await;
            assert_eq!(result.unwrap().unwrap().height(), 5_000);
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut second_permit)
                    .await
                    .is_err()
            );
            drop(permit);
            let permit = second_permit.await;
            let (result, _, permit) = export_budgeted_split(
                prepared,
                5_000..10_000,
                true,
                context.handle(),
                permit,
                None,
            )
            .await;
            assert_eq!(result.unwrap().unwrap().height(), 5_000);
            drop(permit);
        })
        .await
        .unwrap();
    });
}

fn output_stream(
    prepared: Arc<RepeatedScan<ArrayRef>>,
    context: &ExecutionContext,
    budget: PipelineBudget,
) -> futures::stream::BoxStream<'static, PolarsResult<(DataFrame, bool)>> {
    let runtime = context.handle();
    stream::iter([(0..5_000, false), (5_000..10_000, true)])
        .map(move |(range, last)| {
            let prepared = prepared.clone();
            let runtime = runtime.clone();
            let budget = budget.clone();
            async move {
                let permit = budget.acquire(40_000, WindowGrowth::Fixed, 1).await;
                export_budgeted_split(prepared, range, last, runtime, permit, None).await
            }
        })
        .buffered(2)
        .filter_map(nonempty_dataframe)
        .boxed()
}

#[test]
fn first_output_precedes_the_gated_tail_and_full_drain_keeps_order() {
    ASYNC.block_on(async {
        tokio::time::timeout(Duration::from_secs(20), async {
            for filtered in [false, true] {
                let context = ExecutionContext::new(None);
                let (spy, prepared, segments) = fixture_filtered(&context, filtered).await;
                let (tail_read, release) = spy.arm_from(segments[1].start);
                let budget = PipelineBudget::new(Some(1), Some(1), 1);
                let stream = output_stream(prepared, &context, budget);
                let (tx, mut rx) = FileReaderOutputSend::new_serial();
                let task = context.handle().spawn(send_morsels(stream, tx, false, 4));
                // The former nonempty lookahead waits for the gated tail before this receive.
                let first = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .unwrap()
                    .ok()
                    .unwrap()
                    .into_df()
                    .await;
                assert_eq!(first.height(), 5_000);
                let request = tail_read.await.unwrap();
                assert!(covers(&request, &segments[1]));
                release.send(()).unwrap();
                let mut output = first;
                while let Ok(morsel) = rx.recv().await {
                    output.vstack_mut(&morsel.into_df().await).unwrap();
                }
                assert!(task.await.unwrap());
                let expected_end = if filtered { 5_000 } else { 10_000 };
                let actual = output
                    .column("value")
                    .unwrap()
                    .i64()
                    .unwrap()
                    .into_no_null_iter()
                    .collect::<Vec<_>>();
                assert_eq!(actual, (0_i64..expected_end).collect::<Vec<_>>());
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn cancellation_drops_a_gated_tail_and_returns_the_permit() {
    ASYNC.block_on(async {
        tokio::time::timeout(Duration::from_secs(20), async {
            let context = ExecutionContext::new(None);
            let (spy, prepared, segments) = fixture_filtered(&context, true).await;
            let (tail_read, mut release) = spy.arm_from(segments[1].start);
            let budget = PipelineBudget::new(Some(1), Some(1), 1);
            let stream = output_stream(prepared, &context, budget.clone());
            let (tx, mut rx) = FileReaderOutputSend::new_serial();
            let task = context.handle().spawn(send_morsels(stream, tx, false, 4));
            let first = rx.recv().await.ok().unwrap().into_df().await;
            assert_eq!(first.height(), 5_000);
            tail_read.await.unwrap();
            drop(rx);
            // The reader owner cancels its task after the consumer disconnects.
            drop(task);
            release.closed().await;
            let permit = budget.acquire(40_000, WindowGrowth::Fixed, 1).await;
            drop(permit);
        })
        .await
        .unwrap();
    });
}

#[test]
fn output_send_failure_drops_pending_split_futures() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    ASYNC.block_on(async {
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let values = [1_i64];
        let df = DataFrame::new(1, vec![Column::new("value".into(), values.as_slice())]).unwrap();
        let stream = stream::iter([
            async move { Ok((df, false)) }.boxed(),
            async move {
                let _guard = guard;
                futures::future::pending::<PolarsResult<(DataFrame, bool)>>().await
            }
            .boxed(),
        ])
        .buffered(2)
        .boxed();
        let (tx, rx) = FileReaderOutputSend::new_serial();
        drop(rx);
        assert!(!send_morsels(stream, tx, false, 4).await.unwrap());
        assert!(dropped.load(Ordering::Acquire));
    });
}

/// The delegate supplies real data. This wrapper controls only conservative pruning proofs.
struct ControlledPruning {
    delegate: Arc<dyn LayoutReader>,
    skip_first: bool,
    calls: Mutex<Vec<Range<u64>>>,
    gate: Mutex<Option<(oneshot::Sender<Range<u64>>, oneshot::Receiver<()>)>>,
}

impl LayoutReader for ControlledPruning {
    fn name(&self) -> &Arc<str> {
        self.delegate.name()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn dtype(&self) -> &polars_vortex::vortex::dtype::DType {
        self.delegate.dtype()
    }
    fn row_count(&self) -> u64 {
        self.delegate.row_count()
    }
    fn register_splits(
        &self,
        fields: &[polars_vortex::vortex::dtype::FieldMask],
        range: &polars_vortex::vortex::layout::SplitRange,
        splits: &mut polars_vortex::vortex::layout::RowSplits,
    ) -> VortexResult<()> {
        self.delegate.register_splits(fields, range, splits)
    }
    fn pruning_evaluation(
        &self,
        range: &Range<u64>,
        _expression: &BoundExpression,
        mask: Mask,
    ) -> VortexResult<polars_vortex::vortex::array::MaskFuture> {
        use polars_vortex::vortex::array::MaskFuture;
        assert!(mask.all_true());
        assert_eq!(mask.len() as u64, range.end - range.start);
        self.calls.lock().unwrap().push(range.clone());
        let gate = self.gate.lock().unwrap().take();
        if let Some((entered, release)) = gate {
            entered.send(range.clone()).unwrap();
            return Ok(MaskFuture::new(mask.len(), async move {
                release.await.unwrap();
                Ok(mask)
            }));
        }
        let skip = (range.start == 0) == self.skip_first;
        let result = if skip {
            Mask::new_false(mask.len())
        } else {
            // A partial proof cannot act as an exact row filter.
            (0..mask.len()).map(|index| index % 2 == 0).collect()
        };
        Ok(MaskFuture::ready(result))
    }
    fn filter_evaluation(
        &self,
        range: &Range<u64>,
        expression: &BoundExpression,
        mask: polars_vortex::vortex::array::MaskFuture,
    ) -> VortexResult<polars_vortex::vortex::array::MaskFuture> {
        self.delegate.filter_evaluation(range, expression, mask)
    }
    fn projection_evaluation(
        &self,
        range: &Range<u64>,
        expression: &BoundExpression,
        mask: polars_vortex::vortex::array::MaskFuture,
    ) -> VortexResult<polars_vortex::vortex::layout::ArrayFuture> {
        self.delegate.projection_evaluation(range, expression, mask)
    }
}

fn controlled_pruning(reader: Arc<ControlledPruning>) -> SplitPruning {
    use polars_vortex::vortex::expr::lit;
    SplitPruning {
        expression: lit(true).bind(reader.dtype()).unwrap(),
        reader,
    }
}

#[test]
fn pruning_waits_for_admission_and_cancellation_releases_its_gate_permit_and_owner() {
    ASYNC.block_on(async {
        tokio::time::timeout(Duration::from_secs(20), async {
            let context = Arc::new(ExecutionContext::new(None));
            let owner = Arc::downgrade(&context);
            let (spy, prepared, _, reader) = fixture_with_reader(&context, false).await;
            let (entered_tx, mut entered) = oneshot::channel();
            let (mut release, release_rx) = oneshot::channel();
            let reader = Arc::new(ControlledPruning {
                delegate: reader,
                skip_first: true,
                calls: Mutex::new(Vec::new()),
                gate: Mutex::new(Some((entered_tx, release_rx))),
            });
            let budget = PipelineBudget::new(Some(1), Some(1), 1);
            let held = budget.acquire(40_000, WindowGrowth::Fixed, 1).await;
            let task_budget = budget.clone();
            let pruning = controlled_pruning(reader.clone());
            let (mut data_read, _data_release) = spy.arm();
            let task = context.handle().spawn(async move {
                let permit = task_budget.acquire(40_000, WindowGrowth::Fixed, 1).await;
                let result = export_budgeted_split(
                    prepared,
                    0..5_000,
                    false,
                    context.handle(),
                    permit,
                    Some(pruning),
                )
                .await;
                drop(context);
                result
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut entered)
                    .await
                    .is_err()
            );
            assert!(reader.calls.lock().unwrap().is_empty());
            drop(held);
            assert_eq!(entered.await.unwrap(), 0..5_000);
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut data_read)
                    .await
                    .is_err()
            );
            drop(task);
            release.closed().await;
            let permit = budget.acquire(40_000, WindowGrowth::Fixed, 1).await;
            drop(permit);
            while owner.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn all_false_pruning_returns_permits_and_partial_proofs_preserve_rows_and_physical_order() {
    ASYNC.block_on(async {
        tokio::time::timeout(Duration::from_secs(20), async {
            for skip_first in [true, false] {
                let context = ExecutionContext::new(None);
                let (_spy, prepared, _, reader) = fixture_with_reader(&context, false).await;
                let reader = Arc::new(ControlledPruning {
                    delegate: reader,
                    skip_first,
                    calls: Mutex::new(Vec::new()),
                    gate: Mutex::new(None),
                });
                let budget = PipelineBudget::new(Some(1), Some(1), 1);
                let mut output = Vec::new();
                for (range, last) in [(0..5_000, false), (5_000..10_000, true)] {
                    let permit = budget.acquire(40_000, WindowGrowth::Fixed, 1).await;
                    let result = export_budgeted_split(
                        prepared.clone(),
                        range,
                        last,
                        context.handle(),
                        permit,
                        Some(controlled_pruning(reader.clone())),
                    )
                    .await;
                    assert_eq!(result.1, last);
                    if let Some(batch) = nonempty_dataframe(result).await {
                        output.push(batch.unwrap());
                    }
                }
                assert_eq!(
                    reader.calls.lock().unwrap().as_slice(),
                    &[0..5_000, 5_000..10_000]
                );
                assert_eq!(output.len(), 1);
                let (df, last) = output.pop().unwrap();
                assert_eq!(last, skip_first);
                assert_eq!(df.height(), 5_000);
                let actual = df
                    .column("value")
                    .unwrap()
                    .i64()
                    .unwrap()
                    .into_no_null_iter()
                    .collect::<Vec<_>>();
                let start = if skip_first { 5_000_i64 } else { 0 };
                assert_eq!(actual, (start..start + 5_000).collect::<Vec<_>>());
                // A skipped final physical split also returns its budget charge.
                drop(budget.acquire(40_000, WindowGrowth::Fixed, 1).await);
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn pruning_only_fields_join_projection_and_row_predicate_prefetch_charges() {
    use polars_vortex::vortex::array::dtype::{DType, Nullability, PType, StructFields};
    use polars_vortex::vortex::expr::{get_item, gt, lit, lt, root};
    use polars_vortex::vortex::file::SegmentSpec;
    use polars_vortex::vortex::layout::layouts::flat::FlatLayout;
    use polars_vortex::vortex::layout::layouts::struct_::StructLayout;
    use polars_vortex::vortex::session::registry::ReadContext;

    let integer = DType::Primitive(PType::I64, Nullability::NonNullable);
    let dtype = DType::Struct(
        StructFields::from_iter([
            ("payload", integer.clone()),
            ("static_key", integer.clone()),
            ("prune_key", integer.clone()),
            ("unused", integer.clone()),
        ]),
        Nullability::NonNullable,
    );
    let children = (0_u32..4)
        .map(|id| {
            FlatLayout::new(10, integer.clone(), id.into(), ReadContext::new(Vec::new()))
                .into_layout()
        })
        .collect();
    let layout = StructLayout::new(10, dtype.clone(), children).into_layout();
    let mut offset = 0;
    let segments = [8_u32, 80, 800, 8_000]
        .into_iter()
        .map(|length| {
            let segment = SegmentSpec {
                offset,
                length,
                alignment: Alignment::none(),
            };
            offset += u64::from(length);
            segment
        })
        .collect::<Vec<_>>();
    let footer = Footer::new(layout, segments.into(), None, ReadContext::new(Vec::new()));
    let config = CoalesceConfig::new(0, u64::MAX);
    let mut columns = vec!["payload".to_string()];
    let row_filter = lt(get_item("static_key", root()), lit(5_i64))
        .bind(&dtype)
        .unwrap();
    add_filter_budget_columns(&row_filter, &dtype, &mut columns);
    assert_eq!(
        budget::split_costs(&footer, &columns, &[0..10], config).unwrap(),
        [88]
    );
    let pruning = gt(get_item("prune_key", root()), lit(0_i64))
        .bind(&dtype)
        .unwrap();
    add_filter_budget_columns(&pruning, &dtype, &mut columns);
    assert!(columns.iter().any(|field| field == "prune_key"));
    assert!(!columns.iter().any(|field| field == "unused"));
    assert_eq!(
        budget::split_costs(&footer, &columns, &[0..10], config).unwrap(),
        [888]
    );
}
