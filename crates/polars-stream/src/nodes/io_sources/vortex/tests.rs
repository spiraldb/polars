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
    let mut scan = file.scan().unwrap();
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
    (spy, prepared, ranges)
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
            let (result, _, permit) =
                export_budgeted_split(prepared, 5_000..10_000, true, context.handle(), permit)
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
                export_budgeted_split(prepared, range, last, runtime, permit).await
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
