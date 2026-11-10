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
}
struct RecordingReadAt {
    bytes: ByteBuffer,
    probe: Mutex<Probe>,
}
impl RecordingReadAt {
    fn arm(&self) -> (oneshot::Receiver<Range<u64>>, oneshot::Sender<()>) {
        let (first_tx, first_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self.probe.lock().unwrap() = Probe {
            first: Some(first_tx),
            release: Some(release_rx),
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
        let release = if let Some(first) = probe.first.take() {
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
    let scan = file.scan().unwrap();
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
            let (result, permit) = first.await;
            assert_eq!(result.unwrap().unwrap().height(), 5_000);
            assert!(
                tokio::time::timeout(Duration::from_millis(25), &mut second_permit)
                    .await
                    .is_err()
            );
            drop(permit);
            let permit = second_permit.await;
            let (result, permit) =
                export_budgeted_split(prepared, 5_000..10_000, context.handle(), permit).await;
            assert_eq!(result.unwrap().unwrap().height(), 5_000);
            drop(permit);
        })
        .await
        .unwrap();
    });
}
