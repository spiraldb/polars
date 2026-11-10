//! Vortex writer with the Polars target and completion lifecycle.

use std::collections::VecDeque;
use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use polars_async::executor::{self, TaskMetricAggregator, TaskPriority};
use polars_async::primitives::connector;
use polars_core::runtime::ASYNC;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_err};
use polars_utils::index::NonZeroIdxSize;
use polars_vortex::VortexWriteOptions;
use polars_vortex::session::ExecutionContext;
use polars_vortex::vortex::array::stream::ArrayStreamAdapter;
use polars_vortex::vortex::error::vortex_err;
use polars_vortex::vortex::io::{IoBuf, VortexWrite};
use polars_vortex::write::df_to_stream::{
    dataframe_to_vortex_chunks, polars_schema_to_vortex_dtype,
};
use polars_vortex::write::strategy::build_write_options_with_session;

use crate::nodes::io_sinks::components::sink_morsel::SinkMorsel;
use crate::nodes::io_sinks::components::size::{SplitMode, TargetSinkMorselSize};
use crate::nodes::io_sinks::components::writable::Writable;
use crate::nodes::io_sinks::writers::interface::{
    FileOpenTaskHandle, FileWriterStarter, ideal_sink_morsel_size_env,
};
use crate::utils::tokio_handle_ext;

pub struct VortexWriterStarter {
    pub options: Arc<VortexWriteOptions>,
    pub schema: SchemaRef,
}

impl FileWriterStarter for VortexWriterStarter {
    fn writer_name(&self) -> &str {
        "vortex"
    }

    fn target_sink_morsel_size(&self) -> TargetSinkMorselSize {
        let (rows, bytes) = ideal_sink_morsel_size_env();
        TargetSinkMorselSize {
            target_num_rows: rows.unwrap_or(const { NonZeroIdxSize::new(122_880).unwrap() }),
            target_num_bytes: bytes.unwrap_or(NonZeroU64::MAX),
            target_num_bytes_min_rows: const { NonZeroIdxSize::new(16_384).unwrap() },
            target_num_rows_mode: SplitMode::Approximate,
        }
    }

    fn start_file_writer(
        &self,
        morsel_rx: connector::Receiver<SinkMorsel>,
        file: FileOpenTaskHandle,
        _num_pipelines: NonZeroUsize,
        task_metrics: Option<Arc<TaskMetricAggregator>>,
    ) -> PolarsResult<executor::JoinHandle<PolarsResult<()>>> {
        let dtype = polars_schema_to_vortex_dtype(&self.schema)?;
        let options = Arc::clone(&self.options);
        let context = ExecutionContext::new(task_metrics.clone());
        Ok(executor::spawn(
            TaskPriority::Low,
            task_metrics.as_deref(),
            async move {
                let write = tokio_handle_ext::AbortOnDropHandle(ASYNC.spawn(async move {
                    let (mut target, sync, finish) = file.await?;
                    // End-of-input waits for successful query completion. A dropped
                    // completion sender becomes a stream error before Vortex writes its footer.
                    let stream = futures::stream::try_unfold(
                        (
                            morsel_rx,
                            VecDeque::new(),
                            None,
                            Some(finish),
                            context.clone(),
                        ),
                        |(mut receiver, mut chunks, mut permit, mut finish, context)| async move {
                            loop {
                                if let Some(chunk) = chunks.pop_front() {
                                    return Ok(Some((
                                        chunk,
                                        (receiver, chunks, permit, finish, context),
                                    )));
                                }
                                drop(permit.take());
                                match receiver.recv().await {
                                    Ok(morsel) => {
                                        let (df, next_permit) = morsel.into_inner();
                                        let (_, arrays) = context
                                            .handle()
                                            .spawn_cpu(move || dataframe_to_vortex_chunks(&df))
                                            .await
                                            .map_err(|e| vortex_err!("Vortex batch export: {e}"))?;
                                        chunks = arrays.into();
                                        permit = Some(next_permit);
                                    },
                                    Err(_) => {
                                        if let Some(signal) = finish.take() {
                                            signal.wait().await.map_err(|e| vortex_err!("{e}"))?;
                                        }
                                        return Ok(None);
                                    },
                                }
                            }
                        },
                    );
                    let sink = VortexTarget(target.as_writable());
                    build_write_options_with_session(&options, context.session())?
                        .write(sink, ArrayStreamAdapter::new(dtype, stream))
                        .await
                        .map_err(|e| polars_err!(ComputeError: "Vortex write: {e}"))?;
                    target.close(sync).await?;
                    drop(context);
                    Ok(())
                }));
                write
                    .await
                    .map_err(|e| polars_err!(ComputeError: "Vortex writer task: {e}"))?
            },
        ))
    }
}

/// Borrow the target so Polars retains ownership, byte counts, and close policy.
struct VortexTarget<'a>(Writable<'a>);

impl VortexWrite for VortexTarget<'_> {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        self.0.write_all(buffer.as_slice()).await?;
        Ok(buffer)
    }
    async fn flush(&mut self) -> io::Result<()> {
        self.0.flush().await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        self.0.flush().await
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use polars_core::prelude::*;
    use polars_io::utils::file::{Writable as TargetVariant, WritableTrait};
    use polars_io::utils::sync_on_close::SyncOnCloseType;
    use polars_vortex::vortex::file::OpenOptionsSessionExt;

    use super::*;
    use crate::nodes::io_sinks::components::writable::WriteTarget;
    use crate::nodes::io_sinks::writers::interface::finish_signal;

    struct MemoryTarget {
        bytes: Arc<Mutex<Vec<u8>>>,
        closed: Arc<AtomicBool>,
        fail_close: bool,
    }

    impl Write for MemoryTarget {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl WritableTrait for MemoryTarget {
        fn close(&mut self) -> io::Result<()> {
            if self.fail_close {
                return Err(io::Error::other("injected close failure"));
            }
            self.closed.store(true, Ordering::Relaxed);
            Ok(())
        }
        fn sync_all(&self) -> io::Result<()> {
            Ok(())
        }
        fn sync_data(&self) -> io::Result<()> {
            Ok(())
        }
    }

    fn run_writer(success: bool, fail_close: bool) -> (PolarsResult<()>, Vec<u8>, bool) {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let target = MemoryTarget {
            bytes: bytes.clone(),
            closed: closed.clone(),
            fail_close,
        };
        let df = DataFrame::new(3, vec![Column::new("value".into(), &[1i64, 2, 3])]).unwrap();
        let starter = VortexWriterStarter {
            options: Arc::default(),
            schema: df.schema().clone(),
        };
        let (finish_tx, finish) = finish_signal();
        let open = tokio_handle_ext::AbortOnDropHandle(ASYNC.spawn(async move {
            Ok(WriteTarget::new(TargetVariant::Dyn(Box::new(target)), None))
        }));
        let file = FileOpenTaskHandle::new(open, SyncOnCloseType::All, finish);
        let (mut sender, receiver) = connector::connector();
        let handle = starter
            .start_file_writer(receiver, file, NonZeroUsize::new(1).unwrap(), None)
            .unwrap();
        let result = ASYNC.block_on(async move {
            let permit = Arc::new(tokio::sync::Semaphore::new(1))
                .acquire_owned()
                .await
                .unwrap();
            assert!(sender.send(SinkMorsel::new(df, permit)).await.is_ok());
            drop(sender);
            if success {
                finish_tx.send();
            } else {
                drop(finish_tx);
            }
            handle.await
        });
        let data = bytes.lock().unwrap().clone();
        (result, data, closed.load(Ordering::Relaxed))
    }

    #[test]
    fn dynamic_target_closes_after_success() {
        let (result, bytes, closed) = run_writer(true, false);
        result.unwrap();
        assert!(closed);
        let file = polars_vortex::session()
            .open_options()
            .open_buffer(bytes)
            .unwrap();
        assert_eq!(file.row_count(), 3);
    }

    #[test]
    fn failed_finish_signal_prevents_footer_and_close() {
        let (result, bytes, closed) = run_writer(false, false);
        assert!(result.is_err());
        assert!(!closed);
        assert!(
            polars_vortex::session()
                .open_options()
                .open_buffer(bytes)
                .is_err()
        );
    }

    #[test]
    fn target_close_error_reaches_query() {
        let (result, _, closed) = run_writer(true, true);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("injected close failure")
        );
        assert!(!closed);
    }
}
