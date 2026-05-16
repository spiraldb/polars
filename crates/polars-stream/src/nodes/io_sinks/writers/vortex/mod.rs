//! Streaming sink writer for Vortex files.
//!
//! Plugs into Polars' `FileWriterStarter` trait so `LazyFrame::sink_vortex(path)`
//! works alongside `sink_parquet`, `sink_ipc`, etc. Drives Vortex's async
//! `VortexWriteOptions::write` from Polars' streaming `async_executor` by:
//!
//! 1. Deriving the top-level Vortex `DType` from the file schema upfront
//!    (Vortex's `ArrayStream` requires `dtype()` to be known before draining).
//! 2. Spawning a producer task that pulls `SinkMorsel`s, converts each
//!    `DataFrame` chunk into a Vortex `ArrayRef` via the C-ABI bridge, and
//!    pushes through a `futures::channel::mpsc` queue.
//! 3. Wrapping that queue as a Vortex `ArrayStream` and handing it to
//!    `VortexWriteOptions::write(file, stream)`, which runs on Polars'
//!    global `ASYNC` Tokio runtime.

use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::channel::mpsc;
use futures::sink::SinkExt;
use polars_core::runtime::ASYNC;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_bail, polars_err};
use polars_io::utils::file::Writeable;
use polars_utils::index::NonZeroIdxSize;
use polars_vortex::VortexWriteOptions;
use polars_vortex::vortex::array::ArrayRef as VortexArrayRef;
use polars_vortex::vortex::array::stream::ArrayStreamAdapter;
use polars_vortex::vortex::error::VortexResult;
use polars_vortex::write::VortexSink;
use polars_vortex::write::strategy::build_write_options;

use crate::async_executor::{self, TaskPriority};
use crate::async_primitives::connector;
use crate::utils::tokio_handle_ext;
use crate::nodes::io_sinks::components::sink_morsel::SinkMorsel;
use crate::nodes::io_sinks::components::size::{
    NonZeroRowCountAndSize, RowCountAndSize, TakeableRowsProvider,
};
use crate::nodes::io_sinks::writers::interface::{
    FileOpenTaskHandle, FileWriterStarter, ideal_sink_morsel_size_env,
};

pub struct VortexWriterStarter {
    pub options: Arc<VortexWriteOptions>,
    pub schema: SchemaRef,
}

impl FileWriterStarter for VortexWriterStarter {
    fn writer_name(&self) -> &str {
        "vortex"
    }

    fn takeable_rows_provider(&self) -> TakeableRowsProvider {
        // Vortex's layout strategy handles internal chunking; we don't need fine-grained
        // morsel control. Use the same default as IPC.
        let (num_rows, num_bytes) = ideal_sink_morsel_size_env();
        let max_size = NonZeroRowCountAndSize::new(RowCountAndSize {
            num_rows: num_rows.unwrap_or(122_880),
            num_bytes: num_bytes.unwrap_or(u64::MAX),
        })
        .unwrap();
        TakeableRowsProvider {
            max_size,
            byte_size_min_rows: NonZeroIdxSize::new(16384).unwrap(),
            allow_non_max_size: false,
        }
    }

    fn start_file_writer(
        &self,
        mut morsel_rx: connector::Receiver<SinkMorsel>,
        file: FileOpenTaskHandle,
        num_pipelines: NonZeroUsize,
    ) -> PolarsResult<async_executor::JoinHandle<PolarsResult<()>>> {
        let options = Arc::clone(&self.options);
        let schema = Arc::clone(&self.schema);

        // Derive the top-level Vortex DType from the schema upfront. This is what
        // `ArrayStreamAdapter::new` requires to construct a typed stream before any
        // data flows.
        let top_dtype =
            polars_vortex::write::df_to_stream::polars_schema_to_vortex_dtype(schema.as_ref())?;

        let handle = async_executor::spawn(TaskPriority::Low, async move {
            // Await the file. Locals go straight to tokio::fs::File (which has its
            // own VortexWrite impl); cloud writers bridge through
            // tokio_util::compat → vortex::io::AsyncWriteAdapter. Both collapse to a
            // single VortexSink enum so the writer below is generic-free.
            let (writeable, _sync) = file.await?;
            let sink: VortexSink = match writeable {
                Writeable::Local(std_file) => {
                    VortexSink::Local(tokio::fs::File::from_std(std_file))
                },
                #[cfg(feature = "cloud")]
                Writeable::Cloud(cw) => VortexSink::cloud(cw),
                Writeable::Dyn(_) => polars_bail!(ComputeError:
                    "Vortex sink does not support arbitrary file descriptors. \
                     Use a local path or a cloud URL (s3://, gs://, etc.)."),
            };

            // Build a channel for ArrayRef chunks. Bounded by num_pipelines for
            // backpressure: morsel production stalls if the writer falls behind.
            let (chunk_tx, chunk_rx) =
                mpsc::channel::<VortexResult<VortexArrayRef>>(num_pipelines.get());

            // Producer: drain morsel_rx, convert each DataFrame to Vortex ArrayRefs,
            // send through chunk_tx. The chunk-level dtype derived inside
            // `dataframe_to_vortex_chunks` equals `top_dtype` and is discarded here;
            // the consumer side uses `top_dtype` directly via `ArrayStreamAdapter::new`.
            let producer = async_executor::AbortOnDropHandle::new(async_executor::spawn(
                TaskPriority::High,
                async move {
                    let mut tx = chunk_tx;
                    while let Ok(morsel) = morsel_rx.recv().await {
                        let (df, _permit) = morsel.into_inner();
                        let (_chunk_dtype, chunks) =
                            polars_vortex::write::df_to_stream::dataframe_to_vortex_chunks(&df)?;
                        for chunk in chunks {
                            if tx.send(Ok(chunk)).await.is_err() {
                                // Writer dropped; bail.
                                return Ok::<_, polars_error::PolarsError>(());
                            }
                        }
                    }
                    // Sentinel: drop tx → stream end-of-input
                    drop(tx);
                    Ok(())
                },
            ));

            // Build the Vortex ArrayStream and drive the write on ASYNC.
            let stream = ArrayStreamAdapter::new(top_dtype, chunk_rx);
            let write_opts = build_write_options(options.as_ref());

            // Wrap in `AbortOnDropHandle` so the writer task is aborted (not orphaned)
            // if the outer task fails before reaching the await site below. A bare
            // `tokio::task::JoinHandle` would keep running after drop, see the dropped
            // chunk channel as clean EOS, and finalize a truncated-but-valid Vortex
            // file on disk. Mirrors the IPC sink pattern at
            // `crates/polars-stream/src/nodes/io_sinks/writers/ipc/mod.rs:101`.
            let write_handle = tokio_handle_ext::AbortOnDropHandle(ASYNC.spawn(async move {
                write_opts
                    .write(sink, stream)
                    .await
                    .map_err(|e| polars_err!(ComputeError: "vortex sink write: {e}"))
            }));

            // Wait for both the producer and writer.
            producer.await?;
            write_handle
                .await
                .map_err(|e| polars_err!(ComputeError: "vortex sink tokio join: {e}"))??;

            Ok(())
        });

        Ok(handle)
    }
}
