//! Streaming reader for one Vortex file.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use polars_arrow::datatypes::ArrowSchemaRef;
use polars_async::executor::{self, JoinHandle, TaskMetricAggregator};
use polars_core::prelude::DataFrame;
use polars_core::runtime::ASYNC;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_ensure, polars_err};
use polars_io::cloud::CloudOptions;
use polars_io::utils::byte_source::DynByteSourceBuilder;
use polars_plan::dsl::ScanSource;
use polars_utils::IdxSize;
use polars_utils::slice_enum::Slice;
use polars_vortex::VortexScanOptions;
use polars_vortex::read::array_bridge::vortex_array_to_dataframe;
use polars_vortex::read::predicate::polars_to_vortex_predicate;
use polars_vortex::read::read_at::PolarsVortexReadAt;
use polars_vortex::read::schema::vortex_dtype_to_schema;
use polars_vortex::session::session;
use polars_vortex::vortex::array::ArrayRef;
use polars_vortex::vortex::error::VortexResult;
use polars_vortex::vortex::expr::{root, select};
use polars_vortex::vortex::file::{Footer, OpenOptionsSessionExt, VortexFile};
use polars_vortex::vortex::io::runtime::Handle;

use crate::metrics::OptIOMetrics;
use crate::morsel::{Morsel, MorselSeq, SourceToken};
use crate::nodes::TaskPriority;
use crate::nodes::io_sources::multi_scan::reader_interface::output::{
    FileReaderOutputRecv, FileReaderOutputSend,
};
use crate::nodes::io_sources::multi_scan::reader_interface::{
    BeginReadArgs, FileReader, calc_row_position_after_slice,
};

pub mod builder;

pub struct VortexFileReader {
    pub scan_source: ScanSource,
    pub cloud_options: Option<Arc<CloudOptions>>,
    pub options: Arc<VortexScanOptions>,
    pub footer: Option<Arc<Footer>>,
    pub byte_source_builder: DynByteSourceBuilder,
    pub io_metrics: OptIOMetrics,
    pub task_metrics: Option<Arc<TaskMetricAggregator>>,
    pub init_data: Option<InitializedState>,
}

pub struct InitializedState {
    vxf: VortexFile,
    arrow_schema: ArrowSchemaRef,
    pl_schema: SchemaRef,
    row_count: IdxSize,
}

#[async_trait]
impl FileReader for VortexFileReader {
    async fn initialize(&mut self) -> PolarsResult<()> {
        if self.init_data.is_some() {
            return Ok(());
        }
        let source = self.scan_source.clone();
        let builder = self.byte_source_builder.clone();
        let cloud_options = self.cloud_options.clone();
        let io_metrics = self.io_metrics.0.clone();
        let footer = self.footer.clone();
        let options = self.options.clone();
        let vxf = crate::utils::tokio_handle_ext::AbortOnDropHandle(ASYNC.spawn(async move {
            let byte_source = source
                .as_scan_source_ref()
                .to_dyn_byte_source(&builder, cloud_options.as_deref(), io_metrics)
                .await?;
            let read_at = Arc::new(PolarsVortexReadAt::new(Arc::new(byte_source)));
            let mut open = session()
                .open_options()
                .with_segment_cache(options.segment_cache.resolve());
            if let Some(footer) = footer {
                open = open.with_footer(Arc::unwrap_or_clone(footer));
            }
            if let Some(size) = options.initial_read_size {
                open = open.with_initial_read_size(size);
            }
            open.open(read_at)
                .await
                .map_err(|e| polars_err!(ComputeError: "vortex open: {e}"))
        }))
        .await
        .map_err(|e| polars_err!(ComputeError: "vortex open task: {e}"))??;
        let (pl_schema, arrow_schema) = vortex_dtype_to_schema(vxf.dtype())?;
        let row_count = IdxSize::try_from(vxf.row_count()).map_err(
            |_| polars_err!(ComputeError: "Vortex row count exceeds the Polars index size"),
        )?;
        self.init_data = Some(InitializedState {
            vxf,
            arrow_schema,
            pl_schema,
            row_count,
        });
        Ok(())
    }

    async fn file_schema(&mut self) -> PolarsResult<SchemaRef> {
        self.initialize().await?;
        Ok(self.init_data.as_ref().unwrap().pl_schema.clone())
    }

    async fn file_arrow_schema(&mut self) -> PolarsResult<Option<ArrowSchemaRef>> {
        self.initialize().await?;
        Ok(Some(self.init_data.as_ref().unwrap().arrow_schema.clone()))
    }

    async fn n_rows_in_file(&mut self) -> PolarsResult<IdxSize> {
        self.initialize().await?;
        Ok(self.init_data.as_ref().unwrap().row_count)
    }

    async fn fast_n_rows_in_file(&mut self) -> PolarsResult<Option<IdxSize>> {
        self.n_rows_in_file().await.map(Some)
    }

    async fn row_position_after_slice(
        &mut self,
        pre_slice: Option<Slice>,
    ) -> PolarsResult<IdxSize> {
        Ok(calc_row_position_after_slice(
            self.n_rows_in_file().await?,
            pre_slice,
        ))
    }

    fn begin_read(
        &mut self,
        args: BeginReadArgs,
    ) -> PolarsResult<(FileReaderOutputRecv, JoinHandle<PolarsResult<()>>)> {
        let st = self
            .init_data
            .as_ref()
            .expect("initialize must precede begin_read");
        polars_ensure!(!args.projection.has_projection_transforms() && args.row_index.is_none(),
            ComputeError: "Vortex reader received unsupported projection transforms or row indices");
        let normalized_slice = args
            .pre_slice
            .clone()
            .map(|slice| slice.restrict_to_bounds(st.row_count as usize));
        let row_range = normalized_slice.as_ref().map(|slice| match slice {
            Slice::Positive { offset, len } => *offset as u64..offset.saturating_add(*len) as u64,
            Slice::Negative { .. } => unreachable!(),
        });
        if let Some(tx) = args.callbacks.file_schema_tx {
            let _ = tx.send(st.pl_schema.clone());
        }
        if let Some(tx) = args.callbacks.n_rows_in_file_tx {
            let _ = tx.send(st.row_count);
        }
        if let Some(tx) = args.callbacks.row_position_on_end_tx {
            let _ = tx.send(calc_row_position_after_slice(st.row_count, args.pre_slice));
        }
        let requested_schema = args.projection.projected_schema();
        let columns: Vec<String> = requested_schema
            .iter_names()
            .filter(|name| st.pl_schema.contains(name.as_str()))
            .map(|name| name.to_string())
            .collect();
        let filter = if self.options.push_predicate {
            args.predicate.as_ref().and_then(|predicate| {
                polars_to_vortex_predicate(
                    &predicate.scan_io_predicate,
                    requested_schema,
                    &st.pl_schema,
                )
            })
        } else {
            None
        };
        let vxf = st.vxf.clone();
        let height = normalized_slice
            .as_ref()
            .map_or(st.row_count as usize, |slice| slice.len());
        let concurrency = self
            .options
            .scan_concurrency
            .map(|value| value.get())
            .unwrap_or(args.num_pipelines.max(1));
        let (mut tx, rx) = FileReaderOutputSend::new_serial();
        let handle = executor::spawn(
            TaskPriority::Low,
            self.task_metrics.as_deref(),
            async move {
                if height == 0 {
                    return Ok(());
                }
                if columns.is_empty() && filter.is_none() {
                    let _ = tx
                        .send_morsel(Morsel::new_unregistered(
                            DataFrame::empty_with_height(height),
                            MorselSeq::default(),
                            SourceToken::new(),
                        ))
                        .await;
                    return Ok(());
                }
                let projection = select(
                    columns.iter().map(String::as_str).collect::<Vec<_>>(),
                    root(),
                )
                .bind(vxf.dtype())
                .map_err(|e| polars_err!(ComputeError: "vortex projection: {e}"))?;
                let mut scan = vxf
                    .scan()
                    .map_err(|e| polars_err!(ComputeError: "vortex scan: {e}"))?
                    .with_projection(projection);
                if let Some(range) = row_range {
                    scan = scan.with_row_range(range);
                }
                // A failed candidate binding leaves the complete predicate in Polars.
                if let Some(expression) = filter {
                    if let Ok(bound) = expression.bind(vxf.dtype()) {
                        scan = scan.with_filter(bound);
                    }
                }
                let splits = scan
                    .build()
                    .map_err(|e| polars_err!(ComputeError: "vortex scan preparation: {e}"))?;
                let runtime = polars_vortex::session::handle();
                // One slot covers both split IO and Arrow export. Vortex's stream
                // concurrency is per OS thread, so the reader owns this global bound.
                let stream = futures::stream::iter(splits)
                    .map(move |split| export_split(split, runtime.clone()))
                    .buffered(concurrency)
                    .boxed();
                futures::pin_mut!(stream);
                let mut seq = MorselSeq::default();
                let source_token = SourceToken::new();
                while let Some(result) = stream.next().await {
                    let Some(df) = result? else {
                        continue;
                    };

                    let morsel = Morsel::new_unregistered(df, seq, source_token.clone());
                    seq = seq.successor();
                    if tx.send_morsel(morsel).await.is_err() {
                        break;
                    }
                }
                Ok(())
            },
        );
        Ok((rx, handle))
    }
}

/// Keep the scheduled future type independent of the stream adapter closure.
fn export_split(
    split: BoxFuture<'static, VortexResult<Option<ArrayRef>>>,
    runtime: Handle,
) -> BoxFuture<'static, PolarsResult<Option<DataFrame>>> {
    async move {
        // Task handles cancel on drop. Spawns remain inside this lazy future.
        let Some(array) = runtime
            .spawn(split)
            .await
            .map_err(|e| polars_err!(ComputeError: "vortex split: {e}"))?
        else {
            return Ok(None);
        };
        runtime
            .spawn_cpu(move || vortex_array_to_dataframe(array))
            .await
            .map(Some)
    }
    .boxed()
}
