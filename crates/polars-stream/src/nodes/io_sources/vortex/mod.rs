//! Streaming reader for one Vortex file.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use polars_arrow::datatypes::ArrowSchemaRef;
use polars_async::executor::{self, JoinHandle, TaskMetricAggregator};
use polars_async::primitives::wait_group::{WaitGroup, WaitToken};
use polars_core::prelude::DataFrame;
use polars_core::runtime::ASYNC;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_ensure, polars_err};
use polars_io::cloud::CloudOptions;
use polars_io::utils::byte_source::{DynByteSource, DynByteSourceBuilder};
use polars_plan::dsl::ScanSource;
use polars_utils::IdxSize;
use polars_utils::slice_enum::Slice;
use polars_vortex::VortexScanOptions;
use polars_vortex::read::array_bridge::vortex_array_to_dataframe;
use polars_vortex::read::predicate::polars_to_vortex_predicate;
use polars_vortex::read::read_at::PolarsVortexReadAt;
use polars_vortex::read::schema::vortex_dtype_to_schema;
use polars_vortex::session::ExecutionContext;
use polars_vortex::vortex::array::ArrayRef;
use polars_vortex::vortex::error::vortex_ensure;
use polars_vortex::vortex::expr::{root, select};
use polars_vortex::vortex::file::{Footer, OpenOptionsSessionExt, VortexFile};
use polars_vortex::vortex::io::CoalesceConfig;
use polars_vortex::vortex::io::runtime::Handle;
use polars_vortex::vortex::layout::scan::repeated_scan::RepeatedScan;

use crate::metrics::OptIOMetrics;
use crate::morsel::{Morsel, MorselSeq, SourceToken, get_ideal_morsel_size};
use crate::nodes::TaskPriority;
use crate::nodes::io_sources::multi_scan::reader_interface::output::{
    FileReaderOutputRecv, FileReaderOutputSend,
};
use crate::nodes::io_sources::multi_scan::reader_interface::{
    BeginReadArgs, FileReader, calc_row_position_after_slice,
};
use crate::nodes::io_sources::shared::morsels::split_to_morsels;
use crate::nodes::io_sources::shared::pipeline_budget::{
    PipelineBudget, PipelinePermit, WindowGrowth,
};
use crate::utils::tokio_handle_ext::AbortOnDropHandle;

mod budget;
pub mod builder;
#[cfg(test)]
mod tests;

pub struct VortexFileReader {
    pub scan_source: ScanSource,
    pub cloud_options: Option<Arc<CloudOptions>>,
    pub options: Arc<VortexScanOptions>,
    pub footer: Option<Arc<Footer>>,
    pub byte_source_builder: DynByteSourceBuilder,
    pub aexpr_filter: Option<(polars_vortex::vortex::expr::Expression, SchemaRef)>,
    pub io_metrics: OptIOMetrics,
    pub task_metrics: Option<Arc<TaskMetricAggregator>>,
    pub execution_context: ExecutionContext,
    prefetch_sync: PrefetchSync,
    pub init_data: Option<InitializedState>,
}

pub struct InitializedState {
    vxf: VortexFile,
    arrow_schema: ArrowSchemaRef,
    pl_schema: SchemaRef,
    row_count: IdxSize,
    byte_source: Arc<DynByteSource>,
}

struct PrefetchSync {
    budget: PipelineBudget,
    shared_slot: Arc<Mutex<Option<WaitGroup>>>,
    previous: Option<WaitGroup>,
    all_spawned: Option<WaitToken>,
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
        let context = self.execution_context.clone();
        let (vxf, byte_source) = AbortOnDropHandle(ASYNC.spawn(async move {
            let byte_source = Arc::new(
                source
                    .as_scan_source_ref()
                    .to_dyn_byte_source(&builder, cloud_options.as_deref(), io_metrics)
                    .await?,
            );
            let read_at = Arc::new(PolarsVortexReadAt::with_context(
                byte_source.clone(),
                context.clone(),
            ));
            let mut open = context
                .session()
                .open_options()
                .with_segment_cache(options.segment_cache.resolve());
            if let Some(footer) = footer {
                open = open.with_footer(Arc::unwrap_or_clone(footer));
            }
            if let Some(size) = options.initial_read_size {
                open = open.with_initial_read_size(size);
            }
            let file = open
                .open(read_at)
                .await
                .map_err(|e| polars_err!(ComputeError: "vortex open: {e}"))?;
            PolarsResult::Ok((file, byte_source))
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
            byte_source,
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

    fn prepare_read(&mut self) -> PolarsResult<()> {
        let group = WaitGroup::default();
        self.prefetch_sync.all_spawned = Some(group.token());
        self.prefetch_sync.previous = self
            .prefetch_sync
            .shared_slot
            .try_lock()
            .unwrap()
            .replace(group);
        Ok(())
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
                self.aexpr_filter
                    .as_ref()
                    .filter(|(_, proof)| {
                        proof
                            .iter()
                            .all(|(name, dtype)| st.pl_schema.get(name) == Some(dtype))
                    })
                    .filter(|_| {
                        requested_schema
                            .iter()
                            .all(|(name, dtype)| st.pl_schema.get(name) == Some(dtype))
                    })
                    .map(|(expression, _)| expression.clone())
                    .or_else(|| {
                        polars_to_vortex_predicate(
                            &predicate.scan_io_predicate,
                            requested_schema,
                            &st.pl_schema,
                        )
                    })
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
        let context = self.execution_context.clone();
        let byte_source = st.byte_source.clone();
        let budget = self.prefetch_sync.budget.clone();
        budget.begin_file(
            byte_source.window_inflight_budget().is_some(),
            args.maintain_order,
        );
        let previous = self.prefetch_sync.previous.take();
        let all_spawned = self.prefetch_sync.all_spawned.take();
        let disable_morsel_split = args.disable_morsel_split;
        let last_morsel_pipelines = args.last_morsel_pipelines;
        let (mut tx, rx) = FileReaderOutputSend::new_serial();
        let handle = executor::spawn(
            TaskPriority::Low,
            self.task_metrics.as_deref(),
            async move {
                if let Some(previous) = previous {
                    previous.wait().await;
                }
                if height == 0 {
                    return Ok(());
                }
                if columns.is_empty() && filter.is_none() {
                    drop(all_spawned);
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
                let selected_range = row_range.clone().unwrap_or(0..vxf.row_count());
                let mut budget_columns = columns.clone();
                if let Some(range) = row_range {
                    scan = scan.with_row_range(range);
                }
                // A failed candidate binding leaves the complete predicate in Polars.
                if let Some(expression) = filter {
                    if let Ok(bound) = expression.bind(vxf.dtype()) {
                        let paths =
                            polars_vortex::vortex::expr::analysis::referenced_field_paths(&bound);
                        let mut all_fields = false;
                        if let Ok(paths) = paths {
                            for path in paths {
                                match path.parts().first() {
                                    Some(polars_vortex::vortex::dtype::Field::Name(name)) => {
                                        budget_columns.push(name.to_string());
                                    },
                                    _ => all_fields = true,
                                }
                            }
                        } else {
                            all_fields = true;
                        }
                        if all_fields {
                            budget_columns.extend(
                                vxf.dtype()
                                    .as_struct_fields_opt()
                                    .unwrap()
                                    .names()
                                    .iter()
                                    .map(ToString::to_string),
                            );
                        }
                        polars_vortex::read::read_at::record_filter();
                        if vxf.can_prune(&expression).unwrap_or(false) {
                            return Ok(());
                        }
                        scan = scan.with_filter(bound);
                    }
                }
                let boundaries = scan
                    .full_file_splits()
                    .map_err(|e| polars_err!(ComputeError: "Vortex split boundaries: {e}"))?;
                let ranges = boundaries
                    .windows(2)
                    .filter_map(|pair| {
                        let start = pair[0].max(selected_range.start);
                        let end = pair[1].min(selected_range.end);
                        (start < end).then_some(start..end)
                    })
                    .collect::<Vec<_>>();
                let coalesce = if byte_source.is_cloud() {
                    CoalesceConfig::object_storage()
                } else {
                    CoalesceConfig::file()
                };
                budget_columns.sort_unstable();
                budget_columns.dedup();
                let costs = budget::split_costs(vxf.footer(), &budget_columns, &ranges, coalesce)?;
                let prepared = Arc::new(
                    scan.with_natural_splits(boundaries.into())
                        .prepare()
                        .map_err(|e| polars_err!(ComputeError: "vortex scan preparation: {e}"))?,
                );
                let (prefetch_tx, prefetch_rx) = tokio::sync::mpsc::channel(1);
                let prefetch = AbortOnDropHandle(ASYNC.spawn(async move {
                    let inflight = byte_source.window_inflight_budget();
                    let window = WindowGrowth::for_source(&byte_source, inflight.as_deref());
                    // Reserve in file and split order before any work starts.
                    for (range, cost) in ranges.into_iter().zip(costs) {
                        let permit = budget.acquire(cost, window, 1).await;
                        if prefetch_tx.send((range, permit)).await.is_err() {
                            break;
                        }
                    }
                    drop(all_spawned);
                }));
                let runtime = context.handle();
                let stream = futures::stream::unfold(prefetch_rx, |mut receiver| async move {
                    receiver.recv().await.map(|item| (item, receiver))
                })
                .map(move |(range, permit)| {
                    export_budgeted_split(prepared.clone(), range, runtime.clone(), permit)
                })
                .buffered(concurrency)
                .filter_map(nonempty_dataframe)
                .boxed()
                .peekable();
                futures::pin_mut!(stream);
                let mut seq = MorselSeq::default();
                let source_token = SourceToken::new();
                while let Some(result) = stream.next().await {
                    let df = result?;
                    polars_vortex::read::read_at::record_batch(df.width() as u64);
                    if disable_morsel_split {
                        let morsel = Morsel::new_unregistered(df, seq, source_token.clone());
                        seq = seq.successor();
                        if tx.send_morsel(morsel).await.is_err() {
                            return Ok(());
                        }
                    } else {
                        let last = stream.as_mut().peek().await.is_none();
                        for df in split_to_morsels(
                            &df,
                            get_ideal_morsel_size(),
                            last,
                            last_morsel_pipelines,
                        ) {
                            let morsel = Morsel::new_unregistered(df, seq, source_token.clone());
                            seq = seq.successor();
                            if tx.send_morsel(morsel).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                prefetch
                    .await
                    .map_err(|e| polars_err!(ComputeError: "Vortex prefetch task: {e}"))?;
                drop(context);
                Ok(())
            },
        );
        Ok((rx, handle))
    }
}

fn export_budgeted_split(
    prepared: Arc<RepeatedScan<ArrayRef>>,
    range: Range<u64>,
    runtime: Handle,
    permit: PipelinePermit,
) -> BoxFuture<'static, (PolarsResult<Option<DataFrame>>, PipelinePermit)> {
    async move { (export_split(prepared, range, runtime).await, permit) }.boxed()
}

fn nonempty_dataframe(
    (result, permit): (PolarsResult<Option<DataFrame>>, PipelinePermit),
) -> futures::future::Ready<Option<PolarsResult<DataFrame>>> {
    // Release before a lookahead or a blocked output consumer.
    drop(permit);
    futures::future::ready(match result {
        Ok(Some(df)) if df.height() > 0 => Some(Ok(df)),
        Ok(_) => None,
        Err(error) => Some(Err(error)),
    })
}

/// Keep the scheduled future type independent of the stream adapter closure.
fn export_split(
    prepared: Arc<RepeatedScan<ArrayRef>>,
    range: Range<u64>,
    runtime: Handle,
) -> BoxFuture<'static, PolarsResult<Option<DataFrame>>> {
    async move {
        // Task handles cancel on drop. Spawns remain inside this lazy future.
        let Some(array) = runtime
            .spawn(async move {
                // Construct requests only after the shared budget admits this range.
                let mut splits = prepared.execute(Some(range))?;
                vortex_ensure!(
                    splits.len() == 1,
                    "Vortex admitted range must contain one split"
                );
                splits.pop().unwrap().await
            })
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
