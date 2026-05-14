use std::num::NonZeroUsize;

use polars_buffer::Buffer;
use polars_core::prelude::*;
use polars_io::cloud::CloudOptions;
use polars_io::{HiveOptions, RowIndex};
use polars_utils::pl_path::PlRefPath;
use polars_utils::slice_enum::Slice;
use polars_vortex::{VortexCacheMode, VortexScanOptions};

use crate::prelude::*;

/// User-facing argument struct for [`LazyFrame::scan_vortex`]. Mirrors `ScanArgsParquet` so that
/// users moving between the two formats have a consistent API surface.
#[derive(Clone)]
pub struct ScanArgsVortex {
    pub n_rows: Option<usize>,
    pub row_index: Option<RowIndex>,
    pub cloud_options: Option<CloudOptions>,
    pub hive_options: HiveOptions,
    pub schema: Option<SchemaRef>,
    pub rechunk: bool,
    pub cache: bool,
    /// Expand path given via globbing rules.
    pub glob: bool,
    pub include_file_paths: Option<PlSmallStr>,
    pub allow_missing_columns: bool,

    // Vortex-specific knobs.
    pub push_predicate: bool,
    pub initial_read_size: Option<usize>,
    pub scan_concurrency: Option<NonZeroUsize>,
    pub segment_cache: VortexCacheMode,
}

impl Default for ScanArgsVortex {
    fn default() -> Self {
        Self {
            n_rows: None,
            row_index: None,
            cloud_options: None,
            hive_options: Default::default(),
            schema: None,
            rechunk: false,
            cache: true,
            glob: true,
            include_file_paths: None,
            allow_missing_columns: false,
            push_predicate: true,
            initial_read_size: None,
            scan_concurrency: None,
            segment_cache: VortexCacheMode::Global,
        }
    }
}

#[derive(Clone)]
struct LazyVortexReader {
    args: ScanArgsVortex,
    sources: ScanSources,
}

impl LazyVortexReader {
    fn new(args: ScanArgsVortex) -> Self {
        Self {
            args,
            sources: ScanSources::default(),
        }
    }
}

impl LazyFileListReader for LazyVortexReader {
    fn finish(self) -> PolarsResult<LazyFrame> {
        let vortex_options = VortexScanOptions {
            schema: self.args.schema,
            push_predicate: self.args.push_predicate,
            initial_read_size: self.args.initial_read_size,
            scan_concurrency: self.args.scan_concurrency,
            cache: self.args.segment_cache,
        };

        let unified_scan_args = UnifiedScanArgs {
            schema: None,
            cloud_options: self.args.cloud_options,
            hive_options: self.args.hive_options,
            rechunk: self.args.rechunk,
            cache: self.args.cache,
            glob: self.args.glob,
            hidden_file_prefix: None,
            projection: None,
            column_mapping: None,
            default_values: None,
            // Row index is applied via `with_row_index` below so it threads through the schema
            // updates the rest of the LazyFrame expects. Same as the Parquet reader.
            row_index: None,
            pre_slice: self
                .args
                .n_rows
                .map(|len| Slice::Positive { offset: 0, len }),
            cast_columns_policy: CastColumnsPolicy::ERROR_ON_MISMATCH,
            missing_columns_policy: if self.args.allow_missing_columns {
                MissingColumnsPolicy::Insert
            } else {
                MissingColumnsPolicy::Raise
            },
            extra_columns_policy: ExtraColumnsPolicy::Raise,
            include_file_paths: self.args.include_file_paths,
            deletion_files: None,
            table_statistics: None,
            row_count: None,
        };

        let mut lf: LazyFrame =
            DslBuilder::scan_vortex(self.sources, vortex_options, unified_scan_args)?
                .build()
                .into();

        if let Some(row_index) = self.args.row_index {
            lf = lf.with_row_index(row_index.name, Some(row_index.offset))
        }

        Ok(lf)
    }

    fn glob(&self) -> bool {
        self.args.glob
    }

    fn finish_no_glob(self) -> PolarsResult<LazyFrame> {
        unreachable!();
    }

    fn sources(&self) -> &ScanSources {
        &self.sources
    }

    fn with_sources(mut self, sources: ScanSources) -> Self {
        self.sources = sources;
        self
    }

    fn with_n_rows(mut self, n_rows: impl Into<Option<usize>>) -> Self {
        self.args.n_rows = n_rows.into();
        self
    }

    fn with_row_index(mut self, row_index: impl Into<Option<RowIndex>>) -> Self {
        self.args.row_index = row_index.into();
        self
    }

    fn rechunk(&self) -> bool {
        self.args.rechunk
    }

    fn with_rechunk(mut self, toggle: bool) -> Self {
        self.args.rechunk = toggle;
        self
    }

    fn cloud_options(&self) -> Option<&CloudOptions> {
        self.args.cloud_options.as_ref()
    }

    fn n_rows(&self) -> Option<usize> {
        self.args.n_rows
    }

    fn row_index(&self) -> Option<&RowIndex> {
        self.args.row_index.as_ref()
    }
}

impl LazyFrame {
    /// Create a LazyFrame directly from a Vortex scan.
    pub fn scan_vortex(path: PlRefPath, args: ScanArgsVortex) -> PolarsResult<Self> {
        Self::scan_vortex_sources(ScanSources::Paths(Buffer::from_iter([path])), args)
    }

    /// Create a LazyFrame directly from a Vortex scan over an arbitrary [`ScanSources`].
    pub fn scan_vortex_sources(sources: ScanSources, args: ScanArgsVortex) -> PolarsResult<Self> {
        LazyVortexReader::new(args).with_sources(sources).finish()
    }

    /// Create a LazyFrame directly from a Vortex scan over multiple file paths.
    pub fn scan_vortex_files(
        paths: Buffer<PlRefPath>,
        args: ScanArgsVortex,
    ) -> PolarsResult<Self> {
        Self::scan_vortex_sources(ScanSources::Paths(paths), args)
    }
}
