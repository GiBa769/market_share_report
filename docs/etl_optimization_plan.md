# ETL optimization blueprint for the QAQC pipeline

This playbook captures a standard way to run the pipeline on very large vendor datasets (millions of rows) without bloating intermediate exports. Each phase maps to the current code paths (`src/` modules) and highlights the controls needed to keep RAM and storage bounded.

## 1) Normalize once, keep one canonical raw extract
- Emit a single normalized file (e.g., `normalized_raw_vendor_data.csv` or parquet) from `normalize_raw_vendor_data`, and make every downstream step read from it instead of producing multiple snapshots. This removes duplicate exports and guarantees consistent input for attribute and metric checks.
- Store only the columns required by QAQC (SPU id, seller, category URL, country, platform, month, metrics/attributes) and drop unused raw columns during normalization to shrink I/O.

## 2) Stream reads and aggregate incrementally
- Replace full-file `pd.read_csv` calls in metric checks with chunked reads (`chunksize`) and per-chunk aggregation keyed by `(spu_used_id, month, metric)`, followed by a second-stage aggregation of the partial results. This keeps memory roughly proportional to chunk size, not total rows.
- For diff-month history, also stream the historical snapshot and pre-aggregate per key per month; avoid `pd.concat` of whole history frames.

## 3) Use lightweight on-disk state for heavy joins
- Where in-memory dictionaries are used to build mappings (e.g., country x platform), persist mappings to a temporary SQLite table or parquet file with indexes, then merge chunk-by-chunk. This prevents unbounded dict growth when the number of sellers/categories explodes.
- Batch SQLite inserts/updates inside explicit transactions (commit every N chunks) to reduce fsync overhead.

## 4) Guardrails for data quality while streaming
- During per-chunk aggregation, validate metrics defensively (skip or flag zero/negative denominators, NaN/inf, or identical max/min that cause division-by-zero). Log counts of skipped rows so QAQC results explain why records are missing.
- Reject malformed chunks early (missing required columns, wrong dtypes) and continue processing to avoid aborting the entire run.

## 5) Minimize duplicate outputs, surface only the results
- Keep intermediate artifacts to the minimum required for correctness: the canonical normalized extract plus the three QAQC outputs (attribute, metric same-month, metric diff-months). Downstream seller/category/country aggregations should consume those outputs directly.
- Avoid exporting full raw slices or expanded debug dumps unless gated by a debug flag.

## 6) Parallelize safely when possible
- For pure aggregation steps, process independent chunks in parallel (multiprocessing with bounded worker count) and combine partial aggregates with an associative merge (sum/count/max/min). Ensure deterministic output ordering when writing result files.

## 7) Choose efficient file formats and compression
- Prefer columnar formats (parquet with snappy/zstd) for large normalized data to speed up selective column reads and cut disk footprint. Keep CSV only where external compatibility is required.
- Compress final QAQC CSV outputs if they are large and not used interactively; preserve the Excel roll-up as a lightweight summary only.

## 8) Reproducibility and observability
- Record the exact source data version, normalization config, and chunk size in a run manifest next to outputs so reruns are comparable.
- Emit counters per stage (rows read, rows kept, rows skipped for each rule) to help interpret PASS/FAIL at seller/category/country levels without re-exporting raw data.

Following these steps, the pipeline can read, validate, and aggregate millions of rows while keeping RAM bounded, eliminating redundant exports, and producing concise PASS/FAIL reports at SPU, seller, category, and country x platform levels.
