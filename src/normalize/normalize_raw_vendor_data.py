# File: src/normalize/normalize_raw_vendor_data.py
# Purpose: Normalize raw vendor data into a single canonical extract used by all QAQC steps
# Safe for very large input via chunked streaming writes

import os
import sqlite3
import yaml
import pandas as pd

RAW_VENDOR_DATA_DIR = "data/raw_vendor_data"
COMPUTED_DATA_DIR = "data/computed_data"

OUT_DIR = "qaqc_results/spu_level"
OUT_DB = f"{OUT_DIR}/normalized_raw_vendor_data.sqlite"
RUN_MANIFEST = f"{OUT_DIR}/_run_manifest.txt"

CONFIG_PATH = "config/qaqc_constants.yaml"
CHUNK_SIZE = 200_000
SQL_TABLE = "normalized_raw_vendor_data"
SQLITE_MAX_VARIABLES = 900  # conservative safeguard for SQLite placeholder limit

CANONICAL_COLS = [
    "spu_used_id",
    "month",
    "spu_name",
    "spu_url",
    "seller_name",
    "seller_url",
    "seller_used_id",
    "source",  # category_url
    "country",
    "platform",
    "vendor_group",
    "vendor_group_type",
    "asp",
    "historical_quantity",
    "historical_rating",
]


def load_constants():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def _write_manifest(constants, input_root, source_files, total_rows):
    with open(RUN_MANIFEST, "w") as f:
        f.write("chunk_size=" + str(CHUNK_SIZE) + "\n")
        f.write("vendor_group_type=" + str(constants.get("vendor_group_type", {})) + "\n")
        f.write("input_dir=" + input_root + "\n")
        f.write("source_files=" + ",".join(sorted(source_files)) + "\n")
        f.write("stored_as=sqlite\n")
        f.write("row_count=" + str(total_rows) + "\n")


def _create_indexes(conn: sqlite3.Connection):
    cur = conn.cursor()
    # Indexes accelerate downstream group-by operations during QAQC.
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_norm_spu_month ON {SQL_TABLE}(spu_used_id, month);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_norm_vendor_group ON {SQL_TABLE}(vendor_group);"
    )
    conn.commit()


def _pick_input_sources():
    """Pick the first directory that contains CSV vendor inputs.

    Prefer raw vendor drops; if empty, fall back to computed outputs. Returns
    a tuple of (dir, csv_files).
    """

    candidates = [RAW_VENDOR_DATA_DIR, COMPUTED_DATA_DIR]
    for d in candidates:
        if not os.path.isdir(d):
            continue

        csv_files = sorted([f for f in os.listdir(d) if f.endswith(".csv")])
        if csv_files:
            return d, csv_files

    raise FileNotFoundError(
        "No CSV inputs found in data/computed_data or data/raw_vendor_data"
    )


def normalize_raw_vendor_data():
    constants = load_constants()
    vendor_types = constants["vendor_group_type"]

    os.makedirs(OUT_DIR, exist_ok=True)
    for p in [OUT_DB, RUN_MANIFEST]:
        if os.path.exists(p):
            os.remove(p)

    input_dir, input_files = _pick_input_sources()

    conn = sqlite3.connect(OUT_DB)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {SQL_TABLE};")
        conn.commit()

        seen_sources = []
        total_rows = 0
        chunk_idx = 0

        for fname in input_files:
            seen_sources.append(os.path.join(os.path.basename(input_dir), fname))

            reader = pd.read_csv(
                os.path.join(input_dir, fname),
                chunksize=CHUNK_SIZE,
                dtype=str,
                low_memory=False,
            )

            for chunk in reader:
                chunk_idx += 1
                # normalize column presence
                if "historical_review" in chunk.columns and "historical_rating" not in chunk.columns:
                    chunk["historical_rating"] = chunk["historical_review"]

                # metrics to numeric early to prevent string comparisons downstream
                for c in ["asp", "historical_quantity", "historical_rating"]:
                    if c in chunk.columns:
                        chunk[c] = pd.to_numeric(chunk[c], errors="coerce")

                # vendor_group resolution
                if "vendor_id" in chunk.columns and chunk["vendor_id"].notna().any():
                    chunk["vendor_group"] = chunk["vendor_id"].astype(str)
                    chunk["vendor_group_type"] = vendor_types["vendor_id"]
                elif "time_scraped" in chunk.columns and chunk["time_scraped"].notna().any():
                    chunk["vendor_group"] = chunk["time_scraped"].astype(str)
                    chunk["vendor_group_type"] = vendor_types["time_scraped"]
                else:
                    chunk["vendor_group"] = "SINGLE_SOURCE"
                    chunk["vendor_group_type"] = vendor_types["single_source"]

                # keep only canonical columns, dropping rows missing key identifiers
                missing_cols = [c for c in CANONICAL_COLS if c not in chunk.columns]
                for c in missing_cols:
                    chunk[c] = pd.NA

                normalized = chunk[CANONICAL_COLS].dropna(subset=["spu_used_id", "month"])
                if normalized.empty:
                    continue

                # pandas builds a single multi-value INSERT per chunksize; keep the batch small
                # to avoid hitting SQLite's host parameter limit (commonly ~999)
                safe_batch_size = max(1, SQLITE_MAX_VARIABLES // len(CANONICAL_COLS))

                normalized.to_sql(
                    SQL_TABLE,
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=safe_batch_size,
                )

                total_rows += len(normalized)
                if chunk_idx % 5 == 0:
                    print(f"[normalize] processed {total_rows:,} rows ...", flush=True)

        conn.commit()
        print("[normalize] building indexes ...", flush=True)
        _create_indexes(conn)
    finally:
        conn.close()

    _write_manifest(constants, os.path.basename(input_dir), seen_sources, total_rows)


def cleanup_normalized_store():
    for p in [OUT_DB, RUN_MANIFEST]:
        if os.path.exists(p):
            os.remove(p)


if __name__ == "__main__":
    normalize_raw_vendor_data()
