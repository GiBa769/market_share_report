# File: src/normalize/normalize_raw_vendor_data.py
# Purpose: Normalize raw vendor data into a single canonical extract used by all QAQC steps
# Safe for very large input via chunked streaming writes

import os
import yaml
import pandas as pd

RAW_VENDOR_DATA_DIR = "data/raw_vendor_data"

OUT_DIR = "qaqc_results/spu_level"
OUT_NORMALIZED = f"{OUT_DIR}/normalized_raw_vendor_data.csv"
RUN_MANIFEST = f"{OUT_DIR}/_run_manifest.txt"

CONFIG_PATH = "config/qaqc_constants.yaml"
CHUNK_SIZE = 200_000

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


def _write_manifest(constants, source_files):
    with open(RUN_MANIFEST, "w") as f:
        f.write("chunk_size=" + str(CHUNK_SIZE) + "\n")
        f.write("vendor_group_type=" + str(constants.get("vendor_group_type", {})) + "\n")
        f.write("source_files=" + ",".join(sorted(source_files)) + "\n")


def normalize_raw_vendor_data():
    constants = load_constants()
    vendor_types = constants["vendor_group_type"]

    os.makedirs(OUT_DIR, exist_ok=True)
    for p in [OUT_NORMALIZED, RUN_MANIFEST]:
        if os.path.exists(p):
            os.remove(p)

    header_written = False
    seen_sources = []

    for fname in os.listdir(RAW_VENDOR_DATA_DIR):
        if not fname.endswith(".csv"):
            continue

        seen_sources.append(fname)
        reader = pd.read_csv(
            os.path.join(RAW_VENDOR_DATA_DIR, fname),
            chunksize=CHUNK_SIZE,
            dtype=str,
            low_memory=False,
        )

        for chunk in reader:
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

            normalized = (
                chunk[CANONICAL_COLS]
                .dropna(subset=["spu_used_id", "month"])
            )

            normalized.to_csv(
                OUT_NORMALIZED,
                mode="a",
                index=False,
                header=not header_written,
            )
            header_written = True

    _write_manifest(constants, seen_sources)


if __name__ == "__main__":
    normalize_raw_vendor_data()
