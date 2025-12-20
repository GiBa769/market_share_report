# File: src/normalize/normalize_raw_vendor_data.py
# Purpose: Normalize raw vendor data – generate minimal snapshots only
# Safe for very large input

import os
import yaml
import pandas as pd

RAW_VENDOR_DATA_DIR = "data/raw_vendor_data"

OUT_DIR = "qaqc_results/spu_level"
OUT_ATTR = f"{OUT_DIR}/spu_attribute_snapshot.csv"
OUT_METRIC = f"{OUT_DIR}/spu_vendor_metric_snapshot.csv"

CONFIG_PATH = "config/qaqc_constants.yaml"
CHUNK_SIZE = 200_000


def load_constants():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def normalize_raw_vendor_data():
    constants = load_constants()
    vendor_types = constants["vendor_group_type"]

    os.makedirs(OUT_DIR, exist_ok=True)
    for p in [OUT_ATTR, OUT_METRIC]:
        if os.path.exists(p):
            os.remove(p)

    attr_header = True
    metric_header = True

    for fname in os.listdir(RAW_VENDOR_DATA_DIR):
        if not fname.endswith(".csv"):
            continue

        reader = pd.read_csv(
            os.path.join(RAW_VENDOR_DATA_DIR, fname),
            chunksize=CHUNK_SIZE,
            dtype=str,
            low_memory=False,
        )

        for chunk in reader:
            # ---- standard columns ----
            if "historical_review" in chunk.columns and "historical_rating" not in chunk.columns:
                chunk["historical_rating"] = chunk["historical_review"]

            # metrics to numeric
            for c in ["asp", "historical_quantity", "historical_rating"]:
                if c in chunk.columns:
                    chunk[c] = pd.to_numeric(chunk[c], errors="coerce")

            # vendor_group
            if "vendor_id" in chunk.columns and chunk["vendor_id"].notna().any():
                chunk["vendor_group"] = chunk["vendor_id"].astype(str)
                chunk["vendor_group_type"] = vendor_types["vendor_id"]
            elif "time_scraped" in chunk.columns and chunk["time_scraped"].notna().any():
                chunk["vendor_group"] = chunk["time_scraped"].astype(str)
                chunk["vendor_group_type"] = vendor_types["time_scraped"]
            else:
                chunk["vendor_group"] = "SINGLE_SOURCE"
                chunk["vendor_group_type"] = vendor_types["single_source"]

            # ---------- ATTRIBUTE SNAPSHOT ----------
            attr_cols = [
                "spu_used_id", "month",
                "spu_name", "spu_url",
                "seller_name", "seller_url",
            ]
            attr_df = (
                chunk[attr_cols]
                .dropna(subset=["spu_used_id", "month"])
                .drop_duplicates()
            )

            attr_df.to_csv(
                OUT_ATTR,
                mode="a",
                index=False,
                header=attr_header
            )
            attr_header = False

            # ---------- METRIC SNAPSHOT (MINIMAL) ----------
            metric_cols = [
                "spu_used_id", "month",
                "vendor_group",
                "asp", "historical_quantity", "historical_rating",
            ]
            metric_df = (
                chunk[metric_cols]
                .dropna(subset=["spu_used_id", "month"])
            )

            metric_df.to_csv(
                OUT_METRIC,
                mode="a",
                index=False,
                header=metric_header
            )
            metric_header = False


if __name__ == "__main__":
    normalize_raw_vendor_data()
