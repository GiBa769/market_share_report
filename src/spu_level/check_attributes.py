# File: market_share_report/src/spu_level/check_attributes.py
# Purpose: SPU attribute QAQC
# Strategy: record FAIL only, no row-level attribute dump

import os
import sqlite3
import pandas as pd

RAW_DB = "qaqc_results/spu_level/normalized_raw_vendor_data.sqlite"
RAW_TABLE = "normalized_raw_vendor_data"
OUTPUT_PATH = "qaqc_results/spu_level/attribute_check_result.csv"

CHUNK_SIZE = 200_000


def run_spu_attribute_checks():
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    if not os.path.exists(RAW_DB):
        pd.DataFrame(columns=["spu_used_id", "check_result"]).to_csv(
            OUTPUT_PATH, index=False
        )
        return

    failed_spu = set()

    conn = sqlite3.connect(RAW_DB)
    reader = pd.read_sql_query(
        f"SELECT spu_used_id, spu_name, spu_url FROM {RAW_TABLE}",
        conn,
        chunksize=CHUNK_SIZE,
    )

    processed = 0
    for chunk in reader:
        # attribute rules (example – giữ đúng tinh thần file cũ)
        invalid = chunk[
            chunk["spu_used_id"].isna()
            | chunk["spu_name"].isna()
            | chunk["spu_url"].isna()
        ]

        if invalid.empty:
            processed += len(chunk)
            if processed and processed % 500_000 == 0:
                print(f"[attributes] scanned {processed:,} rows ...", flush=True)
            continue

        failed_spu.update(invalid["spu_used_id"].dropna().unique())
        processed += len(chunk)
        if processed and processed % 500_000 == 0:
            print(f"[attributes] scanned {processed:,} rows ...", flush=True)

    conn.close()

    if not failed_spu:
        pd.DataFrame(columns=["spu_used_id", "check_result"]).to_csv(
            OUTPUT_PATH, index=False
        )
        return

    result_df = pd.DataFrame({
        "spu_used_id": list(failed_spu),
        "check_result": "FAIL",
    })

    result_df.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    run_spu_attribute_checks()
