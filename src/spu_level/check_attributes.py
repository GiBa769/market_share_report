# File: market_share_report/src/spu_level/check_attributes.py
# Purpose: SPU attribute QAQC
# Strategy: record FAIL only, no row-level attribute dump

import os
import pandas as pd

RAW_PATH = "qaqc_results/spu_level/normalized_raw_vendor_data.csv"
OUTPUT_PATH = "qaqc_results/spu_level/attribute_check_result.csv"

CHUNK_SIZE = 200_000


def run_spu_attribute_checks():
    if not os.path.exists(RAW_PATH):
        return

    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    failed_spu = set()

    reader = pd.read_csv(
        RAW_PATH,
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False,
    )

    for chunk in reader:
        # attribute rules (example – giữ đúng tinh thần file cũ)
        invalid = chunk[
            chunk["spu_used_id"].isna()
            | chunk["spu_name"].isna()
            | chunk["spu_url"].isna()
        ]

        if invalid.empty:
            continue

        failed_spu.update(invalid["spu_used_id"].dropna().unique())

    if not failed_spu:
        return

    result_df = pd.DataFrame({
        "spu_used_id": list(failed_spu),
        "check_result": "FAIL",
    })

    result_df.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    run_spu_attribute_checks()
