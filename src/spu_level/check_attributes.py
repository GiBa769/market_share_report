# File: market_share_report/src/spu_level/check_attributes.py
# Purpose: SPU attribute consistency check using snapshot (FAST, SAFE)

import os
import yaml
import pandas as pd


INPUT_PATH = "qaqc_results/spu_level/spu_attribute_snapshot.csv"
OUTPUT_PATH = "qaqc_results/spu_level/attribute_check_result.csv"
CONFIG_PATH = "config/qaqc_constants.yaml"

ATTRIBUTE_COLUMNS = [
    "spu_name",
    "spu_url",
    "seller_name",
    "seller_url",
]


def load_constants():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def run_spu_attribute_checks():
    constants = load_constants()
    status = constants["check_result"]

    if not os.path.exists(INPUT_PATH):
        raise ValueError("Attribute snapshot not found")

    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    df = pd.read_csv(INPUT_PATH, dtype=str, low_memory=False)

    results = []

    grouped = df.groupby(["spu_used_id", "month"], dropna=False)

    for (spu_used_id, month), g in grouped:
        for attr in ATTRIBUTE_COLUMNS:
            if attr not in g.columns:
                continue

            distinct_cnt = g[attr].dropna().nunique()
            check_result = (
                status["pass"] if distinct_cnt <= 1 else status["fail"]
            )

            results.append({
                "spu_used_id": spu_used_id,
                "month": month,
                "attribute_name": attr,
                "check_result": check_result,
            })

    pd.DataFrame(results).to_csv(
        OUTPUT_PATH,
        index=False
    )


if __name__ == "__main__":
    run_spu_attribute_checks()
