# File: src/spu_level/check_metric_same_month.py
# Purpose: SPU metric same-month QA – abnormal only, aggregated

import os
import yaml
import pandas as pd

INPUT_PATH = "qaqc_results/spu_level/spu_vendor_metric_snapshot.csv"
OUTPUT_PATH = "qaqc_results/spu_level/metric_same_month_result.csv"

CFG_THRESHOLD = "config/benchmark_thresholds.yaml"
CFG_CONST = "config/qaqc_constants.yaml"

METRICS = ["asp", "historical_quantity", "historical_rating"]


def load_yaml(p):
    with open(p, "r") as f:
        return yaml.safe_load(f)


def run_spu_metric_same_month_checks():
    thresholds = load_yaml(CFG_THRESHOLD)
    constants = load_yaml(CFG_CONST)

    status = constants["check_result"]
    cfg = thresholds["spu_metric"]["same_month"]

    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    results = []

    grouped = df.groupby(["spu_used_id", "month"], dropna=False)

    for (spu, month), g in grouped:
        vendor_cnt = g["vendor_group"].nunique()
        if vendor_cnt < 2:
            continue  # skip silently

        for metric in METRICS:
            values = g[metric].dropna().tolist()
            if len(values) < 2:
                continue

            ratio_pct = max(values) / min(values) * 100
            mcfg = cfg[f"{metric}_ratio"]

            if not (mcfg["min_pct"] <= ratio_pct <= mcfg["max_pct"]):
                results.append({
                    "spu_used_id": spu,
                    "month": month,
                    "metric_name": metric,
                    "vendor_group_count": vendor_cnt,
                    "ratio_pct": ratio_pct,
                    "check_result": status["fail"],
                })

    if results:
        pd.DataFrame(results).to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    run_spu_metric_same_month_checks()
