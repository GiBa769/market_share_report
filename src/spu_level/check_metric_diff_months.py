# File: src/spu_level/check_metric_diff_months.py
# Purpose: SPU metric diff-month QA – abnormal only, aggregated

import os
import yaml
import pandas as pd

CUR_PATH = "qaqc_results/spu_level/spu_vendor_metric_snapshot.csv"
HIST_DIR = "data/computed_data"
OUTPUT_PATH = "qaqc_results/spu_level/metric_diff_months_result.csv"

CFG_THRESHOLD = "config/benchmark_thresholds.yaml"
CFG_CONST = "config/qaqc_constants.yaml"

METRICS = ["asp", "historical_quantity", "historical_rating"]


def load_yaml(p):
    with open(p, "r") as f:
        return yaml.safe_load(f)


def load_history_agg():
    dfs = []
    for f in os.listdir(HIST_DIR):
        if f.endswith(".csv"):
            dfs.append(pd.read_csv(os.path.join(HIST_DIR, f)))
    if not dfs:
        return pd.DataFrame()

    hist = pd.concat(dfs, ignore_index=True)
    return (
        hist
        .groupby("spu_used_id")[METRICS]
        .mean()
        .reset_index()
    )


def run_spu_metric_diff_months_checks():
    thresholds = load_yaml(CFG_THRESHOLD)
    constants = load_yaml(CFG_CONST)

    status = constants["check_result"]
    cfg = thresholds["spu_metric"]["diff_months"]["default"]

    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    cur = (
        pd.read_csv(CUR_PATH)
        .groupby(["spu_used_id", "month"])[METRICS]
        .mean()
        .reset_index()
    )

    hist = load_history_agg()
    if hist.empty:
        return

    merged = cur.merge(hist, on="spu_used_id", suffixes=("_cur", "_hist"))

    results = []

    for _, r in merged.iterrows():
        for m in METRICS:
            cur_v = r[f"{m}_cur"]
            hist_v = r[f"{m}_hist"]

            if pd.isna(cur_v) or pd.isna(hist_v) or hist_v <= 0:
                continue

            ratio_pct = cur_v / hist_v * 100
            if not (cfg["min_pct"] <= ratio_pct <= cfg["max_pct"]):
                results.append({
                    "spu_used_id": r["spu_used_id"],
                    "month": r["month"],
                    "metric_name": m,
                    "ratio_pct": ratio_pct,
                    "check_result": status["fail"],
                })

    if results:
        pd.DataFrame(results).to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    run_spu_metric_diff_months_checks()
