# File: src/spu_level/check_metric_same_month.py
# Purpose: SPU metric same-month QA – abnormal only, aggregated

import os
import sqlite3
import yaml
import pandas as pd

INPUT_DB = "qaqc_results/spu_level/normalized_raw_vendor_data.sqlite"
INPUT_TABLE = "normalized_raw_vendor_data"
OUTPUT_PATH = "qaqc_results/spu_level/metric_same_month_result.csv"

CFG_THRESHOLD = "config/benchmark_thresholds.yaml"
CFG_CONST = "config/qaqc_constants.yaml"

METRICS = ["asp", "historical_quantity", "historical_rating"]
CHUNK_SIZE = 200_000


def load_yaml(p):
    with open(p, "r") as f:
        return yaml.safe_load(f)


def _init_metric_state():
    return {
        "min": None,
        "max": None,
    }


def _update_min_max(state, series):
    # series should already be numeric
    if series.empty:
        return state
    cur_min = series.min()
    cur_max = series.max()

    if pd.isna(cur_min) or pd.isna(cur_max):
        return state

    if state["min"] is None or cur_min < state["min"]:
        state["min"] = cur_min
    if state["max"] is None or cur_max > state["max"]:
        state["max"] = cur_max
    return state


def run_spu_metric_same_month_checks():
    thresholds = load_yaml(CFG_THRESHOLD)
    constants = load_yaml(CFG_CONST)

    status = constants["check_result"]
    cfg = thresholds["spu_metric"]["same_month"]

    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    if not os.path.exists(INPUT_DB):
        return

    # state: {(spu, month): {metric: {min, max}, vendor_groups=set()}}
    stats = {}

    conn = sqlite3.connect(INPUT_DB)
    reader = pd.read_sql_query(
        f"SELECT spu_used_id, month, vendor_group, {', '.join(METRICS)} FROM {INPUT_TABLE}",
        conn,
        chunksize=CHUNK_SIZE,
    )

    processed = 0
    for chunk in reader:
        chunk = chunk.dropna(subset=["spu_used_id", "month"])

        # vendor_group counts need distinct across chunks
        chunk_vendor = (
            chunk[["spu_used_id", "month", "vendor_group"]]
            .dropna(subset=["vendor_group"])
            .drop_duplicates()
        )

        for row in chunk_vendor.itertuples(index=False):
            key = (row.spu_used_id, row.month)
            if key not in stats:
                stats[key] = {m: _init_metric_state() for m in METRICS}
                stats[key]["vendor_groups"] = set()
            stats[key]["vendor_groups"].add(row.vendor_group)

        # numeric metrics for min/max
        for metric in METRICS:
            if metric not in chunk.columns:
                continue
            metric_chunk = chunk[["spu_used_id", "month", metric]].dropna(subset=[metric])
            if metric_chunk.empty:
                continue

            grouped = metric_chunk.groupby(["spu_used_id", "month"])[metric]
            for (spu, month), series in grouped:
                key = (spu, month)
                if key not in stats:
                    stats[key] = {m: _init_metric_state() for m in METRICS}
                    stats[key]["vendor_groups"] = set()
                stats[key][metric] = _update_min_max(stats[key][metric], series)

        processed += len(chunk)
        if processed and processed % 400_000 == 0:
            print(f"[same_month] scanned {processed:,} rows ...", flush=True)

    conn.close()

    results = []
    for (spu, month), agg in stats.items():
        vendor_cnt = len(agg.get("vendor_groups", []))
        if vendor_cnt < 2:
            continue

        for metric in METRICS:
            mstat = agg.get(metric, {})
            min_v, max_v = mstat.get("min"), mstat.get("max")
            if min_v is None or max_v is None:
                continue
            if min_v <= 0:
                continue  # avoid divide by zero or negative baseline

            ratio_pct = max_v / min_v * 100
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
