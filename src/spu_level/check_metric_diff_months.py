# File: src/spu_level/check_metric_diff_months.py
# Purpose: SPU metric diff-month QA – abnormal only, aggregated

import os
import sqlite3
import yaml
import pandas as pd

CUR_DB = "qaqc_results/spu_level/normalized_raw_vendor_data.sqlite"
CUR_TABLE = "normalized_raw_vendor_data"
HIST_DIR = "data/computed_data"
OUTPUT_PATH = "qaqc_results/spu_level/metric_diff_months_result.csv"

CFG_THRESHOLD = "config/benchmark_thresholds.yaml"
CFG_CONST = "config/qaqc_constants.yaml"

METRICS = ["asp", "historical_quantity", "historical_rating"]
CUR_CHUNK_SIZE = 200_000
HIST_CHUNK_SIZE = 200_000


def load_yaml(p):
    with open(p, "r") as f:
        return yaml.safe_load(f)


def _accumulate_means(reader, group_keys):
    # returns dict: key -> {metric: {"sum": x, "count": n}}
    acc = {}
    for chunk in reader:
        chunk = chunk.dropna(subset=list(group_keys))
        for metric in METRICS:
            if metric in chunk.columns:
                chunk[metric] = pd.to_numeric(chunk[metric], errors="coerce")

        grouped = chunk.groupby(list(group_keys))[METRICS]
        summary = grouped.agg(["sum", "count"])

        for idx, row in summary.iterrows():
            key = idx if isinstance(idx, tuple) else (idx,)
            if key not in acc:
                acc[key] = {m: {"sum": 0.0, "count": 0.0} for m in METRICS}
            for metric in METRICS:
                acc[key][metric]["sum"] += row[(metric, "sum")]
                acc[key][metric]["count"] += row[(metric, "count")]
    return acc


def _acc_to_df(acc, key_names):
    records = []
    for key, metrics in acc.items():
        record = dict(zip(key_names, key))
        for metric, stat in metrics.items():
            if stat["count"] > 0:
                record[metric] = stat["sum"] / stat["count"]
            else:
                record[metric] = pd.NA
        records.append(record)
    return pd.DataFrame(records)


def _load_history_means():
    if not os.path.isdir(HIST_DIR):
        return pd.DataFrame()

    acc = {}
    for fname in os.listdir(HIST_DIR):
        if not fname.endswith(".csv"):
            continue

        reader = pd.read_csv(
            os.path.join(HIST_DIR, fname),
            chunksize=HIST_CHUNK_SIZE,
            dtype=str,
            usecols=["spu_used_id", *METRICS],
            low_memory=False,
        )
        file_acc = _accumulate_means(reader, group_keys=["spu_used_id"])

        # merge file_acc into acc
        for key, metrics in file_acc.items():
            if key not in acc:
                acc[key] = {m: {"sum": 0.0, "count": 0.0} for m in METRICS}
            for metric in METRICS:
                acc[key][metric]["sum"] += metrics[metric]["sum"]
                acc[key][metric]["count"] += metrics[metric]["count"]

    if not acc:
        return pd.DataFrame()

    return _acc_to_df(acc, key_names=["spu_used_id"])


def run_spu_metric_diff_months_checks():
    thresholds = load_yaml(CFG_THRESHOLD)
    constants = load_yaml(CFG_CONST)

    status = constants["check_result"]
    cfg = thresholds["spu_metric"]["diff_months"]["default"]

    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    if not os.path.exists(CUR_DB):
        return

    conn = sqlite3.connect(CUR_DB)
    cur_reader = pd.read_sql_query(
        f"SELECT spu_used_id, month, {', '.join(METRICS)} FROM {CUR_TABLE}",
        conn,
        chunksize=CUR_CHUNK_SIZE,
    )
    cur_acc = _accumulate_means(cur_reader, group_keys=["spu_used_id", "month"])
    cur_df = _acc_to_df(cur_acc, key_names=["spu_used_id", "month"])
    conn.close()

    hist_df = _load_history_means()
    if hist_df.empty or cur_df.empty:
        return

    merged = cur_df.merge(hist_df, on="spu_used_id", suffixes=("_cur", "_hist"))

    results = []

    for _, r in merged.iterrows():
        for m in METRICS:
            cur_v = r.get(f"{m}_cur")
            hist_v = r.get(f"{m}_hist")

            if pd.isna(cur_v) or pd.isna(hist_v) or hist_v <= 0:
                continue

            ratio_pct = float(cur_v) / float(hist_v) * 100
            if not (cfg["min_pct"] <= ratio_pct <= cfg["max_pct"]):
                results.append({
                    "spu_used_id": r["spu_used_id"],
                    "month": r.get("month"),
                    "metric_name": m,
                    "ratio_pct": ratio_pct,
                    "check_result": status["fail"],
                })

    if results:
        pd.DataFrame(results).to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    run_spu_metric_diff_months_checks()
