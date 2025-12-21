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
# Allow history files that still use the legacy column name "historical_review".
HIST_ALIASES = {"historical_rating": ["historical_rating", "historical_review"]}
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
        present_metrics = [m for m in METRICS if m in chunk.columns]
        if not present_metrics:
            continue

        for metric in present_metrics:
            chunk[metric] = pd.to_numeric(chunk[metric], errors="coerce")

        grouped = chunk.groupby(list(group_keys))[present_metrics]
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
    total_rows = 0
    for fname in os.listdir(HIST_DIR):
        if not fname.endswith(".csv"):
            continue

        fpath = os.path.join(HIST_DIR, fname)
        header = pd.read_csv(fpath, nrows=0)

        # pick the available alias column for each metric
        metric_cols = []
        rename_map = {}
        for metric in METRICS:
            aliases = HIST_ALIASES.get(metric, [metric])
            for col in aliases:
                if col in header.columns:
                    metric_cols.append(col)
                    if col != metric:
                        rename_map[col] = metric
                    break

        if not metric_cols:
            continue

        usecols = ["spu_used_id", *metric_cols]
        reader = pd.read_csv(
            fpath,
            chunksize=HIST_CHUNK_SIZE,
            dtype=str,
            usecols=usecols,
            low_memory=False,
        )

        if rename_map:
            reader = (chunk.rename(columns=rename_map) for chunk in reader)

        file_acc = _accumulate_means(reader, group_keys=["spu_used_id"])

        # normalize legacy column names to canonical
        if rename_map:
            for key in file_acc:
                for old, new in rename_map.items():
                    file_acc[key][new] = file_acc[key].get(new, {"sum": 0.0, "count": 0.0})
                    file_acc[key][new]["sum"] += file_acc[key].pop(old, {"sum": 0.0})["sum"]
                    file_acc[key][new]["count"] += file_acc[key].pop(old, {"count": 0.0})["count"]

        total_rows += sum(
            max(v[m]["count"] for m in METRICS if m in v)
            for v in file_acc.values()
        )
        print(
            f"[diff_months] loaded history chunk from {fname}, total rows ~{int(total_rows):,} ...",
            flush=True,
        )

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
    print(
        f"[diff_months] built current means for {len(cur_df):,} spu-month pairs",
        flush=True,
    )
    conn.close()

    hist_df = _load_history_means()
    if hist_df.empty or cur_df.empty:
        return

    print(f"[diff_months] built history means for {len(hist_df):,} spu ids", flush=True)

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
        print(f"[diff_months] wrote {len(results):,} failures", flush=True)
    else:
        print("[diff_months] no failures found", flush=True)


if __name__ == "__main__":
    run_spu_metric_diff_months_checks()
