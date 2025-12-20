# File: market_share_report/src/category_level/compute_category_results.py
# Purpose: Aggregate SPU QAQC results to category URL level using config benchmark
# Notes: Keep original paths and output schema. Optimize IO with chunk raw pairs + sqlite for distinct counts.

import os
import yaml
import sqlite3
import pandas as pd


RAW_PATH = "qaqc_results/spu_level/normalized_raw_vendor_data.csv"
ATTR_PATH = "qaqc_results/spu_level/attribute_check_result.csv"
SAME_MONTH_PATH = "qaqc_results/spu_level/metric_same_month_result.csv"
DIFF_MONTH_PATH = "qaqc_results/spu_level/metric_diff_months_result.csv"

OUTPUT_PATH = "qaqc_results/category_level/category_result.csv"

CFG_THRESHOLD = "config/benchmark_thresholds.yaml"
CFG_CONST = "config/qaqc_constants.yaml"

CHUNK_SIZE = 200_000
TMP_DB = "qaqc_results/_tmp_qaqc_category.sqlite"


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _load_checks_minimal():
    def _load(path):
        if not os.path.exists(path):
            return pd.DataFrame(columns=["spu_used_id", "check_result"])
        return pd.read_csv(path, dtype=str, usecols=["spu_used_id", "check_result"], low_memory=False)

    attr_df = _load(ATTR_PATH)
    same_df = _load(SAME_MONTH_PATH)
    diff_df = _load(DIFF_MONTH_PATH)

    checks = pd.concat([attr_df, same_df, diff_df], ignore_index=True)
    if checks.empty:
        return pd.DataFrame(columns=["spu_used_id", "is_normal"])

    spu_status = (
        checks
        .groupby("spu_used_id")["check_result"]
        .apply(lambda x: ("FAIL" not in set(x.dropna().tolist())))
        .reset_index(name="is_normal")
    )
    return spu_status


def _build_category_spu_counts(spu_status_df):
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    if os.path.exists(TMP_DB):
        os.remove(TMP_DB)

    conn = sqlite3.connect(TMP_DB)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    cur.execute("CREATE TABLE category_spu (category_url TEXT, spu_used_id TEXT);")
    cur.execute("CREATE INDEX idx_category_spu_cat ON category_spu(category_url);")
    cur.execute("CREATE INDEX idx_category_spu_spu ON category_spu(spu_used_id);")

    cur.execute("CREATE TABLE spu_status (spu_used_id TEXT PRIMARY KEY, is_normal INTEGER);")

    if not spu_status_df.empty:
        rows = [(r.spu_used_id, 1 if r.is_normal else 0) for r in spu_status_df.itertuples(index=False)]
        cur.executemany("INSERT INTO spu_status(spu_used_id, is_normal) VALUES(?, ?);", rows)

    conn.commit()

    reader = pd.read_csv(
        RAW_PATH,
        chunksize=CHUNK_SIZE,
        dtype=str,
        usecols=["source", "spu_used_id"],
        low_memory=False,
    )

    for chunk in reader:
        chunk = chunk.rename(columns={"source": "category_url"})
        pairs = chunk.dropna(subset=["category_url", "spu_used_id"]).drop_duplicates()
        if pairs.empty:
            continue

        cur.executemany(
            "INSERT INTO category_spu(category_url, spu_used_id) VALUES(?, ?);",
            list(pairs.itertuples(index=False, name=None))
        )
        conn.commit()

    total_df = pd.read_sql_query(
        """
        SELECT category_url, COUNT(DISTINCT spu_used_id) AS total_spu
        FROM category_spu
        GROUP BY category_url
        """,
        conn
    )

    normal_df = pd.read_sql_query(
        """
        SELECT c.category_url, COUNT(DISTINCT c.spu_used_id) AS normal_spu
        FROM category_spu c
        JOIN spu_status t ON t.spu_used_id = c.spu_used_id
        WHERE t.is_normal = 1
        GROUP BY c.category_url
        """,
        conn
    )

    conn.close()
    if os.path.exists(TMP_DB):
        os.remove(TMP_DB)

    return total_df, normal_df


def compute_category_results():
    thresholds = load_yaml(CFG_THRESHOLD)
    constants = load_yaml(CFG_CONST)

    status = constants["check_result"]

    pass_min_pct = thresholds["category_level"]["pass_min_pct"]

    if not os.path.exists(RAW_PATH):
        return

    spu_status = _load_checks_minimal()

    total_df, normal_df = _build_category_spu_counts(spu_status)

    summary = total_df.merge(normal_df, on="category_url", how="left").fillna(0)

    summary["coverage_pct"] = summary["normal_spu"] / summary["total_spu"] * 100
    summary["category_result"] = summary["coverage_pct"].apply(
        lambda x: status["pass"] if x >= pass_min_pct else status["fail"]
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    summary.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    compute_category_results()
