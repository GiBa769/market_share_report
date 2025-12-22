# File: market_share_report/src/seller_level/compute_seller_results.py
# Purpose: Aggregate SPU QAQC results to seller level using config benchmark
# Notes: Keep original paths and output schema. Optimize IO with chunk raw pairs + sqlite for distinct counts.

import os
import yaml
import sqlite3
import pandas as pd


RAW_DB = "qaqc_results/spu_level/normalized_raw_vendor_data.sqlite"
RAW_TABLE = "normalized_raw_vendor_data"
ATTR_PATH = "qaqc_results/spu_level/attribute_check_result.csv"
SAME_MONTH_PATH = "qaqc_results/spu_level/metric_same_month_result.csv"
DIFF_MONTH_PATH = "qaqc_results/spu_level/metric_diff_months_result.csv"

SELLER_SCOPE_PATH = "data/scope/seller_scope.csv"

OUTPUT_PATH = "qaqc_results/seller_level/seller_result.csv"

CFG_THRESHOLD = "config/benchmark_thresholds.yaml"
CFG_CONST = "config/qaqc_constants.yaml"

CHUNK_SIZE = 200_000
TMP_DB = "qaqc_results/_tmp_qaqc_seller.sqlite"
COMMIT_EVERY = 20  # chunks


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _load_scope(path: str, key: str):
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path, dtype=str, low_memory=False)
    if key not in df.columns:
        raise ValueError(f"Scope file {path} missing required column '{key}'")

    return df.dropna(subset=[key]).drop_duplicates(subset=[key])


def _load_checks_minimal():
    # Read only minimal columns to compute spu normal flag
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

    # is_normal = True only if no FAIL exists for that spu
    spu_status = (
        checks
        .groupby("spu_used_id")["check_result"]
        .apply(lambda x: ("FAIL" not in set(x.dropna().tolist())))
        .reset_index(name="is_normal")
    )
    return spu_status


def _build_seller_spu_counts(spu_status_df):
    # Use sqlite to handle large distinct counts safely
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    if os.path.exists(TMP_DB):
        os.remove(TMP_DB)

    conn = sqlite3.connect(TMP_DB)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    cur.execute("CREATE TABLE seller_spu (seller_used_id TEXT, spu_used_id TEXT);")
    cur.execute("CREATE INDEX idx_seller_spu_seller ON seller_spu(seller_used_id);")
    cur.execute("CREATE INDEX idx_seller_spu_spu ON seller_spu(spu_used_id);")

    cur.execute("CREATE TABLE spu_status (spu_used_id TEXT PRIMARY KEY, is_normal INTEGER);")

    if not spu_status_df.empty:
        rows = [(r.spu_used_id, 1 if r.is_normal else 0) for r in spu_status_df.itertuples(index=False)]
        cur.executemany("INSERT INTO spu_status(spu_used_id, is_normal) VALUES(?, ?);", rows)

    conn.commit()

    raw_conn = sqlite3.connect(RAW_DB)
    reader = pd.read_sql_query(
        f"SELECT seller_used_id, spu_used_id FROM {RAW_TABLE}",
        raw_conn,
        chunksize=CHUNK_SIZE,
    )

    chunk_idx = 0
    processed = 0
    for chunk in reader:
        pairs = chunk.dropna(subset=["seller_used_id", "spu_used_id"]).drop_duplicates()
        if pairs.empty:
            continue
        cur.executemany(
            "INSERT INTO seller_spu(seller_used_id, spu_used_id) VALUES(?, ?);",
            list(pairs.itertuples(index=False, name=None))
        )
        chunk_idx += 1
        if chunk_idx % COMMIT_EVERY == 0:
            conn.commit()
        processed += len(chunk)
        if processed and processed % 300_000 == 0:
            print(f"[seller] ingested {processed:,} rows ...", flush=True)

    raw_conn.close()

    conn.commit()

    # total distinct spu per seller
    total_df = pd.read_sql_query(
        """
        SELECT seller_used_id, COUNT(DISTINCT spu_used_id) AS total_spu
        FROM seller_spu
        GROUP BY seller_used_id
        """,
        conn
    )

    # normal distinct spu per seller
    normal_df = pd.read_sql_query(
        """
        SELECT s.seller_used_id, COUNT(DISTINCT s.spu_used_id) AS normal_spu
        FROM seller_spu s
        LEFT JOIN spu_status t ON t.spu_used_id = s.spu_used_id
        WHERE COALESCE(t.is_normal, 1) = 1
        GROUP BY s.seller_used_id
        """,
        conn
    )

    conn.close()
    if os.path.exists(TMP_DB):
        os.remove(TMP_DB)

    return total_df, normal_df


def compute_seller_results():
    thresholds = load_yaml(CFG_THRESHOLD)
    constants = load_yaml(CFG_CONST)

    status = constants["check_result"]

    pass_min_pct = thresholds["seller_level"]["spu_coverage_ratio"]["pass_min_pct"]

    if not os.path.exists(RAW_DB):
        return

    spu_status = _load_checks_minimal()

    total_df, normal_df = _build_seller_spu_counts(spu_status)

    summary = total_df.merge(normal_df, on="seller_used_id", how="left").fillna(0)

    summary["coverage_pct"] = summary.apply(
        lambda r: (r["normal_spu"] / r["total_spu"] * 100) if r["total_spu"] else 0.0,
        axis=1,
    )
    summary["seller_result"] = summary.apply(
        lambda r: status["pass"]
        if r["total_spu"] and r["coverage_pct"] >= pass_min_pct
        else (status["fail"] if r["total_spu"] else status["skipped"]),
        axis=1,
    )

    scope_df = _load_scope(SELLER_SCOPE_PATH, key="seller_used_id")
    if scope_df is not None:
        base = scope_df.copy()
        base["scope_status"] = "in_scope"

        merged = base.merge(summary, on="seller_used_id", how="left")
        merged[["total_spu", "normal_spu"]] = merged[["total_spu", "normal_spu"]].fillna(0)
        merged["coverage_pct"] = merged.apply(
            lambda r: (r["normal_spu"] / r["total_spu"] * 100) if r["total_spu"] else 0.0,
            axis=1,
        )
        merged["seller_result"] = merged.apply(
            lambda r: status["pass"]
            if r["total_spu"] and r["coverage_pct"] >= pass_min_pct
            else (status["fail"] if r["total_spu"] else status["skipped"]),
            axis=1,
        )

        merged.loc[merged["total_spu"] == 0, "scope_status"] = "missed"

        extras = summary.loc[~summary["seller_used_id"].isin(base["seller_used_id"])].copy()
        if not extras.empty:
            extras["scope_status"] = "extra"
        merged = pd.concat([merged, extras], ignore_index=True, sort=False)
    else:
        summary["scope_status"] = "in_scope"
        merged = summary

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    # Keep scope columns (when present) and computed metrics in the final layout
    ordered_cols = []
    if scope_df is not None:
        ordered_cols.extend([c for c in scope_df.columns if c != "seller_used_id"])
    for c in [
        "seller_used_id",
        "total_spu",
        "normal_spu",
        "coverage_pct",
        "seller_result",
        "scope_status",
    ]:
        if c not in ordered_cols:
            ordered_cols.append(c)

    merged[ordered_cols].to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    compute_seller_results()
