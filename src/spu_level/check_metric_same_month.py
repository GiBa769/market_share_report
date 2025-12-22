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
CHUNK_SIZE = 100_000


def load_yaml(p):
    with open(p, "r") as f:
        return yaml.safe_load(f)


def _build_temp_tables(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS tmp_same_month_vendor_cnt;")
    cur.execute(
        f"""
        CREATE TEMP TABLE tmp_same_month_vendor_cnt AS
        SELECT spu_used_id, month, COUNT(DISTINCT vendor_group) AS vendor_group_count
        FROM {INPUT_TABLE}
        WHERE vendor_group IS NOT NULL
        GROUP BY spu_used_id, month;
        """
    )
    cur.execute("SELECT COUNT(*) FROM tmp_same_month_vendor_cnt;")
    print(
        f"[same_month] vendor coverage groups built for {cur.fetchone()[0]:,} spu-month pairs",
        flush=True,
    )

    for metric in METRICS:
        cur.execute(f"DROP TABLE IF EXISTS tmp_same_month_{metric};")
        cur.execute(
            f"""
            CREATE TEMP TABLE tmp_same_month_{metric} AS
            SELECT spu_used_id, month, MIN({metric}) AS min_v, MAX({metric}) AS max_v
            FROM {INPUT_TABLE}
            WHERE {metric} IS NOT NULL
            GROUP BY spu_used_id, month;
            """
        )
        cur.execute(f"SELECT COUNT(*) FROM tmp_same_month_{metric};")
        print(
            f"[same_month] {metric} min/max built for {cur.fetchone()[0]:,} spu-month pairs",
            flush=True,
        )

    conn.commit()


def _build_scan_query():
    metric_joins = []
    select_parts = ["v.spu_used_id", "v.month", "v.vendor_group_count"]
    for metric in METRICS:
        metric_joins.append(
            f"LEFT JOIN tmp_same_month_{metric} {metric} ON {metric}.spu_used_id = v.spu_used_id AND {metric}.month = v.month"
        )
        select_parts.append(f"{metric}.min_v AS {metric}_min")
        select_parts.append(f"{metric}.max_v AS {metric}_max")

    join_sql = "\n".join(metric_joins)
    select_sql = ", \n       ".join(select_parts)

    return f"""
    SELECT {select_sql}
    FROM tmp_same_month_vendor_cnt v
    {join_sql}
    WHERE v.vendor_group_count >= 2;
    """


def run_spu_metric_same_month_checks():
    thresholds = load_yaml(CFG_THRESHOLD)
    constants = load_yaml(CFG_CONST)

    status = constants["check_result"]
    cfg = thresholds["spu_metric"]["same_month"]

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    if not os.path.exists(INPUT_DB):
        return

    conn = sqlite3.connect(INPUT_DB)
    try:
        _build_temp_tables(conn)
        query = _build_scan_query()

        reader = pd.read_sql_query(query, conn, chunksize=CHUNK_SIZE)
        header_written = False
        scanned = 0

        with open(OUTPUT_PATH, "w", newline="") as f:
            for chunk in reader:
                scanned += len(chunk)
                rows = []
                for row in chunk.itertuples(index=False):
                    vendor_cnt = row.vendor_group_count
                    for metric in METRICS:
                        min_v = getattr(row, f"{metric}_min")
                        max_v = getattr(row, f"{metric}_max")

                        if pd.isna(min_v) or pd.isna(max_v) or min_v is None or max_v is None:
                            continue
                        if min_v <= 0:
                            continue  # avoid divide by zero or negative baseline

                        ratio_pct = max_v / min_v * 100
                        mcfg = cfg[f"{metric}_ratio"]

                        if not (mcfg["min_pct"] <= ratio_pct <= mcfg["max_pct"]):
                            rows.append(
                                {
                                    "spu_used_id": row.spu_used_id,
                                    "month": row.month,
                                    "metric_name": metric,
                                    "vendor_group_count": vendor_cnt,
                                    "ratio_pct": ratio_pct,
                                    "check_result": status["fail"],
                                }
                            )

                if rows:
                    pd.DataFrame(rows).to_csv(
                        f, mode="a", header=not header_written, index=False
                    )
                    header_written = True

                if scanned and scanned % 200_000 == 0:
                    print(
                        f"[same_month] evaluated {scanned:,} spu-month pairs ...",
                        flush=True,
                    )

        if not header_written and os.path.exists(OUTPUT_PATH):
            os.remove(OUTPUT_PATH)
    finally:
        conn.close()


if __name__ == "__main__":
    run_spu_metric_same_month_checks()
