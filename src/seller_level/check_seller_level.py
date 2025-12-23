import os
import sqlite3
import pandas as pd
import numpy as np

# =========================
# CONFIG (EXPLICIT, NO MAGIC)
# =========================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "spu_level",
    "normalized_raw_vendor_data.sqlite",
)

DB_TABLE = "normalized_raw_vendor_data"

SELLER_SCOPE_PATH = os.path.join(
    BASE_DIR,
    "data",
    "scope",
    "Seller_in_scope.csv",
)

CURRENT_MONTH = "2025-12"
PAST_N_MONTHS = 3

SELLER_NORMAL_THRESHOLD = 0.95

TREND_MIN_AVG = 10
TREND_RATIO_MIN = 0.8
TREND_RATIO_MAX = 2.0

OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "seller_level",
    "check_seller_level.csv",
)

SPU_RESULT_FILES = {
    "attribute": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_attribute_check_only.csv"),
    "metric_same": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_metric_same_month_only.csv"),
    "metric_diff": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_metric_diff_months_only.csv"),
}

# =========================
# MAIN
# =========================

def run_check_seller_level():

    # ---------- Load seller scope (NO NORMALIZATION) ----------
    seller_scope = pd.read_csv(SELLER_SCOPE_PATH)
    required_cols = ["country", "platform", "seller_used_id", "seller_url", "seller_name"]
    for c in required_cols:
        if c not in seller_scope.columns:
            raise ValueError(f"Seller_in_scope.csv missing column: {c}")

    seller_scope["seller_used_id"] = seller_scope["seller_used_id"].astype(str).str.strip()
    seller_scope = seller_scope[seller_scope["seller_used_id"] != ""]
    seller_scope = seller_scope.drop_duplicates(subset=["seller_used_id"])

    scope_sellers = set(seller_scope["seller_used_id"])

    # ---------- Load SPU abnormal lists (8 checks total) ----------
    spu_abnormal = set()

    for path in SPU_RESULT_FILES.values():
        if not os.path.exists(path):
            continue

        df = pd.read_csv(path)

        if "issue_type" in df.columns:
            df = df[df["issue_type"].astype(str).str.lower() == "abnormal"]

        if "spu_used_id" in df.columns:
            spu_abnormal.update(df["spu_used_id"].dropna().astype(str))

    # ---------- Load current month SPU ----------
    conn = sqlite3.connect(DB_PATH)

    df_cur = pd.read_sql(
        f"""
        SELECT
            seller_used_id,
            spu_used_id
        FROM {DB_TABLE}
        WHERE month = ?
        """,
        conn,
        params=(CURRENT_MONTH,),
    )

    df_cur["seller_used_id"] = df_cur["seller_used_id"].astype(str)
    df_cur["spu_used_id"] = df_cur["spu_used_id"].astype(str)

    # ---------- Past months SPU count ----------
    df_past = pd.read_sql(
        f"""
        SELECT
            seller_used_id,
            month,
            COUNT(DISTINCT spu_used_id) AS spu_cnt
        FROM {DB_TABLE}
        WHERE month < ?
        GROUP BY seller_used_id, month
        """,
        conn,
        params=(CURRENT_MONTH,),
    )

    conn.close()

    # ---------- Aggregate ----------
    results = []

    for seller_id, g in df_cur.groupby("seller_used_id"):

        spu_set = set(g["spu_used_id"])
        total_spu = len(spu_set)

        failed_spu = spu_set & spu_abnormal
        failed_cnt = len(failed_spu)

        normal_cnt = total_spu - failed_cnt
        normal_rate = normal_cnt / total_spu if total_spu > 0 else 0

        status = "Normal" if normal_rate >= SELLER_NORMAL_THRESHOLD else "Abnormal"

        # ---------- Trending ----------
        past = df_past[df_past["seller_used_id"] == seller_id].sort_values("month")
        last_n = past.tail(PAST_N_MONTHS)

        avg_spu = last_n["spu_cnt"].mean() if not last_n.empty else 0

        if avg_spu < TREND_MIN_AVG:
            trend_status = "Normal"
            trend_ratio = ""
        else:
            ratio = total_spu / avg_spu if avg_spu > 0 else 0
            trend_ratio = round(ratio, 4)
            trend_status = (
                "Normal"
                if TREND_RATIO_MIN <= ratio <= TREND_RATIO_MAX
                else "Abnormal"
            )

        results.append({
            "seller_used_id": seller_id,
            "seller_scope_flag": "in_scope" if seller_id in scope_sellers else "out_scope",
            "total_spu_current": total_spu,
            "normal_spu_current": normal_cnt,
            "failed_spu_current": failed_cnt,
            "normal_rate": round(normal_rate, 6),
            "status": status,
            "trend_avg_spu_last_n": round(avg_spu, 4) if avg_spu else "",
            "trend_ratio": trend_ratio,
            "trend_status": trend_status,
        })

    df_out = pd.DataFrame(results)

    # ---------- Attach seller metadata ----------
    df_out = df_out.merge(
        seller_scope,
        how="left",
        on="seller_used_id",
    )

    # ---------- Final columns ----------
    df_out = df_out[
        [
            "seller_scope_flag",
            "country",
            "platform",
            "seller_used_id",
            "seller_name",
            "seller_url",
            "total_spu_current",
            "normal_spu_current",
            "failed_spu_current",
            "normal_rate",
            "status",
            "trend_avg_spu_last_n",
            "trend_ratio",
            "trend_status",
        ]
    ].sort_values(
        ["seller_scope_flag", "country", "platform", "seller_used_id"]
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Seller-level result written to: {OUTPUT_PATH}")
    print(f"[INFO] Sellers processed: {len(df_out)}")


if __name__ == "__main__":
    run_check_seller_level()
