import os
import sqlite3
import pandas as pd
from collections import Counter

# =========================
# CONFIG
# =========================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(
    BASE_DIR, "qaqc_results", "spu_level", "normalized_raw_vendor_data.sqlite"
)
DB_TABLE = "normalized_raw_vendor_data"

CURRENT_MONTH = "2025-12"
PAST_N_MONTHS = 3

# Seller quality (Y)
SELLER_NORMAL_THRESHOLD = 0.95

# Trending (X)
TREND_MIN_AVG = 10
TREND_RATIO_MIN = 0.8
TREND_RATIO_MAX = 2.0

# SPU abnormal threshold (K)
SPU_ABNORMAL_THRESHOLD = 1

# Scope
SELLER_SCOPE_PATH = os.path.join(
    BASE_DIR, "data", "scope", "Seller_in_scope.csv"
)

# SPU-level result files
SPU_RESULT_FILES = {
    "attribute": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_attribute_check_only.csv"),
    "same_month": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_metric_same_month_only.csv"),
    "diff_months": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_metric_diff_months_only.csv"),
}

OUTPUT_PATH = os.path.join(
    BASE_DIR, "qaqc_results", "seller_level", "check_seller_level.csv"
)

# =========================
# HELPERS
# =========================

def load_failed_spu_counts(path: str) -> Counter:
    """
    Count failed checks per SPU following the FINAL rule:
    - attribute / same_month: all rows are failed
    - diff_months:
        - issue_type = abnormal  -> failed
        - issue_type = insufficient_history AND status = Fail -> failed
        - insufficient_history + Pass -> NOT failed
    """
    cnt = Counter()

    if not os.path.exists(path):
        return cnt

    df = pd.read_csv(path)
    if "spu_used_id" not in df.columns:
        return cnt

    fname = os.path.basename(path).lower()

    # Attribute & same month: issue list only
    if "attribute" in fname or "same_month" in fname:
        for spu in df["spu_used_id"].dropna().astype(str):
            cnt[spu] += 1
        return cnt

    # Diff months
    if "diff_months" in fname and "issue_type" in df.columns:
        issue = df["issue_type"].astype(str).str.lower()

        abnormal = issue == "abnormal"
        insuf_fail = (
            (issue == "insufficient_history")
            & ("status" in df.columns)
            & (df["status"].astype(str).str.lower() == "fail")
        )

        df2 = df[abnormal | insuf_fail]
        for spu in df2["spu_used_id"].dropna().astype(str):
            cnt[spu] += 1
        return cnt

    # Fallback
    for spu in df["spu_used_id"].dropna().astype(str):
        cnt[spu] += 1
    return cnt


def load_all_failed_spu_counts() -> Counter:
    total = Counter()
    for p in SPU_RESULT_FILES.values():
        total += load_failed_spu_counts(p)
    return total


def trend_status(current_spu: int, avg_spu: float):
    if avg_spu < TREND_MIN_AVG:
        return "Normal", ""

    if avg_spu <= 0:
        return "Abnormal", ""

    ratio = current_spu / avg_spu
    status = "Normal" if TREND_RATIO_MIN <= ratio <= TREND_RATIO_MAX else "Abnormal"
    return status, round(ratio, 6)


# =========================
# MAIN
# =========================

def run_check_seller_level():

    # ---------- Load scope ----------
    scope_df = pd.read_csv(SELLER_SCOPE_PATH)
    scope_sellers = set(scope_df["seller_used_id"].astype(str))

    # ---------- Load failed SPU counts ----------
    failed_spu_counts = load_all_failed_spu_counts()

    # ---------- Load SQLite ----------
    conn = sqlite3.connect(DB_PATH)

    df_cur = pd.read_sql(
        f"""
        SELECT seller_used_id, country, platform, spu_used_id
        FROM {DB_TABLE}
        WHERE month = ?
        """,
        conn,
        params=(CURRENT_MONTH,),
    )

    df_past = pd.read_sql(
        f"""
        SELECT seller_used_id, month, COUNT(DISTINCT spu_used_id) AS spu_cnt
        FROM {DB_TABLE}
        WHERE month < ?
        GROUP BY seller_used_id, month
        """,
        conn,
        params=(CURRENT_MONTH,),
    )

    conn.close()

    df_cur["seller_used_id"] = df_cur["seller_used_id"].astype(str)
    df_cur["spu_used_id"] = df_cur["spu_used_id"].astype(str)

    # ---------- Aggregate seller ----------
    rows = []

    for seller_id, g in df_cur.groupby("seller_used_id"):

        country = g["country"].iloc[0]
        platform = g["platform"].iloc[0]

        spu_set = set(g["spu_used_id"])
        total_spu = len(spu_set)

        abnormal_spu = {
            spu for spu in spu_set
            if failed_spu_counts.get(spu, 0) >= SPU_ABNORMAL_THRESHOLD
        }

        abnormal_cnt = len(abnormal_spu)
        normal_cnt = total_spu - abnormal_cnt
        normal_rate = normal_cnt / total_spu if total_spu else 0

        y_status = "Normal" if normal_rate >= SELLER_NORMAL_THRESHOLD else "Abnormal"

        past = df_past[df_past["seller_used_id"] == seller_id].sort_values("month")
        last_n = past.tail(PAST_N_MONTHS)
        avg_spu = last_n["spu_cnt"].mean() if not last_n.empty else 0

        trend_stat, trend_ratio = trend_status(total_spu, avg_spu)

        seller_status = (
            "Normal" if (y_status == "Normal" and trend_stat == "Normal") else "Abnormal"
        )

        rows.append({
            "seller_used_id": seller_id,
            "country": country,
            "platform": platform,
            "seller_scope_flag": "in_scope" if seller_id in scope_sellers else "out_scope",

            "total_spu_current": total_spu,
            "normal_spu_current": normal_cnt,
            "normal_rate": round(normal_rate, 6),

            "abnormal_spu_current": abnormal_cnt,
            "spu_abnormal_threshold": SPU_ABNORMAL_THRESHOLD,
            "Y_status": y_status,

            "avg_spu_last_n_months": round(avg_spu, 6),
            "trending_ratio": trend_ratio,
            "trending_status": trend_stat,

            "seller_status": seller_status,
        })

    df_out = pd.DataFrame(rows)

    # ---------- SUMMARY ----------
    seller_total = len(df_out)
    seller_normal = int((df_out["seller_status"] == "Normal").sum())
    seller_abnormal = seller_total - seller_normal
    trend_normal = int((df_out["trending_status"] == "Normal").sum())
    trend_abnormal = seller_total - trend_normal
    in_scope = int((df_out["seller_scope_flag"] == "in_scope").sum())
    out_scope = seller_total - in_scope

    summary = {
        "seller_total": seller_total,
        "seller_normal": seller_normal,
        "seller_abnormal": seller_abnormal,
        "trend_normal": trend_normal,
        "trend_abnormal": trend_abnormal,
        "in_scope": in_scope,
        "out_scope": out_scope,
    }

    # attach summary to first row
    for k, v in summary.items():
        df_out[k] = ""
        if not df_out.empty:
            df_out.at[0, k] = v

    # ---------- Output ----------
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)

    print("✅ Seller level check completed")
    print(f"[SUMMARY] {summary}")


if __name__ == "__main__":
    run_check_seller_level()
