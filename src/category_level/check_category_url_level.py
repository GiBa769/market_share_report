import os
import sqlite3
import pandas as pd
import numpy as np

# =========================
# CONFIG
# =========================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "spu_level",
    "normalized_raw_vendor_data.sqlite",
)
DB_TABLE = "normalized_raw_vendor_data"

CATEGORY_SCOPE_PATH = os.path.join(
    BASE_DIR,
    "data",
    "scope",
    "Category_url_in_scope.csv",
)

CURRENT_MONTH = "2025-12"
PAST_N_MONTHS = 3

CATEGORY_NORMAL_THRESHOLD = 0.95

# Trending rules
NEW_CATEGORY_MIN_SPU = 300
TREND_MIN_AVG = 50
TREND_RATIO_MIN = 0.8
TREND_RATIO_MAX = 2.0

OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "category_level",
    "check_category_url_level.csv",
)

SPU_RESULT_FILES = {
    "attribute": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_attribute_check_only.csv"),
    "metric_same": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_metric_same_month_only.csv"),
    "metric_diff": os.path.join(BASE_DIR, "qaqc_results", "spu_level", "spu_metric_diff_months_only.csv"),
}

# =========================
# MAIN
# =========================

def run_check_category_url_level():

    # ---------- Load category scope ----------
    category_scope = pd.read_csv(CATEGORY_SCOPE_PATH)

    required_cols = ["country", "platform", "category_url"]
    for c in required_cols:
        if c not in category_scope.columns:
            raise ValueError(f"Category_url_in_scope.csv missing column: {c}")

    category_scope["category_url"] = category_scope["category_url"].astype(str).str.strip()
    category_scope = category_scope[category_scope["category_url"] != ""]
    category_scope = category_scope.drop_duplicates(subset=["category_url"])

    scope_categories = set(category_scope["category_url"])

    # ---------- Load SPU abnormal list ----------
    spu_abnormal = set()

    for path in SPU_RESULT_FILES.values():
        if not os.path.exists(path):
            continue

        df = pd.read_csv(path)

        if "issue_type" in df.columns:
            df = df[df["issue_type"].astype(str).str.lower() == "abnormal"]

        if "spu_used_id" in df.columns:
            spu_abnormal.update(df["spu_used_id"].dropna().astype(str))

    # ---------- Load current month category SPU ----------
    conn = sqlite3.connect(DB_PATH)

    df_cur = pd.read_sql(
        f"""
        SELECT
            source AS category_url,
            spu_used_id
        FROM {DB_TABLE}
        WHERE month = ?
          AND source IS NOT NULL
        """,
        conn,
        params=(CURRENT_MONTH,),
    )

    df_cur["category_url"] = df_cur["category_url"].astype(str)
    df_cur["spu_used_id"] = df_cur["spu_used_id"].astype(str)

    # ---------- Past months SPU count ----------
    df_past = pd.read_sql(
        f"""
        SELECT
            source AS category_url,
            month,
            COUNT(DISTINCT spu_used_id) AS spu_cnt
        FROM {DB_TABLE}
        WHERE month < ?
          AND source IS NOT NULL
        GROUP BY source, month
        """,
        conn,
        params=(CURRENT_MONTH,),
    )

    conn.close()

    # ---------- Aggregate ----------
    results = []

    for category_url, g in df_cur.groupby("category_url"):

        spu_set = set(g["spu_used_id"])
        total_spu = len(spu_set)

        failed_spu = spu_set & spu_abnormal
        failed_cnt = len(failed_spu)

        normal_cnt = total_spu - failed_cnt
        normal_rate = normal_cnt / total_spu if total_spu > 0 else 0

        status = "Normal" if normal_rate >= CATEGORY_NORMAL_THRESHOLD else "Abnormal"

        in_scope = category_url in scope_categories

        # ---------- Trending ----------
        past = df_past[df_past["category_url"] == category_url].sort_values("month")
        last_n = past.tail(PAST_N_MONTHS)
        avg_spu = last_n["spu_cnt"].mean() if not last_n.empty else 0

        if not in_scope:
            if total_spu >= NEW_CATEGORY_MIN_SPU:
                trend_status = "Normal"
                trend_ratio = ""
            else:
                trend_status = "Abnormal"
                trend_ratio = ""
        else:
            if avg_spu < TREND_MIN_AVG:
                trend_status = "Abnormal"
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
            "category_scope_flag": "in_scope" if in_scope else "out_scope",
            "category_url": category_url,

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

    # ---------- Attach category dimension ----------
    df_out = df_out.merge(
        category_scope,
        how="left",
        left_on="category_url",
        right_on="category_url",
    )

    # ---------- Reorder ----------
    df_out = df_out[
        [
            "category_scope_flag",
            "country",
            "platform",
            "category_url",

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
        ["category_scope_flag", "country", "platform", "category_url"]
    )

    # # ---------- SUMMARY ----------
    # summary = {
    #     "category_scope_flag": "SUMMARY",
    #     "country": "",
    #     "platform": "",
    #     "category_url": "",

    #     "total_spu_current": "",
    #     "normal_spu_current": "",
    #     "failed_spu_current": "",

    #     "normal_rate": "",
    #     "status": "",

    #     "trend_avg_spu_last_n": "",
    #     "trend_ratio": "",
    #     "trend_status": "",

    #     "#category_normal": (df_out["status"] == "Normal").sum(),
    #     "#category_abnormal": (df_out["status"] == "Abnormal").sum(),
    #     "#category_trend_normal": (df_out["trend_status"] == "Normal").sum(),
    #     "#category_trend_abnormal": (df_out["trend_status"] == "Abnormal").sum(),
    #     "#category_in_scope": (df_out["category_scope_flag"] == "in_scope").sum(),
    #     "#category_out_scope": (df_out["category_scope_flag"] == "out_scope").sum(),
    # }

    # for c in summary.keys():
    #     if c not in df_out.columns:
    #         df_out[c] = ""

    # final_df = pd.concat([df_out, pd.DataFrame([summary])], ignore_index=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Category-level result written to: {OUTPUT_PATH}")
    print(f"[INFO] Categories processed: {len(df_out)}")


if __name__ == "__main__":
    run_check_category_url_level()
