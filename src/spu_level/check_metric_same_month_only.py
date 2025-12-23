import os
import sqlite3
import pandas as pd
from statistics import median

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

TARGET_MONTH = "2025-12"

OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "spu_level",
    "spu_metric_same_month_only.csv",
)

# comparison mode:
# - "value_based": max / median (current)
# - "time_based": reserved for future
COMPARISON_MODE = "value_based"

# FX rate to USD
FX_TO_USD = {
    "PH": 0.0175,
    "VN": 0.000041,
    "ID": 0.000064,
    "MY": 0.21,
    "SG": 0.74,
    "TH": 0.028,
}

# =========================
# HELPER
# =========================

def convert_asp_to_usd(value, country):
    fx = FX_TO_USD.get(country)
    if value is None or fx is None:
        return None
    try:
        return float(value) * fx
    except Exception:
        return None


def calc_ratio_max_median(values):
    if len(values) < 2:
        return None, None, None
    cur = max(values)
    med = median(values)
    if med == 0:
        return cur, med, None
    return cur, med, cur / med


# =========================
# MAIN
# =========================

def run_check_metric_same_month_only():

    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql(
        f"""
        SELECT
            spu_used_id,
            seller_used_id,
            country,
            asp,
            historical_quantity,
            historical_rating
        FROM {DB_TABLE}
        WHERE month = ?
        """,
        conn,
        params=(TARGET_MONTH,),
    )

    conn.close()

    issues = []

    # summary tracking
    spu_set = set()
    asp_abnormal = set()
    qty_abnormal = set()
    rating_abnormal = set()

    for spu_id, g in df.groupby("spu_used_id"):

        spu_set.add(spu_id)
        seller_id = g["seller_used_id"].iloc[0]
        country = g["country"].iloc[0]

        # =========================
        # ASP
        # =========================

        asp_vals = [
            convert_asp_to_usd(v, country)
            for v in g["asp"].dropna().tolist()
            if convert_asp_to_usd(v, country) is not None
        ]
        asp_vals = list(set(asp_vals))

        if len(asp_vals) > 1:
            cur, med, ratio = calc_ratio_max_median(asp_vals)
            if ratio is not None:
                if not (
                    (med >= 5 and 0.8 <= ratio <= 1.2)
                    or (med < 5 and 0.5 <= ratio <= 2.0)
                ):
                    asp_abnormal.add(spu_id)
                    issues.append({
                        "spu_used_id": spu_id,
                        "seller_used_id": seller_id,
                        "metric_name": "asp",
                        "issue_type": "abnormal_variation_within_month",
                        "current_value": cur,
                        "compare_value": med,
                        "ratio": ratio,
                    })

        # =========================
        # HISTORICAL QUANTITY
        # =========================

        qty_vals = list(set(g["historical_quantity"].dropna().tolist()))

        if len(qty_vals) > 1:
            cur, med, ratio = calc_ratio_max_median(qty_vals)
            if ratio is not None:
                if not (
                    (med >= 1000 and 1.0 <= ratio <= 1.2)
                    or (med < 1000 and ratio >= 1.0)
                ):
                    qty_abnormal.add(spu_id)
                    issues.append({
                        "spu_used_id": spu_id,
                        "seller_used_id": seller_id,
                        "metric_name": "historical_quantity",
                        "issue_type": "abnormal_variation_within_month",
                        "current_value": cur,
                        "compare_value": med,
                        "ratio": ratio,
                    })

        # =========================
        # HISTORICAL RATING
        # =========================

        rating_vals = list(set(g["historical_rating"].dropna().tolist()))

        if len(rating_vals) > 1:
            cur, med, ratio = calc_ratio_max_median(rating_vals)
            if ratio is not None:
                if not (
                    (med >= 100 and 1.0 <= ratio <= 1.2)
                    or (med < 100 and ratio >= 1.0)
                ):
                    rating_abnormal.add(spu_id)
                    issues.append({
                        "spu_used_id": spu_id,
                        "seller_used_id": seller_id,
                        "metric_name": "historical_rating",
                        "issue_type": "abnormal_variation_within_month",
                        "current_value": cur,
                        "compare_value": med,
                        "ratio": ratio,
                    })

    result_df = pd.DataFrame(issues)

    # =========================
    # SUMMARY
    # =========================

    summary = {
        "spu_total": len(spu_set),
        "asp_abnormal": len(asp_abnormal),
        "historical_quantity_abnormal": len(qty_abnormal),
        "historical_rating_abnormal": len(rating_abnormal),
    }

    for col, val in summary.items():
        result_df[col] = ""
        if not result_df.empty:
            result_df.at[0, col] = val

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result_df.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ SPU metric same-month result written to: {OUTPUT_PATH}")
    print(f"[SUMMARY] {summary}")


if __name__ == "__main__":
    run_check_metric_same_month_only()
