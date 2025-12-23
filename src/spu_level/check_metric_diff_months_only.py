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

CURRENT_MONTH = "2025-12"

# input from previous checks
ATTRIBUTE_CHECK_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "spu_level",
    "spu_attribute_check_only.csv",
)

SAME_MONTH_CHECK_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "spu_level",
    "spu_metric_same_month_only.csv",
)

OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "spu_level",
    "spu_metric_diff_months_only.csv",
)

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


def load_abnormal_spu_set():
    abnormal = set()

    if os.path.exists(ATTRIBUTE_CHECK_PATH):
        df = pd.read_csv(ATTRIBUTE_CHECK_PATH)
        abnormal.update(df["spu_used_id"].dropna().unique())

    if os.path.exists(SAME_MONTH_CHECK_PATH):
        df = pd.read_csv(SAME_MONTH_CHECK_PATH)
        abnormal.update(df["spu_used_id"].dropna().unique())

    return abnormal


# =========================
# MAIN
# =========================

def run_check_metric_diff_months_only():

    abnormal_spu_from_other_checks = load_abnormal_spu_set()

    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql(
        f"""
        SELECT
            spu_used_id,
            seller_used_id,
            country,
            month,
            asp,
            historical_quantity,
            historical_rating
        FROM {DB_TABLE}
        WHERE month <= ?
        """,
        conn,
        params=(CURRENT_MONTH,),
    )

    conn.close()

    issues = []

    # ===== summary tracking =====
    spu_total = set()

    metric_abnormal = {"asp": set(), "historical_quantity": set(), "historical_rating": set()}
    metric_insufficient_pass = {"asp": set(), "historical_quantity": set(), "historical_rating": set()}
    metric_insufficient_fail = {"asp": set(), "historical_quantity": set(), "historical_rating": set()}

    for spu_id, g in df.groupby("spu_used_id"):

        spu_total.add(spu_id)

        seller_id = g["seller_used_id"].iloc[0]
        country = g["country"].iloc[0]

        cur = g[g["month"] == CURRENT_MONTH]
        past = g[g["month"] < CURRENT_MONTH]

        if cur.empty:
            continue

        cur = cur.iloc[0]

        # =========================
        # ASP
        # =========================

        past_asp = [
            convert_asp_to_usd(v, country)
            for v in past["asp"].dropna().tolist()
            if convert_asp_to_usd(v, country) is not None
        ]

        cur_asp = convert_asp_to_usd(cur["asp"], country)

        if not past_asp or cur_asp is None:
            status = "Pass" if spu_id not in abnormal_spu_from_other_checks else "Fail"
            metric_insufficient_pass["asp"].add(spu_id) if status == "Pass" else metric_insufficient_fail["asp"].add(spu_id)

            issues.append({
                "spu_used_id": spu_id,
                "seller_used_id": seller_id,
                "metric_name": "asp",
                "issue_type": "insufficient_history",
                "current_value": cur_asp,
                "median_value": None,
                "ratio": None,
                "status": status,
            })
        else:
            med = median(past_asp)
            ratio = cur_asp / med if med else None

            if med >= 5:
                normal = ratio is not None and 0.8 <= ratio <= 1.2
            else:
                normal = ratio is not None and 0.5 <= ratio <= 2.0

            if not normal:
                metric_abnormal["asp"].add(spu_id)
                issues.append({
                    "spu_used_id": spu_id,
                    "seller_used_id": seller_id,
                    "metric_name": "asp",
                    "issue_type": "abnormal",
                    "current_value": cur_asp,
                    "median_value": med,
                    "ratio": ratio,
                    "status": "Fail",
                })

        # =========================
        # HISTORICAL QUANTITY
        # =========================

        past_q = past["historical_quantity"].dropna().tolist()
        cur_q = cur["historical_quantity"]

        if not past_q or cur_q is None:
            status = "Pass" if spu_id not in abnormal_spu_from_other_checks else "Fail"
            metric_insufficient_pass["historical_quantity"].add(spu_id) if status == "Pass" else metric_insufficient_fail["historical_quantity"].add(spu_id)

            issues.append({
                "spu_used_id": spu_id,
                "seller_used_id": seller_id,
                "metric_name": "historical_quantity",
                "issue_type": "insufficient_history",
                "current_value": cur_q,
                "median_value": None,
                "ratio": None,
                "status": status,
            })
        else:
            med = median(past_q)
            ratio = cur_q / med if med else None

            if cur_q >= 1000:
                normal = ratio is not None and 1.0 <= ratio <= 1.5
            else:
                normal = ratio is not None and ratio >= 1.0

            if not normal:
                metric_abnormal["historical_quantity"].add(spu_id)
                issues.append({
                    "spu_used_id": spu_id,
                    "seller_used_id": seller_id,
                    "metric_name": "historical_quantity",
                    "issue_type": "abnormal",
                    "current_value": cur_q,
                    "median_value": med,
                    "ratio": ratio,
                    "status": "Fail",
                })

        # =========================
        # HISTORICAL RATING
        # =========================

        past_r = past["historical_rating"].dropna().tolist()
        cur_r = cur["historical_rating"]

        if not past_r or cur_r is None:
            status = "Pass" if spu_id not in abnormal_spu_from_other_checks else "Fail"
            metric_insufficient_pass["historical_rating"].add(spu_id) if status == "Pass" else metric_insufficient_fail["historical_rating"].add(spu_id)

            issues.append({
                "spu_used_id": spu_id,
                "seller_used_id": seller_id,
                "metric_name": "historical_rating",
                "issue_type": "insufficient_history",
                "current_value": cur_r,
                "median_value": None,
                "ratio": None,
                "status": status,
            })
        else:
            med = median(past_r)
            ratio = cur_r / med if med else None

            if cur_r >= 100:
                normal = ratio is not None and 1.0 <= ratio <= 1.5
            else:
                normal = ratio is not None and ratio >= 1.0

            if not normal:
                metric_abnormal["historical_rating"].add(spu_id)
                issues.append({
                    "spu_used_id": spu_id,
                    "seller_used_id": seller_id,
                    "metric_name": "historical_rating",
                    "issue_type": "abnormal",
                    "current_value": cur_r,
                    "median_value": med,
                    "ratio": ratio,
                    "status": "Fail",
                })

    result_df = pd.DataFrame(issues)

    # =========================
    # SUMMARY
    # =========================

    summary = {
        "spu_total": len(spu_total),
    }

    for metric in ["asp", "historical_quantity", "historical_rating"]:
        summary[f"{metric}_abnormal"] = len(metric_abnormal[metric])
        summary[f"{metric}_insufficient_pass"] = len(metric_insufficient_pass[metric])
        summary[f"{metric}_insufficient_fail"] = len(metric_insufficient_fail[metric])
        summary[f"{metric}_normal"] = (
            len(spu_total)
            - summary[f"{metric}_abnormal"]
            - summary[f"{metric}_insufficient_fail"]
        )

    for col, val in summary.items():
        result_df[col] = ""
        if not result_df.empty:
            result_df.at[0, col] = val

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result_df.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ SPU metric diff-months result written to: {OUTPUT_PATH}")
    print(f"[SUMMARY] {summary}")


if __name__ == "__main__":
    run_check_metric_diff_months_only()
