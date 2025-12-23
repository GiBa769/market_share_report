import os
import sqlite3
import pandas as pd
import numpy as np

# =====================================================
# CONFIG
# =====================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "spu_level",
    "normalized_raw_vendor_data.sqlite"
)

OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "spu_level",
    "spu_metric_diff_months_only.csv"
)

CURRENT_MONTH = "2025-12"

# FX snapshot (editable)
FX_TO_USD = {
    "PH": 0.0175,
    "VN": 0.000041,
    "ID": 0.000064,
    "MY": 0.21,
    "SG": 0.74,
    "TH": 0.028,
}

# =====================================================
# CORE
# =====================================================

def run_check_metric_diff_months_only():

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    query = f"""
        SELECT
            spu_used_id,
            seller_used_id,
            country,
            month,
            asp,
            historical_quantity,
            historical_rating
        FROM normalized_raw_vendor_data
        WHERE month <= '{CURRENT_MONTH}'
    """

    df = pd.read_sql(query, conn)
    conn.close()

    issues = []

    # -------------------------------
    # SUMMARY SETS
    # -------------------------------
    spu_set = set()

    asp_normal = set()
    qty_normal = set()
    rating_normal = set()

    asp_insuf = set()
    qty_insuf = set()
    rating_insuf = set()

    # -------------------------------
    # PROCESS PER SPU
    # -------------------------------
    for spu_used_id, g in df.groupby("spu_used_id"):
        spu_set.add(spu_used_id)

        cur = g[g["month"] == CURRENT_MONTH]
        past = g[g["month"] < CURRENT_MONTH]

        if cur.empty:
            continue

        cur = cur.iloc[0]
        seller_used_id = cur["seller_used_id"]
        country = cur["country"]
        fx = FX_TO_USD.get(country)

        # ===============================
        # ASP
        # ===============================
        cur_asp = cur["asp"]
        past_asp = past["asp"].dropna()

        if not fx or past_asp.empty:
            asp_insuf.add(spu_used_id)
            issues.append({
                "spu_used_id": spu_used_id,
                "seller_used_id": seller_used_id,
                "metric_name": "asp",
                "issue_type": "insufficient_history",
                "current_value": cur_asp,
                "baseline_value": None,
                "ratio": None,
                "status": "FAIL",
            })
        else:
            baseline_usd = np.median(past_asp) * fx
            cur_usd = cur_asp * fx

            if baseline_usd == 0:
                asp_insuf.add(spu_used_id)
            else:
                ratio = cur_usd / baseline_usd

                # THRESHOLD BASED ON CURRENT VALUE
                if cur_usd >= 5:
                    normal = 0.8 <= ratio <= 1.2
                else:
                    normal = 0.5 <= ratio <= 2.0

                if normal:
                    asp_normal.add(spu_used_id)
                else:
                    issues.append({
                        "spu_used_id": spu_used_id,
                        "seller_used_id": seller_used_id,
                        "metric_name": "asp",
                        "issue_type": "abnormal",
                        "current_value": round(cur_usd, 4),
                        "baseline_value": round(baseline_usd, 4),
                        "ratio": round(ratio, 4),
                        "status": "FAIL",
                    })

        # ===============================
        # HISTORICAL QUANTITY
        # ===============================
        cur_q = cur["historical_quantity"]
        past_q = past["historical_quantity"].dropna()

        if past_q.empty:
            qty_insuf.add(spu_used_id)
            issues.append({
                "spu_used_id": spu_used_id,
                "seller_used_id": seller_used_id,
                "metric_name": "historical_quantity",
                "issue_type": "insufficient_history",
                "current_value": cur_q,
                "baseline_value": None,
                "ratio": None,
                "status": "FAIL",
            })
        else:
            baseline_q = np.median(past_q)

            if baseline_q == 0:
                qty_insuf.add(spu_used_id)
            else:
                ratio = cur_q / baseline_q

                # THRESHOLD BASED ON CURRENT VALUE
                if cur_q >= 1000:
                    normal = 1.0 <= ratio <= 1.5
                else:
                    normal = ratio >= 1.0

                if normal:
                    qty_normal.add(spu_used_id)
                else:
                    issues.append({
                        "spu_used_id": spu_used_id,
                        "seller_used_id": seller_used_id,
                        "metric_name": "historical_quantity",
                        "issue_type": "abnormal",
                        "current_value": cur_q,
                        "baseline_value": baseline_q,
                        "ratio": round(ratio, 4),
                        "status": "FAIL",
                    })

        # ===============================
        # HISTORICAL RATING
        # ===============================
        cur_r = cur["historical_rating"]
        past_r = past["historical_rating"].dropna()

        if past_r.empty:
            rating_insuf.add(spu_used_id)
            issues.append({
                "spu_used_id": spu_used_id,
                "seller_used_id": seller_used_id,
                "metric_name": "historical_rating",
                "issue_type": "insufficient_history",
                "current_value": cur_r,
                "baseline_value": None,
                "ratio": None,
                "status": "FAIL",
            })
        else:
            baseline_r = np.median(past_r)

            if baseline_r == 0:
                rating_insuf.add(spu_used_id)
            else:
                ratio = cur_r / baseline_r

                # THRESHOLD BASED ON CURRENT VALUE
                if cur_r >= 100:
                    normal = 1.0 <= ratio <= 1.5
                else:
                    normal = ratio >= 1.0

                if normal:
                    rating_normal.add(spu_used_id)
                else:
                    issues.append({
                        "spu_used_id": spu_used_id,
                        "seller_used_id": seller_used_id,
                        "metric_name": "historical_rating",
                        "issue_type": "abnormal",
                        "current_value": cur_r,
                        "baseline_value": baseline_r,
                        "ratio": round(ratio, 4),
                        "status": "FAIL",
                    })

    # =====================================================
    # BUILD RESULT + SUMMARY
    # =====================================================
    result_df = pd.DataFrame(issues)

    spu_total = len(spu_set)

    summary = {
        "spu_total": spu_total,
        "asp_normal": len(asp_normal),
        "historical_quantity_normal": len(qty_normal),
        "historical_rating_normal": len(rating_normal),
        "asp_insufficient_history": len(asp_insuf),
        "quantity_insufficient_history": len(qty_insuf),
        "rating_insufficient_history": len(rating_insuf),
    }

    for col in summary:
        result_df[col] = ""

    if not result_df.empty:
        for col, val in summary.items():
            result_df.at[0, col] = val

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result_df.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Output written to: {OUTPUT_PATH}")
    print("[SUMMARY]", summary)


if __name__ == "__main__":
    run_check_metric_diff_months_only()
