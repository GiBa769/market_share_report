import os
import sqlite3
import pandas as pd

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
    "spu_metric_same_month_only.csv"
)

CURRENT_MONTH = "2025-12"
PREVIOUS_MONTH = "2025-11"

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

def run_check_metric_same_month_only():

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
        WHERE month IN ('{CURRENT_MONTH}', '{PREVIOUS_MONTH}')
    """

    df = pd.read_sql(query, conn)
    conn.close()

    issues = []

    # -------------------------------
    # SUMMARY COUNTERS
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
        prev = g[g["month"] == PREVIOUS_MONTH]

        if cur.empty or prev.empty:
            continue

        cur = cur.iloc[0]
        prev = prev.iloc[0]

        seller_used_id = cur["seller_used_id"]
        country = cur["country"]
        fx = FX_TO_USD.get(country)

        # ===============================
        # ASP
        # ===============================
        cur_asp = cur["asp"]
        prev_asp = prev["asp"]

        if not prev_asp or prev_asp == 0 or not fx:
            asp_insuf.add(spu_used_id)
            issues.append({
                "spu_used_id": spu_used_id,
                "seller_used_id": seller_used_id,
                "metric_name": "asp",
                "issue_type": "insufficient_history",
                "current_value": cur_asp,
                "previous_value": prev_asp,
                "ratio": None,
                "status": "FAIL",
            })
        else:
            cur_usd = cur_asp * fx
            prev_usd = prev_asp * fx
            ratio = cur_usd / prev_usd

            if prev_usd >= 5:
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
                    "previous_value": round(prev_usd, 4),
                    "ratio": round(ratio, 4),
                    "status": "FAIL",
                })

        # ===============================
        # HISTORICAL QUANTITY
        # ===============================
        cur_q = cur["historical_quantity"]
        prev_q = prev["historical_quantity"]

        if not prev_q or prev_q == 0:
            qty_insuf.add(spu_used_id)
            issues.append({
                "spu_used_id": spu_used_id,
                "seller_used_id": seller_used_id,
                "metric_name": "historical_quantity",
                "issue_type": "insufficient_history",
                "current_value": cur_q,
                "previous_value": prev_q,
                "ratio": None,
                "status": "FAIL",
            })
        else:
            ratio = cur_q / prev_q

            if prev_q >= 1000:
                normal = 1.0 <= ratio <= 1.2
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
                    "previous_value": prev_q,
                    "ratio": round(ratio, 4),
                    "status": "FAIL",
                })

        # ===============================
        # HISTORICAL RATING
        # ===============================
        cur_r = cur["historical_rating"]
        prev_r = prev["historical_rating"]

        if not prev_r or prev_r == 0:
            rating_insuf.add(spu_used_id)
            issues.append({
                "spu_used_id": spu_used_id,
                "seller_used_id": seller_used_id,
                "metric_name": "historical_rating",
                "issue_type": "insufficient_history",
                "current_value": cur_r,
                "previous_value": prev_r,
                "ratio": None,
                "status": "FAIL",
            })
        else:
            ratio = cur_r / prev_r

            if prev_r >= 100:
                normal = 1.0 <= ratio <= 1.2
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
                    "previous_value": prev_r,
                    "ratio": round(ratio, 4),
                    "status": "FAIL",
                })

    # =====================================================
    # BUILD RESULT DF
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
    run_check_metric_same_month_only()
