import os
import sqlite3
import pandas as pd
from collections import Counter
from datetime import datetime

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
    "spu_attribute_check_only.csv"
)

TARGET_MONTH = datetime.now().strftime("%Y-%m")
assert "-" in TARGET_MONTH, "TARGET_MONTH must be in YYYY-MM format"

# =====================================================
# HELPER FUNCTIONS
# =====================================================

def detect_platform_from_url(url):
    if not isinstance(url, str):
        return None
    url = url.lower()
    if "shopee" in url:
        return "SHP"
    if "lazada" in url:
        return "LAZ"
    if "tiktok" in url:
        return "TTK"
    return None


def detect_country_from_url(url):
    if not isinstance(url, str):
        return None
    url = url.lower()
    if ".ph" in url:
        return "PH"
    if ".vn" in url:
        return "VN"
    if ".id" in url:
        return "ID"
    if ".my" in url:
        return "MY"
    if ".th" in url:
        return "TH"
    return None


def parse_platform_from_seller_used_id(seller_used_id):
    try:
        return seller_used_id.split(".")[1].upper()
    except Exception:
        return None


def build_row_signature(row):
    return (
        row.get("spu_name"),
        row.get("spu_url"),
        row.get("country"),
        row.get("platform"),
    )

# =====================================================
# CORE LOGIC
# =====================================================

def run_check_attribute_only():

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"SQLite DB not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    query = f"""
        SELECT
            spu_used_id,
            seller_used_id,
            month,
            spu_name,
            spu_url,
            country,
            platform
        FROM normalized_raw_vendor_data
        WHERE month = '{TARGET_MONTH}'
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"[DEBUG] Rows loaded for month {TARGET_MONTH}: {len(df)}")

    # =================================================
    # ISSUE COLLECTION (FAIL ONLY)
    # =================================================
    issues = []

    for spu_used_id, g in df.groupby("spu_used_id"):

        total_rows = len(g)
        seller_used_ids = g["seller_used_id"].dropna().unique().tolist()
        seller_used_id = seller_used_ids[0] if seller_used_ids else None

        # ---------- single_line check ----------
        if total_rows == 1:
            row = g.iloc[0]

            detected_platform = detect_platform_from_url(row["spu_url"])
            detected_country = detect_country_from_url(row["spu_url"])

            platform = row["platform"]
            if not platform:
                platform = parse_platform_from_seller_used_id(row["seller_used_id"])

            if platform and detected_platform and platform != detected_platform:
                issues.append({
                    "spu_used_id": spu_used_id,
                    "seller_used_id": seller_used_id,
                    "check_type": "single_line",
                    "issue_type": "platform_vs_url",
                    "total_rows": "",
                    "diff_rows": "",
                    "status": "Fail",
                })

            if row["country"] and detected_country and row["country"] != detected_country:
                issues.append({
                    "spu_used_id": spu_used_id,
                    "seller_used_id": seller_used_id,
                    "check_type": "single_line",
                    "issue_type": "country_vs_url",
                    "total_rows": "",
                    "diff_rows": "",
                    "status": "Fail",
                })

        # ---------- multi_lines check ----------
        if total_rows >= 2:
            signatures = g.apply(build_row_signature, axis=1).tolist()
            counter = Counter(signatures)

            if len(counter) > 1:
                most_common_signature, most_common_count = counter.most_common(1)[0]
                diff_rows = total_rows - most_common_count

                issues.append({
                    "spu_used_id": spu_used_id,
                    "seller_used_id": seller_used_id,
                    "check_type": "multi_lines",
                    "issue_type": "cross_row_attribute_inconsistent",
                    "total_rows": total_rows,
                    "diff_rows": diff_rows,
                    "status": "Fail",
                })

    # =================================================
    # BUILD RESULT DF (FAIL LIST)
    # =================================================
    result_df = pd.DataFrame(issues)

    # =================================================
    # GLOBAL SUMMARY (ONE ROW ONLY)
    # =================================================
    total_spu = df["spu_used_id"].nunique()

    failed_spu_set = set(result_df["spu_used_id"]) if not result_df.empty else set()
    failed_spu = len(failed_spu_set)
    normal_spu = total_spu - failed_spu
    normal_rate = round(normal_spu / total_spu, 4) if total_spu else 0

    failed_spu_single_line = len(
        {r["spu_used_id"] for r in issues if r["check_type"] == "single_line"}
    )
    failed_spu_multi_lines = len(
        {r["spu_used_id"] for r in issues if r["check_type"] == "multi_lines"}
    )

    issue_type_counter = Counter(
        r["issue_type"] for r in issues
    )

    failed_spu_by_issue_type = ";".join(
        f"{k}={v}" for k, v in issue_type_counter.items()
    )

    summary_values = {
        "total_spu": total_spu,
        "failed_spu": failed_spu,
        "normal_spu": normal_spu,
        "normal_rate": normal_rate,
        "failed_spu_single_line": failed_spu_single_line,
        "failed_spu_multi_lines": failed_spu_multi_lines,
        "failed_spu_by_issue_type": failed_spu_by_issue_type,
    }

    # append summary columns (empty by default)
    for col in summary_values:
        result_df[col] = ""

    # fill summary in FIRST ROW ONLY
    if not result_df.empty:
        for col, val in summary_values.items():
            result_df.at[0, col] = val

    # =================================================
    # EXPORT
    # =================================================
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result_df.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Output written to: {OUTPUT_PATH}")
    print(
        f"[SUMMARY] total_spu={total_spu}, "
        f"failed_spu={failed_spu}, "
        f"normal_spu={normal_spu}, "
        f"normal_rate={normal_rate}"
    )


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    run_check_attribute_only()
