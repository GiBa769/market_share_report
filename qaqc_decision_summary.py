import os
import pandas as pd

# ============================================================
# CONFIG – CHANGE SCOPE HERE ONLY
# ============================================================

# None  → auto (ALL countries found in data)
# ["PH"] → check PH only
# ["PH", "TH"] → check multiple countries
SCOPE_COUNTRIES = ["PH"]

# Optional future extension
SCOPE_PLATFORMS = None  # e.g. ["LAZ", "SHP"]

# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULT_DIR = os.path.join(BASE_DIR, "result")
OUTPUT_PATH = os.path.join(RESULT_DIR, "qaqc_decision_summary.csv")

FILES = {
    "vendor_coverage": "qaqc_vendor_coverage.csv",
    "vendor_seller_trend_mm": "qaqc_vendor_seller_trend_multi_month.csv",
    "vendor_category_trend_mm": "qaqc_vendor_category_trend_multi_month.csv",
    "market_abnormal": "qaqc_market_share_latest_abnormal.csv",
    "market_trend_mm": "qaqc_market_share_latest_trend_multi_month.csv",
}

# ============================================================
# HELPERS
# ============================================================

def apply_scope_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if SCOPE_COUNTRIES and "country" in df.columns:
        df = df[df["country"].isin(SCOPE_COUNTRIES)]

    if SCOPE_PLATFORMS and "platform" in df.columns:
        df = df[df["platform"].isin(SCOPE_PLATFORMS)]

    return df


def safe_read_csv(path: str):
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    return apply_scope_filter(df)


def get_scope_country_label():
    if SCOPE_COUNTRIES:
        return ",".join(SCOPE_COUNTRIES)
    return "ALL"


# ============================================================
# DECISION RULES (NO QC LOGIC HERE)
# ============================================================

def vendor_coverage_decision(df: pd.DataFrame):
    if df is None or df.empty:
        return None

    has_fail = (df["coverage_status"].str.lower() == "fail").any()

    return {
        "scope_country": get_scope_country_label(),
        "stage": "vendor-input",
        "check_name": "vendor_coverage",
        "status": "FAIL" if has_fail else "PASS",
        "key_metric": "coverage_ratio",
        "benchmark": ">=95%",
        "reference_file": FILES["vendor_coverage"],
    }


def vendor_trend_decision(df: pd.DataFrame, level: str):
    if df is None or df.empty:
        return None

    abnormal = df["trend_status"].isin(
        ["abnormal-drop", "abnormal-increase"]
    ).any()

    return {
        "scope_country": get_scope_country_label(),
        "stage": "vendor-input",
        "check_name": f"vendor_trend_{level}",
        "status": "WARN" if abnormal else "PASS",
        "key_metric": "trend_ratio",
        "benchmark": "drop<80%, increase>=200%",
        "reference_file": FILES[f"vendor_{level}_trend_mm"],
    }


def market_abnormal_decision(df: pd.DataFrame):
    if df is None:
        return None

    return {
        "scope_country": get_scope_country_label(),
        "stage": "computed-output",
        "check_name": "market_share_pairwise",
        "status": "WARN" if not df.empty else "PASS",
        "key_metric": "abnormal_type",
        "benchmark": "spike / source-switch",
        "reference_file": FILES["market_abnormal"],
    }


def market_trend_decision(df: pd.DataFrame):
    if df is None or df.empty:
        return None

    abnormal = (
        df["trend_status_revenue"].isin(
            ["abnormal-drop", "abnormal-increase"]
        ).any()
        or df["trend_status_quantity"].isin(
            ["abnormal-drop", "abnormal-increase"]
        ).any()
    )

    return {
        "scope_country": get_scope_country_label(),
        "stage": "computed-output",
        "check_name": "market_share_trend_multi_month",
        "status": "WARN" if abnormal else "PASS",
        "key_metric": "trend_ratio",
        "benchmark": "drop<80%, increase>=200%",
        "reference_file": FILES["market_trend_mm"],
    }


# ============================================================
# BUILD DECISION SUMMARY
# ============================================================

def build_decision_summary():
    rows = []

    # ---- Vendor layer ----
    vendor_coverage = safe_read_csv(os.path.join(RESULT_DIR, FILES["vendor_coverage"]))
    vendor_seller_trend = safe_read_csv(os.path.join(RESULT_DIR, FILES["vendor_seller_trend_mm"]))
    vendor_category_trend = safe_read_csv(os.path.join(RESULT_DIR, FILES["vendor_category_trend_mm"]))

    for r in [
        vendor_coverage_decision(vendor_coverage),
        vendor_trend_decision(vendor_seller_trend, "seller"),
        vendor_trend_decision(vendor_category_trend, "category"),
    ]:
        if r:
            rows.append(r)

    # ---- Computed layer ----
    market_abnormal = safe_read_csv(os.path.join(RESULT_DIR, FILES["market_abnormal"]))
    market_trend = safe_read_csv(os.path.join(RESULT_DIR, FILES["market_trend_mm"]))

    for r in [
        market_abnormal_decision(market_abnormal),
        market_trend_decision(market_trend),
    ]:
        if r:
            rows.append(r)

    if not rows:
        raise RuntimeError("No QAQC result files found to build decision summary.")

    df = pd.DataFrame(rows)

    # Waterfall ordering
    stage_order = {"vendor-input": 1, "computed-output": 2}
    df["_order"] = df["stage"].map(stage_order)
    df = df.sort_values("_order").drop(columns="_order")

    df = df[
        [
            "scope_country",
            "stage",
            "check_name",
            "status",
            "key_metric",
            "benchmark",
            "reference_file",
        ]
    ]

    df.to_csv(OUTPUT_PATH, index=False)
    print("QAQC decision summary generated:")
    print(f"- {OUTPUT_PATH}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    build_decision_summary()
