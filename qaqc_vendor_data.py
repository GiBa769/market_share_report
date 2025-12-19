import os
import pandas as pd
import numpy as np

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw")
RESULT_DIR = os.path.join(BASE_DIR, "result")
os.makedirs(RESULT_DIR, exist_ok=True)

# Trend window (multi-month)
LOOKBACK_MONTHS = 3

# Benchmarks / thresholds (để dễ đổi nhanh)
COVERAGE_PASS_RATIO = 0.95          # ≥ 95%
SELLER_DROP_THRESHOLD = 0.80        # abnormal-drop if < 80% vs baseline
ABNORMAL_INCREASE_THRESHOLD = 2.00  # abnormal-increase if ≥ 200%

REQUIRED_COLS = {
    "country",
    "platform",
    "month",
    "seller_used_id",
    "spu_used_id",
    "source",
}

# ============================================================
# HELPERS
# ============================================================

def list_raw_files():
    if not os.path.exists(RAW_DIR):
        raise FileNotFoundError(f"Missing folder: {RAW_DIR}")
    files = [
        os.path.join(RAW_DIR, f)
        for f in os.listdir(RAW_DIR)
        if f.lower().endswith(".csv")
    ]
    if not files:
        raise FileNotFoundError(f"No CSV files found in: {RAW_DIR}")
    return files


def normalize_text_series(s: pd.Series) -> pd.Series:
    # Normalize null / empty / whitespace
    s = s.astype("string")
    s = s.fillna("")
    s = s.str.strip()
    return s


def normalize_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["country"] = normalize_text_series(df["country"])
    df["platform"] = normalize_text_series(df["platform"])
    df["month"] = normalize_text_series(df["month"])
    df["seller_used_id"] = normalize_text_series(df["seller_used_id"])
    df["spu_used_id"] = normalize_text_series(df["spu_used_id"])

    # platform: empty -> UNKNOWN (áp dụng đồng đều)
    df["platform"] = df["platform"].replace("", "UNKNOWN")

    # source: keep raw meaning, but normalized for filtering
    # seller-level: source == "" after strip
    # category-url-level: source != ""
    df["source"] = normalize_text_series(df["source"])

    return df


def parse_month_col(df: pd.DataFrame) -> pd.Series:
    # month format: yyyy-mm
    m = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    return m


def detect_latest_prev_month(df: pd.DataFrame):
    m = parse_month_col(df)
    months = sorted(m.dropna().unique())
    if len(months) < 2:
        raise ValueError("Need at least 2 months in raw data to run pairwise QAQC (latest vs prev).")

    latest_dt = months[-1]
    prev_dt = (latest_dt - pd.offsets.MonthBegin(1))

    latest = latest_dt.strftime("%Y-%m")
    prev = prev_dt.strftime("%Y-%m")
    return latest, prev


def format_percent_ratio(v) -> str:
    # v is ratio (e.g., 1.2 -> 120.00%)
    if pd.isna(v) or np.isinf(v):
        return "-"
    return f"{round(v * 100, 2)}%"


def safe_ratio(num: float, den: float) -> float:
    if den == 0:
        return np.nan
    return num / den


# ============================================================
# READ RAW (all files)
# ============================================================

def read_raw_all() -> pd.DataFrame:
    dfs = []
    for path in list_raw_files():
        df = pd.read_csv(path, low_memory=False)

        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            raise ValueError(f"{os.path.basename(path)} missing columns: {missing}")

        dfs.append(df)

    df_all = pd.concat(dfs, ignore_index=True)
    df_all = normalize_dimensions(df_all)

    # Drop rows with invalid month (cannot be used)
    m = parse_month_col(df_all)
    df_all = df_all[m.notna()].copy()

    return df_all


# ============================================================
# DATA SPLITS (no assumptions, based on source semantics)
# ============================================================

def split_seller_level(df: pd.DataFrame) -> pd.DataFrame:
    # Seller-level: source empty
    return df[df["source"] == ""].copy()


def split_category_url_level(df: pd.DataFrame) -> pd.DataFrame:
    # Category-url-level: source non-empty
    return df[df["source"] != ""].copy()


# ============================================================
# STATUS RULES (pairwise)
# ============================================================

def classify_pairwise_status(prev_val: float, latest_val: float, ratio: float) -> str:
    # prev_val, latest_val are counts (spu_count)
    if prev_val == 0 and latest_val > 0:
        return "new-in-latest"
    if prev_val > 0 and latest_val == 0:
        return "missing-in-latest"
    if pd.isna(ratio):
        return "no-baseline"
    if ratio >= ABNORMAL_INCREASE_THRESHOLD:
        return "abnormal-increase"
    if ratio < SELLER_DROP_THRESHOLD:
        return "abnormal-drop"
    return "normal"


def classify_coverage_status(prev_scope: float, latest_scope: float, coverage_ratio: float) -> (str, str):
    # coverage_ratio = latest_seller / prev_seller (when prev_seller>0)
    # Use union-scope reporting:
    # - if prev=0 latest>0 => new-in-latest
    # - if prev>0 latest=0 => missing-in-latest
    # - else compare with coverage threshold
    if prev_scope == 0 and latest_scope > 0:
        return "fail", "new-in-latest"
    if prev_scope > 0 and latest_scope == 0:
        return "fail", "missing-in-latest"
    if pd.isna(coverage_ratio):
        return "fail", "no-baseline"
    if coverage_ratio >= COVERAGE_PASS_RATIO:
        return "pass", "normal"
    return "fail", "low-coverage"


# ============================================================
# PAIRWISE QAQC (keep core calculation, improve scope & status)
# ============================================================

def build_coverage(df_prev: pd.DataFrame, df_latest: pd.DataFrame, latest_month: str, prev_month: str) -> pd.DataFrame:
    # Union scope on (country, platform)
    prev_scope = (
        df_prev.groupby(["country", "platform"])
        .agg(total_seller_scope_prev=("seller_used_id", "nunique"))
        .reset_index()
    )

    latest_scope = (
        df_latest.groupby(["country", "platform"])
        .agg(seller_with_vendor_data_latest=("seller_used_id", "nunique"))
        .reset_index()
    )

    scope = pd.merge(prev_scope, latest_scope, on=["country", "platform"], how="outer").fillna(0)

    scope["coverage_ratio_raw"] = scope.apply(
        lambda r: safe_ratio(r["seller_with_vendor_data_latest"], r["total_seller_scope_prev"]),
        axis=1
    )
    scope["coverage_ratio"] = scope["coverage_ratio_raw"].apply(format_percent_ratio)

    status_pairs = scope.apply(
        lambda r: classify_coverage_status(
            r["total_seller_scope_prev"], r["seller_with_vendor_data_latest"], r["coverage_ratio_raw"]
        ),
        axis=1
    )
    scope["coverage_status"] = [p[0] for p in status_pairs]
    scope["coverage_status_detail"] = [p[1] for p in status_pairs]

    # Column order (keep ratio near metrics)
    scope.insert(2, "month_latest", latest_month)
    scope.insert(3, "month_prev", prev_month)

    scope = scope[
        [
            "country",
            "platform",
            "month_latest",
            "month_prev",
            "total_seller_scope_prev",
            "seller_with_vendor_data_latest",
            "coverage_ratio",
            "coverage_status",
            "coverage_status_detail",
        ]
    ].copy()

    return scope


def build_seller_trend_pairwise(df_prev_seller: pd.DataFrame, df_latest_seller: pd.DataFrame,
                               latest_month: str, prev_month: str) -> pd.DataFrame:
    prev = (
        df_prev_seller.groupby(["country", "platform", "seller_used_id"])
        .agg(spu_count_prev=("spu_used_id", "nunique"))
        .reset_index()
    )
    latest = (
        df_latest_seller.groupby(["country", "platform", "seller_used_id"])
        .agg(spu_count_latest=("spu_used_id", "nunique"))
        .reset_index()
    )

    out = pd.merge(prev, latest, on=["country", "platform", "seller_used_id"], how="outer").fillna(0)

    out["spu_ratio_vs_prev_raw"] = out.apply(
        lambda r: safe_ratio(r["spu_count_latest"], r["spu_count_prev"]),
        axis=1
    )
    out["spu_ratio_vs_prev"] = out["spu_ratio_vs_prev_raw"].apply(format_percent_ratio)

    out["status"] = out.apply(
        lambda r: classify_pairwise_status(r["spu_count_prev"], r["spu_count_latest"], r["spu_ratio_vs_prev_raw"]),
        axis=1
    )

    out.insert(3, "month_latest", latest_month)
    out.insert(4, "month_prev", prev_month)

    # Keep structure consistent, ratio not pushed to the end
    out = out[
        [
            "country",
            "platform",
            "seller_used_id",
            "month_latest",
            "month_prev",
            "spu_count_prev",
            "spu_count_latest",
            "spu_ratio_vs_prev",
            "status",
        ]
    ].copy()

    return out


def build_category_trend_pairwise(df_prev_cat: pd.DataFrame, df_latest_cat: pd.DataFrame,
                                 latest_month: str, prev_month: str) -> pd.DataFrame:
    # category_url is stored in source column (non-empty only)
    df_prev_cat = df_prev_cat.copy()
    df_latest_cat = df_latest_cat.copy()
    df_prev_cat["category_url"] = df_prev_cat["source"]
    df_latest_cat["category_url"] = df_latest_cat["source"]

    prev = (
        df_prev_cat.groupby(["country", "platform", "category_url"])
        .agg(spu_count_prev=("spu_used_id", "nunique"))
        .reset_index()
    )
    latest = (
        df_latest_cat.groupby(["country", "platform", "category_url"])
        .agg(spu_count_latest=("spu_used_id", "nunique"))
        .reset_index()
    )

    out = pd.merge(prev, latest, on=["country", "platform", "category_url"], how="outer").fillna(0)

    out["spu_ratio_vs_prev_raw"] = out.apply(
        lambda r: safe_ratio(r["spu_count_latest"], r["spu_count_prev"]),
        axis=1
    )
    out["spu_ratio_vs_prev"] = out["spu_ratio_vs_prev_raw"].apply(format_percent_ratio)

    out["status"] = out.apply(
        lambda r: classify_pairwise_status(r["spu_count_prev"], r["spu_count_latest"], r["spu_ratio_vs_prev_raw"]),
        axis=1
    )

    out.insert(3, "month_latest", latest_month)
    out.insert(4, "month_prev", prev_month)

    out = out[
        [
            "country",
            "platform",
            "category_url",
            "month_latest",
            "month_prev",
            "spu_count_prev",
            "spu_count_latest",
            "spu_ratio_vs_prev",
            "status",
        ]
    ].copy()

    return out


# ============================================================
# MULTI-MONTH TREND (new files, status included)
# Principle: baseline = months in window excluding latest
# ============================================================

def month_window(latest_month: str, lookback_months: int):
    latest_dt = pd.to_datetime(latest_month, format="%Y-%m")
    start_dt = latest_dt - pd.DateOffset(months=lookback_months)
    return start_dt, latest_dt


def classify_trend_status(months_observed: int, latest_spu: float, median_spu: float) -> str:
    # months_observed is baseline months count (excluding latest)
    if months_observed < 2:
        return "insufficient-history"
    if median_spu == 0:
        # baseline is 0 but we have enough months; treat as no-baseline for decision
        return "no-baseline"
    ratio = latest_spu / median_spu
    if ratio >= ABNORMAL_INCREASE_THRESHOLD:
        return "abnormal-increase"
    if ratio < SELLER_DROP_THRESHOLD:
        return "abnormal-drop"
    return "normal"


def build_seller_trend_multi_month(df_seller: pd.DataFrame, latest_month: str) -> pd.DataFrame:
    df = df_seller.copy()
    df["_month_dt"] = parse_month_col(df)

    start_dt, latest_dt = month_window(latest_month, LOOKBACK_MONTHS)
    df = df[(df["_month_dt"] <= latest_dt) & (df["_month_dt"] > start_dt)].copy()

    # monthly spu_count per seller
    hist = (
        df.groupby(["country", "platform", "seller_used_id", "month"])
        .agg(spu_count=("spu_used_id", "nunique"))
        .reset_index()
    )

    # baseline months exclude latest
    baseline = hist[hist["month"] != latest_month].copy()

    base_agg = (
        baseline.groupby(["country", "platform", "seller_used_id"])
        .agg(
            months_observed=("month", "nunique"),
            median_spu=("spu_count", "median"),
        )
        .reset_index()
    )

    latest_agg = (
        hist[hist["month"] == latest_month]
        .groupby(["country", "platform", "seller_used_id"])
        .agg(latest_spu=("spu_count", "max"))
        .reset_index()
    )

    out = pd.merge(base_agg, latest_agg, on=["country", "platform", "seller_used_id"], how="outer").fillna(0)

    out["trend_ratio_raw"] = out.apply(
        lambda r: safe_ratio(r["latest_spu"], r["median_spu"]),
        axis=1
    )
    out["trend_ratio"] = out["trend_ratio_raw"].apply(format_percent_ratio)

    out["trend_status"] = out.apply(
        lambda r: classify_trend_status(int(r["months_observed"]), r["latest_spu"], r["median_spu"]),
        axis=1
    )

    out.insert(3, "month_latest", latest_month)
    out.insert(4, "lookback_months", LOOKBACK_MONTHS)

    out = out[
        [
            "country",
            "platform",
            "seller_used_id",
            "month_latest",
            "lookback_months",
            "months_observed",
            "median_spu",
            "latest_spu",
            "trend_ratio",
            "trend_status",
        ]
    ].copy()

    return out


def build_category_trend_multi_month(df_cat: pd.DataFrame, latest_month: str) -> pd.DataFrame:
    # category_url is source (non-empty only)
    df = df_cat.copy()
    df["category_url"] = df["source"]
    df["_month_dt"] = parse_month_col(df)

    start_dt, latest_dt = month_window(latest_month, LOOKBACK_MONTHS)
    df = df[(df["_month_dt"] <= latest_dt) & (df["_month_dt"] > start_dt)].copy()

    hist = (
        df.groupby(["country", "platform", "category_url", "month"])
        .agg(spu_count=("spu_used_id", "nunique"))
        .reset_index()
    )

    baseline = hist[hist["month"] != latest_month].copy()

    base_agg = (
        baseline.groupby(["country", "platform", "category_url"])
        .agg(
            months_observed=("month", "nunique"),
            median_spu=("spu_count", "median"),
        )
        .reset_index()
    )

    latest_agg = (
        hist[hist["month"] == latest_month]
        .groupby(["country", "platform", "category_url"])
        .agg(latest_spu=("spu_count", "max"))
        .reset_index()
    )

    out = pd.merge(base_agg, latest_agg, on=["country", "platform", "category_url"], how="outer").fillna(0)

    out["trend_ratio_raw"] = out.apply(
        lambda r: safe_ratio(r["latest_spu"], r["median_spu"]),
        axis=1
    )
    out["trend_ratio"] = out["trend_ratio_raw"].apply(format_percent_ratio)

    out["trend_status"] = out.apply(
        lambda r: classify_trend_status(int(r["months_observed"]), r["latest_spu"], r["median_spu"]),
        axis=1
    )

    out.insert(3, "month_latest", latest_month)
    out.insert(4, "lookback_months", LOOKBACK_MONTHS)

    out = out[
        [
            "country",
            "platform",
            "category_url",
            "month_latest",
            "lookback_months",
            "months_observed",
            "median_spu",
            "latest_spu",
            "trend_ratio",
            "trend_status",
        ]
    ].copy()

    return out


# ============================================================
# MAIN
# ============================================================

def run_qaqc_vendor_data():
    print("Reading raw vendor data...")
    df_all = read_raw_all()

    latest_month, prev_month = detect_latest_prev_month(df_all)

    # Filter months (no filename assumptions)
    df_prev = df_all[df_all["month"] == prev_month].copy()
    df_latest = df_all[df_all["month"] == latest_month].copy()

    # Split datasets by semantics (source null vs non-null)
    df_prev_seller = split_seller_level(df_prev)
    df_latest_seller = split_seller_level(df_latest)

    df_prev_cat = split_category_url_level(df_prev)
    df_latest_cat = split_category_url_level(df_latest)

    # Pairwise outputs (union scope + status)
    coverage = build_coverage(df_prev, df_latest, latest_month, prev_month)
    seller_trend = build_seller_trend_pairwise(df_prev_seller, df_latest_seller, latest_month, prev_month)
    category_trend = build_category_trend_pairwise(df_prev_cat, df_latest_cat, latest_month, prev_month)

    coverage.to_csv(os.path.join(RESULT_DIR, "qaqc_vendor_coverage.csv"), index=False)
    seller_trend.to_csv(os.path.join(RESULT_DIR, "qaqc_vendor_seller_trend.csv"), index=False)
    category_trend.to_csv(os.path.join(RESULT_DIR, "qaqc_vendor_category_trend.csv"), index=False)

    # Multi-month trend outputs (status included; category excludes seller-level by split)
    df_seller_all = split_seller_level(df_all)
    df_cat_all = split_category_url_level(df_all)

    seller_trend_mm = build_seller_trend_multi_month(df_seller_all, latest_month)
    category_trend_mm = build_category_trend_multi_month(df_cat_all, latest_month)

    seller_trend_mm.to_csv(os.path.join(RESULT_DIR, "qaqc_vendor_seller_trend_multi_month.csv"), index=False)
    category_trend_mm.to_csv(os.path.join(RESULT_DIR, "qaqc_vendor_category_trend_multi_month.csv"), index=False)

    print(f"Latest month: {latest_month}, Previous month: {prev_month}")
    print("QAQC vendor data completed.")
    print("Generated files:")
    print("- qaqc_vendor_coverage.csv")
    print("- qaqc_vendor_seller_trend.csv")
    print("- qaqc_vendor_category_trend.csv")
    print("- qaqc_vendor_seller_trend_multi_month.csv")
    print("- qaqc_vendor_category_trend_multi_month.csv")


if __name__ == "__main__":
    run_qaqc_vendor_data()
