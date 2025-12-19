import os
import pandas as pd
import numpy as np

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COMPUTED_DIR = os.path.join(BASE_DIR, "computed")
RESULT_DIR = os.path.join(BASE_DIR, "result")
os.makedirs(RESULT_DIR, exist_ok=True)

KEY_COLS = ["country", "platform", "seller_used_id"]

VALUE_COLS = [
    "month",
    "seller_name",
    "final_quantity",
    "final_revenue",
    "final_asp",
    "computation_label",
    "historical_quantity",
    "historical_review",
    "has_S1", "has_S2", "has_S3", "has_S4", "has_S5", "has_S6",
]

# Pairwise config (KEEP logic)
SPIKE_GROWTH_RATE_REVENUE = 2          # >= 200%
BASE_EFFECT_REVENUE_PREV = 100.0
CHUNK_SIZE = 200_000

# Trend config (multi-month)
LOOKBACK_MONTHS = 3
TREND_DROP_THRESHOLD = 0.80
TREND_INCREASE_THRESHOLD = 2.00
MIN_MONTHS_OBSERVED_FOR_TREND = 2


# ============================================================
# HELPERS
# ============================================================

def list_computed_files():
    if not os.path.exists(COMPUTED_DIR):
        raise FileNotFoundError(f"Missing folder: {COMPUTED_DIR}")

    files = [
        os.path.join(COMPUTED_DIR, f)
        for f in os.listdir(COMPUTED_DIR)
        if f.lower().endswith(".csv") and not f.lower().endswith("_sample.csv")
    ]
    if len(files) < 2:
        raise ValueError(f"Need at least 2 computed CSV files, found {len(files)}")
    return sorted(files)


def first_non_null(series: pd.Series):
    s = series.dropna()
    return s.iloc[0] if not s.empty else None


def safe_div(a: pd.Series, b: pd.Series):
    b = b.replace(0, pd.NA)
    return a / b


def format_percent_series(series: pd.Series, digits: int = 2) -> pd.Series:
    """
    Input: ratio series (e.g. 0.5)
    Output: '50.00%' or '-'
    Rules:
      - NaN / inf / -inf / None => '-'
      - valid numeric => '<xx.xx>%'
      - never output '-%'
    """
    s = pd.to_numeric(series, errors="coerce")

    invalid = s.isna() | np.isinf(s)
    pct = (s * 100).round(digits)

    out = pct.astype("string")
    out = out.where(~invalid, "-")
    out = out.where(out == "-", out + "%")
    return out


def detect_latest_prev_month_from_all(df_months: pd.DataFrame):
    m = pd.to_datetime(df_months["month"], format="%Y-%m", errors="coerce")
    months = sorted(m.dropna().unique())
    if len(months) < 2:
        raise ValueError("Need at least 2 distinct months to compare")

    latest_dt = months[-1]
    prev_dt = latest_dt - pd.offsets.MonthBegin(1)

    month_latest = latest_dt.strftime("%Y-%m")
    month_prev = prev_dt.strftime("%Y-%m")

    existing = set([x.strftime("%Y-%m") for x in months])
    if month_prev not in existing:
        raise ValueError(
            f"Missing previous month (contiguous) data. "
            f"latest={month_latest}, expected prev={month_prev}, but prev not found in computed files."
        )

    return month_latest, month_prev


def month_window(latest_month: str, lookback_months: int):
    latest_dt = pd.to_datetime(latest_month, format="%Y-%m")
    start_dt = latest_dt - pd.DateOffset(months=lookback_months)
    return start_dt, latest_dt


def classify_trend_status(months_observed: int, ratio: float) -> str:
    if months_observed < MIN_MONTHS_OBSERVED_FOR_TREND:
        return "insufficient-history"
    if pd.isna(ratio) or np.isinf(ratio):
        return "no-baseline"
    if ratio >= TREND_INCREASE_THRESHOLD:
        return "abnormal-increase"
    if ratio < TREND_DROP_THRESHOLD:
        return "abnormal-drop"
    return "normal"


# ============================================================
# AGGREGATION
# ============================================================

def aggregate_computed_by_seller(path: str) -> pd.DataFrame:
    print(f"Reading & aggregating: {os.path.basename(path)}")
    chunks = []

    for i, chunk in enumerate(pd.read_csv(path, chunksize=CHUNK_SIZE, low_memory=False)):
        print(f"  Chunk {i + 1} processed...")

        use_cols = [c for c in (KEY_COLS + VALUE_COLS) if c in chunk.columns]
        chunk = chunk.loc[:, use_cols].copy()

        for c in ["final_quantity", "final_revenue", "final_asp", "historical_quantity", "historical_review"]:
            if c in chunk.columns:
                chunk.loc[:, c] = pd.to_numeric(chunk[c], errors="coerce")

        agg_dict = {
            "month": "max",
            "seller_name": first_non_null,
            "final_quantity": "sum",
            "final_revenue": "sum",
            "final_asp": "mean",
            "historical_quantity": "max",
            "historical_review": "max",
            "computation_label": first_non_null,
            "has_S1": "max",
            "has_S2": "max",
            "has_S3": "max",
            "has_S4": "max",
            "has_S5": "max",
            "has_S6": "max",
        }
        agg_dict = {k: v for k, v in agg_dict.items() if k in chunk.columns}

        agg = chunk.groupby(KEY_COLS, as_index=False).agg(agg_dict)
        chunks.append(agg)

    df = pd.concat(chunks, ignore_index=True)

    agg_dict2 = {
        "month": "max",
        "seller_name": first_non_null,
        "final_quantity": "sum",
        "final_revenue": "sum",
        "final_asp": "mean",
        "historical_quantity": "max",
        "historical_review": "max",
        "computation_label": first_non_null,
        "has_S1": "max",
        "has_S2": "max",
        "has_S3": "max",
        "has_S4": "max",
        "has_S5": "max",
        "has_S6": "max",
    }
    agg_dict2 = {k: v for k, v in agg_dict2.items() if k in df.columns}

    df = df.groupby(KEY_COLS, as_index=False).agg(agg_dict2)

    print(f"Done aggregating {os.path.basename(path)}")
    return df


def aggregate_all_months() -> pd.DataFrame:
    files = list_computed_files()
    dfs = [aggregate_computed_by_seller(p) for p in files]
    df_all = pd.concat(dfs, ignore_index=True)

    m = pd.to_datetime(df_all["month"], format="%Y-%m", errors="coerce")
    df_all = df_all[m.notna()].copy()

    return df_all


# ============================================================
# PAIRWISE QAQC (KEEP LOGIC)
# ============================================================

def run_pairwise_qaqc(df_latest_raw: pd.DataFrame, df_prev_raw: pd.DataFrame,
                      month_latest: str, month_prev: str):

    df_latest = df_latest_raw.add_suffix("_latest")
    df_prev = df_prev_raw.add_suffix("_prev")

    for k in KEY_COLS:
        df_latest[k] = df_latest[f"{k}_latest"]
        df_prev[k] = df_prev[f"{k}_prev"]

    df = pd.merge(df_prev, df_latest, on=KEY_COLS, how="inner")

    # Delta & rate
    df["delta_final_revenue"] = df["final_revenue_latest"] - df["final_revenue_prev"]
    df["growth_rate_revenue_raw"] = safe_div(df["delta_final_revenue"], df["final_revenue_prev"])

    df["delta_final_quantity"] = df["final_quantity_latest"] - df["final_quantity_prev"]
    df["growth_rate_quantity_raw"] = safe_div(df["delta_final_quantity"], df["final_quantity_prev"])

    # Abnormal classification (UNCHANGED)
    df["is_spike"] = df["growth_rate_revenue_raw"] >= SPIKE_GROWTH_RATE_REVENUE
    df["is_base_effect"] = df["final_revenue_prev"] < BASE_EFFECT_REVENUE_PREV

    if "computation_label_prev" in df.columns and "computation_label_latest" in df.columns:
        df["is_source_switch"] = (df["computation_label_prev"] != df["computation_label_latest"])
    else:
        df["is_source_switch"] = False

    def classify(row):
        if not row["is_spike"]:
            return "none"
        if row["is_base_effect"]:
            return "base-effect"
        if row["is_source_switch"]:
            return "source-switch"
        return "spike-out-trend"

    df["abnormal_type"] = df.apply(classify, axis=1)
    df["abnormal_flag"] = df["abnormal_type"] != "none"

    def risk(row):
        if row["abnormal_type"] == "spike-out-trend":
            return "high"
        if row["abnormal_type"] == "source-switch":
            return "medium"
        if row["abnormal_type"] == "base-effect":
            return "low"
        return "none"

    df["risk_level"] = df.apply(risk, axis=1)

    # Pre-format percent columns (fix)
    df["growth_rate_revenue"] = format_percent_series(df["growth_rate_revenue_raw"])
    df["growth_rate_quantity"] = format_percent_series(df["growth_rate_quantity_raw"])

    # ----------------------------
    # SUMMARY (latest view)
    # ----------------------------
    summary = df[[
        "country", "platform",
        "seller_used_id", "seller_name_latest",
        "final_revenue_latest", "final_revenue_prev",
        "delta_final_revenue", "growth_rate_revenue",
        "final_quantity_latest", "final_quantity_prev",
        "delta_final_quantity", "growth_rate_quantity",
        "abnormal_flag", "abnormal_type", "risk_level"
    ]].copy()

    summary.insert(2, "month_latest", month_latest)
    summary.insert(3, "month_prev", month_prev)

    summary = summary.rename(columns={
        "seller_name_latest": "seller_name",
        "final_revenue_latest": "final_revenue",
        "final_quantity_latest": "final_quantity",
    })

    summary.to_csv(os.path.join(RESULT_DIR, "qaqc_market_share_latest_summary.csv"), index=False)

    # ----------------------------
    # ABNORMAL
    # ----------------------------
    abnormal = df[df["abnormal_flag"]].copy()

    abnormal_out_cols = [
        "country", "platform",
        "seller_used_id", "seller_name_latest",
        "final_revenue_latest", "final_revenue_prev",
        "delta_final_revenue", "growth_rate_revenue",
        "final_quantity_latest", "final_quantity_prev",
        "delta_final_quantity", "growth_rate_quantity",
        "abnormal_type", "risk_level",
    ]
    for c in ["computation_label_prev", "computation_label_latest"]:
        if c in abnormal.columns:
            abnormal_out_cols.append(c)

    abnormal_out = abnormal[abnormal_out_cols].copy()

    abnormal_out.insert(2, "month_latest", month_latest)
    abnormal_out.insert(3, "month_prev", month_prev)

    abnormal_out = abnormal_out.rename(columns={
        "seller_name_latest": "seller_name",
        "final_revenue_latest": "final_revenue",
        "final_quantity_latest": "final_quantity",
    })

    abnormal_out.to_csv(os.path.join(RESULT_DIR, "qaqc_market_share_latest_abnormal.csv"), index=False)

    # ----------------------------
    # EXPLAIN
    # ----------------------------
    explain_cols = [
        "country", "platform",
        "seller_used_id", "seller_name_latest",
        "abnormal_type",
    ]
    optional_explain = [
        "computation_label_prev", "computation_label_latest",
        "has_S1_prev", "has_S1_latest",
        "has_S3_prev", "has_S3_latest",
        "historical_quantity_prev", "historical_quantity_latest",
        "historical_review_prev", "historical_review_latest",
        "final_asp_prev", "final_asp_latest",
    ]
    for c in optional_explain:
        if c in abnormal.columns:
            explain_cols.append(c)

    explain = abnormal[explain_cols].copy()
    explain.insert(2, "month_latest", month_latest)
    explain.insert(3, "month_prev", month_prev)
    explain = explain.rename(columns={"seller_name_latest": "seller_name"})

    explain.to_csv(os.path.join(RESULT_DIR, "qaqc_market_share_latest_explain.csv"), index=False)

    return summary, abnormal_out, explain


# ============================================================
# MULTI-MONTH TREND (added, fixed seller_name + % format)
# ============================================================

def build_trend_multi_month(df_all: pd.DataFrame, month_latest: str) -> pd.DataFrame:
    df = df_all.copy()
    df["_month_dt"] = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")

    start_dt, latest_dt = month_window(month_latest, LOOKBACK_MONTHS)
    df = df[(df["_month_dt"] <= latest_dt) & (df["_month_dt"] > start_dt)].copy()

    hist = (
        df.groupby(KEY_COLS + ["month"], as_index=False)
        .agg({
            "final_revenue": "sum",
            "final_quantity": "sum",
            "seller_name": first_non_null,
        })
    )

    baseline = hist[hist["month"] != month_latest].copy()
    latest = hist[hist["month"] == month_latest].copy()

    base_agg = (
        baseline.groupby(KEY_COLS, as_index=False)
        .agg({
            "month": "nunique",
            "final_revenue": "median",
            "final_quantity": "median",
            "seller_name": first_non_null,
        })
        .rename(columns={
            "month": "months_observed",
            "final_revenue": "median_revenue",
            "final_quantity": "median_quantity",
            "seller_name": "seller_name_baseline",
        })
    )

    latest_agg = (
        latest.groupby(KEY_COLS, as_index=False)
        .agg({
            "final_revenue": "sum",
            "final_quantity": "sum",
            "seller_name": first_non_null,
        })
        .rename(columns={
            "final_revenue": "latest_revenue",
            "final_quantity": "latest_quantity",
            "seller_name": "seller_name_latest",
        })
    )

    out = pd.merge(base_agg, latest_agg, on=KEY_COLS, how="outer")

    # resolve seller_name (metadata only)
    out["seller_name"] = out["seller_name_latest"].combine_first(out["seller_name_baseline"])
    out.drop(columns=["seller_name_latest", "seller_name_baseline"], inplace=True)

    for c in ["months_observed", "median_revenue", "median_quantity", "latest_revenue", "latest_quantity"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    out["trend_ratio_revenue_raw"] = out.apply(
        lambda r: (r["latest_revenue"] / r["median_revenue"]) if r["median_revenue"] != 0 else np.nan,
        axis=1
    )
    out["trend_ratio_quantity_raw"] = out.apply(
        lambda r: (r["latest_quantity"] / r["median_quantity"]) if r["median_quantity"] != 0 else np.nan,
        axis=1
    )

    # percent format (fix)
    out["trend_ratio_revenue"] = format_percent_series(out["trend_ratio_revenue_raw"])
    out["trend_ratio_quantity"] = format_percent_series(out["trend_ratio_quantity_raw"])

    out["trend_status_revenue"] = out.apply(
        lambda r: classify_trend_status(int(r["months_observed"]), r["trend_ratio_revenue_raw"]),
        axis=1
    )
    out["trend_status_quantity"] = out.apply(
        lambda r: classify_trend_status(int(r["months_observed"]), r["trend_ratio_quantity_raw"]),
        axis=1
    )

    out.insert(3, "month_latest", month_latest)
    out.insert(4, "lookback_months", LOOKBACK_MONTHS)

    out = out[
        [
            "country", "platform", "seller_used_id",
            "month_latest", "lookback_months",
            "seller_name",
            "months_observed",
            "median_revenue", "latest_revenue", "trend_ratio_revenue", "trend_status_revenue",
            "median_quantity", "latest_quantity", "trend_ratio_quantity", "trend_status_quantity",
        ]
    ].copy()

    return out


# ============================================================
# MAIN
# ============================================================

def run_qaqc():
    df_all = aggregate_all_months()

    month_latest, month_prev = detect_latest_prev_month_from_all(df_all[["month"]])

    df_latest_raw = df_all[df_all["month"] == month_latest].copy()
    df_prev_raw = df_all[df_all["month"] == month_prev].copy()

    # pairwise outputs
    run_pairwise_qaqc(df_latest_raw, df_prev_raw, month_latest, month_prev)

    # trend output
    trend = build_trend_multi_month(df_all, month_latest)
    trend.to_csv(os.path.join(RESULT_DIR, "qaqc_market_share_latest_trend_multi_month.csv"), index=False)

    print("QAQC market share completed successfully.")
    print("Files generated:")
    print("- qaqc_market_share_latest_summary.csv")
    print("- qaqc_market_share_latest_abnormal.csv")
    print("- qaqc_market_share_latest_explain.csv")
    print("- qaqc_market_share_latest_trend_multi_month.csv")


if __name__ == "__main__":
    run_qaqc()
