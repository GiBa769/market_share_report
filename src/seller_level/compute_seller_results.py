# File: market_share_report/src/seller_level/compute_seller_results.py
# Purpose: Aggregate SPU QAQC results to seller level using config benchmark

import os
import yaml
import pandas as pd


RAW_PATH = "qaqc_results/spu_level/normalized_raw_vendor_data.csv"
ATTR_PATH = "qaqc_results/spu_level/attribute_check_result.csv"
SAME_MONTH_PATH = "qaqc_results/spu_level/metric_same_month_result.csv"
DIFF_MONTH_PATH = "qaqc_results/spu_level/metric_diff_months_result.csv"

OUTPUT_PATH = "qaqc_results/seller_level/seller_result.csv"

CFG_THRESHOLD = "config/benchmark_thresholds.yaml"
CFG_CONST = "config/qaqc_constants.yaml"


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def compute_seller_results():
    thresholds = load_yaml(CFG_THRESHOLD)
    constants = load_yaml(CFG_CONST)

    status = constants["check_result"]
    pass_min_pct = thresholds["seller_level"]["spu_coverage_ratio"]["pass_min_pct"]

    raw_df = pd.read_csv(RAW_PATH)
    raw_df = raw_df[raw_df["source"].isna()]

    attr_df = pd.read_csv(ATTR_PATH)
    same_df = pd.read_csv(SAME_MONTH_PATH)
    diff_df = pd.read_csv(DIFF_MONTH_PATH)

    # Determine SPU overall result
    spu_checks = (
        attr_df[["spu_used_id", "check_result"]]
        .append(same_df[["spu_used_id", "check_result"]])
        .append(diff_df[["spu_used_id", "check_result"]])
    )

    spu_status = (
        spu_checks
        .groupby("spu_used_id")["check_result"]
        .apply(lambda x: status["fail"] not in x.values)
        .reset_index(name="is_normal")
    )

    seller_spu = (
        raw_df[["seller_used_id", "spu_used_id"]]
        .drop_duplicates()
        .merge(spu_status, on="spu_used_id", how="left")
    )

    summary = (
        seller_spu
        .groupby("seller_used_id")
        .agg(
            total_spu=("spu_used_id", "count"),
            normal_spu=("is_normal", "sum"),
        )
        .reset_index()
    )

    summary["coverage_pct"] = summary["normal_spu"] / summary["total_spu"] * 100
    summary["seller_result"] = summary["coverage_pct"].apply(
        lambda x: status["pass"] if x >= pass_min_pct else status["fail"]
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    summary.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    compute_seller_results()
