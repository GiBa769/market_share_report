# File: market_share_report/src/country_platform_level/compute_country_platform_results.py
# Purpose: Aggregate seller and category QAQC results to country x platform level

import os
import pandas as pd


RAW_PATH = "qaqc_results/spu_level/normalized_raw_vendor_data.csv"
SELLER_PATH = "qaqc_results/seller_level/seller_result.csv"
CATEGORY_PATH = "qaqc_results/category_level/category_result.csv"

OUTPUT_PATH = "qaqc_results/country_platform_level/country_platform_result.csv"


def compute_country_platform_results():
    raw_df = pd.read_csv(RAW_PATH)
    seller_df = pd.read_csv(SELLER_PATH)
    category_df = pd.read_csv(CATEGORY_PATH)

    seller_map = raw_df[["seller_used_id", "country", "platform"]].drop_duplicates()
    seller_df = seller_df.merge(seller_map, on="seller_used_id", how="left")

    seller_summary = (
        seller_df
        .groupby(["country", "platform"])
        .agg(
            seller_count=("seller_used_id", "count"),
            seller_pass=("seller_result", lambda x: (x == "PASS").sum()),
        )
        .reset_index()
    )

    category_map = (
        raw_df[raw_df["source"].notna()]
        .rename(columns={"source": "category_url"})
        [["category_url", "country", "platform"]]
        .drop_duplicates()
    )

    category_df = category_df.merge(category_map, on="category_url", how="left")

    category_summary = (
        category_df
        .groupby(["country", "platform"])
        .agg(
            category_count=("category_url", "count"),
            category_pass=("category_result", lambda x: (x == "PASS").sum()),
        )
        .reset_index()
    )

    final_df = seller_summary.merge(
        category_summary,
        on=["country", "platform"],
        how="outer"
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    final_df.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    compute_country_platform_results()
