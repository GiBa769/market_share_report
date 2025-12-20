# File: market_share_report/src/country_platform_level/compute_country_platform_results.py
# Purpose: Aggregate seller and category QAQC results to country x platform level
# Notes: Keep original paths and output schema. Optimize IO by chunk-reading raw map only.

import os
import pandas as pd


RAW_PATH = "qaqc_results/spu_level/normalized_raw_vendor_data.csv"
SELLER_PATH = "qaqc_results/seller_level/seller_result.csv"
CATEGORY_PATH = "qaqc_results/category_level/category_result.csv"

OUTPUT_PATH = "qaqc_results/country_platform_level/country_platform_result.csv"

CHUNK_SIZE = 200_000


def _build_maps_from_raw():
    # Build mapping without loading whole RAW into memory
    # seller_used_id -> (country, platform)
    seller_map = {}
    # category_url -> (country, platform) where category_url comes from source column (non-null)
    category_map = {}

    usecols = ["seller_used_id", "country", "platform", "source"]

    reader = pd.read_csv(
        RAW_PATH,
        chunksize=CHUNK_SIZE,
        dtype=str,
        usecols=usecols,
        low_memory=False,
    )

    for chunk in reader:
        # seller map
        s = chunk[["seller_used_id", "country", "platform"]].dropna(subset=["seller_used_id", "country", "platform"])
        s = s.drop_duplicates()
        for row in s.itertuples(index=False):
            sid = row.seller_used_id
            if sid not in seller_map:
                seller_map[sid] = (row.country, row.platform)

        # category map: source as category_url
        c = chunk[chunk["source"].notna()][["source", "country", "platform"]].dropna(subset=["source", "country", "platform"])
        c = c.drop_duplicates()
        for row in c.itertuples(index=False):
            curl = row.source
            if curl not in category_map:
                category_map[curl] = (row.country, row.platform)

    return seller_map, category_map


def compute_country_platform_results():
    if not os.path.exists(SELLER_PATH) or not os.path.exists(CATEGORY_PATH) or not os.path.exists(RAW_PATH):
        return

    seller_map, category_map = _build_maps_from_raw()

    # seller_result.csv: keep same columns/meaning as original
    seller_df = pd.read_csv(
        SELLER_PATH,
        dtype=str,
        usecols=["seller_used_id", "seller_result"],
        low_memory=False,
    )

    seller_df["country"] = seller_df["seller_used_id"].map(lambda x: seller_map.get(x, (None, None))[0])
    seller_df["platform"] = seller_df["seller_used_id"].map(lambda x: seller_map.get(x, (None, None))[1])
    seller_df = seller_df.dropna(subset=["country", "platform"])

    seller_summary = (
        seller_df
        .groupby(["country", "platform"])
        .agg(
            seller_count=("seller_used_id", "count"),
            seller_pass=("seller_result", lambda x: (x == "PASS").sum()),
        )
        .reset_index()
    )

    # category_result.csv: keep same columns/meaning as original
    category_df = pd.read_csv(
        CATEGORY_PATH,
        dtype=str,
        usecols=["category_url", "category_result"],
        low_memory=False,
    )

    category_df["country"] = category_df["category_url"].map(lambda x: category_map.get(x, (None, None))[0])
    category_df["platform"] = category_df["category_url"].map(lambda x: category_map.get(x, (None, None))[1])
    category_df = category_df.dropna(subset=["country", "platform"])

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
