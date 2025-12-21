# File: market_share_report/src/country_platform_level/compute_country_platform_results.py
# Purpose: Aggregate seller and category QAQC results to country x platform level
# Notes: Keep original paths and output schema. Optimize IO by chunk-reading raw map only.

import os
import sqlite3
import pandas as pd


RAW_DB = "qaqc_results/spu_level/normalized_raw_vendor_data.sqlite"
RAW_TABLE = "normalized_raw_vendor_data"
SELLER_PATH = "qaqc_results/seller_level/seller_result.csv"
CATEGORY_PATH = "qaqc_results/category_level/category_result.csv"

OUTPUT_PATH = "qaqc_results/country_platform_level/country_platform_result.csv"

CHUNK_SIZE = 200_000
TMP_DB = "qaqc_results/_tmp_country_platform.sqlite"
COMMIT_EVERY = 20


def _build_maps_from_raw():
    if os.path.exists(TMP_DB):
        os.remove(TMP_DB)

    conn = sqlite3.connect(TMP_DB)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    cur.execute(
        "CREATE TABLE seller_map (seller_used_id TEXT PRIMARY KEY, country TEXT, platform TEXT);"
    )
    cur.execute(
        "CREATE TABLE category_map (category_url TEXT PRIMARY KEY, country TEXT, platform TEXT);"
    )

    raw_conn = sqlite3.connect(RAW_DB)
    reader = pd.read_sql_query(
        f"SELECT seller_used_id, country, platform, source FROM {RAW_TABLE}",
        raw_conn,
        chunksize=CHUNK_SIZE,
    )

    chunk_idx = 0
    processed = 0
    for chunk in reader:
        seller_rows = (
            chunk[["seller_used_id", "country", "platform"]]
            .dropna(subset=["seller_used_id", "country", "platform"])
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
        cur.executemany(
            "INSERT OR IGNORE INTO seller_map(seller_used_id, country, platform) VALUES(?, ?, ?);",
            list(seller_rows),
        )

        category_rows = (
            chunk.rename(columns={"source": "category_url"})[
                ["category_url", "country", "platform"]
            ]
            .dropna(subset=["category_url", "country", "platform"])
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
        cur.executemany(
            "INSERT OR IGNORE INTO category_map(category_url, country, platform) VALUES(?, ?, ?);",
            list(category_rows),
        )

        chunk_idx += 1
        if chunk_idx % COMMIT_EVERY == 0:
            conn.commit()
        processed += len(chunk)
        if processed and processed % 300_000 == 0:
            print(f"[country_platform] ingested {processed:,} rows ...", flush=True)

    conn.commit()

    seller_map = pd.read_sql_query("SELECT * FROM seller_map", conn)
    category_map = pd.read_sql_query("SELECT * FROM category_map", conn)

    conn.close()
    raw_conn.close()
    if os.path.exists(TMP_DB):
        os.remove(TMP_DB)

    return seller_map, category_map


def compute_country_platform_results():
    if not os.path.exists(SELLER_PATH) or not os.path.exists(CATEGORY_PATH) or not os.path.exists(RAW_DB):
        return

    seller_map_df, category_map_df = _build_maps_from_raw()

    # seller_result.csv: keep same columns/meaning as original
    seller_df = pd.read_csv(
        SELLER_PATH,
        dtype=str,
        usecols=["seller_used_id", "seller_result"],
        low_memory=False,
    )

    seller_df = seller_df.merge(seller_map_df, on="seller_used_id", how="left")
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

    category_df = category_df.merge(category_map_df.rename(columns={"category_url": "category_url"}), on="category_url", how="left")
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
        how="outer",
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    final_df.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    compute_country_platform_results()
