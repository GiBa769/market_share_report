import os
import pandas as pd

# =========================
# CONFIG
# =========================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SELLER_RESULT_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "seller_level",
    "check_seller_level.csv",
)

CATEGORY_RESULT_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "category_level",
    "check_category_url_level.csv",
)

OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "qaqc_results",
    "country_platform_level",
    "check_country_platform_level.csv",
)

THRESHOLD = 0.95

# =========================
# MAIN
# =========================

def run_check_country_platform_level():

    # ---------- Load inputs ----------
    seller_df = pd.read_csv(SELLER_RESULT_PATH)
    category_df = pd.read_csv(CATEGORY_RESULT_PATH)

    # Remove SUMMARY rows
    seller_df = seller_df[seller_df["seller_scope_flag"] != "SUMMARY"]
    category_df = category_df[category_df["category_scope_flag"] != "SUMMARY"]

    # ---------- Seller aggregation ----------
    seller_agg = (
        seller_df[seller_df["seller_scope_flag"] == "in_scope"]
        .groupby(["country", "platform"])
        .agg(
            seller_total=("seller_used_id", "count"),
            seller_normal=("status", lambda x: (x == "Normal").sum()),
        )
        .reset_index()
    )

    seller_agg["seller_rate"] = (
        seller_agg["seller_normal"] / seller_agg["seller_total"]
    )

    seller_agg["seller_check_good"] = seller_agg["seller_rate"] >= THRESHOLD

    # ---------- Category aggregation ----------
    category_agg = (
        category_df[category_df["category_scope_flag"] == "in_scope"]
        .groupby(["country", "platform"])
        .agg(
            category_total=("category_url", "count"),
            category_normal=("status", lambda x: (x == "Normal").sum()),
        )
        .reset_index()
    )

    category_agg["category_rate"] = (
        category_agg["category_normal"] / category_agg["category_total"]
    )

    category_agg["category_check_good"] = category_agg["category_rate"] >= THRESHOLD

    # ---------- Combine country × platform universe ----------
    cp_universe = pd.concat(
        [
            seller_df[["country", "platform"]],
            category_df[["country", "platform"]],
        ],
        ignore_index=True,
    ).drop_duplicates()

    # ---------- Merge ----------
    result = (
        cp_universe
        .merge(seller_agg, on=["country", "platform"], how="left")
        .merge(category_agg, on=["country", "platform"], how="left")
    )

    # Fill missing (no seller/category in scope)
    result["seller_total"] = result["seller_total"].fillna(0).astype(int)
    result["seller_normal"] = result["seller_normal"].fillna(0).astype(int)
    result["seller_rate"] = result["seller_rate"].fillna(0)
    result["seller_check_good"] = result["seller_check_good"].fillna(False)

    result["category_total"] = result["category_total"].fillna(0).astype(int)
    result["category_normal"] = result["category_normal"].fillna(0).astype(int)
    result["category_rate"] = result["category_rate"].fillna(0)
    result["category_check_good"] = result["category_check_good"].fillna(False)

    # ---------- Final decision ----------
    result["good_to_use"] = (
        result["seller_check_good"] & result["category_check_good"]
    )

    # ---------- Reorder ----------
    result = result[
        [
            "country",
            "platform",

            "seller_total",
            "seller_normal",
            "seller_rate",
            "seller_check_good",

            "category_total",
            "category_normal",
            "category_rate",
            "category_check_good",

            "good_to_use",
        ]
    ].sort_values(["country", "platform"])

    # ---------- Write ----------
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Country × Platform result written to: {OUTPUT_PATH}")
    print(f"[INFO] Rows: {len(result)}")


if __name__ == "__main__":
    run_check_country_platform_level()
