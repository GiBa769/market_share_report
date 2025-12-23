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

    seller_df = pd.read_csv(SELLER_RESULT_PATH)
    category_df = pd.read_csv(CATEGORY_RESULT_PATH)

    # =========================
    # SELLER AGGREGATION
    # =========================

    seller_agg = (
        seller_df
        .groupby(["country", "platform"])
        .apply(lambda g: pd.Series({
            # denominator: sellers IN SCOPE only
            "seller_total_in_scope": (g["seller_scope_flag"] == "in_scope").sum(),

            # numerator: NORMAL sellers (both in + out scope)
            "seller_normal_all": (g["seller_status"] == "Normal").sum(),
        }))
        .reset_index()
    )

    seller_agg["seller_rate"] = seller_agg.apply(
        lambda r: (
            r["seller_normal_all"] / r["seller_total_in_scope"]
            if r["seller_total_in_scope"] > 0 else 0
        ),
        axis=1,
    )

    seller_agg["seller_check_good"] = seller_agg["seller_rate"] >= THRESHOLD

    # =========================
    # CATEGORY AGGREGATION
    # =========================

    category_agg = (
        category_df
        .groupby(["country", "platform"])
        .apply(lambda g: pd.Series({
            "category_total_in_scope": (g["category_scope_flag"] == "in_scope").sum(),
            "category_normal_all": (g["category_status"] == "Normal").sum(),
        }))
        .reset_index()
    )

    category_agg["category_rate"] = category_agg.apply(
        lambda r: (
            r["category_normal_all"] / r["category_total_in_scope"]
            if r["category_total_in_scope"] > 0 else 0
        ),
        axis=1,
    )

    category_agg["category_check_good"] = category_agg["category_rate"] >= THRESHOLD

    # =========================
    # COUNTRY × PLATFORM UNIVERSE
    # =========================

    cp_universe = pd.concat(
        [
            seller_df[["country", "platform"]],
            category_df[["country", "platform"]],
        ],
        ignore_index=True,
    ).drop_duplicates()

    # =========================
    # MERGE
    # =========================

    result = (
        cp_universe
        .merge(seller_agg, on=["country", "platform"], how="left")
        .merge(category_agg, on=["country", "platform"], how="left")
    )

    # fill NA
    for col in [
        "seller_total_in_scope",
        "seller_normal_all",
        "category_total_in_scope",
        "category_normal_all",
    ]:
        result[col] = result[col].fillna(0).astype(int)

    for col in [
        "seller_rate",
        "category_rate",
    ]:
        result[col] = result[col].fillna(0.0)

    for col in [
        "seller_check_good",
        "category_check_good",
    ]:
        result[col] = result[col].fillna(False)

    # =========================
    # FINAL DECISION
    # =========================

    result["good_to_use"] = (
        result["seller_check_good"] & result["category_check_good"]
    )

    # =========================
    # OUTPUT
    # =========================

    result = result[
        [
            "country",
            "platform",

            "seller_total_in_scope",
            "seller_normal_all",
            "seller_rate",
            "seller_check_good",

            "category_total_in_scope",
            "category_normal_all",
            "category_rate",
            "category_check_good",

            "good_to_use",
        ]
    ].sort_values(["country", "platform"])

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Country × Platform result written to: {OUTPUT_PATH}")
    print(f"[INFO] Rows: {len(result)}")


if __name__ == "__main__":
    run_check_country_platform_level()
