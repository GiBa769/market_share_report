# File: market_share_report/src/build_report/build_excel_report.py
# Purpose: Build final QAQC Excel report with full traceability
# Notes:
# - Separate RESULT vs DECISION vs SUMMARY
# - No recomputation
# - Numbers must be traceable end-to-end

import os
import pandas as pd


OUTPUT_DIR = "qaqc_report"
OUTPUT_FILE = "market_share_qaqc_report.xlsx"

FILES = {
    "SPU_Attribute_Result": "qaqc_results/spu_level/attribute_check_result.csv",
    "SPU_Metric_Same_Month": "qaqc_results/spu_level/metric_same_month_result.csv",
    "SPU_Metric_Diff_Months": "qaqc_results/spu_level/metric_diff_months_result.csv",
    "Seller_Decision": "qaqc_results/seller_level/seller_result.csv",
    "Category_Decision": "qaqc_results/category_level/category_result.csv",
    "Country_Platform_Summary": "qaqc_results/country_platform_level/country_platform_result.csv",
}


def _safe_read_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def _build_spu_decision_sheet():
    # Build SPU NORMAL / ABNORMAL base for traceability

    attr_df = _safe_read_csv(FILES["SPU_Attribute_Result"])
    same_df = _safe_read_csv(FILES["SPU_Metric_Same_Month"])
    diff_df = _safe_read_csv(FILES["SPU_Metric_Diff_Months"])

    all_checks = pd.concat([
        attr_df[["spu_used_id", "check_result"]],
        same_df[["spu_used_id", "check_result"]],
        diff_df[["spu_used_id", "check_result"]],
    ])

    spu_decision = (
        all_checks
        .groupby("spu_used_id")["check_result"]
        .apply(lambda x: "PASS" if "FAIL" not in x.values else "FAIL")
        .reset_index(name="spu_overall_result")
    )

    return spu_decision


def _build_readme_sheet():
    return pd.DataFrame({
        "Section": [
            "SPU Attribute Result",
            "SPU Metric Same Month",
            "SPU Metric Diff Months",
            "SPU Decision Base",
            "Seller Decision",
            "Category Decision",
            "Country Platform Summary",
        ],
        "Description": [
            "Attribute consistency check at SPU level",
            "Metric comparison across vendor_group within same month",
            "Metric comparison against historical months",
            "Derived SPU NORMAL or ABNORMAL based on all checks",
            "Seller-level decision after applying benchmark",
            "Category-level decision after applying benchmark",
            "Final summary for Product team review",
        ],
    })


def build_excel_report():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        # README
        _build_readme_sheet().to_excel(
            writer, sheet_name="README", index=False
        )

        # SPU results
        _safe_read_csv(FILES["SPU_Attribute_Result"]).to_excel(
            writer, sheet_name="SPU_Attribute_Result", index=False
        )
        _safe_read_csv(FILES["SPU_Metric_Same_Month"]).to_excel(
            writer, sheet_name="SPU_Metric_Same_Month", index=False
        )
        _safe_read_csv(FILES["SPU_Metric_Diff_Months"]).to_excel(
            writer, sheet_name="SPU_Metric_Diff_Months", index=False
        )

        # SPU decision base
        _build_spu_decision_sheet().to_excel(
            writer, sheet_name="SPU_Decision_Base", index=False
        )

        # Aggregated decisions
        _safe_read_csv(FILES["Seller_Decision"]).to_excel(
            writer, sheet_name="Seller_Decision", index=False
        )
        _safe_read_csv(FILES["Category_Decision"]).to_excel(
            writer, sheet_name="Category_Decision", index=False
        )
        _safe_read_csv(FILES["Country_Platform_Summary"]).to_excel(
            writer, sheet_name="Country_Platform_Summary", index=False
        )


if __name__ == "__main__":
    build_excel_report()
