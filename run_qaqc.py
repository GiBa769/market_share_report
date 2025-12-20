# File: market_share_report/run_qaqc.py
# Purpose: Entry point to run full QAQC pipeline

from src.normalize.normalize_raw_vendor_data import normalize_raw_vendor_data
from src.spu_level.check_attributes import run_spu_attribute_checks
from src.spu_level.check_metric_same_month import run_spu_metric_same_month_checks
from src.spu_level.check_metric_diff_months import run_spu_metric_diff_months_checks
from src.seller_level.compute_seller_results import compute_seller_results
from src.category_level.compute_category_results import compute_category_results
from src.country_platform_level.compute_country_platform_results import (
    compute_country_platform_results
)
from src.build_report.build_excel_report import build_excel_report


def _run_step(step_name, step_func):
    # Run step with basic logging
    try:
        print(f"[RUN] {step_name}")
        step_func()
        print(f"[DONE] {step_name}")
    except Exception as exc:
        print(f"[SKIPPED] {step_name} | reason: {exc}")


def run_qaqc_pipeline():
    _run_step("Normalize raw vendor data", normalize_raw_vendor_data)

    _run_step("SPU attribute checks", run_spu_attribute_checks)
    _run_step("SPU metric same month checks", run_spu_metric_same_month_checks)
    _run_step("SPU metric diff months checks", run_spu_metric_diff_months_checks)

    _run_step("Seller level aggregation", compute_seller_results)
    _run_step("Category level aggregation", compute_category_results)
    _run_step("Country x platform aggregation", compute_country_platform_results)

    _run_step("Build QAQC Excel report", build_excel_report)


if __name__ == "__main__":
    run_qaqc_pipeline()
