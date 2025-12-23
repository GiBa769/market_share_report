# File: market_share_report/run_qaqc.py
# Purpose: QAQC pipeline controller

from src.normalize.normalize_raw_vendor_data import normalize_raw_vendor_data, cleanup_normalized_store
from src.spu_level.check_attributes import run_spu_attribute_checks
from src.spu_level.check_attribute_only import run_check_attribute_only
from src.spu_level.check_metric_same_month_only import run_check_metric_same_month_only
from src.spu_level.check_metric_diff_months_only import run_check_metric_diff_months_only
from src.seller_level.check_seller_level import run_check_seller_level
from src.category_level.check_category_url_level import run_check_category_url_level
from src.country_platform_level.check_country_platform_level import run_check_country_platform_level
from src.spu_level.check_metric_same_month import run_spu_metric_same_month_checks
from src.spu_level.check_metric_diff_months import run_spu_metric_diff_months_checks

from src.seller_level.compute_seller_results import compute_seller_results
from src.category_level.compute_category_results import compute_category_results
from src.country_platform_level.compute_country_platform_results import compute_country_platform_results

from src.build_report.build_excel_report import build_excel_report


def _run(step_name, func):
    print(f"[RUN] {step_name}")
    func()
    print(f"[DONE] {step_name}")


def run_qaqc_pipeline():
    try:
        # _run("Normalize raw vendor data", normalize_raw_vendor_data)
        # _run("SPU attribute check (only)", run_check_attribute_only)
        # _run("SPU metric check (same month only)", run_check_metric_same_month_only)
        # _run("SPU metric check (diff months only)", run_check_metric_diff_months_only)
        # _run("Seller level check", run_check_seller_level)
        # _run("Category URL level check", run_check_category_url_level)
        # _run("Country x Platform level check", run_check_country_platform_level)


        # _run("SPU attribute checks", run_spu_attribute_checks)
        # _run("SPU metric same month checks", run_spu_metric_same_month_checks)
        # _run("SPU metric diff months checks", run_spu_metric_diff_months_checks)
        # _run("Seller level aggregation", compute_seller_results)
        # _run("Category level aggregation", compute_category_results)
        # _run("Country x Platform aggregation", compute_country_platform_results)

        _run("Build Excel report", build_excel_report)
    finally: 
        pass
        # cleanup_normalized_store()


if __name__ == "__main__":
    run_qaqc_pipeline()
