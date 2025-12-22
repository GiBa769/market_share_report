# File: market_share_report/src/build_report/build_excel_report.py
# Purpose: Build final QAQC Excel report (summary only, no raw dump)

import importlib.util
import os
import pandas as pd

OUT_PATH = "qaqc_report/QAQC_Market_Share_Report.xlsx"

FILES = {
    "Country_Platform": "qaqc_results/country_platform_level/country_platform_result.csv",
    "Seller": "qaqc_results/seller_level/seller_result.csv",
    "Category": "qaqc_results/category_level/category_result.csv",
}


def _select_engine() -> str:
    """Pick an available Excel writer engine without requiring optional deps.

    Prefers ``xlsxwriter`` (for speed) but falls back to ``openpyxl`` when the
    former is not installed. Raises a clear error if neither engine is present
    so the user can install one instead of seeing a low-level ImportError.
    """

    for engine, module_name in ("xlsxwriter", "xlsxwriter"), ("openpyxl", "openpyxl"):
        if importlib.util.find_spec(module_name) is not None:
            return engine

    raise RuntimeError(
        "No Excel writer engine found; install either 'xlsxwriter' or 'openpyxl'."
    )


def build_excel_report():
    engine = _select_engine()
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with pd.ExcelWriter(OUT_PATH, engine=engine) as writer:
        for sheet, path in FILES.items():
            if not os.path.exists(path):
                continue

            df = pd.read_csv(path)
            df.to_excel(writer, sheet_name=sheet, index=False)


if __name__ == "__main__":
    build_excel_report()
