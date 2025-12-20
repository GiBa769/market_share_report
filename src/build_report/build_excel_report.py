# File: market_share_report/src/build_excel_report.py
# Purpose: Build final QAQC Excel report (summary only, no raw dump)

import os
import pandas as pd

OUT_PATH = "qaqc_results/QAQC_Market_Share_Report.xlsx"

FILES = {
    "Country_Platform": "qaqc_results/country_platform_level/country_platform_result.csv",
    "Seller": "qaqc_results/seller_level/seller_result.csv",
    "Category": "qaqc_results/category_level/category_result.csv",
}


def build_excel_report():
    with pd.ExcelWriter(OUT_PATH, engine="xlsxwriter") as writer:
        for sheet, path in FILES.items():
            if not os.path.exists(path):
                continue

            df = pd.read_csv(path)
            df.to_excel(writer, sheet_name=sheet, index=False)


if __name__ == "__main__":
    build_excel_report()
