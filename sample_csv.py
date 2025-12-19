import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_SIZE = 10000


def sample_csv_file(input_path: str, output_path: str, n_rows: int = 10000):
    """
    Read first N rows of a large CSV file and save to a new CSV.
    """
    print(f"Sampling: {input_path}")

    df = pd.read_csv(
        input_path,
        nrows=n_rows,
        low_memory=False
    )

    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path} ({len(df)} rows)")


def process_folder(folder_name: str):
    folder_path = os.path.join(BASE_DIR, folder_name)

    if not os.path.isdir(folder_path):
        print(f"Folder not found: {folder_path}")
        return

    for file_name in os.listdir(folder_path):
        if not file_name.lower().endswith(".csv"):
            continue

        input_file = os.path.join(folder_path, file_name)
        output_file = os.path.join(
            folder_path,
            file_name.replace(".csv", "_sample.csv")
        )

        sample_csv_file(input_file, output_file, SAMPLE_SIZE)


def main():
    print("=== START CSV SAMPLING ===")

    process_folder("computed")
    process_folder("raw")

    print("=== DONE ===")


if __name__ == "__main__":
    main()
