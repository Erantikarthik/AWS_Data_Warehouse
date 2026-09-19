import pandas as pd
import os

# Input and output folders
input_folder = "data/processed"
output_folder = "data/processed/parquet"

# Create the Parquet folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Files to convert
files = [
    "customers_clean.csv",
    "products_clean.csv",
    "stores_clean.csv",
    "sales_clean.csv"
]

for file in files:

    # Read CSV
    input_path = os.path.join(input_folder, file)
    df = pd.read_csv(input_path)

    # Create Parquet filename
    parquet_file = file.replace(".csv", ".parquet")
    output_path = os.path.join(output_folder, parquet_file)

    # Convert CSV to Parquet with Snappy compression
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    print(f"Converted {file} → {parquet_file}")

print("All files converted to Parquet successfully!")