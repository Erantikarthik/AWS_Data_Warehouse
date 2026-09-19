import pandas as pd
import os

# Gold input and output folders
input_folder = "data/gold"
output_folder = "data/gold/parquet"

# Create output folder
os.makedirs(output_folder, exist_ok=True)

# Gold warehouse tables
files = [
    "dim_customer.csv",
    "dim_product.csv",
    "dim_store.csv",
    "fact_sales.csv"
]

for file in files:

    # Read Gold CSV
    input_path = os.path.join(input_folder, file)
    df = pd.read_csv(input_path)

    # Create Parquet filename
    parquet_file = file.replace(".csv", ".parquet")
    output_path = os.path.join(output_folder, parquet_file)

    # Save as Parquet with Snappy compression
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    print(f"Converted {file} → {parquet_file}")

print("Gold layer Parquet conversion completed successfully!")