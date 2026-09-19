import pandas as pd

# Read raw datasets
customers = pd.read_csv("data/raw/customers.csv")
products = pd.read_csv("data/raw/products.csv")
stores = pd.read_csv("data/raw/stores.csv")
sales = pd.read_csv("data/raw/sales.csv")

# Remove duplicate records
customers = customers.drop_duplicates()
products = products.drop_duplicates()
stores = stores.drop_duplicates()
sales = sales.drop_duplicates()

# Handle missing values
customers = customers.dropna(subset=["customer_id"])
products = products.dropna(subset=["product_id"])
stores = stores.dropna(subset=["store_id"])
sales = sales.dropna(
    subset=["sale_id", "customer_id", "product_id", "store_id"]
)

# Convert date columns
customers["signup_date"] = pd.to_datetime(customers["signup_date"])
sales["sale_date"] = pd.to_datetime(sales["sale_date"])

# Make sure numeric columns have the correct type
products["price"] = pd.to_numeric(products["price"])
sales["quantity"] = pd.to_numeric(sales["quantity"])
sales["unit_price"] = pd.to_numeric(sales["unit_price"])

# Save cleaned data
customers.to_csv("data/processed/customers_clean.csv", index=False)
products.to_csv("data/processed/products_clean.csv", index=False)
stores.to_csv("data/processed/stores_clean.csv", index=False)
sales.to_csv("data/processed/sales_clean.csv", index=False)

print("Data cleaning completed successfully!")