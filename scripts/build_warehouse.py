import pandas as pd
import os

# --------------------------------------------------
# 1. Create warehouse folder
# --------------------------------------------------

warehouse = "data/warehouse"
os.makedirs(warehouse, exist_ok=True)


# --------------------------------------------------
# 2. Read cleaned data
# --------------------------------------------------

customers = pd.read_csv("data/processed/customers_clean.csv")
products = pd.read_csv("data/processed/products_clean.csv")
stores = pd.read_csv("data/processed/stores_clean.csv")
sales = pd.read_csv("data/processed/sales_clean.csv")


# --------------------------------------------------
# 3. Dimension: Customer
# SCD Type 2 columns
# --------------------------------------------------

dim_customer = customers.copy()

dim_customer["customer_sk"] = range(1, len(dim_customer) + 1)

dim_customer["effective_date"] = dim_customer["signup_date"]

dim_customer["expiry_date"] = "9999-12-31"

dim_customer["is_current"] = True

dim_customer = dim_customer[
    [
        "customer_sk",
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "effective_date",
        "expiry_date",
        "is_current"
    ]
]


# --------------------------------------------------
# 4. Dimension: Product
# --------------------------------------------------

dim_product = products.copy()

dim_product["product_sk"] = range(1, len(dim_product) + 1)

dim_product = dim_product[
    [
        "product_sk",
        "product_id",
        "product_name",
        "category",
        "price"
    ]
]


# --------------------------------------------------
# 5. Dimension: Store
# --------------------------------------------------

dim_store = stores.copy()

dim_store["store_sk"] = range(1, len(dim_store) + 1)

dim_store = dim_store[
    [
        "store_sk",
        "store_id",
        "store_name",
        "city",
        "state"
    ]
]


# --------------------------------------------------
# 6. Fact: Sales
# --------------------------------------------------

fact_sales = sales.copy()

# Join customer surrogate key
fact_sales = fact_sales.merge(
    dim_customer[["customer_id", "customer_sk"]],
    on="customer_id",
    how="left"
)

# Join product surrogate key
fact_sales = fact_sales.merge(
    dim_product[["product_id", "product_sk"]],
    on="product_id",
    how="left"
)

# Join store surrogate key
fact_sales = fact_sales.merge(
    dim_store[["store_id", "store_sk"]],
    on="store_id",
    how="left"
)

# Calculate total sales amount
fact_sales["sales_amount"] = (
    fact_sales["quantity"] * fact_sales["unit_price"]
)

fact_sales = fact_sales[
    [
        "sale_id",
        "sale_date",
        "customer_sk",
        "product_sk",
        "store_sk",
        "quantity",
        "unit_price",
        "sales_amount"
    ]
]


# --------------------------------------------------
# 7. Save warehouse tables
# --------------------------------------------------

dim_customer.to_csv(
    f"{warehouse}/dim_customer.csv",
    index=False
)

dim_product.to_csv(
    f"{warehouse}/dim_product.csv",
    index=False
)

dim_store.to_csv(
    f"{warehouse}/dim_store.csv",
    index=False
)

fact_sales.to_csv(
    f"{warehouse}/fact_sales.csv",
    index=False
)


print("Warehouse tables created successfully!")
print(f"Customers: {len(dim_customer)}")
print(f"Products: {len(dim_product)}")
print(f"Stores: {len(dim_store)}")
print(f"Sales: {len(fact_sales)}")