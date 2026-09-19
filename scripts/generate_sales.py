import pandas as pd
import random
from datetime import datetime, timedelta

# Read existing datasets
customers = pd.read_csv("data/raw/customers.csv")
products = pd.read_csv("data/raw/products.csv")
stores = pd.read_csv("data/raw/stores.csv")

# Create 500 sales transactions
sales = []

start_date = datetime(2026, 1, 1)

for i in range(1, 501):

    customer = random.choice(customers["customer_id"].tolist())
    product = random.choice(products["product_id"].tolist())
    store = random.choice(stores["store_id"].tolist())

    quantity = random.randint(1, 5)

    product_price = products.loc[
        products["product_id"] == product, "price"
    ].iloc[0]

    sale_date = start_date + timedelta(
        days=random.randint(0, 250)
    )

    sales.append([
        i,
        sale_date.strftime("%Y-%m-%d"),
        customer,
        product,
        store,
        quantity,
        product_price
    ])

# Create DataFrame
sales_df = pd.DataFrame(
    sales,
    columns=[
        "sale_id",
        "sale_date",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "unit_price"
    ]
)

# Save to sales.csv
sales_df.to_csv(
    "data/raw/sales.csv",
    index=False
)

print("Successfully generated 500 sales records!")