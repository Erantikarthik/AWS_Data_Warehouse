import pandas as pd
from datetime import datetime

# Read the current customer dimension
dim_customer = pd.read_csv(
    "data/warehouse/dim_customer.csv"
)

# Select one existing customer for our SCD Type 2 demonstration
customer_id = "C001"

# Find the current record
current_record = dim_customer[
    (dim_customer["customer_id"] == customer_id) &
    (dim_customer["is_current"] == True)
]

if not current_record.empty:

    # New customer information
    new_city = "Bangalore"
    new_state = "Karnataka"

    old_city = current_record.iloc[0]["city"]
    old_state = current_record.iloc[0]["state"]

    # Check whether customer information changed
    if old_city != new_city or old_state != new_state:

        today = datetime.now().strftime("%Y-%m-%d")

        # ------------------------------------------
        # Step 1: Expire the old record
        # ------------------------------------------

        dim_customer.loc[
            (dim_customer["customer_id"] == customer_id) &
            (dim_customer["is_current"] == True),
            "expiry_date"
        ] = today

        dim_customer.loc[
            (dim_customer["customer_id"] == customer_id) &
            (dim_customer["is_current"] == True),
            "is_current"
        ] = False

        # ------------------------------------------
        # Step 2: Create a new record
        # ------------------------------------------

        new_customer_sk = dim_customer["customer_sk"].max() + 1

        new_record = current_record.iloc[0].copy()

        new_record["customer_sk"] = new_customer_sk
        new_record["city"] = new_city
        new_record["state"] = new_state
        new_record["effective_date"] = today
        new_record["expiry_date"] = "9999-12-31"
        new_record["is_current"] = True

        # Add new version
        dim_customer = pd.concat(
            [
                dim_customer,
                pd.DataFrame([new_record])
            ],
            ignore_index=True
        )

        # Save updated dimension
        dim_customer.to_csv(
            "data/warehouse/dim_customer.csv",
            index=False
        )

        print("SCD Type 2 update completed successfully!")
        print()
        print("Old City:", old_city)
        print("New City:", new_city)
        print("New Customer Surrogate Key:", new_customer_sk)

    else:
        print("No customer information changed.")

else:
    print("Customer C001 was not found.")