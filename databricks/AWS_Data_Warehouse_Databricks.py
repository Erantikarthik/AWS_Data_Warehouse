# Databricks notebook source
# ============================================
# AWS DATA WAREHOUSE - DATABRICKS PROJECT
# Step 1: Create Bronze, Silver and Gold schemas
# ============================================

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

print("Bronze, Silver and Gold schemas created successfully!")

# COMMAND ----------

# ============================================
# STEP 2: BRONZE LAYER
# Copy source tables into Bronze
# ============================================

source_tables = ["customers", "products", "sales", "stores"]

for table_name in source_tables:
    spark.sql(f"""
        CREATE OR REPLACE TABLE workspace.bronze.{table_name}
        AS
        SELECT *
        FROM workspace.default.{table_name}
    """)

print("Bronze layer created successfully!")

for table_name in source_tables:
    count = spark.table(f"workspace.bronze.{table_name}").count()
    print(f"{table_name}: {count} records")

# COMMAND ----------

# ============================================
# STEP 3: SILVER LAYER - CORRECTED
# ============================================

from pyspark.sql.functions import col, trim, to_date

# -----------------------------
# Customers
# -----------------------------
customers_silver = (
    spark.table("workspace.bronze.customers")
    .dropDuplicates(["customer_id"])
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("signup_date", to_date(col("signup_date")))
    .dropna(subset=["customer_id"])
)

customers_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.customers"
)


# -----------------------------
# Products
# -----------------------------
products_silver = (
    spark.table("workspace.bronze.products")
    .dropDuplicates(["product_id"])
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("price", col("price").cast("double"))
    .dropna(subset=["product_id"])
)

products_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.products"
)


# -----------------------------
# Stores
# -----------------------------
stores_silver = (
    spark.table("workspace.bronze.stores")
    .dropDuplicates(["store_id"])
    .withColumn("store_id", trim(col("store_id")))
    .withColumn("store_name", trim(col("store_name")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .dropna(subset=["store_id"])
)

stores_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.stores"
)


# -----------------------------
# Sales
# -----------------------------
sales_silver = (
    spark.table("workspace.bronze.sales")
    .dropDuplicates(["sale_id"])
    .withColumn("sale_id", col("sale_id").cast("long"))
    .withColumn("sale_date", to_date(col("sale_date")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("store_id", trim(col("store_id")))
    .withColumn("quantity", col("quantity").cast("long"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn(
        "sales_amount",
        col("quantity") * col("unit_price")
    )
    .dropna(
        subset=["sale_id", "customer_id", "product_id", "store_id"]
    )
)

sales_silver.write.mode("overwrite").saveAsTable(
    "workspace.silver.sales"
)


# -----------------------------
# Validation
# -----------------------------
print("Silver layer created successfully!")

print("Customers:", spark.table("workspace.silver.customers").count())
print("Products:", spark.table("workspace.silver.products").count())
print("Stores:", spark.table("workspace.silver.stores").count())
print("Sales:", spark.table("workspace.silver.sales").count())

# COMMAND ----------

# ============================================
# STEP 4: GOLD LAYER
# Create Dimension and Fact Tables
# ============================================

from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window

# ============================================
# 1. DIM CUSTOMER
# ============================================

customers = spark.table("workspace.silver.customers")

customer_window = Window.orderBy("customer_id")

dim_customer = (
    customers
    .withColumn(
        "customer_sk",
        row_number().over(customer_window)
    )
    .select(
        "customer_sk",
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "signup_date"
    )
    .withColumnRenamed("signup_date", "effective_date")
    .withColumn(
        "expiry_date",
        lit("9999-12-31").cast("date")
    )
    .withColumn(
        "is_current",
        lit(True)
    )
)

dim_customer.write.mode("overwrite").saveAsTable(
    "workspace.gold.dim_customer"
)


# ============================================
# 2. DIM PRODUCT
# ============================================

products = spark.table("workspace.silver.products")

product_window = Window.orderBy("product_id")

dim_product = (
    products
    .withColumn(
        "product_sk",
        row_number().over(product_window)
    )
    .select(
        "product_sk",
        "product_id",
        "product_name",
        "category",
        "price"
    )
)

dim_product.write.mode("overwrite").saveAsTable(
    "workspace.gold.dim_product"
)


# ============================================
# 3. DIM STORE
# ============================================

stores = spark.table("workspace.silver.stores")

store_window = Window.orderBy("store_id")

dim_store = (
    stores
    .withColumn(
        "store_sk",
        row_number().over(store_window)
    )
    .select(
        "store_sk",
        "store_id",
        "store_name",
        "city",
        "state"
    )
)

dim_store.write.mode("overwrite").saveAsTable(
    "workspace.gold.dim_store"
)


# ============================================
# 4. FACT SALES
# ============================================

sales = spark.table("workspace.silver.sales")

customer_dim = spark.table("workspace.gold.dim_customer")
product_dim = spark.table("workspace.gold.dim_product")
store_dim = spark.table("workspace.gold.dim_store")

fact_sales = (
    sales.alias("s")

    .join(
        customer_dim.alias("c"),
        col("s.customer_id") == col("c.customer_id"),
        "left"
    )

    .join(
        product_dim.alias("p"),
        col("s.product_id") == col("p.product_id"),
        "left"
    )

    .join(
        store_dim.alias("st"),
        col("s.store_id") == col("st.store_id"),
        "left"
    )

    .select(
        col("s.sale_id"),
        col("s.sale_date"),
        col("c.customer_sk"),
        col("p.product_sk"),
        col("st.store_sk"),
        col("s.quantity"),
        col("s.unit_price"),
        col("s.sales_amount")
    )
)

fact_sales.write.mode("overwrite").saveAsTable(
    "workspace.gold.fact_sales"
)


# ============================================
# VALIDATION
# ============================================

print("============================================")
print("GOLD LAYER CREATED SUCCESSFULLY!")
print("============================================")

print(
    "dim_customer:",
    spark.table("workspace.gold.dim_customer").count()
)

print(
    "dim_product:",
    spark.table("workspace.gold.dim_product").count()
)

print(
    "dim_store:",
    spark.table("workspace.gold.dim_store").count()
)

print(
    "fact_sales:",
    spark.table("workspace.gold.fact_sales").count()
)

# COMMAND ----------

# ============================================
# STEP 5: SCD TYPE 2
# Customer C001: Mysore -> Bangalore
# ============================================

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_date, max as spark_max

table_name = "workspace.gold.dim_customer"

# Check whether the SCD2 change has already been applied
already_updated = (
    spark.table(table_name)
    .filter(
        (col("customer_id") == "C001") &
        (col("city") == "Bangalore") &
        (col("is_current") == True)
    )
    .count()
)

if already_updated == 0:

    # Load Delta table
    dim_customer_delta = DeltaTable.forName(spark, table_name)

    # Expire the current record
    dim_customer_delta.update(
        condition="customer_id = 'C001' AND is_current = true",
        set={
            "expiry_date": "current_date()",
            "is_current": "false"
        }
    )

    # Get the expired customer record
    old_record = (
        spark.table(table_name)
        .filter(col("customer_id") == "C001")
        .orderBy(col("customer_sk").desc())
        .limit(1)
    )

    # Get next surrogate key
    max_sk = (
        spark.table(table_name)
        .agg(spark_max("customer_sk"))
        .collect()[0][0]
    )

    # Create new customer version
    new_record = (
        old_record
        .withColumn("customer_sk", lit(max_sk + 1))
        .withColumn("city", lit("Bangalore"))
        .withColumn("state", lit("Karnataka"))
        .withColumn("effective_date", current_date())
        .withColumn(
            "expiry_date",
            lit("9999-12-31").cast("date")
        )
        .withColumn("is_current", lit(True))
    )

    # Append new version
    new_record.write.mode("append").saveAsTable(table_name)

    print("SCD Type 2 update completed successfully!")

else:
    print("SCD Type 2 update already exists. No duplicate version created.")


# ============================================
# VALIDATION
# ============================================

print("\nCustomer C001 history:")

spark.sql("""
SELECT
    customer_sk,
    customer_id,
    customer_name,
    city,
    state,
    effective_date,
    expiry_date,
    is_current
FROM workspace.gold.dim_customer
WHERE customer_id = 'C001'
ORDER BY customer_sk
""").show(truncate=False)

# COMMAND ----------

# ============================================
# STEP 6: GOLD ANALYTICS & BUSINESS KPIs
# ============================================

# Create a reusable sales summary view
spark.sql("""
CREATE OR REPLACE VIEW workspace.gold.v_sales_summary AS
SELECT
    f.sale_id,
    f.sale_date,
    c.customer_id,
    c.customer_name,
    c.city AS customer_city,
    c.state AS customer_state,
    p.product_id,
    p.product_name,
    p.category,
    st.store_id,
    st.store_name,
    st.city AS store_city,
    st.state AS store_state,
    f.quantity,
    f.unit_price,
    f.sales_amount
FROM workspace.gold.fact_sales f
LEFT JOIN workspace.gold.dim_customer c
    ON f.customer_sk = c.customer_sk
LEFT JOIN workspace.gold.dim_product p
    ON f.product_sk = p.product_sk
LEFT JOIN workspace.gold.dim_store st
    ON f.store_sk = st.store_sk
""")


# ============================================
# KPI 1 - Overall Business Metrics
# ============================================

print("========== OVERALL KPIs ==========")

spark.sql("""
SELECT
    COUNT(*) AS total_transactions,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(sales_amount), 2) AS total_sales,
    ROUND(AVG(sales_amount), 2) AS average_transaction_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM workspace.gold.v_sales_summary
""").show()


# ============================================
# KPI 2 - Sales by Product Category
# ============================================

print("========== SALES BY CATEGORY ==========")

spark.sql("""
SELECT
    category,
    SUM(quantity) AS units_sold,
    ROUND(SUM(sales_amount), 2) AS total_sales
FROM workspace.gold.v_sales_summary
GROUP BY category
ORDER BY total_sales DESC
""").show()


# ============================================
# KPI 3 - Sales by State
# ============================================

print("========== SALES BY STATE ==========")

spark.sql("""
SELECT
    store_state AS state,
    ROUND(SUM(sales_amount), 2) AS total_sales
FROM workspace.gold.v_sales_summary
GROUP BY store_state
ORDER BY total_sales DESC
""").show()


# ============================================
# KPI 4 - Top 10 Customers
# ============================================

print("========== TOP 10 CUSTOMERS ==========")

spark.sql("""
SELECT
    customer_id,
    customer_name,
    ROUND(SUM(sales_amount), 2) AS total_sales
FROM workspace.gold.v_sales_summary
GROUP BY customer_id, customer_name
ORDER BY total_sales DESC
LIMIT 10
""").show(truncate=False)


print("Analytics and KPI layer created successfully!")

# COMMAND ----------

# ============================================
# FINAL PROJECT VALIDATION
# ============================================

print("============================================")
print("AWS DATA WAREHOUSE - FINAL VALIDATION")
print("============================================")

tables = [
    "workspace.bronze.customers",
    "workspace.bronze.products",
    "workspace.bronze.sales",
    "workspace.bronze.stores",
    "workspace.silver.customers",
    "workspace.silver.products",
    "workspace.silver.sales",
    "workspace.silver.stores",
    "workspace.gold.dim_customer",
    "workspace.gold.dim_product",
    "workspace.gold.dim_store",
    "workspace.gold.fact_sales"
]

for table in tables:
    print(f"{table} -> {spark.table(table).count()} rows")

print("============================================")
print("PROJECT VALIDATION COMPLETED SUCCESSFULLY!")
print("============================================")