# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Cleansed & Conformed Data
# MAGIC **Retail Lakehouse | Bronze → Silver → Gold**
# MAGIC
# MAGIC This notebook reads from Bronze Delta tables, applies:
# MAGIC - Null / duplicate removal
# MAGIC - Type casting & standardisation
# MAGIC - Derived columns (revenue, gross_profit, day_of_week, etc.)
# MAGIC - SCD-Type-1 upserts into Silver Delta tables

# COMMAND ----------

# MAGIC %md ## 1. Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = "retail_catalog"
BRONZE  = "bronze"
SILVER  = "silver"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SILVER}")

SILVER_PATH = "dbfs:/mnt/retail/silver"
print("[silver] Ready.")

# COMMAND ----------

# MAGIC %md ## 2. Sales — Cleanse & Enrich

# COMMAND ----------

# Read bronze
raw_sales = spark.table(f"{CATALOG}.{BRONZE}.raw_sales")

silver_sales = (
    raw_sales
    # ── Drop dupes & nulls on key columns ─────────────────────────────────────
    .dropDuplicates(["transaction_id"])
    .dropna(subset=["transaction_id", "product_id", "transaction_date"])

    # ── Revenue & margin columns ──────────────────────────────────────────────
    .withColumn("revenue",
        F.round(
            F.col("quantity") * F.col("unit_price") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0))),
            2
        )
    )
    .withColumn("gross_revenue",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    .withColumn("discount_amount",
        F.round(F.col("gross_revenue") - F.col("revenue"), 2)
    )

    # ── Date dimensions ───────────────────────────────────────────────────────
    .withColumn("sale_date",        F.to_date("transaction_date"))
    .withColumn("sale_year",        F.year("transaction_date"))
    .withColumn("sale_month",       F.month("transaction_date"))
    .withColumn("sale_week",        F.weekofyear("transaction_date"))
    .withColumn("day_of_week",      F.dayofweek("transaction_date"))
    .withColumn("day_name",         F.date_format("transaction_date", "EEEE"))
    .withColumn("is_weekend",       F.col("day_of_week").isin([1, 7]))

    # ── Normalise channel ─────────────────────────────────────────────────────
    .withColumn("channel", F.upper(F.trim(F.col("channel"))))

    # ── Metadata ──────────────────────────────────────────────────────────────
    .withColumn("_processed_at", F.current_timestamp())

    # ── Select final columns ──────────────────────────────────────────────────
    .select(
        "transaction_id", "order_id", "customer_id", "product_id", "store_id",
        "transaction_date", "sale_date", "sale_year", "sale_month", "sale_week",
        "day_of_week", "day_name", "is_weekend",
        "quantity", "unit_price", "discount_pct", "discount_amount",
        "gross_revenue", "revenue", "payment_method", "channel",
        "_processed_at"
    )
)

print(f"[silver] Sales records: {silver_sales.count():,}")
silver_sales.printSchema()

# COMMAND ----------

# MAGIC %md ## 3. Upsert Sales into Silver Delta

# COMMAND ----------

SILVER_SALES_PATH = f"{SILVER_PATH}/sales"
SILVER_SALES_TABLE = f"{CATALOG}.{SILVER}.sales"

silver_sales.write.format("delta").mode("overwrite").save(SILVER_SALES_PATH)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_SALES_TABLE}
    USING DELTA LOCATION '{SILVER_SALES_PATH}'
""")
print(f"[silver] ✅ Sales written to {SILVER_SALES_TABLE}")

# COMMAND ----------

# MAGIC %md ## 4. Orders — Cleanse & Enrich

# COMMAND ----------

raw_orders = spark.table(f"{CATALOG}.{BRONZE}.raw_orders")

silver_orders = (
    raw_orders
    .dropDuplicates(["order_id"])
    .dropna(subset=["order_id", "customer_id", "order_date"])
    .withColumn("order_date",    F.to_date("order_date"))
    .withColumn("ship_date",     F.to_date("ship_date"))
    .withColumn("days_to_ship",
        F.datediff(F.col("ship_date"), F.col("order_date"))
    )
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("order_year",   F.year("order_date"))
    .withColumn("order_month",  F.month("order_date"))
    .withColumn("order_week",   F.weekofyear("order_date"))
    .withColumn("_processed_at", F.current_timestamp())
)

SILVER_ORDERS_PATH  = f"{SILVER_PATH}/orders"
SILVER_ORDERS_TABLE = f"{CATALOG}.{SILVER}.orders"

silver_orders.write.format("delta").mode("overwrite").save(SILVER_ORDERS_PATH)
spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_ORDERS_TABLE} USING DELTA LOCATION '{SILVER_ORDERS_PATH}'")
print(f"[silver] ✅ Orders written → {SILVER_ORDERS_TABLE}  ({silver_orders.count():,} rows)")

# COMMAND ----------

# MAGIC %md ## 5. Inventory — Cleanse

# COMMAND ----------

raw_inv = spark.table(f"{CATALOG}.{BRONZE}.raw_inventory")

silver_inventory = (
    raw_inv
    .dropDuplicates(["product_id", "store_id", "snapshot_date"])
    .dropna(subset=["product_id", "snapshot_date"])
    .withColumn("is_low_stock",
        F.col("stock_on_hand") <= F.col("reorder_point")
    )
    .withColumn("days_of_supply",
        F.when(F.col("units_sold_7d") > 0,
            F.round(F.col("stock_on_hand") / (F.col("units_sold_7d") / 7), 1)
        ).otherwise(F.lit(None))
    )
    .withColumn("_processed_at", F.current_timestamp())
)

SILVER_INV_PATH  = f"{SILVER_PATH}/inventory"
SILVER_INV_TABLE = f"{CATALOG}.{SILVER}.inventory"

silver_inventory.write.format("delta").mode("overwrite").save(SILVER_INV_PATH)
spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_INV_TABLE} USING DELTA LOCATION '{SILVER_INV_PATH}'")
print(f"[silver] ✅ Inventory written → {SILVER_INV_TABLE}  ({silver_inventory.count():,} rows)")

# COMMAND ----------

# MAGIC %md ## 6. Data Quality Report

# COMMAND ----------

for tbl, key in [("sales","transaction_id"), ("orders","order_id"), ("inventory","product_id")]:
    spark.sql(f"""
        SELECT
            '{tbl}'                           AS table_name,
            COUNT(*)                          AS total_rows,
            COUNT(DISTINCT {key})             AS distinct_keys,
            SUM(CASE WHEN {key} IS NULL THEN 1 ELSE 0 END) AS null_keys,
            MAX(_processed_at)                AS last_processed
        FROM {CATALOG}.{SILVER}.{tbl}
    """).show(truncate=False)
