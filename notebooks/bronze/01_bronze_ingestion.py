# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Raw Data Ingestion
# MAGIC **Retail Lakehouse | Bronze → Silver → Gold**
# MAGIC
# MAGIC This notebook ingests raw retail data (sales transactions, orders, inventory)
# MAGIC into the Bronze Delta tables with minimal transformation (schema enforcement only).

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType, LongType
)
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from delta.tables import DeltaTable
import logging

spark = SparkSession.builder.getOrCreate()
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
RAW_BASE_PATH    = "dbfs:/mnt/retail/raw"
BRONZE_BASE_PATH = "dbfs:/mnt/retail/bronze"

CATALOG  = "retail_catalog"
DATABASE = "bronze"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")

print(f"[bronze] Raw path   : {RAW_BASE_PATH}")
print(f"[bronze] Bronze path: {BRONZE_BASE_PATH}")

# COMMAND ----------

# MAGIC %md ## 2. Schema Definitions

# COMMAND ----------

# Sales transactions schema
sales_schema = StructType([
    StructField("transaction_id",   StringType(),    False),
    StructField("order_id",         StringType(),    False),
    StructField("customer_id",      StringType(),    True),
    StructField("product_id",       StringType(),    False),
    StructField("store_id",         StringType(),    True),
    StructField("transaction_date", TimestampType(), True),
    StructField("quantity",         IntegerType(),   True),
    StructField("unit_price",       DoubleType(),    True),
    StructField("discount_pct",     DoubleType(),    True),
    StructField("payment_method",   StringType(),    True),
    StructField("channel",          StringType(),    True),  # online / in-store
])

# Orders schema
orders_schema = StructType([
    StructField("order_id",         StringType(),    False),
    StructField("customer_id",      StringType(),    True),
    StructField("order_date",       TimestampType(), True),
    StructField("ship_date",        TimestampType(), True),
    StructField("status",           StringType(),    True),
    StructField("total_amount",     DoubleType(),    True),
    StructField("region",           StringType(),    True),
    StructField("store_id",         StringType(),    True),
])

# Inventory schema
inventory_schema = StructType([
    StructField("product_id",       StringType(),    False),
    StructField("store_id",         StringType(),    True),
    StructField("snapshot_date",    DateType(),      True),
    StructField("stock_on_hand",    IntegerType(),   True),
    StructField("reorder_point",    IntegerType(),   True),
    StructField("units_sold_7d",    IntegerType(),   True),
    StructField("supplier_id",      StringType(),    True),
])

print("[bronze] Schemas defined.")

# COMMAND ----------

# MAGIC %md ## 3. Ingestion Functions

# COMMAND ----------

def ingest_to_bronze(
    source_path: str,
    target_table: str,
    schema: StructType,
    format: str = "csv",
    partition_cols: list = None
) -> int:
    """
    Reads raw files from source_path and upserts into a Bronze Delta table.
    Adds ingestion metadata columns: _ingested_at, _source_file.
    Returns number of new records written.
    """
    target_full = f"{CATALOG}.{DATABASE}.{target_table}"
    target_path  = f"{BRONZE_BASE_PATH}/{target_table}"

    read_opts = {"header": "true", "inferSchema": "false"} if format == "csv" else {}
    df = (
        spark.read
             .format(format)
             .options(**read_opts)
             .schema(schema)
             .load(source_path)
             .withColumn("_ingested_at",  current_timestamp())
             .withColumn("_source_file",  input_file_name())
    )

    count = df.count()
    if count == 0:
        logger.warning(f"[bronze] No records found at {source_path}. Skipping.")
        return 0

    # Write as Delta (append — idempotent via MERGE if needed)
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .partitionBy(*(partition_cols or []))
       .save(target_path))

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_full}
        USING DELTA LOCATION '{target_path}'
    """)

    logger.info(f"[bronze] ✅ {count:,} records → {target_full}")
    return count

# COMMAND ----------

# MAGIC %md ## 4. Run Ingestion

# COMMAND ----------

results = {}

# 4a. Sales transactions
results["sales"] = ingest_to_bronze(
    source_path   = f"{RAW_BASE_PATH}/sales/",
    target_table  = "raw_sales",
    schema        = sales_schema,
    format        = "csv",
    partition_cols = ["channel"]
)

# 4b. Orders
results["orders"] = ingest_to_bronze(
    source_path   = f"{RAW_BASE_PATH}/orders/",
    target_table  = "raw_orders",
    schema        = orders_schema,
    format        = "csv",
    partition_cols = ["region"]
)

# 4c. Inventory snapshots
results["inventory"] = ingest_to_bronze(
    source_path   = f"{RAW_BASE_PATH}/inventory/",
    target_table  = "raw_inventory",
    schema        = inventory_schema,
    format        = "csv",
    partition_cols = ["snapshot_date"]
)

print("\n[bronze] Ingestion Summary:")
for k, v in results.items():
    print(f"  {k:12s}: {v:>10,} records")

# COMMAND ----------

# MAGIC %md ## 5. Data Quality — Row Counts & Freshness

# COMMAND ----------

for tbl in ["raw_sales", "raw_orders", "raw_inventory"]:
    spark.sql(f"""
        SELECT
            '{tbl}'           AS table_name,
            COUNT(*)          AS total_rows,
            MAX(_ingested_at) AS last_ingested_at
        FROM {CATALOG}.{DATABASE}.{tbl}
    """).show(truncate=False)
