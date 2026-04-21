# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold — Product Performance
# MAGIC **Retail Lakehouse | Gold Layer**
# MAGIC
# MAGIC Ranks products by revenue, units sold, and margin.
# MAGIC Combines Silver sales + inventory for sell-through & stock health KPIs.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG   = "retail_catalog"
GOLD_PATH = "dbfs:/mnt/retail/gold"
TABLE_NAME = f"{CATALOG}.gold.product_performance"

# COMMAND ----------

# MAGIC %md ## 1. Product Revenue & Units Aggregation

# COMMAND ----------

sales = spark.table(f"{CATALOG}.silver.sales")
inv   = spark.table(f"{CATALOG}.silver.inventory")

product_sales = (
    sales
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("gross_revenue").alias("total_gross_revenue"),
        F.sum("discount_amount").alias("total_discounts"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("transaction_id").alias("num_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("revenue").alias("avg_order_value"),
        F.min("sale_date").alias("first_sale_date"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .withColumn("total_revenue",       F.round("total_revenue", 2))
    .withColumn("total_gross_revenue", F.round("total_gross_revenue", 2))
    .withColumn("total_discounts",     F.round("total_discounts", 2))
    .withColumn("avg_order_value",     F.round("avg_order_value", 2))
    .withColumn("discount_rate_pct",
        F.round(F.col("total_discounts") / F.col("total_gross_revenue") * 100, 2)
    )
)

# COMMAND ----------

# MAGIC %md ## 2. Join Inventory for Stock Health

# COMMAND ----------

latest_inv = (
    inv
    .groupBy("product_id")
    .agg(
        F.sum("stock_on_hand").alias("total_stock_on_hand"),
        F.avg("days_of_supply").alias("avg_days_of_supply"),
        F.sum(F.col("is_low_stock").cast("int")).alias("stores_low_stock"),
        F.count("store_id").alias("stores_carrying"),
    )
    .withColumn("avg_days_of_supply", F.round("avg_days_of_supply", 1))
)

product_perf = product_sales.join(latest_inv, on="product_id", how="left")

# COMMAND ----------

# MAGIC %md ## 3. Rankings

# COMMAND ----------

w_rev  = Window.orderBy(F.desc("total_revenue"))
w_units = Window.orderBy(F.desc("total_units_sold"))

product_perf = (
    product_perf
    .withColumn("revenue_rank", F.rank().over(w_rev))
    .withColumn("units_rank",   F.rank().over(w_units))
    .withColumn("revenue_pct_of_total",
        F.round(
            F.col("total_revenue") / F.sum("total_revenue").over(Window.orderBy(F.lit(1)).rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing)) * 100, 2
        )
    )
    .withColumn("_refreshed_at", F.current_timestamp())
)

print(f"[gold] Product performance rows: {product_perf.count():,}")

# COMMAND ----------

# MAGIC %md ## 4. Write Gold Table

# COMMAND ----------

TABLE_PATH = f"{GOLD_PATH}/product_performance"

(product_perf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(TABLE_PATH))

spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} USING DELTA LOCATION '{TABLE_PATH}'")
print(f"[gold] ✅ {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md ## 5. Top 20 Products by Revenue

# COMMAND ----------

spark.sql(f"""
    SELECT
        product_id,
        revenue_rank,
        total_revenue,
        total_units_sold,
        unique_customers,
        discount_rate_pct,
        avg_days_of_supply,
        stores_low_stock,
        revenue_pct_of_total
    FROM {TABLE_NAME}
    ORDER BY revenue_rank ASC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 6. Low-Stock Products With High Revenue (Action Required)

# COMMAND ----------

spark.sql(f"""
    SELECT product_id, total_revenue, revenue_rank, total_stock_on_hand, avg_days_of_supply, stores_low_stock
    FROM {TABLE_NAME}
    WHERE stores_low_stock > 0
      AND revenue_rank <= 50
    ORDER BY revenue_rank
""").show(truncate=False)
