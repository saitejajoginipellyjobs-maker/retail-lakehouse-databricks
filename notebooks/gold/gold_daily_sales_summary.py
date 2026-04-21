# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold — Daily Sales Summary
# MAGIC **Retail Lakehouse | Gold Layer**
# MAGIC
# MAGIC Aggregates Silver sales data into a daily summary consumed by BI dashboards.
# MAGIC Metrics: revenue, orders, units sold, AOV, discount rate, channel mix.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "retail_catalog"
SILVER  = "silver"
GOLD    = "gold"
GOLD_PATH = "dbfs:/mnt/retail/gold"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{GOLD}")

# COMMAND ----------

# MAGIC %md ## Build Daily Sales Aggregate

# COMMAND ----------

silver_sales = spark.table(f"{CATALOG}.{SILVER}.sales")

daily_sales = (
    silver_sales
    .groupBy("sale_date", "sale_year", "sale_month", "sale_week", "day_name", "is_weekend", "channel", "store_id")
    .agg(
        F.countDistinct("transaction_id").alias("num_transactions"),
        F.countDistinct("order_id").alias("num_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("gross_revenue"),    2).alias("gross_revenue"),
        F.round(F.sum("discount_amount"),  2).alias("total_discounts"),
        F.round(F.sum("revenue"),          2).alias("net_revenue"),
        F.round(F.avg("revenue"),          2).alias("avg_order_value"),
        F.round(F.avg("discount_pct"),     4).alias("avg_discount_pct"),
    )
    .withColumn("discount_rate_pct",
        F.round(F.col("total_discounts") / F.col("gross_revenue") * 100, 2)
    )
    .withColumn("_refreshed_at", F.current_timestamp())
    .orderBy("sale_date", "channel")
)

print(f"[gold] Daily sales rows: {daily_sales.count():,}")
daily_sales.printSchema()

# COMMAND ----------

# MAGIC %md ## Write to Gold Delta

# COMMAND ----------

TABLE_PATH  = f"{GOLD_PATH}/daily_sales_summary"
TABLE_NAME  = f"{CATALOG}.{GOLD}.daily_sales_summary"

(daily_sales.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("sale_year", "sale_month")
    .option("overwriteSchema", "true")
    .save(TABLE_PATH))

spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} USING DELTA LOCATION '{TABLE_PATH}'")
print(f"[gold] ✅ {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md ## Preview KPIs

# COMMAND ----------

spark.sql(f"""
    SELECT
        sale_date,
        channel,
        num_transactions,
        total_units_sold,
        gross_revenue,
        total_discounts,
        net_revenue,
        avg_order_value,
        ROUND(discount_rate_pct, 1) AS discount_rate_pct
    FROM {TABLE_NAME}
    ORDER BY sale_date DESC, net_revenue DESC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 7-Day Rolling Revenue Trend

# COMMAND ----------

from pyspark.sql.window import Window

w7 = Window.partitionBy("channel").orderBy("sale_date").rowsBetween(-6, 0)

spark.sql(f"SELECT sale_date, channel, net_revenue FROM {TABLE_NAME}") \
    .withColumn("rolling_7d_revenue", F.round(F.sum("net_revenue").over(w7), 2)) \
    .orderBy("sale_date", "channel") \
    .show(30, truncate=False)
