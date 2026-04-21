# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold — Weekly Trends
# MAGIC **Retail Lakehouse | Gold Layer**
# MAGIC
# MAGIC Computes week-over-week (WoW) revenue trends, channel performance,
# MAGIC inventory health per week, and leading demand indicators.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG   = "retail_catalog"
GOLD_PATH = "dbfs:/mnt/retail/gold"
TABLE_NAME = f"{CATALOG}.gold.weekly_trends"

# COMMAND ----------

# MAGIC %md ## 1. Weekly Sales Aggregation

# COMMAND ----------

sales = spark.table(f"{CATALOG}.silver.sales")

weekly = (
    sales
    .groupBy("sale_year", "sale_week", "channel")
    .agg(
        F.min("sale_date").alias("week_start_date"),
        F.max("sale_date").alias("week_end_date"),
        F.sum("revenue").alias("net_revenue"),
        F.sum("gross_revenue").alias("gross_revenue"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("transaction_id").alias("num_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.avg("revenue"), 2).alias("avg_order_value"),
        F.round(F.sum("discount_amount"), 2).alias("total_discounts"),
    )
    .withColumn("net_revenue",   F.round("net_revenue", 2))
    .withColumn("gross_revenue", F.round("gross_revenue", 2))
)

# COMMAND ----------

# MAGIC %md ## 2. Week-over-Week Calculations

# COMMAND ----------

w_channel = Window.partitionBy("channel").orderBy("sale_year", "sale_week")

weekly_trends = (
    weekly
    # Previous week revenue
    .withColumn("prev_week_revenue",  F.lag("net_revenue",    1).over(w_channel))
    .withColumn("prev_week_units",    F.lag("units_sold",     1).over(w_channel))
    .withColumn("prev_week_customers",F.lag("unique_customers",1).over(w_channel))

    # WoW % changes
    .withColumn("wow_revenue_pct",
        F.when(F.col("prev_week_revenue") > 0,
            F.round((F.col("net_revenue") - F.col("prev_week_revenue")) / F.col("prev_week_revenue") * 100, 2)
        )
    )
    .withColumn("wow_units_pct",
        F.when(F.col("prev_week_units") > 0,
            F.round((F.col("units_sold") - F.col("prev_week_units")) / F.col("prev_week_units") * 100, 2)
        )
    )
    .withColumn("wow_customers_pct",
        F.when(F.col("prev_week_customers") > 0,
            F.round((F.col("unique_customers") - F.col("prev_week_customers")) / F.col("prev_week_customers") * 100, 2)
        )
    )

    # 4-week rolling average
    .withColumn("rolling_4w_revenue",
        F.round(F.avg("net_revenue").over(w_channel.rowsBetween(-3, 0)), 2)
    )

    # Trend signal
    .withColumn("trend_signal",
        F.when(F.col("wow_revenue_pct") > 5,  F.lit("ACCELERATING"))
         .when(F.col("wow_revenue_pct") > 0,  F.lit("GROWING"))
         .when(F.col("wow_revenue_pct") == 0, F.lit("FLAT"))
         .when(F.col("wow_revenue_pct") < -5, F.lit("DECLINING"))
         .otherwise(F.lit("SLOWING"))
    )

    .withColumn("_refreshed_at", F.current_timestamp())
    .orderBy("sale_year", "sale_week", "channel")
)

print(f"[gold] Weekly trends rows: {weekly_trends.count():,}")

# COMMAND ----------

# MAGIC %md ## 3. Write Gold Table

# COMMAND ----------

TABLE_PATH = f"{GOLD_PATH}/weekly_trends"

(weekly_trends.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sale_year")
    .save(TABLE_PATH))

spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} USING DELTA LOCATION '{TABLE_PATH}'")
print(f"[gold] ✅ {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md ## 4. Latest 8-Week Trend by Channel

# COMMAND ----------

spark.sql(f"""
    SELECT
        sale_year,
        sale_week,
        week_start_date,
        channel,
        net_revenue,
        units_sold,
        unique_customers,
        wow_revenue_pct,
        rolling_4w_revenue,
        trend_signal
    FROM {TABLE_NAME}
    ORDER BY sale_year DESC, sale_week DESC, channel
    LIMIT 32
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 5. Channel Revenue Share by Week

# COMMAND ----------

spark.sql(f"""
    WITH weekly_total AS (
        SELECT sale_year, sale_week,
               SUM(net_revenue) AS total_weekly_revenue
        FROM {TABLE_NAME}
        GROUP BY sale_year, sale_week
    )
    SELECT
        t.sale_year,
        t.sale_week,
        t.channel,
        t.net_revenue,
        ROUND(t.net_revenue / wt.total_weekly_revenue * 100, 2) AS channel_share_pct
    FROM {TABLE_NAME} t
    JOIN weekly_total wt USING (sale_year, sale_week)
    ORDER BY t.sale_year DESC, t.sale_week DESC, channel_share_pct DESC
    LIMIT 30
""").show(truncate=False)
