# 🏪 Retail Lakehouse on Databricks

> **Medallion Architecture** · Delta Lake · PySpark · GitHub Pages Dashboard

[![Dashboard](https://img.shields.io/badge/Live%20Dashboard-View%20Now-6366f1?style=for-the-badge&logo=github)](https://saitejajoginipellyjobs-maker.github.io/retail-lakehouse-databricks/)
[![Databricks](https://img.shields.io/badge/Databricks-Workspace-FF3621?style=for-the-badge&logo=databricks)](https://dbc-49776dda-bee7.cloud.databricks.com/browse/folders/1395523958705727?o=7474646922785403)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Enabled-00ADD8?style=for-the-badge)](https://delta.io/)

---

## 📐 Architecture

```
Raw Sources  ──►  🥉 Bronze  ──►  🥈 Silver  ──►  🥇 Gold  ──►  Dashboard
(CSV / JSON)       (Ingest)       (Cleanse)      (Aggregate)    (GitHub Pages)
```

| Layer | Path | Tables | Description |
|-------|------|--------|-------------|
| 🥉 Bronze | `dbfs:/mnt/retail/bronze/` | raw_sales, raw_orders, raw_inventory | Schema-enforced raw ingestion, full audit trail |
| 🥈 Silver | `dbfs:/mnt/retail/silver/` | sales, orders, inventory | Deduped, type-cast, enriched with revenue & date dims |
| 🥇 Gold | `dbfs:/mnt/retail/gold/` | daily_sales_summary, product_performance, weekly_trends | Business aggregations for BI consumption |

---

## 📁 Repository Structure

```
retail-lakehouse-databricks/
│
├── notebooks/
│   ├── bronze/
│   │   └── 01_bronze_ingestion.py          # Raw ingestion (sales, orders, inventory)
│   ├── silver/
│   │   └── silver_transformation.py        # Cleanse, dedupe, enrich
│   └── gold/
│       ├── gold_daily_sales_summary.py     # Daily revenue & channel KPIs
│       ├── gold_product_performance.py     # Product rankings & stock health
│       └── gold_weekly_trends.py           # WoW trends & signals
│
├── dashboard/
│   └── index.html                          # Live GitHub Pages dashboard
│
├── docs/
│   ├── architecture.md                     # Detailed architecture guide
│   └── data_dictionary.md                  # Column definitions & lineage
│
└── .github/
    └── workflows/
        └── deploy-dashboard.yml            # Auto-deploy dashboard to GitHub Pages
```

---

## 🚀 Quick Start

### 1. Import Notebooks to Databricks

In your Databricks workspace, import notebooks in order:

```bash
# Bronze first — must run before Silver
01_bronze_ingestion.py

# Silver second — depends on Bronze tables
silver_transformation.py

# Gold — depends on Silver tables (run in any order)
gold_daily_sales_summary.py
gold_product_performance.py
gold_weekly_trends.py
```

### 2. Set Up Mount Points

```python
# Run once in Databricks to create DBFS mounts
dbutils.fs.mkdirs("dbfs:/mnt/retail/raw")
dbutils.fs.mkdirs("dbfs:/mnt/retail/bronze")
dbutils.fs.mkdirs("dbfs:/mnt/retail/silver")
dbutils.fs.mkdirs("dbfs:/mnt/retail/gold")
```

### 3. View the Live Dashboard

👉 **[https://saitejajoginipellyjobs-maker.github.io/retail-lakehouse-databricks/](https://saitejajoginipellyjobs-maker.github.io/retail-lakehouse-databricks/)**

---

## 📊 Gold Layer — KPIs Produced

### `daily_sales_summary`
| Metric | Description |
|--------|-------------|
| `net_revenue` | Revenue after discounts, by day and channel |
| `num_transactions` | Distinct transactions per day |
| `avg_order_value` | Average revenue per order |
| `discount_rate_pct` | Total discount as % of gross revenue |
| `total_units_sold` | Units sold per day |

### `product_performance`
| Metric | Description |
|--------|-------------|
| `revenue_rank` | Product ranking by total revenue |
| `total_revenue` | Lifetime revenue per SKU |
| `unique_customers` | Distinct customers who bought this SKU |
| `avg_days_of_supply` | Inventory days remaining across stores |
| `stores_low_stock` | Count of stores below reorder point |

### `weekly_trends`
| Metric | Description |
|--------|-------------|
| `wow_revenue_pct` | Week-over-week revenue % change |
| `rolling_4w_revenue` | 4-week rolling average revenue |
| `trend_signal` | ACCELERATING / GROWING / FLAT / SLOWING / DECLINING |

---

## 🛠️ Tech Stack

- **Compute**: Databricks Runtime 13.x+
- **Storage**: Delta Lake on DBFS / ADLS Gen2
- **Language**: PySpark (Python 3.10+)
- **Catalog**: Unity Catalog (`retail_catalog`)
- **Dashboard**: HTML + Chart.js (hosted on GitHub Pages)
- **CI/CD**: GitHub Actions

---

## 📈 Dashboard Features

- **Live KPI cards** — Revenue, Orders, Units, AOV, Discount Rate, Low-Stock SKUs
- **Daily Sales tab** — Revenue by channel, channel mix donut, units/transactions trend, AOV
- **Product Performance tab** — Top-10 bar chart, Pareto curve, low-stock alert table
- **Weekly Trends tab** — Multi-channel stacked bar, WoW % line, trend signal distribution
- **Architecture tab** — Medallion diagram, pipeline metrics, table row counts, storage

---

## 🔄 Refreshing the Dashboard

Export aggregated Gold data to JSON from Databricks and commit to the repo — GitHub Actions redeploys Pages automatically:

```python
# In Databricks, export Gold → JSON
(spark.table("retail_catalog.gold.daily_sales_summary")
     .toPandas()
     .to_json("/dbfs/mnt/retail/exports/daily_sales.json", orient="records"))
```

Then commit `dashboard/data/*.json` and the GitHub Action handles the rest.

---

*Built with ❤️ using Databricks, Delta Lake, and GitHub Pages.*
