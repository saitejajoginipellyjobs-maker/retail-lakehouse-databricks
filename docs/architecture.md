# Architecture Guide — Retail Lakehouse

## Overview

This project implements a **Medallion Architecture** on Databricks with Delta Lake, transforming raw retail data through three quality layers (Bronze → Silver → Gold) into business-ready analytics served by a live GitHub Pages dashboard.

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│  ┌────────────┐   ┌────────────┐   ┌─────────────────┐                 │
│  │  POS/Sales │   │   Orders   │   │  Inventory MGMT │                 │
│  │   (CSV)    │   │   (CSV)    │   │     (CSV)        │                 │
│  └─────┬──────┘   └─────┬──────┘   └────────┬────────┘                 │
└────────┼────────────────┼───────────────────┼─────────────────────────┘
         │                │                   │
         ▼                ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE — Raw Ingestion (01_bronze_ingestion.py)                     │
│  • Schema enforcement only                                              │
│  • Partition: channel (sales), region (orders), snapshot_date (inv)     │
│  • Added: _ingested_at, _source_file metadata                           │
│  • Tables: raw_sales, raw_orders, raw_inventory                         │
│  • Path: dbfs:/mnt/retail/bronze/                                       │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER — Cleansed & Conformed (silver_transformation.py)            │
│  • Deduplication on primary keys                                        │
│  • Null dropping on critical columns                                    │
│  • Derived columns: revenue, margins, day dimensions, is_weekend        │
│  • Inventory: is_low_stock, days_of_supply                              │
│  • Tables: sales, orders, inventory                                     │
│  • Path: dbfs:/mnt/retail/silver/                                       │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌─────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  🥇 GOLD        │ │  🥇 GOLD         │ │  🥇 GOLD         │
│  Daily Sales    │ │  Product Perf.   │ │  Weekly Trends   │
│  Summary        │ │                  │ │                  │
└────────┬────────┘ └────────┬─────────┘ └────────┬─────────┘
         │                   │                    │
         └───────────────────┼────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  📊 GitHub Pages Dashboard (dashboard/index.html)                       │
│  • KPI cards, daily revenue, channel mix, WoW trends                   │
│  • Product rankings, low-stock alerts, Pareto analysis                  │
│  • Architecture diagrams, table metrics                                 │
└─────────────────────────────────────────────────────────────────────────┘
```

## Unity Catalog Structure

```
retail_catalog
├── bronze
│   ├── raw_sales
│   ├── raw_orders
│   └── raw_inventory
├── silver
│   ├── sales
│   ├── orders
│   └── inventory
└── gold
    ├── daily_sales_summary
    ├── product_performance
    └── weekly_trends
```

## Cluster Recommendations

| Workload | Node Type | Workers | Auto-Terminate |
|----------|-----------|---------|----------------|
| Bronze ingestion | Standard DS3_v2 | 2–4 | 30 min |
| Silver transformation | Standard DS4_v2 | 4–8 | 30 min |
| Gold aggregation | Standard DS3_v2 | 2–4 | 20 min |
| Interactive exploration | Standard DS3_v2 | 1 (single) | 60 min |

## Delta Lake Optimizations

Run these periodically (weekly recommended) to keep tables fast:

```sql
-- Compact small files
OPTIMIZE retail_catalog.gold.daily_sales_summary ZORDER BY (sale_date, channel);
OPTIMIZE retail_catalog.gold.product_performance ZORDER BY (revenue_rank);
OPTIMIZE retail_catalog.gold.weekly_trends       ZORDER BY (sale_year, sale_week);

-- Remove old snapshots
VACUUM retail_catalog.bronze.raw_sales RETAIN 168 HOURS;
VACUUM retail_catalog.silver.sales     RETAIN 168 HOURS;
```

## Scheduling (Databricks Workflows)

| Job | Schedule | Depends On |
|-----|----------|------------|
| Bronze Ingestion | Every 4 hours | — |
| Silver Transformation | Every 4 hours +5min | Bronze |
| Gold — Daily Sales | Daily 06:00 UTC | Silver |
| Gold — Product Perf. | Daily 06:15 UTC | Silver |
| Gold — Weekly Trends | Monday 07:00 UTC | Silver |
