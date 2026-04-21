# Data Dictionary — Retail Lakehouse

## Bronze Layer

### `bronze.raw_sales`
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | STRING | Unique transaction identifier (PK) |
| order_id | STRING | Parent order identifier |
| customer_id | STRING | Customer identifier |
| product_id | STRING | SKU / product identifier |
| store_id | STRING | Store or fulfilment centre ID |
| transaction_date | TIMESTAMP | When the transaction occurred |
| quantity | INT | Units purchased |
| unit_price | DOUBLE | Price per unit before discount |
| discount_pct | DOUBLE | Discount fraction (0.0–1.0) |
| payment_method | STRING | CARD / CASH / DIGITAL |
| channel | STRING | ONLINE / IN-STORE |
| _ingested_at | TIMESTAMP | When this record was ingested into Bronze |
| _source_file | STRING | Source file path |

### `bronze.raw_orders`
| Column | Type | Description |
|--------|------|-------------|
| order_id | STRING | Unique order identifier (PK) |
| customer_id | STRING | Customer identifier |
| order_date | TIMESTAMP | Order placement timestamp |
| ship_date | TIMESTAMP | Shipment dispatch timestamp |
| status | STRING | PENDING / PROCESSING / SHIPPED / DELIVERED / CANCELLED |
| total_amount | DOUBLE | Total order value |
| region | STRING | Geographic region |
| store_id | STRING | Originating store |
| _ingested_at | TIMESTAMP | Ingestion timestamp |
| _source_file | STRING | Source file path |

### `bronze.raw_inventory`
| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING | SKU identifier |
| store_id | STRING | Store location |
| snapshot_date | DATE | Date of inventory snapshot |
| stock_on_hand | INT | Current units in stock |
| reorder_point | INT | Minimum stock level before reorder |
| units_sold_7d | INT | Units sold in past 7 days |
| supplier_id | STRING | Primary supplier ID |
| _ingested_at | TIMESTAMP | Ingestion timestamp |

---

## Silver Layer

### `silver.sales`
All Bronze columns plus:

| Column | Type | Description |
|--------|------|-------------|
| revenue | DOUBLE | Net revenue (qty × price × (1 − discount)) |
| gross_revenue | DOUBLE | Revenue before discount |
| discount_amount | DOUBLE | Discount value in dollars |
| sale_date | DATE | Date component of transaction_date |
| sale_year | INT | Year |
| sale_month | INT | Month (1–12) |
| sale_week | INT | ISO week number |
| day_of_week | INT | 1=Sunday … 7=Saturday |
| day_name | STRING | Full day name (Monday, Tuesday, …) |
| is_weekend | BOOLEAN | True if Saturday or Sunday |
| _processed_at | TIMESTAMP | Silver processing timestamp |

### `silver.orders`
All Bronze columns plus:

| Column | Type | Description |
|--------|------|-------------|
| days_to_ship | INT | Calendar days from order_date to ship_date |
| order_year | INT | Year from order_date |
| order_month | INT | Month from order_date |
| order_week | INT | ISO week from order_date |
| _processed_at | TIMESTAMP | Silver processing timestamp |

### `silver.inventory`
All Bronze columns plus:

| Column | Type | Description |
|--------|------|-------------|
| is_low_stock | BOOLEAN | True if stock_on_hand ≤ reorder_point |
| days_of_supply | DOUBLE | stock_on_hand ÷ (units_sold_7d ÷ 7) |
| _processed_at | TIMESTAMP | Silver processing timestamp |

---

## Gold Layer

### `gold.daily_sales_summary`
| Column | Type | Description |
|--------|------|-------------|
| sale_date | DATE | Business date |
| sale_year | INT | Year |
| sale_month | INT | Month |
| sale_week | INT | ISO week |
| day_name | STRING | Day of week |
| is_weekend | BOOLEAN | Weekend flag |
| channel | STRING | ONLINE / IN-STORE |
| store_id | STRING | Store identifier |
| num_transactions | LONG | Distinct transactions |
| num_orders | LONG | Distinct orders |
| total_units_sold | LONG | Total units sold |
| gross_revenue | DOUBLE | Revenue before discounts |
| total_discounts | DOUBLE | Total discount value |
| net_revenue | DOUBLE | Revenue after discounts |
| avg_order_value | DOUBLE | Net revenue ÷ num_transactions |
| avg_discount_pct | DOUBLE | Average discount fraction |
| discount_rate_pct | DOUBLE | Total discounts ÷ gross revenue × 100 |
| _refreshed_at | TIMESTAMP | When this row was last computed |

### `gold.product_performance`
| Column | Type | Description |
|--------|------|-------------|
| product_id | STRING | SKU identifier |
| total_revenue | DOUBLE | All-time net revenue |
| total_gross_revenue | DOUBLE | All-time gross revenue |
| total_discounts | DOUBLE | All-time discount value |
| total_units_sold | LONG | Total units sold |
| num_transactions | LONG | Number of transactions |
| unique_customers | LONG | Distinct customers |
| avg_order_value | DOUBLE | Average revenue per transaction |
| first_sale_date | DATE | Earliest recorded sale |
| last_sale_date | DATE | Most recent sale |
| discount_rate_pct | DOUBLE | Total discounts as % of gross |
| total_stock_on_hand | LONG | Current total stock across stores |
| avg_days_of_supply | DOUBLE | Average days of supply across stores |
| stores_low_stock | LONG | Stores currently below reorder point |
| stores_carrying | LONG | Total stores carrying this SKU |
| revenue_rank | INT | Rank by total_revenue (1 = highest) |
| units_rank | INT | Rank by total_units_sold |
| revenue_pct_of_total | DOUBLE | % share of all-product revenue |
| _refreshed_at | TIMESTAMP | Last computation timestamp |

### `gold.weekly_trends`
| Column | Type | Description |
|--------|------|-------------|
| sale_year | INT | Year |
| sale_week | INT | ISO week number |
| channel | STRING | ONLINE / IN-STORE / MOBILE |
| week_start_date | DATE | First day of the week |
| week_end_date | DATE | Last day of the week |
| net_revenue | DOUBLE | Net revenue for week+channel |
| gross_revenue | DOUBLE | Gross revenue for week+channel |
| units_sold | LONG | Units sold |
| num_transactions | LONG | Transactions |
| unique_customers | LONG | Distinct customers |
| avg_order_value | DOUBLE | Net revenue ÷ transactions |
| total_discounts | DOUBLE | Discount value |
| prev_week_revenue | DOUBLE | Prior week net revenue (lag 1) |
| wow_revenue_pct | DOUBLE | WoW revenue change % |
| wow_units_pct | DOUBLE | WoW units change % |
| wow_customers_pct | DOUBLE | WoW unique customers change % |
| rolling_4w_revenue | DOUBLE | 4-week rolling average revenue |
| trend_signal | STRING | ACCELERATING / GROWING / FLAT / SLOWING / DECLINING |
| _refreshed_at | TIMESTAMP | Last computation timestamp |
