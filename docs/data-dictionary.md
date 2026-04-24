# Data Dictionary

> This document describes all tables, columns, and business definitions in the Contoso data platform.

---

## Silver Layer

### `silver.sales_orders_cleansed`

Grain: one row per unique `order_id` (SCD2 — multiple rows per order if attributes changed).

| Column               | Type              | Nullable | Description                                              |
|----------------------|-------------------|----------|----------------------------------------------------------|
| `order_id`           | STRING            | No       | Unique order identifier (from ERP VBELN field)           |
| `order_date`         | DATE              | No       | Date the order was placed                                |
| `order_year`         | INT               | No       | Year of order_date (partition helper)                    |
| `order_month`        | INT               | No       | Month of order_date (partition helper)                   |
| `customer_id`        | STRING            | No       | Standardised customer ID (from ERP KUNNR)                |
| `customer_name`      | STRING            | Yes      | Customer display name                                    |
| `product_id`         | STRING            | No       | Standardised product/material ID                         |
| `product_name`       | STRING            | Yes      | Product display name                                     |
| `category`           | STRING            | Yes      | Product category (Hardware / Software / Service)         |
| `quantity`           | DECIMAL(18,4)     | No       | Ordered quantity                                         |
| `unit_price_usd`     | DECIMAL(18,4)     | No       | Unit price converted to USD                              |
| `total_amount_usd`   | DECIMAL(18,4)     | No       | Total order value in USD (quantity × unit_price_usd)     |
| `original_currency`  | STRING            | Yes      | Source document currency (e.g. EUR, GBP)                 |
| `fx_rate_to_usd`     | DECIMAL(18,6)     | Yes      | FX rate applied at conversion (from silver.fx_rates)     |
| `status`             | STRING            | No       | Standardised order status (see Status Codes below)       |
| `region`             | STRING            | Yes      | Sales region code (EMEA / AMER / APAC / UNKNOWN)         |
| `sales_rep_id`       | STRING            | Yes      | Sales representative employee ID                         |
| `_valid_from`        | TIMESTAMP         | No       | SCD2 — record validity start (UTC)                       |
| `_valid_to`          | TIMESTAMP         | Yes      | SCD2 — record validity end (null = current record)       |
| `_is_current`        | BOOLEAN           | No       | SCD2 — true if this is the current version of the record |
| `_updated_ts`        | TIMESTAMP         | No       | Last time this record was processed                      |
| `_source_hash`       | STRING (MD5)      | No       | Hash of tracked columns for change detection             |

**Status Codes:**

| Code        | Meaning                          |
|-------------|----------------------------------|
| `Completed` | Order fulfilled and invoiced     |
| `Pending`   | Order confirmed, not yet shipped |
| `Cancelled` | Order cancelled by customer/system |
| `Refunded`  | Order completed but refunded     |
| `Unknown`   | Unrecognised source status code  |

---

## Gold Layer

### `fact.sales_orders`

Grain: one row per order line item.

| Column               | Type              | Description                                              |
|----------------------|-------------------|----------------------------------------------------------|
| `sales_order_sk`     | BIGINT (PK)       | Surrogate key (identity)                                 |
| `order_id`           | VARCHAR(30)       | Business key from ERP                                    |
| `customer_sk`        | BIGINT (FK)       | Foreign key → `dim.customer.customer_sk`                 |
| `product_sk`         | BIGINT (FK)       | Foreign key → `dim.product.product_sk`                   |
| `sales_rep_sk`       | BIGINT (FK)       | Foreign key → `dim.employee.employee_sk`                 |
| `date_sk`            | INT (FK)          | Foreign key → `dim.date.date_sk` (YYYYMMDD integer)      |
| `order_date`         | DATE              | Order date (denormalised for query performance)          |
| `status`             | VARCHAR(30)       | Standardised order status                                |
| `region`             | VARCHAR(20)       | Sales region                                             |
| `quantity`           | DECIMAL(18,4)     | Ordered quantity                                         |
| `unit_price_usd`     | DECIMAL(18,4)     | Unit price in USD                                        |
| `total_amount_usd`   | DECIMAL(18,4)     | Total order value in USD                                 |
| `original_currency`  | CHAR(3)           | Source currency                                          |
| `fx_rate_to_usd`     | DECIMAL(18,6)     | FX rate applied                                          |
| `gross_margin_usd`   | DECIMAL(18,4)     | Revenue minus cost (populated from product cost)         |
| `cost_amount_usd`    | DECIMAL(18,4)     | Cost of goods sold in USD                                |
| `created_ts`         | DATETIME2         | Row creation timestamp                                   |
| `pipeline_run_id`    | VARCHAR(60)       | Pipeline run ID for lineage tracing                      |

### `agg.sales_daily_summary`

Grain: one row per (order_date, region, category, status).

| Column               | Type             | Description                                              |
|----------------------|------------------|----------------------------------------------------------|
| `summary_sk`         | BIGINT (PK)      | Surrogate key                                            |
| `order_date`         | DATE             | Aggregation date                                         |
| `order_year`         | SMALLINT         | Year                                                     |
| `order_month`        | TINYINT          | Month (1–12)                                             |
| `order_quarter`      | TINYINT          | Quarter (1–4)                                            |
| `region`             | VARCHAR(20)      | Sales region                                             |
| `category`           | VARCHAR(100)     | Product category                                         |
| `status`             | VARCHAR(30)      | Order status                                             |
| `order_count`        | INT              | Number of orders                                         |
| `customer_count`     | INT              | Distinct customers                                       |
| `total_quantity`     | DECIMAL(18,4)    | Sum of quantity                                          |
| `total_revenue_usd`  | DECIMAL(18,4)    | Sum of total_amount_usd                                  |
| `total_cost_usd`     | DECIMAL(18,4)    | Sum of cost_amount_usd                                   |
| `gross_margin_usd`   | DECIMAL(18,4)    | Revenue minus cost                                       |
| `avg_order_value_usd`| DECIMAL(18,4)    | Average order value                                      |
| `refreshed_ts`       | DATETIME2        | When this aggregate row was last built                   |

---

## Business Glossary

| Term              | Definition                                                             |
|-------------------|------------------------------------------------------------------------|
| **Order**         | A confirmed purchase agreement between a customer and Contoso          |
| **Order Date**    | The date the customer placed the order (not ship or delivery date)     |
| **Revenue**       | The total invoiced amount for completed orders (excl. VAT)             |
| **Gross Margin**  | Revenue minus cost of goods sold                                       |
| **Region**        | Geographic sales territory: EMEA, AMER, or APAC                        |
| **Watermark**     | The high-water mark timestamp used for incremental data extraction     |
| **SCD2**          | Slowly Changing Dimension Type 2 — tracks historical changes with validity dates |
| **DirectLake**    | Power BI connection mode that reads directly from OneLake Delta files  |
