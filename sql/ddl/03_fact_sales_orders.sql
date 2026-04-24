-- =============================================================================
-- Script  : 03_fact_sales_orders.sql
-- Purpose : Create the Sales Orders fact table (Gold layer)
-- Schema  : fact
-- Author  : Data Engineering Team
-- Date    : 2024-01-01
-- Notes   : Grain = one row per order line item.
--           All monetary values stored in USD.
-- =============================================================================

DROP TABLE IF EXISTS fact.sales_orders;

CREATE TABLE fact.sales_orders (
    -- Surrogate key
    sales_order_sk      BIGINT          NOT NULL IDENTITY(1,1),

    -- Business / natural key
    order_id            VARCHAR(30)     NOT NULL,

    -- Foreign keys (dimension surrogates)
    customer_sk         BIGINT          NOT NULL,   -- → dim.customer
    product_sk          BIGINT          NOT NULL,   -- → dim.product
    sales_rep_sk        BIGINT          NULL,        -- → dim.employee
    date_sk             INT             NOT NULL,   -- → dim.date (YYYYMMDD)

    -- Order attributes
    order_date          DATE            NOT NULL,
    status              VARCHAR(30)     NOT NULL,
    region              VARCHAR(20)     NULL,

    -- Measures (USD)
    quantity            DECIMAL(18, 4)  NOT NULL,
    unit_price_usd      DECIMAL(18, 4)  NOT NULL,
    total_amount_usd    DECIMAL(18, 4)  NOT NULL,
    original_currency   CHAR(3)         NULL,
    fx_rate_to_usd      DECIMAL(18, 6)  NULL,

    -- Derived measures
    gross_margin_usd    DECIMAL(18, 4)  NULL,   -- populated during gold build
    cost_amount_usd     DECIMAL(18, 4)  NULL,

    -- Audit
    created_ts          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_ts          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    pipeline_run_id     VARCHAR(60)     NULL,

    CONSTRAINT pk_fact_sales_orders PRIMARY KEY NONCLUSTERED (sales_order_sk)
);

-- Clustered index on the most common filter / join pattern
CREATE CLUSTERED INDEX ix_fact_sales_orders_date
    ON fact.sales_orders (order_date, status);

-- Dimension join indexes
CREATE INDEX ix_fact_sales_customer_sk
    ON fact.sales_orders (customer_sk)
    INCLUDE (order_date, total_amount_usd, status);

CREATE INDEX ix_fact_sales_product_sk
    ON fact.sales_orders (product_sk)
    INCLUDE (order_date, total_amount_usd, quantity);

CREATE INDEX ix_fact_sales_date_sk
    ON fact.sales_orders (date_sk)
    INCLUDE (total_amount_usd, quantity, status);

PRINT 'fact.sales_orders created.';
