-- =============================================================================
-- Script  : 04_agg_sales_summary.sql
-- Purpose : Pre-aggregated daily sales summary for fast Power BI queries
-- Schema  : agg
-- Author  : Data Engineering Team
-- Date    : 2024-01-10
-- Notes   : Rebuilt daily by the gold transform pipeline.
--           Grain = one row per (order_date, region, category, status).
-- =============================================================================

DROP TABLE IF EXISTS agg.sales_daily_summary;

CREATE TABLE agg.sales_daily_summary (
    summary_sk          BIGINT          NOT NULL IDENTITY(1,1),

    -- Grain columns
    order_date          DATE            NOT NULL,
    order_year          SMALLINT        NOT NULL,
    order_month         TINYINT         NOT NULL,
    order_quarter       TINYINT         NOT NULL,
    region              VARCHAR(20)     NOT NULL,
    category            VARCHAR(100)    NOT NULL,
    status              VARCHAR(30)     NOT NULL,

    -- Measures
    order_count         INT             NOT NULL DEFAULT 0,
    customer_count      INT             NOT NULL DEFAULT 0,
    total_quantity      DECIMAL(18, 4)  NOT NULL DEFAULT 0,
    total_revenue_usd   DECIMAL(18, 4)  NOT NULL DEFAULT 0,
    total_cost_usd      DECIMAL(18, 4)  NOT NULL DEFAULT 0,
    gross_margin_usd    DECIMAL(18, 4)  NOT NULL DEFAULT 0,
    avg_order_value_usd DECIMAL(18, 4)  NULL,

    -- Audit
    refreshed_ts        DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    pipeline_run_id     VARCHAR(60)     NULL,

    CONSTRAINT pk_agg_sales_daily PRIMARY KEY NONCLUSTERED (summary_sk)
);

CREATE CLUSTERED INDEX ix_agg_sales_daily_date
    ON agg.sales_daily_summary (order_date, region, category);

CREATE INDEX ix_agg_sales_daily_year_month
    ON agg.sales_daily_summary (order_year, order_month, region)
    INCLUDE (total_revenue_usd, order_count);

PRINT 'agg.sales_daily_summary created.';
