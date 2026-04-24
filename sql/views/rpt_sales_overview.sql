-- =============================================================================
-- View    : rpt.sales_overview
-- Purpose : Top-level sales reporting view for Power BI DirectLake / SQL clients
-- Schema  : rpt
-- Author  : Data Engineering Team
-- Date    : 2024-01-15
-- Notes   : This is the primary view consumed by the "Sales Overview" Power BI
--           report. Do not alter column names without updating the report.
--           Row-level security is applied via rpt.fn_rls_region().
-- =============================================================================

CREATE OR ALTER VIEW rpt.sales_overview AS
SELECT
    -- Time dimensions
    fo.order_date,
    fo.order_year,
    CAST(fo.order_date AS VARCHAR(7))           AS year_month,          -- 'YYYY-MM'
    CONCAT(fo.order_year, '-Q', fo.order_quarter) AS year_quarter,

    -- Customer dimensions
    dc.customer_id,
    dc.customer_name,
    dc.industry,
    dc.country,
    dc.account_type,

    -- Product dimensions
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.subcategory,

    -- Geography
    fo.region,

    -- Sales rep
    de.full_name                                AS sales_rep_name,
    de.team                                     AS sales_team,

    -- Order attributes
    fo.order_id,
    fo.status,

    -- Measures
    fo.quantity,
    fo.unit_price_usd,
    fo.total_amount_usd                         AS revenue_usd,
    fo.cost_amount_usd,
    fo.gross_margin_usd,
    CASE
        WHEN fo.total_amount_usd > 0
        THEN ROUND(fo.gross_margin_usd / fo.total_amount_usd * 100.0, 2)
        ELSE NULL
    END                                         AS gross_margin_pct,
    fo.original_currency,
    fo.fx_rate_to_usd

FROM
    fact.sales_orders   fo
    JOIN dim.customer   dc ON fo.customer_sk   = dc.customer_sk  AND dc.is_current   = 1
    JOIN dim.product    dp ON fo.product_sk    = dp.product_sk   AND dp.is_current   = 1
    LEFT JOIN dim.employee de ON fo.sales_rep_sk = de.employee_sk AND de.is_current  = 1

WHERE
    fo.status != 'Cancelled'
;
GO

PRINT 'rpt.sales_overview created.';
