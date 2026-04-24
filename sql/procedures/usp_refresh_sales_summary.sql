-- =============================================================================
-- Procedure : fact.usp_refresh_sales_summary
-- Purpose   : Refresh the agg.sales_daily_summary table for a given date.
--             Called by the gold transform pipeline.
-- Author    : Data Engineering Team
-- Date      : 2024-01-15
-- Parameters:
--   @processing_date  DATE     The date to refresh (default: yesterday UTC)
--   @full_refresh     BIT      If 1, rebuild entire table (default: 0)
-- =============================================================================

CREATE OR ALTER PROCEDURE fact.usp_refresh_sales_summary
    @processing_date    DATE    = NULL,
    @full_refresh       BIT     = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Default to yesterday UTC if no date provided
    IF @processing_date IS NULL
        SET @processing_date = CAST(DATEADD(DAY, -1, GETUTCDATE()) AS DATE);

    DECLARE @start_ts   DATETIME2 = GETUTCDATE();
    DECLARE @rows_upserted INT    = 0;

    PRINT CONCAT('usp_refresh_sales_summary — processing_date=', @processing_date,
                 ', full_refresh=', @full_refresh);

    BEGIN TRANSACTION;

    BEGIN TRY

        IF @full_refresh = 1
        BEGIN
            TRUNCATE TABLE agg.sales_daily_summary;
            PRINT 'Full refresh: table truncated.';
        END
        ELSE
        BEGIN
            -- Remove existing rows for the processing date only
            DELETE FROM agg.sales_daily_summary
            WHERE order_date = @processing_date;
        END;

        -- Rebuild aggregates from fact table
        INSERT INTO agg.sales_daily_summary (
            order_date, order_year, order_month, order_quarter,
            region, category, status,
            order_count, customer_count, total_quantity,
            total_revenue_usd, total_cost_usd, gross_margin_usd, avg_order_value_usd,
            refreshed_ts
        )
        SELECT
            fo.order_date,
            fo.order_year                               AS order_year,
            MONTH(fo.order_date)                        AS order_month,
            DATEPART(QUARTER, fo.order_date)            AS order_quarter,
            ISNULL(fo.region, 'UNKNOWN')                AS region,
            ISNULL(dp.category, 'UNKNOWN')              AS category,
            fo.status,

            COUNT(*)                                    AS order_count,
            COUNT(DISTINCT fo.customer_sk)              AS customer_count,
            SUM(fo.quantity)                            AS total_quantity,
            SUM(fo.total_amount_usd)                    AS total_revenue_usd,
            SUM(ISNULL(fo.cost_amount_usd, 0))          AS total_cost_usd,
            SUM(ISNULL(fo.gross_margin_usd, 0))         AS gross_margin_usd,
            AVG(fo.total_amount_usd)                    AS avg_order_value_usd,
            GETUTCDATE()                                AS refreshed_ts

        FROM
            fact.sales_orders fo
            JOIN dim.product dp ON fo.product_sk = dp.product_sk AND dp.is_current = 1

        WHERE
            (@full_refresh = 1 OR fo.order_date = @processing_date)
            AND fo.status != 'Cancelled'

        GROUP BY
            fo.order_date, fo.order_year,
            MONTH(fo.order_date), DATEPART(QUARTER, fo.order_date),
            ISNULL(fo.region, 'UNKNOWN'), ISNULL(dp.category, 'UNKNOWN'),
            fo.status;

        SET @rows_upserted = @@ROWCOUNT;

        COMMIT TRANSACTION;

        PRINT CONCAT('Refresh complete. Rows inserted: ', @rows_upserted,
                     ' | Duration: ', DATEDIFF(MILLISECOND, @start_ts, GETUTCDATE()), 'ms');

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        DECLARE @err_msg  NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @err_line INT            = ERROR_LINE();
        RAISERROR('usp_refresh_sales_summary failed at line %d: %s', 16, 1, @err_line, @err_msg);
    END CATCH;
END;
GO

PRINT 'fact.usp_refresh_sales_summary created.';
