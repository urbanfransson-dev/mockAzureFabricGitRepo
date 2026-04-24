-- =============================================================================
-- Script  : 02_dim_customer.sql
-- Purpose : Create the Customer dimension table (SCD Type 2)
-- Schema  : dim
-- Author  : Data Engineering Team
-- Date    : 2024-01-01
-- =============================================================================

DROP TABLE IF EXISTS dim.customer;

CREATE TABLE dim.customer (
    -- Surrogate key
    customer_sk         BIGINT          NOT NULL IDENTITY(1,1),

    -- Natural / business key
    customer_id         VARCHAR(20)     NOT NULL,

    -- Attributes
    customer_name       NVARCHAR(200)   NOT NULL,
    account_type        VARCHAR(50)     NULL,
    industry            VARCHAR(100)    NULL,
    country             VARCHAR(100)    NULL,
    city                NVARCHAR(200)   NULL,
    region              VARCHAR(20)     NULL,
    email               VARCHAR(254)    NULL,
    account_manager_id  VARCHAR(20)     NULL,
    credit_limit        DECIMAL(18, 2)  NULL,
    currency            CHAR(3)         NULL,
    is_active           BIT             NOT NULL DEFAULT 1,

    -- SCD2 metadata
    valid_from          DATETIME2       NOT NULL,
    valid_to            DATETIME2       NULL,
    is_current          BIT             NOT NULL DEFAULT 1,

    -- Audit
    created_ts          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    updated_ts          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    source_hash         CHAR(32)        NULL,

    CONSTRAINT pk_dim_customer PRIMARY KEY NONCLUSTERED (customer_sk)
);

-- Index for business key lookups
CREATE CLUSTERED INDEX ix_dim_customer_id
    ON dim.customer (customer_id, is_current);

CREATE INDEX ix_dim_customer_region
    ON dim.customer (region, is_current)
    INCLUDE (customer_name, industry);

PRINT 'dim.customer created.';
