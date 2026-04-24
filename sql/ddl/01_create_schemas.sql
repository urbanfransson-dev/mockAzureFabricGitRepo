-- =============================================================================
-- Script  : 01_create_schemas.sql
-- Purpose : Create logical schemas in the Fabric Warehouse
-- Author  : Data Engineering Team
-- Date    : 2024-01-01
-- Notes   : Run once during initial workspace provisioning.
--           Requires db_owner or schema-create permission.
-- =============================================================================

-- Fact tables (transactional grain)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fact')
    EXEC('CREATE SCHEMA fact');
GO

-- Dimension tables (slowly-changing)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
    EXEC('CREATE SCHEMA dim');
GO

-- Pre-aggregated summary tables for reporting performance
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'agg')
    EXEC('CREATE SCHEMA agg');
GO

-- Reporting views — the public interface for Power BI / SQL clients
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt')
    EXEC('CREATE SCHEMA rpt');
GO

-- Control / metadata tables
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ctrl')
    EXEC('CREATE SCHEMA ctrl');
GO

PRINT 'Schemas created successfully.';
