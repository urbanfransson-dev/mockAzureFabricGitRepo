# Architecture — Contoso Data Platform

## Overview

The Contoso data platform is built on **Microsoft Fabric**, using a **Medallion Architecture** (Bronze / Silver / Gold) stored in OneLake.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Source Systems                                 │
│   SAP S/4HANA ERP    Salesforce CRM    SharePoint    External APIs      │
└──────────┬───────────────────┬─────────────┬──────────────┬────────────┘
           │                   │             │              │
           ▼                   ▼             ▼              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Ingestion Layer (Bronze)                           │
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │  Microsoft Fabric Data Pipelines                                 │  │
│   │  • Watermark-based incremental copy                              │  │
│   │  • Full loads for reference data                                 │  │
│   │  • Error handling + retry logic                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                │                                        │
│                                ▼                                        │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │  Lakehouse — Bronze Layer (OneLake / Delta Lake)                 │  │
│   │  • Raw, append-only data                                         │  │
│   │  • No transformations applied                                    │  │
│   │  • Partitioned by ingestion_date                                 │  │
│   └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Transform Layer (Silver)                           │
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │  PySpark Notebooks (Fabric Spark)                                │  │
│   │  • Schema enforcement + type casting                             │  │
│   │  • Deduplication                                                 │  │
│   │  • Currency normalisation (→ USD)                                │  │
│   │  • SCD Type 2 merge                                              │  │
│   │  • Data quality checks (Great Expectations)                      │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                │                                        │
│                                ▼                                        │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │  Lakehouse — Silver Layer (OneLake / Delta Lake)                 │  │
│   │  • Conformed, deduplicated data                                  │  │
│   │  • Change Data Feed enabled                                      │  │
│   │  • Partitioned by year/month                                     │  │
│   └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Serving Layer (Gold)                               │
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │  Fabric Data Warehouse (T-SQL)                                   │  │
│   │  • Star schema (fact + dim tables)                               │  │
│   │  • Pre-aggregated summary tables                                 │  │
│   │  • Row-level security                                            │  │
│   │  • Reporting views (rpt schema)                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                │                                        │
│                                ▼                                        │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │  Reporting                                                        │  │
│   │  • Power BI — DirectLake connection                              │  │
│   │  • SQL clients (SSMS, Azure Data Studio)                         │  │
│   │  • Excel (via ODBC)                                              │  │
│   └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Data Layers

### Bronze (Raw)

| Property       | Value                          |
|----------------|--------------------------------|
| Storage        | OneLake (Delta Lake)           |
| Format         | Delta (Parquet + txn log)      |
| Schema         | `bronze`                       |
| Partitioning   | `ingestion_date`               |
| Retention      | 90 days hot, 7 years archive   |
| Update pattern | Append-only                    |

### Silver (Cleansed)

| Property       | Value                          |
|----------------|--------------------------------|
| Storage        | OneLake (Delta Lake)           |
| Format         | Delta with CDF enabled         |
| Schema         | `silver`                       |
| Partitioning   | `order_year`, `order_month`    |
| Update pattern | SCD Type 2 merge               |
| Currency       | All monetary values → USD      |

### Gold (Serving)

| Property       | Value                          |
|----------------|--------------------------------|
| Storage        | Fabric Warehouse               |
| Format         | T-SQL tables + views           |
| Schemas        | `fact`, `dim`, `agg`, `rpt`   |
| Update pattern | Full refresh or upsert daily   |
| Security       | Row-level security by region   |

---

## Pipeline Dependency Chain

```
[Source DB]
    │
    ▼  (every 4h)
[ingest_sales_orders]  ──→  bronze.sales_orders_raw
    │
    ▼  (daily 04:00 UTC)
[transform_silver_sales]  ──→  silver.sales_orders_cleansed
    │
    ▼  (daily 06:00 UTC)
[transform_gold_sales_summary]  ──→  fact.sales_orders
                                 ──→  agg.sales_daily_summary
```

---

## Security Model

- **Authentication:** Azure Active Directory (service principals + managed identities)
- **Key management:** Azure Key Vault (no secrets in code or config files)
- **Network:** Private endpoints for production; public access for dev/test
- **Row-level security:** Applied at the Warehouse `rpt` schema level; filters by `region` based on AAD group membership
- **RBAC:** Fabric workspace roles (Admin / Member / Contributor / Viewer) per environment

---

## Technology Stack

| Component         | Technology                        |
|-------------------|-----------------------------------|
| Storage           | Microsoft Fabric OneLake          |
| Processing        | Apache Spark 3.4 (Fabric Runtime) |
| Warehouse         | Microsoft Fabric Warehouse (T-SQL)|
| Orchestration     | Fabric Data Pipelines             |
| IaC               | Azure Bicep                       |
| CI/CD             | GitHub Actions                    |
| Monitoring        | Azure Monitor + Log Analytics     |
| Secrets           | Azure Key Vault                   |
| Source control    | GitHub                            |
| Languages         | Python 3.11, T-SQL, PySpark       |
