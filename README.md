# Contoso Data Platform — Microsoft Fabric

[![CI Pipeline](https://github.com/contoso/fabric-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/contoso/fabric-data-platform/actions/workflows/ci.yml)
[![Deploy to Dev](https://github.com/contoso/fabric-data-platform/actions/workflows/deploy-dev.yml/badge.svg)](https://github.com/contoso/fabric-data-platform/actions/workflows/deploy-dev.yml)
[![Code Quality](https://github.com/contoso/fabric-data-platform/actions/workflows/code-quality.yml/badge.svg)](https://github.com/contoso/fabric-data-platform/actions/workflows/code-quality.yml)

> **⚠️ This is a test/demo repository.** All credentials, IDs, and connection strings are placeholder values only.

A production-grade Microsoft Fabric data platform for the Contoso enterprise analytics workload. This repository contains all lakehouse definitions, warehouse schemas, data pipelines, notebooks, infrastructure-as-code, and CI/CD automation.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Environments](#environments)
- [Data Layers](#data-layers)
- [Pipelines](#pipelines)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Contributing](#contributing)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   Microsoft Fabric Workspace                │
│                                                             │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────┐  │
│  │  OneLake    │   │  Lakehouse  │   │   Data Warehouse │  │
│  │  (Storage)  │──▶│  (Bronze/   │──▶│   (Gold Layer)  │  │
│  │             │   │  Silver)    │   │                 │  │
│  └─────────────┘   └─────────────┘   └─────────────────┘  │
│         │                │                    │             │
│         ▼                ▼                    ▼             │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────┐  │
│  │  Data       │   │  Spark      │   │   Power BI      │  │
│  │  Pipelines  │   │  Notebooks  │   │   Reports       │  │
│  └─────────────┘   └─────────────┘   └─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

The platform follows a **Medallion Architecture** (Bronze → Silver → Gold):

| Layer  | Location              | Description                                      |
|--------|-----------------------|--------------------------------------------------|
| Bronze | `lakehouse/bronze/`   | Raw ingested data, unchanged from source         |
| Silver | `lakehouse/silver/`   | Cleansed, conformed, deduplicated                |
| Gold   | `warehouse/`          | Business-ready aggregates for reporting          |

---

## Repository Structure

```
fabric-data-platform/
├── .github/
│   ├── workflows/          # CI/CD GitHub Actions
│   └── PULL_REQUEST_TEMPLATE.md
├── data/
│   └── sample/             # Sample/seed data for testing
├── docs/
│   ├── architecture.md
│   ├── data-dictionary.md
│   └── runbooks/
├── fabric/
│   ├── lakehouse/          # Lakehouse definitions & metadata
│   ├── warehouse/          # Warehouse DDL & views
│   ├── notebooks/          # Spark notebooks (.ipynb)
│   └── pipelines/          # Data Factory / Fabric pipeline JSON
├── infra/
│   ├── bicep/              # Azure Bicep IaC templates
│   └── terraform/          # Terraform modules (alternative)
├── sql/
│   ├── ddl/                # Table definitions
│   ├── views/              # Analytical views
│   └── procedures/         # Stored procedures
├── src/
│   ├── ingestion/          # Source connectors & loaders
│   ├── transforms/         # PySpark transformation logic
│   └── utils/              # Shared utilities
├── tests/
│   ├── unit/
│   └── integration/
├── .env.dev.example
├── .env.test.example
├── .env.prod.example
├── .gitignore
├── LICENSE
├── Makefile
├── pyproject.toml
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- Azure CLI (`az` version 2.55+)
- Microsoft Fabric workspace access
- `pip install -r requirements.txt`

### Local Setup

```bash
# Clone the repository
git clone https://github.com/contoso/fabric-data-platform.git
cd fabric-data-platform

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"

# Copy environment template and fill in your values
cp .env.dev.example .env
# Edit .env with your actual (non-production) values

# Verify setup
make check
```

---

## Environments

| Environment | Fabric Workspace         | Branch        | Deployment         |
|-------------|--------------------------|---------------|--------------------|
| Development | `contoso-fabric-dev`     | `feature/*`   | Manual / PR        |
| Test        | `contoso-fabric-test`    | `main`        | Automatic on merge |
| Production  | `contoso-fabric-prod`    | `release/*`   | Manual approval    |

---

## Data Layers

### Bronze (Raw)
- Ingested from: SAP ERP, Salesforce CRM, REST APIs, Azure SQL DB
- Format: Delta Lake (Parquet + transaction log)
- Retention: 90 days raw, 7 years archived

### Silver (Cleansed)
- Standardized schemas, null handling, deduplication
- SCD Type 2 for slowly-changing dimensions
- Processed by Spark notebooks on schedule

### Gold (Serving)
- Pre-aggregated facts and dimensions in Fabric Warehouse
- Optimized for DirectLake Power BI connections
- Row-level security applied

---

## Pipelines

| Pipeline                          | Schedule        | Source              | Target          |
|-----------------------------------|-----------------|---------------------|-----------------|
| `ingest_sales_orders`             | Every 4h        | Azure SQL DB        | Bronze lakehouse |
| `ingest_crm_accounts`             | Daily 02:00 UTC | Salesforce REST API | Bronze lakehouse |
| `transform_silver_sales`          | Daily 04:00 UTC | Bronze lakehouse    | Silver lakehouse |
| `transform_gold_sales_summary`    | Daily 06:00 UTC | Silver lakehouse    | Gold warehouse  |
| `ingest_reference_data`           | Weekly Sunday   | SharePoint Lists    | Bronze lakehouse |

---

## Development Workflow

```bash
# Create a feature branch
git checkout -b feature/your-feature-name

# Make changes, run tests
make test

# Lint & format
make lint

# Open a pull request → triggers CI checks
```

All PRs require:
- ✅ CI pipeline passes
- ✅ At least 1 reviewer approval
- ✅ No merge conflicts with `main`

---

## Testing

```bash
# Run unit tests
make test-unit

# Run integration tests (requires .env configured)
make test-integration

# Full test suite with coverage report
make test-coverage
```

---

## Contributing

See [docs/contributing.md](docs/contributing.md) for guidelines.

**Team contacts:**
- Data Engineering: `data-engineering@contoso.com`
- Platform Ops: `platform-ops@contoso.com`
- Slack: `#data-platform` (internal)
