"""
conftest.py
-----------
Shared pytest fixtures and configuration.
This file is discovered automatically by pytest.
"""

import os
import pytest
import pandas as pd


# ---------------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Set dummy environment variables for tests that require them."""
    os.environ.setdefault("ERP_SQL_SERVER",      "sql-erp-test.database.windows.net")
    os.environ.setdefault("ERP_SQL_DATABASE",    "erp_db_test")
    os.environ.setdefault("AZURE_CLIENT_ID",     "00000000-0000-0000-0000-000000000001")
    os.environ.setdefault("AZURE_TENANT_ID",     "00000000-0000-0000-0000-000000000002")
    os.environ.setdefault("AZURE_KEY_VAULT_URL", "https://kv-test.vault.azure.net/")
    os.environ.setdefault("FABRIC_WORKSPACE_ID", "00000000-0000-0000-0000-000000000003")


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

SAMPLE_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "sample")


@pytest.fixture(scope="session")
def sample_sales_orders() -> pd.DataFrame:
    """Load the sample sales orders CSV."""
    path = os.path.join(SAMPLE_DATA_DIR, "sales_orders.csv")
    return pd.read_csv(path, parse_dates=["order_date"])


@pytest.fixture(scope="session")
def sample_customers() -> pd.DataFrame:
    """Load the sample customers CSV."""
    path = os.path.join(SAMPLE_DATA_DIR, "customers.csv")
    return pd.read_csv(path)


@pytest.fixture(scope="session")
def sample_products() -> dict:
    """Load the sample products JSON."""
    import json
    path = os.path.join(SAMPLE_DATA_DIR, "products.json")
    with open(path) as f:
        return json.load(f)
