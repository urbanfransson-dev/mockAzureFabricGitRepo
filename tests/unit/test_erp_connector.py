"""
Unit tests for src/ingestion/erp_sql_connector.py

Uses mocks to avoid requiring a real database connection.
Run with: pytest tests/unit/ -v
"""

import os
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd

from src.ingestion.erp_sql_connector import ErpSqlConfig, ErpSqlConnector, _require_env


# ---------------------------------------------------------------------------
# ErpSqlConfig tests
# ---------------------------------------------------------------------------

class TestErpSqlConfig:

    def test_from_env_reads_variables(self, monkeypatch):
        monkeypatch.setenv("ERP_SQL_SERVER",     "sql-erp-test.database.windows.net")
        monkeypatch.setenv("ERP_SQL_DATABASE",   "erp_db")
        monkeypatch.setenv("AZURE_CLIENT_ID",    "00000000-0000-0000-0000-000000000001")
        monkeypatch.setenv("AZURE_TENANT_ID",    "00000000-0000-0000-0000-000000000002")
        monkeypatch.setenv("AZURE_KEY_VAULT_URL","https://kv-test.vault.azure.net/")

        config = ErpSqlConfig.from_env()

        assert config.server      == "sql-erp-test.database.windows.net"
        assert config.database    == "erp_db"
        assert config.client_id   == "00000000-0000-0000-0000-000000000001"
        assert config.tenant_id   == "00000000-0000-0000-0000-000000000002"
        assert config.port        == 1433  # default

    def test_from_env_raises_on_missing_var(self, monkeypatch):
        monkeypatch.delenv("ERP_SQL_SERVER", raising=False)
        with pytest.raises(EnvironmentError, match="ERP_SQL_SERVER"):
            ErpSqlConfig.from_env()

    def test_custom_port_from_env(self, monkeypatch):
        monkeypatch.setenv("ERP_SQL_SERVER",     "server.example.com")
        monkeypatch.setenv("ERP_SQL_DATABASE",   "db")
        monkeypatch.setenv("AZURE_CLIENT_ID",    "client-id")
        monkeypatch.setenv("AZURE_TENANT_ID",    "tenant-id")
        monkeypatch.setenv("AZURE_KEY_VAULT_URL","https://kv.vault.azure.net/")
        monkeypatch.setenv("ERP_SQL_PORT",       "1434")

        config = ErpSqlConfig.from_env()
        assert config.port == 1434


# ---------------------------------------------------------------------------
# ErpSqlConnector tests
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_config() -> ErpSqlConfig:
    return ErpSqlConfig(
        server="sql-erp-test.database.windows.net",
        database="erp_db",
        client_id="00000000-0000-0000-0000-000000000001",
        tenant_id="00000000-0000-0000-0000-000000000002",
        key_vault_url="https://kv-test.vault.azure.net/",
    )


class TestErpSqlConnector:

    @patch("src.ingestion.erp_sql_connector.pyodbc.connect")
    @patch.object(ErpSqlConnector, "_get_secret", return_value="fake-secret")
    def test_connect_calls_pyodbc(self, mock_secret, mock_connect, mock_config):
        connector = ErpSqlConnector(mock_config)
        connector.connect()
        mock_connect.assert_called_once()
        assert "DRIVER={ODBC Driver 18 for SQL Server}" in mock_connect.call_args[0][0]

    @patch("src.ingestion.erp_sql_connector.pyodbc.connect")
    @patch.object(ErpSqlConnector, "_get_secret", return_value="fake-secret")
    def test_read_table_returns_dataframe(self, mock_secret, mock_connect, mock_config):
        # Set up a mock cursor with sample data
        mock_cursor = MagicMock()
        mock_cursor.description = [("order_id",), ("order_date",), ("total_amount",)]
        mock_cursor.fetchmany.side_effect = [
            [("ORD-001", "2024-01-15", 1000.0), ("ORD-002", "2024-01-16", 2000.0)],
            [],  # second call returns empty → stop
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        connector = ErpSqlConnector(mock_config)
        df = connector.read_table("dbo.SalesOrders")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["order_id", "order_date", "total_amount"]

    @patch("src.ingestion.erp_sql_connector.pyodbc.connect")
    @patch.object(ErpSqlConnector, "_get_secret", return_value="fake-secret")
    def test_read_table_with_watermark_adds_where_clause(self, mock_secret, mock_connect, mock_config):
        mock_cursor = MagicMock()
        mock_cursor.description = [("order_id",)]
        mock_cursor.fetchmany.side_effect = [[("ORD-001",)], []]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        connector = ErpSqlConnector(mock_config)
        connector.read_table(
            "dbo.SalesOrders",
            watermark_column="AEDAT",
            watermark_value="2024-01-01",
        )

        executed_query = mock_cursor.execute.call_args[0][0]
        assert "WHERE AEDAT > ?" in executed_query

    @patch("src.ingestion.erp_sql_connector.pyodbc.connect")
    @patch.object(ErpSqlConnector, "_get_secret", return_value="fake-secret")
    def test_read_table_returns_empty_df_when_no_rows(self, mock_secret, mock_connect, mock_config):
        mock_cursor = MagicMock()
        mock_cursor.description = [("order_id",)]
        mock_cursor.fetchmany.return_value = []
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        connector = ErpSqlConnector(mock_config)
        df = connector.read_table("dbo.SalesOrders")

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    @patch("src.ingestion.erp_sql_connector.pyodbc.connect")
    @patch.object(ErpSqlConnector, "_get_secret", return_value="fake-secret")
    def test_context_manager_disconnects(self, mock_secret, mock_connect, mock_config):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with ErpSqlConnector(mock_config):
            pass

        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# _require_env helper
# ---------------------------------------------------------------------------

class TestRequireEnv:

    def test_returns_value_when_set(self, monkeypatch):
        monkeypatch.setenv("MY_TEST_VAR", "hello")
        assert _require_env("MY_TEST_VAR") == "hello"

    def test_raises_when_not_set(self, monkeypatch):
        monkeypatch.delenv("MY_TEST_VAR", raising=False)
        with pytest.raises(EnvironmentError, match="MY_TEST_VAR"):
            _require_env("MY_TEST_VAR")
