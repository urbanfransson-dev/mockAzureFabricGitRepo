"""
erp_sql_connector.py
--------------------
Connector for reading data from the source ERP Azure SQL Database.
Supports incremental watermark-based extraction and full loads.

Usage:
    connector = ErpSqlConnector.from_env()
    df = connector.read_table("dbo.SalesOrders", watermark="2024-01-01")
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pyodbc
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass
class ErpSqlConfig:
    """Connection configuration for the ERP SQL Database.

    All values are loaded from environment variables — never hardcode credentials.
    See .env.dev.example for variable names.
    """
    server:        str
    database:      str
    client_id:     str                    # Azure AD service principal client ID
    tenant_id:     str                    # Azure AD tenant ID
    # client_secret intentionally not stored here; fetched from Key Vault at runtime
    key_vault_url: str
    secret_name:   str = "erp-sql-sp-secret"
    port:          int = 1433
    connect_timeout: int = 30
    query_timeout:   int = 7200          # 2 hours
    extra_params:    dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "ErpSqlConfig":
        """Build config from environment variables."""
        return cls(
            server=        _require_env("ERP_SQL_SERVER"),
            database=      _require_env("ERP_SQL_DATABASE"),
            client_id=     _require_env("AZURE_CLIENT_ID"),
            tenant_id=     _require_env("AZURE_TENANT_ID"),
            key_vault_url= _require_env("AZURE_KEY_VAULT_URL"),
            secret_name=   os.getenv("ERP_SQL_SECRET_NAME", "erp-sql-sp-secret"),
            port=          int(os.getenv("ERP_SQL_PORT", "1433")),
            connect_timeout=int(os.getenv("ERP_SQL_CONNECT_TIMEOUT", "30")),
            query_timeout= int(os.getenv("ERP_SQL_QUERY_TIMEOUT", "7200")),
        )


class ErpSqlConnector:
    """Reads data from the source ERP Azure SQL Database."""

    def __init__(self, config: ErpSqlConfig) -> None:
        self.config = config
        self._connection: Optional[pyodbc.Connection] = None
        logger.info(
            "ErpSqlConnector initialised — server=%s, database=%s",
            config.server, config.database
        )

    @classmethod
    def from_env(cls) -> "ErpSqlConnector":
        return cls(ErpSqlConfig.from_env())

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_secret(self) -> str:
        """Retrieve the service principal secret from Azure Key Vault."""
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        credential = DefaultAzureCredential()
        client = SecretClient(
            vault_url=self.config.key_vault_url,
            credential=credential
        )
        secret = client.get_secret(self.config.secret_name)
        logger.debug("Secret retrieved from Key Vault: %s", self.config.secret_name)
        return secret.value

    def _build_connection_string(self) -> str:
        """Build ODBC connection string using AAD service principal auth."""
        client_secret = self._get_secret()
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.config.server},{self.config.port};"
            f"DATABASE={self.config.database};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={self.config.client_id}@{self.config.tenant_id};"
            f"PWD={client_secret};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout={self.config.connect_timeout};"
        )
        return conn_str

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        reraise=True
    )
    def connect(self) -> None:
        """Establish database connection with retry logic."""
        logger.info("Connecting to ERP SQL Database…")
        conn_str = self._build_connection_string()
        self._connection = pyodbc.connect(conn_str, timeout=self.config.connect_timeout)
        self._connection.timeout = self.config.query_timeout
        logger.info("Connection established.")

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Connection closed.")

    def __enter__(self) -> "ErpSqlConnector":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()

    # ------------------------------------------------------------------
    # Data reading
    # ------------------------------------------------------------------

    def read_table(
        self,
        table_name: str,
        watermark_column: Optional[str] = None,
        watermark_value: Optional[str] = None,
        batch_size: int = 50_000,
        columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Read a table with optional watermark-based incremental filtering.

        Args:
            table_name:       Fully qualified table name (e.g. "dbo.SalesOrders").
            watermark_column: Column to use as the watermark (e.g. "AEDAT").
            watermark_value:  Lower bound for watermark filter (exclusive).
            batch_size:       Number of rows per chunk for large tables.
            columns:          Optional list of columns to select (default: all).

        Returns:
            DataFrame containing the queried rows.
        """
        if self._connection is None:
            self.connect()

        col_clause = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_clause} FROM {table_name}"

        params: list = []
        if watermark_column and watermark_value:
            query += f" WHERE {watermark_column} > ?"
            params.append(watermark_value)

        logger.info("Executing query: %s | params=%s", query, params)

        chunks = []
        cursor = self._connection.cursor()
        cursor.execute(query, params)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            col_names = [col[0] for col in cursor.description]
            chunks.append(pd.DataFrame.from_records(rows, columns=col_names))
            logger.debug("Fetched %d rows", len(rows))

        cursor.close()

        if not chunks:
            logger.warning("Query returned 0 rows: %s", query)
            return pd.DataFrame()

        df = pd.concat(chunks, ignore_index=True)
        logger.info("Read %d rows from %s", len(df), table_name)
        return df

    def get_max_watermark(self, table_name: str, watermark_column: str) -> Optional[str]:
        """Get the maximum value of a watermark column in a table."""
        if self._connection is None:
            self.connect()

        query = f"SELECT MAX({watermark_column}) AS max_wm FROM {table_name}"
        cursor = self._connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        result = str(row[0]) if row and row[0] is not None else None
        logger.info("Max watermark for %s.%s = %s", table_name, watermark_column, result)
        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_env(name: str) -> str:
    """Get a required environment variable or raise a clear error."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            f"See .env.dev.example for configuration."
        )
    return value
