"""
Unit tests for src/transforms/silver_sales_transform.py

These tests use a local Spark session (no Fabric connection required).
Run with: pytest tests/unit/ -v
"""

import pytest
from datetime import datetime, date
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DecimalType, TimestampType, BooleanType
)

from src.transforms.silver_sales_transform import (
    cast_and_cleanse,
    deduplicate,
    add_scd2_columns,
    run_quality_checks,
    apply_fx_conversion,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Shared local SparkSession for all unit tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("contoso-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def raw_sales_df(spark):
    """Minimal raw Bronze sales order DataFrame for testing."""
    data = [
        ("ORD-001", "2024-01-15", "CUST-001", "PROD-001", "10.0", "100.0", "1000.0", "USD", "COMP",  "EMEA", "REP-001", datetime(2024, 1, 15, 8, 0, 0), "BATCH-001", False, date(2024, 1, 15)),
        ("ORD-002", "2024-01-16", "CUST-002", "PROD-002", "5.0",  "200.0", "1000.0", "EUR", "P",     "AMER", "REP-002", datetime(2024, 1, 16, 8, 0, 0), "BATCH-001", False, date(2024, 1, 15)),
        ("ORD-003", "2024-01-17", "CUST-003", "PROD-003", "2.0",  "500.0", "1000.0", "GBP", "X",     "APAC", "REP-003", datetime(2024, 1, 17, 8, 0, 0), "BATCH-001", False, date(2024, 1, 15)),
        # Duplicate of ORD-001 — newer ingestion timestamp
        ("ORD-001", "2024-01-15", "CUST-001", "PROD-001", "10.0", "100.0", "1000.0", "USD", "COMP",  "EMEA", "REP-001", datetime(2024, 1, 15, 9, 0, 0), "BATCH-002", False, date(2024, 1, 15)),
    ]
    columns = [
        "order_id", "order_date", "customer_id", "product_id",
        "quantity", "unit_price", "total_amount", "currency",
        "status", "region", "sales_rep_id",
        "_ingestion_ts", "_source_file", "_is_deleted", "ingestion_date"
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def fx_rates_df(spark):
    """Sample FX rates DataFrame."""
    data = [
        ("2024-01-15", "EUR", 1.085),
        ("2024-01-15", "GBP", 1.270),
        ("2024-01-15", "CAD", 0.741),
    ]
    return spark.createDataFrame(data, ["rate_date", "from_currency", "rate_to_usd"])


# ---------------------------------------------------------------------------
# cast_and_cleanse
# ---------------------------------------------------------------------------

class TestCastAndCleanse:

    def test_maps_completed_status(self, raw_sales_df):
        result = cast_and_cleanse(raw_sales_df)
        completed = result.filter("order_id = 'ORD-001'").collect()[0]
        assert completed["status"] == "Completed"

    def test_maps_pending_status(self, raw_sales_df):
        result = cast_and_cleanse(raw_sales_df)
        pending = result.filter("order_id = 'ORD-002'").collect()[0]
        assert pending["status"] == "Pending"

    def test_maps_cancelled_status(self, raw_sales_df):
        result = cast_and_cleanse(raw_sales_df)
        cancelled = result.filter("order_id = 'ORD-003'").collect()[0]
        assert cancelled["status"] == "Cancelled"

    def test_adds_order_year_column(self, raw_sales_df):
        result = cast_and_cleanse(raw_sales_df)
        row = result.filter("order_id = 'ORD-001'").collect()[0]
        assert row["order_year"] == 2024

    def test_adds_order_month_column(self, raw_sales_df):
        result = cast_and_cleanse(raw_sales_df)
        row = result.filter("order_id = 'ORD-001'").collect()[0]
        assert row["order_month"] == 1

    def test_casts_quantity_to_decimal(self, raw_sales_df):
        result = cast_and_cleanse(raw_sales_df)
        row = result.filter("order_id = 'ORD-001'").collect()[0]
        assert row["quantity"] == Decimal("10.0000")

    def test_fills_null_region(self, spark, raw_sales_df):
        from pyspark.sql import functions as F
        null_region_df = raw_sales_df.withColumn("region", F.lit(None).cast(StringType()))
        result = cast_and_cleanse(null_region_df)
        row = result.filter("order_id = 'ORD-001'").collect()[0]
        assert row["region"] == "UNKNOWN"

    def test_drops_rows_with_null_order_id(self, spark):
        data = [
            (None,    "2024-01-15", "CUST-001", "PROD-001", "1.0", "100.0", "100.0", "USD", "COMP", "EMEA", "REP-001", datetime(2024,1,15), "B1", False, date(2024,1,15)),
            ("ORD-X", "2024-01-15", "CUST-002", "PROD-002", "1.0", "100.0", "100.0", "USD", "COMP", "EMEA", "REP-002", datetime(2024,1,15), "B1", False, date(2024,1,15)),
        ]
        columns = ["order_id","order_date","customer_id","product_id","quantity","unit_price","total_amount","currency","status","region","sales_rep_id","_ingestion_ts","_source_file","_is_deleted","ingestion_date"]
        df = spark.createDataFrame(data, columns)
        result = cast_and_cleanse(df)
        assert result.count() == 1
        assert result.collect()[0]["order_id"] == "ORD-X"


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:

    def test_removes_duplicate_order_ids(self, raw_sales_df):
        # raw_sales_df has ORD-001 twice — deduplicate should keep only 1
        result = deduplicate(raw_sales_df)
        count = result.filter("order_id = 'ORD-001'").count()
        assert count == 1

    def test_keeps_latest_by_ingestion_ts(self, raw_sales_df):
        result = deduplicate(raw_sales_df)
        row = result.filter("order_id = 'ORD-001'").collect()[0]
        # The later BATCH-002 row (ingestion_ts 09:00) should be kept
        assert row["_source_file"] == "BATCH-002"

    def test_does_not_drop_unique_rows(self, raw_sales_df):
        result = deduplicate(raw_sales_df)
        # 4 input rows with 1 duplicate → 3 unique orders
        assert result.count() == 3


# ---------------------------------------------------------------------------
# add_scd2_columns
# ---------------------------------------------------------------------------

class TestAddScd2Columns:

    def test_adds_valid_from_column(self, raw_sales_df):
        now = datetime(2024, 1, 20)
        result = add_scd2_columns(raw_sales_df, valid_from=now)
        row = result.collect()[0]
        assert row["_valid_from"] == now

    def test_valid_to_is_null(self, raw_sales_df):
        result = add_scd2_columns(raw_sales_df)
        row = result.collect()[0]
        assert row["_valid_to"] is None

    def test_is_current_is_true(self, raw_sales_df):
        result = add_scd2_columns(raw_sales_df)
        row = result.collect()[0]
        assert row["_is_current"] is True

    def test_source_hash_is_not_null(self, raw_sales_df):
        result = add_scd2_columns(raw_sales_df)
        for row in result.collect():
            assert row["_source_hash"] is not None
            assert len(row["_source_hash"]) == 32  # MD5 hex length


# ---------------------------------------------------------------------------
# apply_fx_conversion
# ---------------------------------------------------------------------------

class TestApplyFxConversion:

    def test_usd_rows_have_rate_1(self, spark, raw_sales_df, fx_rates_df):
        cleansed = cast_and_cleanse(raw_sales_df)
        result = apply_fx_conversion(cleansed, fx_rates_df, "2024-01-15")
        usd_row = result.filter("order_id = 'ORD-001'").collect()[0]
        assert float(usd_row["fx_rate_to_usd"]) == pytest.approx(1.0)

    def test_eur_rows_converted(self, spark, raw_sales_df, fx_rates_df):
        cleansed = cast_and_cleanse(raw_sales_df)
        result = apply_fx_conversion(cleansed, fx_rates_df, "2024-01-15")
        eur_row = result.filter("order_id = 'ORD-002'").collect()[0]
        assert float(eur_row["fx_rate_to_usd"]) == pytest.approx(1.085)
        # unit_price was 200 EUR → expect ~217 USD
        assert float(eur_row["unit_price_usd"]) == pytest.approx(200.0 * 1.085, rel=1e-3)


# ---------------------------------------------------------------------------
# run_quality_checks
# ---------------------------------------------------------------------------

class TestRunQualityChecks:

    def test_passes_on_clean_data(self, raw_sales_df, fx_rates_df):
        cleansed = cast_and_cleanse(raw_sales_df)
        converted = apply_fx_conversion(cleansed, fx_rates_df, "2024-01-15")
        results = run_quality_checks(converted)
        assert results["non_empty"]["passed"] is True
        assert results["non_negative_revenue"]["passed"] is True

    def test_fails_non_empty_check_on_empty_df(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, DecimalType
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("quantity", DecimalType(18, 4)),
            StructField("status",   StringType()),
            StructField("total_amount_usd", DecimalType(18, 4)),
        ])
        empty_df = spark.createDataFrame([], schema)
        results = run_quality_checks(empty_df)
        assert results["non_empty"]["passed"] is False
