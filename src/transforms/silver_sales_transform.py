"""
silver_sales_transform.py
--------------------------
PySpark transformation logic for Bronze → Silver sales orders.
This module is imported by the Fabric notebook nb_transform_silver_sales.ipynb.

Keeping business logic in .py files (rather than inline notebook cells) allows:
- Unit testing without a live Spark session (via mocks)
- Reuse across notebooks and pipelines
- Standard code review via pull requests
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATUS_MAP = {
    # ERP codes → standardised status
    "C":    "Completed",
    "COMP": "Completed",
    "COMPLETED": "Completed",
    "P":    "Pending",
    "PEND": "Pending",
    "PENDING": "Pending",
    "X":    "Cancelled",
    "CANC": "Cancelled",
    "CANCELLED": "Cancelled",
    "R":    "Refunded",
    "REFD": "Refunded",
}

HASH_COLUMNS = [
    "customer_id", "product_id", "quantity",
    "unit_price", "status", "region",
]


# ---------------------------------------------------------------------------
# Public transform functions
# ---------------------------------------------------------------------------

def cast_and_cleanse(df: DataFrame) -> DataFrame:
    """Apply schema enforcement, type casting, and null handling.

    Args:
        df: Raw Bronze DataFrame.

    Returns:
        Cleansed DataFrame with standardised types and values.
    """
    logger.info("cast_and_cleanse: input rows=%d", df.count())

    # Build status mapping expression
    status_expr = F.lit("Unknown")
    for src, tgt in STATUS_MAP.items():
        status_expr = F.when(
            F.upper(F.trim(F.col("status"))) == src, tgt
        ).otherwise(status_expr)

    cleansed = (
        df
        .withColumn("order_date",   F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("quantity",     F.col("quantity").cast(DecimalType(18, 4)))
        .withColumn("unit_price",   F.col("unit_price").cast(DecimalType(18, 4)))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(18, 4)))
        .withColumn("status",       status_expr)
        .withColumn("order_year",   F.year("order_date"))
        .withColumn("order_month",  F.month("order_date"))
        .fillna({"region": "UNKNOWN", "sales_rep_id": "UNASSIGNED"})
    )

    # Drop rows where required fields are null
    required_cols = ["order_id", "order_date", "customer_id", "product_id"]
    before = cleansed.count()
    cleansed = cleansed.dropna(subset=required_cols)
    dropped = before - cleansed.count()

    if dropped > 0:
        logger.warning("cast_and_cleanse: dropped %d rows with null required fields", dropped)

    logger.info("cast_and_cleanse: output rows=%d", cleansed.count())
    return cleansed


def apply_fx_conversion(
    df: DataFrame,
    fx_df: DataFrame,
    rate_date: str,
    price_col: str = "unit_price",
    amount_col: str = "total_amount",
    currency_col: str = "currency",
) -> DataFrame:
    """Normalise monetary values to USD using FX rates.

    Args:
        df:           Input DataFrame with currency amounts.
        fx_df:        FX rates DataFrame with columns [rate_date, from_currency, rate_to_usd].
        rate_date:    Date for which to apply rates.
        price_col:    Column containing unit price.
        amount_col:   Column containing total amount.
        currency_col: Column containing the source currency code.

    Returns:
        DataFrame with `unit_price_usd`, `total_amount_usd`, `fx_rate_to_usd` columns added.
    """
    rates = (
        fx_df
        .filter(F.col("rate_date") == rate_date)
        .select("from_currency", "rate_to_usd")
        .union(  # always include USD = 1.0
            df.sparkSession.createDataFrame([("USD", 1.0)], ["from_currency", "rate_to_usd"])
        )
    )

    enriched = (
        df
        .join(rates, df[currency_col] == rates["from_currency"], "left")
        .withColumn("fx_rate_to_usd",   F.coalesce(F.col("rate_to_usd"), F.lit(1.0)))
        .withColumn("unit_price_usd",   F.round(F.col(price_col)  * F.col("fx_rate_to_usd"), 4))
        .withColumn("total_amount_usd", F.round(F.col(amount_col) * F.col("fx_rate_to_usd"), 4))
        .withColumn("original_currency", F.col(currency_col))
        .drop("from_currency", "rate_to_usd", currency_col, price_col, amount_col)
    )
    return enriched


def deduplicate(df: DataFrame, key_col: str = "order_id", ts_col: str = "_ingestion_ts") -> DataFrame:
    """Remove duplicates, keeping the most recently ingested row per key.

    Args:
        df:      Input DataFrame.
        key_col: Column(s) that identify a unique record.
        ts_col:  Timestamp column used to pick the latest duplicate.

    Returns:
        Deduplicated DataFrame.
    """
    w = Window.partitionBy(key_col).orderBy(F.desc(ts_col))
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def add_scd2_columns(
    df: DataFrame,
    hash_cols: List[str] = HASH_COLUMNS,
    valid_from: Optional[datetime] = None,
) -> DataFrame:
    """Add SCD Type 2 tracking columns and row hash.

    Args:
        df:          Input DataFrame.
        hash_cols:   Columns to include in the change-detection hash.
        valid_from:  Validity start timestamp (default: now UTC).

    Returns:
        DataFrame with `_valid_from`, `_valid_to`, `_is_current`, `_updated_ts`,
        `_source_hash` columns added.
    """
    now_ts = valid_from or datetime.utcnow()

    hash_expr = F.md5(
        F.concat_ws(
            "|",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols],
        )
    )

    return (
        df
        .withColumn("_valid_from",  F.lit(now_ts).cast(TimestampType()))
        .withColumn("_valid_to",    F.lit(None).cast(TimestampType()))
        .withColumn("_is_current",  F.lit(True))
        .withColumn("_updated_ts",  F.lit(now_ts).cast(TimestampType()))
        .withColumn("_source_hash", hash_expr)
    )


def run_quality_checks(df: DataFrame) -> dict:
    """Run basic data quality checks on the transformed DataFrame.

    Args:
        df: Transformed Silver DataFrame.

    Returns:
        Dict with check name → pass/fail and details.
    """
    results = {}
    total_rows = df.count()

    # Check: no null order IDs
    null_order_ids = df.filter(F.col("order_id").isNull()).count()
    results["no_null_order_ids"] = {
        "passed": null_order_ids == 0,
        "detail": f"{null_order_ids} rows with null order_id"
    }

    # Check: quantity > 0
    non_positive_qty = df.filter(F.col("quantity") <= 0).count()
    results["positive_quantity"] = {
        "passed": non_positive_qty == 0,
        "detail": f"{non_positive_qty} rows with quantity <= 0"
    }

    # Check: known status values
    unknown_status = df.filter(F.col("status") == "Unknown").count()
    results["known_status"] = {
        "passed": unknown_status == 0,
        "detail": f"{unknown_status} rows with 'Unknown' status"
    }

    # Check: non-negative revenue
    negative_rev = df.filter(F.col("total_amount_usd") < 0).count()
    results["non_negative_revenue"] = {
        "passed": negative_rev == 0,
        "detail": f"{negative_rev} rows with total_amount_usd < 0"
    }

    # Check: minimum row count (must have data)
    results["non_empty"] = {
        "passed": total_rows > 0,
        "detail": f"Total rows: {total_rows}"
    }

    failed = [k for k, v in results.items() if not v["passed"]]
    if failed:
        logger.error("Data quality FAILED checks: %s", failed)
    else:
        logger.info("All data quality checks PASSED (%d checks, %d rows)", len(results), total_rows)

    return results
