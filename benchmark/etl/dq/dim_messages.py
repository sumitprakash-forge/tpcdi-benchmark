"""
DimMessages (gold.dim_messages) - TPC-DI audit table for DQ failures.

Every Silver DQ rule that triggers inserts a row here:
message_timestamp, batch_id, component_name, message_text, severity, source_table.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

logger = logging.getLogger(__name__)

DIM_MESSAGES_SCHEMA = StructType([
    StructField("message_timestamp", TimestampType(), False),
    StructField("batch_id", IntegerType(), True),
    StructField("component_name", StringType(), False),
    StructField("message_text", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("source_table", StringType(), False),
])


def ensure_dim_messages_exists(spark, table_name: str, platform) -> None:
    """
    Create gold.dim_messages if it does not exist.

    Always runs CREATE TABLE IF NOT EXISTS so the table is registered before any
    append (avoids Delta "table doesn't exist" on Dataproc; tableExists can be
    wrong or stale). If DDL fails, falls back to empty DataFrame overwrite.

    Args:
        spark: SparkSession
        table_name: Full table name (e.g. database.gold_dim_messages or catalog.database.gold_dim_messages)
        platform: Platform adapter (for table_format: delta | parquet)
    """
    fmt = getattr(platform, "table_format", "delta") or "delta"
    fmt = str(fmt).lower().strip()
    if fmt not in ("delta", "parquet"):
        fmt = "delta"
    parts = [p.strip() for p in table_name.split(".") if p.strip()]
    quoted = ".".join(f"`{p}`" for p in parts)
    # Use full 3-part name (catalog.database.table) when only 2 parts given (Spark 3.4+)
    try:
        current_catalog = getattr(spark.catalog, "currentCatalog", None)
        if callable(current_catalog) and len(parts) == 2:
            cat = current_catalog()
            if cat:
                quoted = f"`{cat}`." + quoted
    except Exception:
        pass
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {quoted} ("
        "message_timestamp TIMESTAMP NOT NULL, "
        "batch_id INT, "
        "component_name STRING NOT NULL, "
        "message_text STRING NOT NULL, "
        "severity STRING NOT NULL, "
        "source_table STRING NOT NULL"
        f") USING {fmt}"
    )
    try:
        spark.sql(ddl)
        logger.info(f"Created DimMessages table: {table_name} (USING {fmt})")
    except Exception as e:
        logger.warning(f"CREATE TABLE IF NOT EXISTS failed for {table_name}: {e}; falling back to empty DataFrame overwrite")
        empty = spark.createDataFrame([], DIM_MESSAGES_SCHEMA)
        platform.write_table(empty, table_name, mode="overwrite")
        logger.info(f"Created DimMessages table: {table_name}")


def log_message(spark, platform, table_name: str, batch_id: Optional[int],
                component_name: str, message_text: str, severity: str = "Alert",
                source_table: str = "") -> None:
    """
    Append a single message to gold.dim_messages.

    Args:
        spark: SparkSession
        platform: Platform adapter for write_table
        table_name: gold_dim_messages full name
        batch_id: Batch number (optional)
        component_name: e.g. 'Silver_Customer_Validation'
        message_text: e.g. 'Invalid TaxID format'
        severity: 'Alert' or 'Reject'
        source_table: Silver table that was validated (e.g. silver_customers)
    """
    ensure_dim_messages_exists(spark, table_name, platform)
    row = spark.range(1).select(
        current_timestamp().alias("message_timestamp"),
        lit(batch_id).alias("batch_id"),
        lit(component_name).alias("component_name"),
        lit(message_text).alias("message_text"),
        lit(severity).alias("severity"),
        lit(source_table or "").alias("source_table"),
    )
    platform.write_table(row, table_name, mode="append")
    logger.info(f"DimMessages: [{severity}] {component_name} - {message_text}")


def log_messages(spark, platform, table_name: str, messages_df: DataFrame) -> None:
    """
    Append a DataFrame of messages to gold.dim_messages.
    DataFrame must have columns: message_timestamp, batch_id, component_name, message_text, severity, source_table.

    Args:
        spark: SparkSession
        platform: Platform adapter for write_table
        table_name: gold_dim_messages full name
        messages_df: DataFrame with DIM_MESSAGES_SCHEMA columns
    """
    if messages_df.isEmpty():
        return
    ensure_dim_messages_exists(spark, table_name, platform)
    platform.write_table(messages_df, table_name, mode="append")
    logger.info(f"DimMessages: appended {messages_df.count()} message(s)")
