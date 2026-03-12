"""
Base class for Bronze layer ETL loaders.

Provides common functionality for raw data ingestion.
"""

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Optional, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed
from benchmark.config import LoadType

logger = logging.getLogger(__name__)

# Standard bronze table schema (raw_line + metadata) for empty-table creation during incremental
BRONZE_EMPTY_SCHEMA = StructType([
    StructField("raw_line", StringType(), True),
    StructField("_load_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_batch_id", LongType(), True),
])


def ensure_bronze_table_exists(spark: SparkSession, platform: Any, table_name: str) -> None:
    """
    During incremental run, bronze_customer / bronze_account may not exist yet (created in same run).
    If the table is not in the catalog, create it by writing empty Delta data to the table path
    then registering it, so Delta append can succeed (CREATE TABLE ... USING delta alone may not
    create a layout that Delta's append path recognizes).
    """
    try:
        if spark.catalog.tableExists(table_name):
            return
    except Exception:
        pass
    logger.info("Creating bronze table %s for incremental run (table did not exist)", table_name)
    parts = table_name.split(".")
    if len(parts) < 2:
        return
    db, tbl = parts[-2], parts[-1]
    # Do not CREATE DATABASE when table is catalog.schema.table (3+ parts): schema already
    # exists in the user's catalog (runner created it). Creating with just schema name would
    # use default catalog 'main' and fail with PERMISSION_DENIED.
    if len(parts) < 3:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    # spark.sql.warehouse.dir is not available on Databricks serverless (Spark Connect); avoid reading it
    try:
        warehouse = spark.conf.get("spark.sql.warehouse.dir", "").rstrip("/")
    except Exception:
        warehouse = ""
    if not warehouse and getattr(platform, "gcs_bucket", None):
        warehouse = f"gs://{platform.gcs_bucket}/spark-warehouse"
    # Unity Catalog (3-part name) and dbfs: warehouse do not support CREATE TABLE with LOCATION
    use_path = warehouse and not warehouse.startswith("dbfs:") and len(parts) < 3
    if not warehouse or not use_path:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            f"(raw_line STRING, _load_timestamp TIMESTAMP, _source_file STRING, _batch_id BIGINT) USING delta"
        )
        return
    table_path = f"{warehouse}/{db}.db/{tbl}"
    fmt = getattr(platform, "table_format", "delta").lower()
    empty_df = spark.createDataFrame([], BRONZE_EMPTY_SCHEMA)
    try:
        # Write empty Delta to path so table has proper _delta_log; then register
        empty_df.write.format(fmt).mode("overwrite").save(table_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING {fmt} LOCATION '{table_path}'")
    except Exception as e:
        logger.warning("Failed to create bronze table %s at path %s: %s", table_name, table_path, e)
        # Fallback: register via SQL only (may still fail on append if Delta expects path layout)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} "
            f"(raw_line STRING, _load_timestamp TIMESTAMP, _source_file STRING, _batch_id BIGINT) USING {fmt}"
        )


def _get_table_size_bytes(platform, table_name: str) -> Optional[int]:
    """Return table size in bytes from platform if available; else None."""
    try:
        get_mb = getattr(platform, "get_table_size_mb", None)
        if get_mb:
            mb = get_mb(table_name)
            if mb and mb > 0:
                bytes_val = int(mb * 1024 * 1024)
                logger.debug(f"Table {table_name} size: {mb:.2f} MB ({bytes_val:,} bytes)")
                return bytes_val
            else:
                logger.warning(f"Table {table_name} size returned 0 or None from platform.get_table_size_mb()")
        else:
            logger.debug(f"Platform does not have get_table_size_mb method")
    except Exception as e:
        logger.warning(f"Could not get table size for {table_name}: {e}", exc_info=True)
    return None


class BronzeLoaderBase:
    """
    Base class for Bronze layer data loaders.
    
    Provides common functionality:
    - Platform adapter access
    - Metadata column addition
    - Standardized write operations
    """
    
    def __init__(self, platform):
        """
        Initialize Bronze loader.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
    
    def _add_metadata_columns(self, df: DataFrame, source_file: str, batch_id: int) -> DataFrame:
        """
        Add standard metadata columns to raw DataFrame.
        
        Args:
            df: Input DataFrame
            source_file: Name of source file
            batch_id: Batch number
            
        Returns:
            DataFrame with metadata columns added
        """
        # Table schema uses _batch_id BIGINT (LongType); lit(batch_id) is INT — cast so append doesn't fail with DELTA_MERGE_INCOMPATIBLE_DATATYPE
        return df.withColumn("_load_timestamp", current_timestamp()) \
                 .withColumn("_source_file", lit(source_file)) \
                 .withColumn("_batch_id", lit(batch_id).cast(LongType()))
    
    def _write_bronze_table(self, df: DataFrame, target_table: str, 
                            batch_id: int, source_file: str) -> DataFrame:
        """
        Write DataFrame to Bronze table with metadata.
        
        Args:
            df: Input DataFrame
            target_table: Full table name (catalog.schema.table)
            batch_id: Batch number
            source_file: Name of source file
            
        Returns:
            DataFrame that was written
        """
        bronze_df = self._add_metadata_columns(df, source_file, batch_id)
        
        # Batch mode: always overwrite. Incremental: overwrite for batch_id 1, append for 2+
        load_type = getattr(self.platform, "_tpcdi_load_type", None)
        mode = "overwrite" if load_type == LoadType.BATCH else ("overwrite" if batch_id == 1 else "append")
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(bronze_df, target_table, mode=mode)
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = bronze_df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: {mode}")
        bytes_processed = _get_table_size_bytes(self.platform, target_table)
        table_timing_end(target_table, row_count, bytes_processed=bytes_processed)
        
        return bronze_df
