"""
Base class for Gold layer ETL loaders.

Gold layer provides business-ready, query-optimized tables:
- Current versions only (no SCD Type 2)
- Denormalized star schema (facts + dimensions)
- Pre-joined tables for analytics
"""

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


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


class GoldLoaderBase:
    """
    Base class for Gold layer data loaders.
    
    Provides common functionality:
    - Platform adapter access
    - Current version selection from Silver
    - Standardized write operations
    """
    
    def __init__(self, platform):
        """
        Initialize Gold loader.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()

    def _ensure_table_registered_from_warehouse(self, target_table: str) -> bool:
        """
        If the target table is not in the catalog but its data exists under the Spark warehouse
        (e.g. GCS path from a previous run / different session), register it so MERGE can run.
        Returns True if the table is now in the catalog (was already there or we registered it).
        On Spark Connect (Databricks serverless) warehouse config and JVM FS are not available — return False.
        """
        if self.spark.catalog.tableExists(target_table):
            return True
        # Spark Connect (serverless): spark.sql.warehouse.dir and JVM FS are not available
        try:
            if getattr(self.spark, "client", None) is not None:
                return False
        except Exception:
            pass
        # Parse database.table (or catalog.schema.table)
        parts = target_table.split(".")
        if len(parts) < 2:
            return False
        db, table_name = parts[-2], parts[-1]
        # Warehouse path: spark.sql.warehouse.dir or gs://bucket/spark-warehouse (not available on Connect)
        try:
            warehouse = self.spark.conf.get("spark.sql.warehouse.dir", "").rstrip("/")
        except Exception:
            return False
        if not warehouse and getattr(self.platform, "gcs_bucket", None):
            warehouse = f"gs://{self.platform.gcs_bucket}/spark-warehouse"
        if not warehouse:
            return False
        location = f"{warehouse}/{db}.db/{table_name}"
        # Check if path exists (e.g. Delta table on GCS from batch 1)
        try:
            jvm = self.spark._jvm
            path = jvm.org.apache.hadoop.fs.Path(location)
            fs = path.getFileSystem(self.spark._jsc.hadoopConfiguration())
            if not fs.exists(path):
                return False
        except Exception as e:
            logger.debug("Could not check warehouse path %s: %s", location, e)
            return False
        # Register external table so MERGE can run against existing data.
        # Do not CREATE DATABASE when target is catalog.schema.table (3+ parts): use user's catalog/schema only.
        try:
            if len(parts) < 3:
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
            fmt = getattr(self.platform, "table_format", None) or "delta"
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {target_table} USING {fmt} LOCATION '{location}'"
            )
            logger.info("Registered existing table %s from warehouse location %s", target_table, location)
            return self.spark.catalog.tableExists(target_table)
        except Exception as e:
            logger.warning("Could not register table %s from %s: %s", target_table, location, e)
            return False

    def _select_current_version(self, silver_table: str) -> DataFrame:
        """
        Select only current versions from Silver table (is_current = true).
        
        Args:
            silver_table: Silver table name
            
        Returns:
            DataFrame with only current records
        """
        silver_df = self.spark.table(silver_table)
        
        # Filter for current versions only
        if "is_current" in silver_df.columns:
            current_df = silver_df.filter(col("is_current") == True)
            logger.debug(f"Selected {current_df.count()} current records from {silver_table}")
            return current_df
        else:
            # If no is_current column, assume all records are current
            logger.warning(f"No is_current column in {silver_table}, using all records")
            return silver_df
    
    def _write_gold_table(self, df: DataFrame, target_table: str, 
                         mode: str = "overwrite") -> DataFrame:
        """
        Write DataFrame to Gold table.
        
        Args:
            df: Input DataFrame
            target_table: Full table name (catalog.schema.table)
            mode: Write mode (overwrite or append)
            
        Returns:
            DataFrame that was written
        """
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(df, target_table, mode=mode)
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: {mode}")
        bytes_processed = _get_table_size_bytes(self.platform, target_table)
        table_timing_end(target_table, row_count, bytes_processed=bytes_processed)

        return df
