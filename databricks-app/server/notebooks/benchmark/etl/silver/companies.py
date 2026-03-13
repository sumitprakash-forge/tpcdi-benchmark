"""
Silver layer loader for Companies.

Parses company records from bronze_finwire (CMP records).
Uses v2 silver parsing: same substring positions and column set.
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, trim, substring, monotonically_increasing_id
from benchmark.etl.spark_compat import try_to_date

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverCompanies(SilverLoaderBase):
    """
    Silver layer loader for Companies (v2 parsing logic).
    FINWIRE CMP: substring positions and columns match v2 transform_silver_companies.
    """

    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """
        Parse company records from bronze_finwire using v2 substring positions.
        """
        logger.info(f"Loading silver_companies from {bronze_table}")
        bronze_df = self.spark.table(bronze_table)
        cmp_df = bronze_df.filter(substring(col("raw_line"), 16, 3) == "CMP").filter(
            col("raw_line").isNotNull() & (length(col("raw_line")) >= 394)
        )
        silver_df = cmp_df.select(
            monotonically_increasing_id().alias("sk_company_id"),
            trim(substring(col("raw_line"), 79, 10)).alias("company_id"),
            trim(substring(col("raw_line"), 19, 60)).alias("company_name"),
            trim(substring(col("raw_line"), 93, 2)).alias("industry_id"),
            trim(substring(col("raw_line"), 95, 4)).alias("sp_rating"),
            trim(substring(col("raw_line"), 89, 4)).alias("status"),
            col("raw_line"),  # for to_date in next step
            col("_batch_id"),
            col("_load_timestamp"),
        )
        silver_df = silver_df.select(
            col("sk_company_id"),
            col("company_id"),
            col("company_name"),
            col("industry_id"),
            col("sp_rating"),
            col("status"),
            try_to_date(substring(col("raw_line"), 99, 8), "yyyyMMdd").alias("founding_date"),
            trim(substring(col("raw_line"), 348, 46)).alias("ceo_name"),
            trim(substring(col("raw_line"), 107, 80)).alias("address_line1"),
            trim(substring(col("raw_line"), 187, 80)).alias("address_line2"),
            trim(substring(col("raw_line"), 267, 12)).alias("postal_code"),
            trim(substring(col("raw_line"), 279, 25)).alias("city"),
            trim(substring(col("raw_line"), 304, 20)).alias("state_province"),
            trim(substring(col("raw_line"), 324, 24)).alias("country"),
            trim(substring(col("raw_line"), 394, 150)).alias("description"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = silver_df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        logger.info(f"Loaded silver_companies: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df
