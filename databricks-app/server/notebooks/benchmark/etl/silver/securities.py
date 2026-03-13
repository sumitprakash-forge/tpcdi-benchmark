"""
Silver layer loader for Securities.

Parses security records from bronze_finwire (SEC records).
Uses v2 silver parsing: same substring positions and column set.
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, trim, substring, expr
from benchmark.etl.spark_compat import try_to_date

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverSecurities(SilverLoaderBase):
    """
    Silver layer loader for Securities (v2 parsing logic).
    FINWIRE SEC: substring positions and columns match v2 transform_silver_securities.
    """

    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """
        Parse security records from bronze_finwire using v2 substring positions.
        """
        logger.info(f"Loading silver_securities from {bronze_table}")
        bronze_df = self.spark.table(bronze_table)
        sec_df = bronze_df.filter(substring(col("raw_line"), 16, 3) == "SEC").filter(
            col("raw_line").isNotNull() & (length(col("raw_line")) >= 220)
        )
        silver_df = sec_df.select(
            trim(substring(col("raw_line"), 19, 15)).alias("symbol"),
            trim(substring(col("raw_line"), 34, 6)).alias("issue_type"),
            trim(substring(col("raw_line"), 40, 4)).alias("status"),
            trim(substring(col("raw_line"), 44, 70)).alias("name"),
            trim(substring(col("raw_line"), 114, 6)).alias("ex_id"),
            expr("CAST(TRIM(substring(raw_line, 120, 13)) AS BIGINT)").alias("sh_out"),
            try_to_date(substring(col("raw_line"), 133, 8), "yyyyMMdd").alias("first_trade_date"),
            trim(substring(col("raw_line"), 141, 8)).alias("first_trade_exchg"),
            expr("CAST(TRIM(substring(raw_line, 149, 12)) AS DOUBLE)").alias("dividend"),
            trim(substring(col("raw_line"), 161, 60)).alias("co_name_or_cik"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )

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
        logger.info(f"Loaded silver_securities: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df
