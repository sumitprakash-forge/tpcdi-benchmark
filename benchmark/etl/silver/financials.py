"""
Silver layer loader for Financials.

Parses financial records from bronze_finwire (FIN records).
Uses v2 silver parsing: same substring positions and column order.
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


class SilverFinancials(SilverLoaderBase):
    """
    Silver layer loader for Financials (v2 parsing logic).
    FINWIRE FIN: substring positions and column order match v2 transform_silver_financials.
    """

    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """
        Parse financial records from bronze_finwire using v2 substring positions.
        """
        logger.info(f"Loading silver_financials from {bronze_table}")
        bronze_df = self.spark.table(bronze_table)
        fin_df = bronze_df.filter(substring(col("raw_line"), 16, 3) == "FIN").filter(
            col("raw_line").isNotNull() & (length(col("raw_line")) >= 246)
        )
        silver_df = fin_df.select(
            trim(substring(col("raw_line"), 187, 60)).alias("co_name_or_cik"),
            expr("CAST(TRIM(substring(raw_line, 19, 4)) AS INT)").alias("year"),
            expr("CAST(TRIM(substring(raw_line, 23, 1)) AS INT)").alias("quarter"),
            try_to_date(substring(col("raw_line"), 24, 8), "yyyyMMdd").alias("qtr_start_date"),
            try_to_date(substring(col("raw_line"), 32, 8), "yyyyMMdd").alias("posting_date"),
            expr("CAST(TRIM(substring(raw_line, 40, 17)) AS DOUBLE)").alias("revenue"),
            expr("CAST(TRIM(substring(raw_line, 57, 17)) AS DOUBLE)").alias("earnings"),
            expr("CAST(TRIM(substring(raw_line, 74, 12)) AS DOUBLE)").alias("eps"),
            expr("CAST(TRIM(substring(raw_line, 86, 12)) AS DOUBLE)").alias("diluted_eps"),
            expr("CAST(TRIM(substring(raw_line, 98, 12)) AS DOUBLE)").alias("margin"),
            expr("CAST(TRIM(substring(raw_line, 110, 17)) AS DOUBLE)").alias("inventory"),
            expr("CAST(TRIM(substring(raw_line, 127, 17)) AS DOUBLE)").alias("assets"),
            expr("CAST(TRIM(substring(raw_line, 144, 17)) AS DOUBLE)").alias("liabilities"),
            expr("CAST(TRIM(substring(raw_line, 161, 13)) AS BIGINT)").alias("sh_out"),
            expr("CAST(TRIM(substring(raw_line, 174, 13)) AS BIGINT)").alias("diluted_sh_out"),
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
        logger.info(f"Loaded silver_financials: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df
