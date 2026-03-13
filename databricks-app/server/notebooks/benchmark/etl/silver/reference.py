"""
Silver layer loaders for reference data.

Includes: Date, StatusType, TradeType, Industry
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, to_date, to_timestamp, concat_ws, coalesce, expr, trim
from pyspark.sql.types import IntegerType

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverDate(SilverLoaderBase):
    """Silver layer loader for Date dimension."""
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse and clean Date dimension from bronze_date."""
        logger.info(f"Loading silver_date from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 18)
        
        # v2-style parsing: same column order; try_cast for date_value and holiday_flag
        silver_df = parsed_df.select(
            expr("CAST(TRIM(_c0) AS INT)").alias("sk_date_id"),
            expr("try_cast(TRIM(_c1) AS DATE)").alias("date_value"),
            col("_c2").alias("date_desc"),
            expr("CAST(TRIM(_c3) AS INT)").alias("calendar_year_id"),
            col("_c4").alias("calendar_year_desc"),
            expr("CAST(TRIM(_c5) AS INT)").alias("calendar_qtr_id"),
            col("_c6").alias("calendar_qtr_desc"),
            expr("CAST(TRIM(_c7) AS INT)").alias("calendar_month_id"),
            col("_c8").alias("calendar_month_desc"),
            expr("CAST(TRIM(_c9) AS INT)").alias("calendar_week_id"),
            col("_c10").alias("calendar_week_desc"),
            expr("CAST(TRIM(_c11) AS INT)").alias("day_of_week_num"),
            col("_c12").alias("day_of_week_desc"),
            expr("CAST(TRIM(_c13) AS INT)").alias("fiscal_year_id"),
            col("_c14").alias("fiscal_year_desc"),
            expr("CAST(TRIM(_c15) AS INT)").alias("fiscal_qtr_id"),
            col("_c16").alias("fiscal_qtr_desc"),
            expr("try_cast(TRIM(_c17) AS BOOLEAN)").alias("holiday_flag"),
            col("_batch_id").alias("batch_id"),
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
        logger.info(f"Loaded silver_date: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df


class SilverTime(SilverLoaderBase):
    """Silver layer loader for Time dimension (from bronze_time). v2-style: pipe-delimited 10 cols."""

    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse Time.txt: sk_time_id, time_value, hour_id, hour_desc, minute_id, minute_desc, second_id, second_desc, market_hours_flag, office_hours_flag."""
        logger.info(f"Loading silver_time from {bronze_table}")
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 10)
        silver_df = parsed_df.select(
            expr("CAST(TRIM(_c0) AS INT)").alias("sk_time_id"),
            col("_c1").alias("time_value"),
            expr("CAST(TRIM(_c2) AS INT)").alias("hour_id"),
            col("_c3").alias("hour_desc"),
            expr("CAST(TRIM(_c4) AS INT)").alias("minute_id"),
            col("_c5").alias("minute_desc"),
            expr("CAST(TRIM(_c6) AS INT)").alias("second_id"),
            col("_c7").alias("second_desc"),
            when(trim(col("_c8")).isin("1", "true", "t", "y", "yes"), lit(True)).otherwise(lit(False)).alias("market_hours_flag"),
            when(trim(col("_c9")).isin("1", "true", "t", "y", "yes"), lit(True)).otherwise(lit(False)).alias("office_hours_flag"),
            col("_batch_id").alias("batch_id"),
        )
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        end_time = time.time()
        row_count = silver_df.count()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
        logger.info(f"Loaded silver_time: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df


class SilverStatusType(SilverLoaderBase):
    """Silver layer loader for StatusType."""
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse StatusType.txt (ST_ID|ST_NAME)."""
        logger.info(f"Loading silver_status_type from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 2)
        
        silver_df = parsed_df.select(
            col("_c0").alias("st_id"),
            col("_c1").alias("st_name"),
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
        logger.info(f"Loaded silver_status_type: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df


class SilverTradeType(SilverLoaderBase):
    """Silver layer loader for TradeType."""
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse TradeType.txt (TT_ID|TT_NAME|TT_IS_SELL|TT_IS_MRKT)."""
        logger.info(f"Loading silver_trade_type from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 4)
        
        silver_df = parsed_df.select(
            col("_c0").alias("tt_id"),
            col("_c1").alias("tt_name"),
            when(col("_c2") == "1", lit(True)).otherwise(lit(False)).alias("tt_is_sell"),
            when(col("_c3") == "1", lit(True)).otherwise(lit(False)).alias("tt_is_mrkt"),
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
        logger.info(f"Loaded silver_trade_type: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df


class SilverIndustry(SilverLoaderBase):
    """Silver layer loader for Industry. Spec 2.2.11: IN_ID|IN_NAME|IN_SC_ID (3 columns only)."""
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse Industry.txt. Spec Table 2.2.11: IN_ID, IN_NAME, IN_SC_ID (pipe-delimited, 3 cols)."""
        logger.info(f"Loading silver_industry from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 3)
        
        silver_df = parsed_df.select(
            col("_c0").alias("in_id"),
            col("_c1").alias("in_name"),
            col("_c2").alias("in_sc_id"),
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
        logger.info(f"Loaded silver_industry: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df


class SilverTaxRate(SilverLoaderBase):
    """Silver layer loader for TaxRate."""

    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse TaxRate.txt (TX_ID|TX_NAME|TX_RATE)."""
        logger.info(f"Loading silver_tax_rate from {bronze_table}")

        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 3)

        silver_df = parsed_df.select(
            col("_c0").alias("tx_id"),
            col("_c1").alias("tx_name"),
            expr("try_cast(trim(_c2) AS DOUBLE)").alias("tx_rate"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )

        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")

        self.platform.write_table(silver_df, target_table, mode="overwrite")

        end_time = time.time()
        row_count = silver_df.count()
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table}")

        logger.info(f"Loaded silver_tax_rate: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df


class SilverWatchHistory(SilverLoaderBase):
    """Silver layer loader for WatchHistory. Implements SCD Type 2 for incremental."""

    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """Parse WatchHistory.txt. Batch 1: W_C_ID|W_S_SYMB|W_DTS|W_ACTION (spec 2.2.18). Batch 2+: CDC_FLAG|CDC_DSN + 4 cols."""
        logger.info(f"Loading silver_watch_history from {bronze_table}")

        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        num_cols = 6 if batch_id > 1 else 4  # Spec: incremental has CDC_FLAG, CDC_DSN
        parsed_df = self._parse_pipe_delimited(bronze_df, num_cols)

        if batch_id > 1 and "_c0" in parsed_df.columns:
            record_type_expr = col("_c0").alias("record_type")
            col_offset = 2  # skip CDC_FLAG, CDC_DSN
        else:
            record_type_expr = lit("I").alias("record_type")
            col_offset = 0

        silver_df = parsed_df.select(
            record_type_expr,
            expr("try_cast(trim(_c" + str(col_offset) + ") AS BIGINT)").alias("w_c_id"),   # Spec: W_C_ID = Customer identifier
            col("_c" + str(col_offset + 1)).alias("w_s_symb"),
            col("_c" + str(col_offset + 2)).alias("w_dts"),
            col("_c" + str(col_offset + 3)).alias("w_action"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )

        silver_df = silver_df.withColumn(
            "wh_key",
            concat_ws("_", col("w_c_id").cast("string"), col("w_s_symb"))
        ).withColumn(
            "effective_date",
            expr("try_cast(w_dts AS TIMESTAMP)")
        ).withColumn(
            "end_date",
            lit(None).cast("timestamp")
        ).withColumn(
            "is_current",
            when(col("record_type") == "D", lit(False)).otherwise(lit(True))
        )

        if batch_id == 1:
            self.platform.write_table(silver_df, target_table, mode="overwrite")
            row_count = silver_df.count()
            table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
            return silver_df

        logger.info(f"Applying SCD Type 2 CDC for watch_history batch {batch_id} (record_type I/U/D)")
        return self._apply_scd_type2(
            incoming_df=silver_df,
            target_table=target_table,
            key_column="wh_key",
            effective_date_col="effective_date",
            record_type_col="record_type",
            exclude_record_types=["D"],
        )
