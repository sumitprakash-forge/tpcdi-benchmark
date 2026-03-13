"""
Silver layer loader for Trades.

Parses and cleans trade data from bronze_trade.
Implements SCD Type 2 for CDC (Change Data Capture) on incremental loads.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, to_timestamp, coalesce, expr
from pyspark.sql.types import LongType, IntegerType, DoubleType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverTrades(SilverLoaderBase):
    """
    Silver layer loader for Trades.
    
    Parses trade data from bronze_trade pipe-delimited data.
    
    Trade.txt format (14 columns per TPC-DI spec):
    T_ID|T_DTS|T_ST_ID|T_TT_ID|T_IS_CASH|T_S_SYMB|T_QTY|T_BID_PRICE|
    T_CA_ID|T_EXEC_NAME|T_TRADE_PRICE|T_CHRG|T_COMM|T_TAX
    
    CDC Handling:
    - Batch 1 (Historical): Full load, overwrite
    - Batch 2+ (Incremental): SCD Type 2 on trade_id
      - I/U: new version with is_current=true, effective_date=trade_dts
      - D: close current row (is_current=false, end_date set), no insert
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean trade data from bronze_trade.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned trade data
        """
        logger.info(f"Loading silver_trades from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Parse pipe-delimited (15 columns: record_type + 14 data columns)
        # For incremental loads (batch_id > 1), first column is record_type
        # Format: I|T_ID|T_DTS|T_ST_ID|T_TT_ID|T_IS_CASH|T_S_SYMB|T_QTY|T_BID_PRICE|
        #        T_CA_ID|T_EXEC_NAME|T_TRADE_PRICE|T_CHRG|T_COMM|T_TAX
        num_cols = 15 if batch_id > 1 else 14  # Batch 1 has no record_type
        parsed_df = self._parse_pipe_delimited(bronze_df, num_cols)
        
        # Check actual column count
        actual_cols = [c for c in parsed_df.columns if c.startswith("_c")]
        max_col_idx = max([int(c[2:]) for c in actual_cols]) if actual_cols else -1
        logger.info(f"Trade.txt parsed with {max_col_idx + 1} columns (indices 0-{max_col_idx})")
        
        # Helper to check if column exists
        def col_exists(idx: int) -> bool:
            return f"_c{idx}" in parsed_df.columns
        
        # Build select list dynamically
        select_cols = []
        
        # For incremental loads (batch_id > 1), capture record_type from _c0
        if batch_id > 1 and col_exists(0):
            select_cols.append(col("_c0").alias("record_type"))
            # Shift all column indices by 1
            col_offset = 1
        else:
            # Batch 1: no record_type, use default
            select_cols.append(lit("I").alias("record_type"))
            col_offset = 0
        
        # Transform to silver schema with proper types
        # Column mapping after record_type:
        # _c{0+offset}: T_ID, _c{1+offset}: T_DTS, etc.
        field_mappings = [
            (0, "trade_id", LongType, True),
            (1, "trade_dts", None, False),  # Will use to_timestamp
            (2, "status_id", None, False),
            (3, "trade_type_id", None, False),
            (4, "is_cash", None, False),  # Will use when
            (5, "symbol", None, False),
            (6, "quantity", IntegerType, True),
            (7, "bid_price", DoubleType, True),
            (8, "account_id", LongType, True),
            (9, "exec_name", None, False),
            (10, "trade_price", DoubleType, True),
            (11, "charge", DoubleType, True),
            (12, "commission", DoubleType, True),
            (13, "tax", DoubleType, True),
        ]
        
        for idx, alias_name, cast_type, is_numeric in field_mappings:
            col_idx = idx + col_offset
            if col_exists(col_idx):
                if alias_name == "trade_dts":
                    select_cols.append(expr(f"try_cast(trim(_c{col_idx}) AS TIMESTAMP)").alias(alias_name))
                elif alias_name == "is_cash":
                    select_cols.append(when(col(f"_c{col_idx}") == "1", lit(True)).otherwise(lit(False)).alias(alias_name))
                elif cast_type:
                    if cast_type == LongType:
                        sql_type = "BIGINT"
                    elif cast_type == IntegerType:
                        sql_type = "INT"
                    elif cast_type == DoubleType:
                        sql_type = "DOUBLE"
                    else:
                        sql_type = "DOUBLE"
                    default_val = "0" if is_numeric else "0"
                    select_cols.append(
                        expr(f"coalesce(try_cast(trim(_c{col_idx}) AS {sql_type}), {default_val})").alias(alias_name)
                    )
                else:
                    select_cols.append(coalesce(col(f"_c{col_idx}"), lit("")).alias(alias_name))
            else:
                # Column doesn't exist, use default
                if alias_name == "trade_dts":
                    select_cols.append(lit(None).cast("timestamp").alias(alias_name))
                elif alias_name == "is_cash":
                    select_cols.append(lit(False).alias(alias_name))
                elif cast_type:
                    select_cols.append(lit(0 if is_numeric else 0.0).cast(cast_type()).alias(alias_name))
                else:
                    select_cols.append(lit("").alias(alias_name))
        
        select_cols.extend([
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        ])
        
        silver_df = parsed_df.select(*select_cols)
        
        # Add SCD Type 2 columns: effective_date, end_date, is_current (use try_cast to avoid CAST_INVALID_INPUT from misaligned columns)
        silver_df = silver_df.withColumn(
            "effective_date",
            expr("try_cast(trade_dts AS TIMESTAMP)")
        ).withColumn(
            "end_date",
            lit(None).cast(TimestampType())
        ).withColumn(
            "is_current",
            when(col("record_type") == "D", lit(False)).otherwise(lit(True))
        )
        
        # Batch 1: Full historical load (overwrite)
        # Batch 2+: Incremental SCD Type 2
        if batch_id == 1:
            return self._write_silver_table(silver_df, target_table, batch_id)
        else:
            logger.info(f"Applying SCD Type 2 CDC for trades batch {batch_id} (record_type I/U/D)")
            return self._apply_scd_type2(
                incoming_df=silver_df,
                target_table=target_table,
                key_column="trade_id",
                effective_date_col="effective_date",
                record_type_col="record_type",
                exclude_record_types=["D"],
            )
