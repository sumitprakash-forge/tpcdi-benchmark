"""
Silver layer loader for Holding History.

Parses holding history from bronze_holding_history.
TPC-DI: HH_H_T_ID|HH_T_ID|HH_BEFORE_QTY|HH_AFTER_QTY. Incremental: record_type + 4 cols.
Implements SCD Type 2 for CDC.
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date, to_timestamp, when, expr, current_timestamp
from pyspark.sql.types import LongType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverHoldingHistory(SilverLoaderBase):
    """
    Silver layer loader for Holding History.
    Parses HH_H_T_ID|HH_T_ID|HH_BEFORE_QTY|HH_AFTER_QTY.
    Joins with silver_trades to enrich. SCD Type 2 on hh_h_t_id.
    """

    def load(
        self,
        bronze_table: str,
        target_table: str,
        silver_trades_table: str,
        batch_id: int,
    ) -> DataFrame:
        logger.info(f"Loading silver_holding_history from {bronze_table}")

        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)

        num_cols = 5 if batch_id > 1 else 4
        parsed_df = self._parse_pipe_delimited(bronze_df, num_cols)

        if batch_id > 1:
            record_type_expr = col("_c0").alias("record_type")
            offset = 1
        else:
            record_type_expr = lit("I").alias("record_type")
            offset = 0

        silver_df = parsed_df.select(
            record_type_expr,
            expr("try_cast(trim(_c" + str(offset) + ") AS BIGINT)").alias("hh_h_t_id"),
            expr("try_cast(trim(_c" + str(offset + 1) + ") AS BIGINT)").alias("hh_t_id"),
            expr("coalesce(try_cast(trim(_c" + str(offset + 2) + ") AS BIGINT), 0)").alias("hh_before_qty"),
            expr("coalesce(try_cast(trim(_c" + str(offset + 3) + ") AS BIGINT), 0)").alias("hh_after_qty"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )

        # Join with silver_trades to get account_id, symbol, trade_dts
        try:
            trades_df = self.spark.table(silver_trades_table).filter(
                col("batch_id") == batch_id
            ).select(
                col("trade_id"),
                col("account_id"),
                col("symbol"),
                col("trade_dts").alias("holding_date"),
                col("trade_price").alias("purchase_price"),
            )
            silver_df = silver_df.join(
                trades_df,
                silver_df["hh_t_id"] == trades_df["trade_id"],
                "left",
            ).select(
                silver_df["record_type"],
                silver_df["hh_h_t_id"],
                silver_df["hh_t_id"],
                silver_df["hh_before_qty"],
                silver_df["hh_after_qty"],
                col("account_id"),
                col("symbol"),
                col("holding_date"),
                col("hh_after_qty").alias("quantity"),
                col("purchase_price"),
                to_date(col("holding_date")).alias("purchase_date"),
                silver_df["batch_id"],
                silver_df["load_timestamp"],
            )
        except Exception as e:
            logger.warning(f"Could not join with silver_trades, outputting base holding columns only: {e}")
            silver_df = silver_df.select(
                col("record_type"),
                col("hh_h_t_id"),
                col("hh_t_id"),
                col("hh_before_qty"),
                col("hh_after_qty"),
                lit(None).cast(LongType()).alias("account_id"),
                lit(None).cast("string").alias("symbol"),
                lit(None).cast("timestamp").alias("holding_date"),
                col("hh_after_qty").alias("quantity"),
                lit(None).cast("double").alias("purchase_price"),
                lit(None).cast("date").alias("purchase_date"),
                col("batch_id"),
                col("load_timestamp"),
            )

        silver_df = silver_df.withColumn(
            "effective_date",
            when(col("holding_date").isNotNull(), to_timestamp(col("holding_date")))
            .otherwise(current_timestamp())
        ).withColumn(
            "end_date",
            lit(None).cast(TimestampType())
        ).withColumn(
            "is_current",
            when(col("record_type") == "D", lit(False)).otherwise(lit(True))
        )

        if batch_id == 1:
            self.platform.write_table(silver_df, target_table, mode="overwrite")
            row_count = silver_df.count()
            table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
            return silver_df

        logger.info(f"Applying SCD Type 2 CDC for holding_history batch {batch_id} (record_type I/U/D)")
        return self._apply_scd_type2(
            incoming_df=silver_df,
            target_table=target_table,
            key_column="hh_h_t_id",
            effective_date_col="effective_date",
            record_type_col="record_type",
            exclude_record_types=["D"],
        )
