"""
Gold layer loader for Financials (SCD Type 1).

TPC-DI spec: Financials use SCD Type 1 - simple MERGE (upsert) so latest figures are reflected.
Batch mode: always overwrite. Incremental: MERGE (upsert).
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from benchmark.config import LoadType
from benchmark.etl.gold.base import GoldLoaderBase
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)

# Merge key: one row per company/period (co_name_or_cik, year, quarter)
GOLD_FINANCIALS_MERGE_KEYS = ["co_name_or_cik", "year", "quarter"]


class GoldFinancials(GoldLoaderBase):
    """Gold table: Financials (SCD Type 1 MERGE from silver_financials)."""

    def load(self, silver_table: str, target_table: str, load_type: Optional[LoadType] = None) -> DataFrame:
        """
        Load Gold financials from silver_financials.
        Batch: overwrite table. Incremental: MERGE (upsert) by (co_name_or_cik, year, quarter).
        """
        is_batch = load_type == LoadType.BATCH
        logger.info("Loading gold_financials from %s (%s)", silver_table, "overwrite" if is_batch else "MERGE upsert")
        start_time = __import__("time").time()
        start_dt = __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info("[TIMING] Starting load for %s at %s", target_table, start_dt)

        silver_df = self.spark.table(silver_table)
        # Gold = Silver columns + etl_timestamp (MERGE key: co_name_or_cik, year, quarter)
        gold_df = silver_df.select("*").withColumn("etl_timestamp", current_timestamp())

        if is_batch:
            fmt = getattr(self.platform, "table_format", None) or "delta"
            self.platform.write_table(gold_df, target_table, mode="overwrite", format=fmt)
        else:
            self.platform.merge_upsert(gold_df, target_table, key_columns=GOLD_FINANCIALS_MERGE_KEYS)

        row_count = gold_df.count()
        if table_timing_is_detailed():
            logger.info("[TIMING] Completed %s for %s at %s", "overwrite" if is_batch else "MERGE", target_table, start_dt)
        table_timing_end(target_table, row_count)
        return gold_df
