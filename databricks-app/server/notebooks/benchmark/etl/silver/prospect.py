"""
Silver layer loader for Prospect.

Parses prospect data from bronze_prospect (comma-delimited).
Uses v2 silver parsing: 22 comma columns and marketing_nameplate logic.
Batch 1: overwrite. Incremental (batch 2+): append only.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, coalesce, when, concat_ws, expr

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverProspect(SilverLoaderBase):
    """
    Silver layer loader for Prospect (v2 parsing logic).
    Comma-delimited: columns 1-22 same as v2; marketing_nameplate from HighValue/YoungAdult/HighCredit.
    """

    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        logger.info(f"Loading silver_prospect from {bronze_table}")
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        num_cols = 22  # v2: 22 comma-separated fields (same for batch and incremental)
        parsed_df = self._parse_csv_delimited(bronze_df, num_cols)
        # _c0.._c21 = v2 columns 1..22 (no extra leading column in incremental per v2 SQL)
        def c(i: int):
            return coalesce(col(f"_c{i}"), lit(""))
        offset = 0  # v2 batch and incremental both use split_part(raw_line, ',', 1)..22
        income = expr("try_cast(TRIM(_c" + str(offset + 12) + ") AS INT)")
        age = expr("try_cast(TRIM(_c" + str(offset + 16) + ") AS INT)")
        credit_rating = expr("try_cast(TRIM(_c" + str(offset + 17) + ") AS INT)")
        net_worth = expr("try_cast(TRIM(_c" + str(offset + 21) + ") AS BIGINT)")
        marketing_nameplate = concat_ws(
            ",",
            when((net_worth > 1000000) | (income > 200000), lit("HighValue")),
            when(age < 25, lit("YoungAdult")),
            when(credit_rating > 700, lit("HighCredit")),
        )
        silver_df = parsed_df.select(
            c(offset).alias("agency_id"),
            c(offset + 1).alias("last_name"),
            c(offset + 2).alias("first_name"),
            c(offset + 3).alias("middle_initial"),
            c(offset + 4).alias("gender"),
            c(offset + 5).alias("address_line1"),
            c(offset + 6).alias("address_line2"),
            c(offset + 7).alias("postal_code"),
            c(offset + 8).alias("city"),
            c(offset + 9).alias("state"),
            c(offset + 10).alias("country"),
            c(offset + 11).alias("phone"),
            income.alias("income"),
            expr("try_cast(TRIM(_c" + str(offset + 13) + ") AS INT)").alias("number_cars"),
            expr("try_cast(TRIM(_c" + str(offset + 14) + ") AS INT)").alias("number_children"),
            c(offset + 15).alias("marital_status"),
            age.alias("age"),
            credit_rating.alias("credit_rating"),
            c(offset + 18).alias("own_or_rent_flag"),
            c(offset + 19).alias("employer"),
            expr("try_cast(TRIM(_c" + str(offset + 20) + ") AS BOOLEAN)").alias("is_customer"),
            net_worth.alias("net_worth"),
            marketing_nameplate.alias("marketing_nameplate"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        return self._write_silver_table(silver_df, target_table, batch_id)
