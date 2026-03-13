"""
Gold layer dimension table loaders.

Dimensions from Silver tables, ready for star schema joins.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, row_number, split, element_at, size, lower, trim,
    min as spark_min, coalesce, to_date, max as spark_max, monotonically_increasing_id,
)
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, TimestampType, DoubleType

# Placeholder IDs for late-arriving dimension (TPC-DI: trade arrives before account/customer)
PLACEHOLDER_CUSTOMER_ID = -1
PLACEHOLDER_ACCOUNT_ID = -1

from benchmark.etl.gold.base import GoldLoaderBase

logger = logging.getLogger(__name__)


class GoldDimCustomer(GoldLoaderBase):
    """Gold dimension table: DimCustomer. Batch and incremental aligned with v2 (SCD2 columns)."""

    def load(self, silver_table: str, target_table: str, load_type=None, batch_id: int = 1) -> DataFrame:
        """
        Create/update DimCustomer from silver_customers.
        - Batch load: Overwrite with SCD2 columns (is_current, start_date, end_date, batch_id).
        - Incremental load: MERGE close current row then INSERT new versions (v2-style).
        """
        from benchmark.config import LoadType
        is_incremental = load_type == LoadType.INCREMENTAL

        logger.info("Loading gold.DimCustomer from %s (%s)", silver_table, "incremental (close+insert)" if is_incremental else "overwrite")
        current_df = self.spark.table(silver_table)
        current_df = current_df.filter(col("customer_id") != lit(PLACEHOLDER_CUSTOMER_ID))

        if is_incremental:
            return self._load_dim_customer_incremental(current_df, target_table, batch_id)

        # Batch: filter current batch and is_current
        current_df = current_df.filter(col("is_current") == lit(True)).filter(col("batch_id") == lit(batch_id))
        current_df = current_df.dropDuplicates(["customer_id"])
        want = [
            "sk_customer_id", "customer_id", "tax_id", "status", "last_name", "first_name",
            "middle_name", "gender", "tier", "dob", "address_line1", "address_line2",
            "postal_code", "city", "state_prov", "country", "email1", "email2",
            "local_tax_id", "national_tax_id",
        ]
        select_cols = [c for c in want if c in current_df.columns]
        # Keep effective_date/load_timestamp for start_date, then drop
        extra = [c for c in ["effective_date", "load_timestamp", "batch_id"] if c in current_df.columns]
        gold_df = current_df.select(*select_cols, *[col(c) for c in extra])
        gold_df = gold_df.withColumn("is_current", lit(True))
        gold_df = gold_df.withColumn(
            "start_date",
            to_date(coalesce(col("effective_date"), col("load_timestamp"))),
        )
        gold_df = gold_df.withColumn("end_date", lit("9999-12-31").cast("date"))
        gold_df = gold_df.withColumn("batch_id", col("batch_id").cast("int") if "batch_id" in gold_df.columns else lit(batch_id))
        gold_df = gold_df.withColumn("etl_timestamp", current_timestamp())
        for c in ["effective_date", "load_timestamp"]:
            if c in gold_df.columns:
                gold_df = gold_df.drop(c)
        ph = self.spark.range(1).select(
            lit(PLACEHOLDER_CUSTOMER_ID).alias("sk_customer_id"),
            lit(PLACEHOLDER_CUSTOMER_ID).alias("customer_id"),
            lit("Unknown").alias("tax_id"), lit("Unknown").alias("status"),
            lit("Unknown").alias("last_name"), lit("Unknown").alias("first_name"),
            lit("Unknown").alias("middle_name"), lit("Unknown").alias("gender"),
            lit(1).alias("tier"), lit(None).cast("date").alias("dob"),
            lit("").alias("address_line1"), lit("").alias("address_line2"),
            lit("").alias("postal_code"), lit("").alias("city"),
            lit("").alias("state_prov"), lit("").alias("country"),
            lit("").alias("email1"), lit("").alias("email2"),
            lit("").alias("local_tax_id"), lit("").alias("national_tax_id"),
            lit(True).alias("is_current"),
            lit(None).cast("date").alias("start_date"),
            lit("9999-12-31").cast("date").alias("end_date"),
            lit(1).alias("batch_id"),
            current_timestamp().alias("etl_timestamp"),
        )
        gold_df = gold_df.unionByName(ph, allowMissingColumns=True)
        self._write_gold_table(gold_df, target_table, mode="overwrite")
        return gold_df

    def _load_dim_customer_incremental(self, silver_df: DataFrame, target_table: str, batch_id: int) -> DataFrame:
        """V2-style: MERGE close current row then INSERT new versions from silver (batch_id)."""
        # If target is not in the catalog, try to register from warehouse location (e.g. GCS from batch 1)
        # so MERGE runs against existing data; otherwise skip MERGE and only INSERT.
        target_exists = False
        try:
            target_exists = self.spark.catalog.tableExists(target_table)
        except Exception:
            pass
        if not target_exists:
            target_exists = self._ensure_table_registered_from_warehouse(target_table)
        if target_exists:
            # MERGE requires SCD2 columns (is_current, end_date). If table was created by an older run or
            # different pipeline (e.g. SF1000 batch 1 with old schema), fail with clear instructions.
            target_cols = [f.name for f in self.spark.table(target_table).schema.fields]
            required = ["is_current", "end_date"]
            missing = [c for c in required if c not in target_cols]
            if missing:
                raise ValueError(
                    f"Gold table {target_table} is missing SCD2 columns: {missing}. "
                    "Incremental MERGE requires is_current and end_date. "
                    "Drop the table and re-run from Batch 1, or run a full batch load first to recreate it with the correct schema."
                )
            # Close: one row per customer_id with latest effective_date from this batch
            updates_to_close = silver_df.filter(col("batch_id") == batch_id).groupBy("customer_id").agg(
                spark_max(coalesce(col("effective_date"), col("load_timestamp"))).alias("new_effective_date"),
            )
            view_close = "_gold_close_customer_" + target_table.replace(".", "_")
            updates_to_close.createOrReplaceTempView(view_close)
            merge_close_sql = f"""
            MERGE INTO {target_table} AS target
            USING (SELECT customer_id, CAST(new_effective_date AS DATE) AS new_effective_date FROM {view_close}) AS source
            ON target.customer_id = source.customer_id AND target.is_current = true
            WHEN MATCHED THEN UPDATE SET
                target.is_current = false,
                target.end_date = source.new_effective_date,
                target.etl_timestamp = current_timestamp()
            """
            try:
                self.spark.sql(merge_close_sql)
            finally:
                self.spark.catalog.dropTempView(view_close)

        # Insert new versions from silver (batch_id, is_current, exclude placeholder)
        insert_df = silver_df.filter(col("batch_id") == batch_id).filter(col("is_current") == lit(True)).filter(
            col("customer_id") != lit(PLACEHOLDER_CUSTOMER_ID),
        )
        want = [
            "sk_customer_id", "customer_id", "tax_id", "status", "last_name", "first_name",
            "middle_name", "gender", "tier", "dob", "address_line1", "address_line2",
            "postal_code", "city", "state_prov", "country", "email1", "email2",
            "local_tax_id", "national_tax_id",
        ]
        select_cols = [c for c in want if c in insert_df.columns]
        # Need effective_date/load_timestamp for start_date; include then drop
        extra = [c for c in ["effective_date", "load_timestamp"] if c in insert_df.columns]
        gold_insert = insert_df.select(*select_cols, *[col(c) for c in extra])
        gold_insert = gold_insert.withColumn("is_current", lit(True))
        gold_insert = gold_insert.withColumn(
            "start_date",
            to_date(coalesce(col("effective_date"), col("load_timestamp"))),
        )
        gold_insert = gold_insert.withColumn("end_date", lit("9999-12-31").cast("date"))
        gold_insert = gold_insert.withColumn("batch_id", lit(batch_id).cast("int"))
        gold_insert = gold_insert.withColumn("etl_timestamp", current_timestamp())
        for c in ["effective_date", "load_timestamp"]:
            if c in gold_insert.columns:
                gold_insert = gold_insert.drop(c)
        self.platform.write_table(gold_insert, target_table, mode="append", format=getattr(self.platform, "table_format", None) or "delta")
        return gold_insert


class GoldDimAccount(GoldLoaderBase):
    """Gold dimension table: DimAccount. Batch and incremental aligned with v2 (SCD2 + sk_customer_id)."""

    def load(self, silver_table: str, target_table: str, load_type=None, batch_id: int = 1,
             dim_customer_table: str = None) -> DataFrame:
        """
        Create/update DimAccount from silver_accounts.
        - Batch load: Join to gold_dim_customer for sk_customer_id; SCD2 columns (is_current, start_date, end_date, batch_id).
        - Incremental load: MERGE close current row then INSERT new versions (v2-style).
        """
        from benchmark.config import LoadType
        is_incremental = load_type == LoadType.INCREMENTAL

        logger.info("Loading gold.DimAccount from %s (%s)", silver_table, "incremental (close+insert)" if is_incremental else "overwrite")
        current_df = self.spark.table(silver_table)
        current_df = current_df.filter(col("account_id") != lit(PLACEHOLDER_ACCOUNT_ID))

        if is_incremental:
            return self._load_dim_account_incremental(current_df, target_table, batch_id, dim_customer_table)

        # Batch: filter is_current and batch_id; join to dim_customer for sk_customer_id (point-in-time)
        current_df = current_df.filter(col("is_current") == lit(True)).filter(col("batch_id") == lit(batch_id))
        current_df = current_df.dropDuplicates(["account_id"])
        if dim_customer_table:
            dim_customer = self.spark.table(dim_customer_table)
            sa = current_df.alias("sa")
            dc = dim_customer.alias("dc")
            eff = coalesce(sa["effective_date"], sa["load_timestamp"])
            current_df = sa.join(
                dc,
                (sa["customer_id"] == dc["customer_id"])
                & (dc["is_current"] == lit(True))
                & (to_date(eff) >= dc["start_date"])
                & (dc["end_date"].isNull() | (to_date(eff) < dc["end_date"])),
                "left",
            )
            sk_customer_id = coalesce(dc["sk_customer_id"], lit(PLACEHOLDER_CUSTOMER_ID))
            base_cols = ["account_id", "broker_id", "customer_id", "account_name", "tax_status", "status_id"]
            extra = [c for c in ["effective_date", "load_timestamp", "batch_id"] if c in sa.columns]
            gold_df = current_df.select(
                sa["account_id"].alias("sk_account_id"),
                sk_customer_id.alias("sk_customer_id"),
                *[sa[c] for c in base_cols if c in sa.columns],
                *[sa[c] for c in extra],
            )
        else:
            sk_customer_id = lit(PLACEHOLDER_CUSTOMER_ID)
            base_cols = ["account_id", "broker_id", "customer_id", "account_name", "tax_status", "status_id"]
            extra = [c for c in ["effective_date", "load_timestamp", "batch_id"] if c in current_df.columns]
            gold_df = current_df.select(
                col("account_id").alias("sk_account_id"),
                coalesce(sk_customer_id, lit(PLACEHOLDER_CUSTOMER_ID)).alias("sk_customer_id"),
                *[col(c) for c in base_cols if c in current_df.columns],
                *[col(c) for c in extra],
            )
        gold_df = gold_df.withColumn("is_current", lit(True))
        gold_df = gold_df.withColumn(
            "start_date",
            to_date(coalesce(col("effective_date"), col("load_timestamp"))),
        )
        gold_df = gold_df.withColumn("end_date", lit("9999-12-31").cast("date"))
        gold_df = gold_df.withColumn("batch_id", col("batch_id").cast("int") if "batch_id" in gold_df.columns else lit(batch_id))
        gold_df = gold_df.withColumn("etl_timestamp", current_timestamp())
        for c in ["effective_date", "load_timestamp"]:
            if c in gold_df.columns:
                gold_df = gold_df.drop(c)
        ph = self.spark.range(1).select(
            lit(PLACEHOLDER_ACCOUNT_ID).alias("sk_account_id"),
            lit(PLACEHOLDER_ACCOUNT_ID).alias("account_id"),
            lit(PLACEHOLDER_ACCOUNT_ID).alias("broker_id"),
            lit(PLACEHOLDER_CUSTOMER_ID).alias("sk_customer_id"),
            lit(PLACEHOLDER_CUSTOMER_ID).alias("customer_id"),
            lit("Unknown").alias("account_name"),
            lit(0).alias("tax_status"),
            lit("ACTV").alias("status_id"),
            lit(True).alias("is_current"),
            lit(None).cast("date").alias("start_date"),
            lit("9999-12-31").cast("date").alias("end_date"),
            lit(1).alias("batch_id"),
            current_timestamp().alias("etl_timestamp"),
        )
        gold_df = gold_df.unionByName(ph, allowMissingColumns=True)
        self._write_gold_table(gold_df, target_table, mode="overwrite")
        return gold_df

    def _load_dim_account_incremental(self, silver_df: DataFrame, target_table: str, batch_id: int,
                                      dim_customer_table: str = None) -> DataFrame:
        """V2-style: MERGE close current row then INSERT new versions (join to dim_customer for sk_customer_id)."""
        # If target is not in the catalog, try to register from warehouse location (e.g. GCS from batch 1)
        # so MERGE runs against existing data; otherwise skip MERGE and only INSERT.
        target_exists = False
        try:
            target_exists = self.spark.catalog.tableExists(target_table)
        except Exception:
            pass
        if not target_exists:
            target_exists = self._ensure_table_registered_from_warehouse(target_table)
        if target_exists:
            # MERGE requires SCD2 columns (is_current, end_date). Fail fast if table has old schema.
            target_cols = [f.name for f in self.spark.table(target_table).schema.fields]
            required = ["is_current", "end_date"]
            missing = [c for c in required if c not in target_cols]
            if missing:
                raise ValueError(
                    f"Gold table {target_table} is missing SCD2 columns: {missing}. "
                    "Incremental MERGE requires is_current and end_date. "
                    "Drop the table and re-run from Batch 1, or run a full batch load first to recreate it with the correct schema."
                )
            # Close: one row per account_id with latest effective_date from this batch (U/D)
            updates_to_close = silver_df.filter(col("batch_id") == batch_id).filter(
                col("record_type").isin("U", "D"),
            ).groupBy("account_id").agg(
                spark_max(coalesce(col("effective_date"), col("load_timestamp"))).alias("new_effective_date"),
            )
            view_close = "_gold_close_account_" + target_table.replace(".", "_")
            updates_to_close.createOrReplaceTempView(view_close)
            merge_close_sql = f"""
            MERGE INTO {target_table} AS target
            USING (SELECT account_id, CAST(new_effective_date AS DATE) AS new_effective_date FROM {view_close}) AS source
            ON target.account_id = source.account_id AND target.is_current = true
            WHEN MATCHED THEN UPDATE SET
                target.is_current = false,
                target.end_date = source.new_effective_date,
                target.etl_timestamp = current_timestamp()
            """
            try:
                self.spark.sql(merge_close_sql)
            finally:
                self.spark.catalog.dropTempView(view_close)

        # Insert: silver batch_id and record_type I/U; join to dim_customer for sk_customer_id (point-in-time)
        insert_df = silver_df.filter(col("batch_id") == batch_id).filter(col("record_type").isin("I", "U")).filter(
            col("account_id") != lit(PLACEHOLDER_ACCOUNT_ID),
        )
        if dim_customer_table:
            dim_customer = self.spark.table(dim_customer_table)
            sa_acc = insert_df.alias("sa_acc")
            dc = dim_customer.alias("dc")
            eff = coalesce(sa_acc["effective_date"], sa_acc["load_timestamp"])
            insert_df = sa_acc.join(
                dc,
                (sa_acc["customer_id"] == dc["customer_id"])
                & (to_date(eff) >= dc["start_date"])
                & (dc["end_date"].isNull() | (to_date(eff) < dc["end_date"])),
                "left",
            )
            sk_customer_id = coalesce(dc["sk_customer_id"], lit(PLACEHOLDER_CUSTOMER_ID))
            base_cols = ["account_id", "broker_id", "customer_id", "account_name", "tax_status", "status_id"]
            extra = [c for c in ["effective_date", "load_timestamp"] if c in sa_acc.columns]
            gold_insert = insert_df.select(
                monotonically_increasing_id().alias("sk_account_id"),
                sk_customer_id.alias("sk_customer_id"),
                *[sa_acc[c] for c in base_cols if c in sa_acc.columns],
                *[sa_acc[c] for c in extra],
            )
        else:
            sk_customer_id = lit(PLACEHOLDER_CUSTOMER_ID)
            base_cols = ["account_id", "broker_id", "customer_id", "account_name", "tax_status", "status_id"]
            extra = [c for c in ["effective_date", "load_timestamp"] if c in insert_df.columns]
            gold_insert = insert_df.select(
                monotonically_increasing_id().alias("sk_account_id"),
                sk_customer_id.alias("sk_customer_id"),
                *[col(c) for c in base_cols if c in insert_df.columns],
                *[col(c) for c in extra],
            )
        gold_insert = gold_insert.withColumn("is_current", lit(True))
        gold_insert = gold_insert.withColumn(
            "start_date",
            to_date(coalesce(col("effective_date"), col("load_timestamp"))),
        )
        gold_insert = gold_insert.withColumn("end_date", lit("9999-12-31").cast("date"))
        gold_insert = gold_insert.withColumn("batch_id", lit(batch_id).cast("int"))
        gold_insert = gold_insert.withColumn("etl_timestamp", current_timestamp())
        for c in ["effective_date", "load_timestamp"]:
            if c in gold_insert.columns:
                gold_insert = gold_insert.drop(c)
        self.platform.write_table(gold_insert, target_table, mode="append", format=getattr(self.platform, "table_format", None) or "delta")
        return gold_insert


class GoldDimCompany(GoldLoaderBase):
    """Gold dimension table: DimCompany (from silver_companies)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimCompany from silver_companies.
        If silver_companies does not exist (e.g. FinWire not loaded), create empty gold_dim_company.
        """
        logger.info(f"Loading gold.DimCompany from {silver_table}")
        try:
            silver_df = self.spark.table(silver_table)
        except Exception as e:
            err_msg = str(e).lower()
            if "table_or_view_not_found" in err_msg or "cannot be found" in err_msg:
                logger.warning(f"Silver table {silver_table} not found; creating empty {target_table}: {e}")
                schema = StructType([
                    StructField("sk_company_id", LongType()),
                    StructField("company_id", StringType()),
                    StructField("company_name", StringType()),
                    StructField("industry_id", StringType()),
                    StructField("sector", StringType()),
                    StructField("status", StringType()),
                    StructField("address_line1", StringType()),
                    StructField("address_line2", StringType()),
                    StructField("postal_code", StringType()),
                    StructField("city", StringType()),
                    StructField("state_prov", StringType()),
                    StructField("country", StringType()),
                    StructField("description", StringType()),
                    StructField("founding_date", DateType()),
                    StructField("ceo_name", StringType()),
                    StructField("etl_timestamp", TimestampType()),
                ])
                gold_df = self.spark.createDataFrame([], schema)
                return self._write_gold_table(gold_df, target_table, mode="overwrite")
            raise
        gold_df = silver_df.select(
            col("sk_company_id"),
            col("company_id"),
            col("company_name"),
            col("industry_id"),
            col("sp_rating").alias("sector"),
            col("status"),
            col("address_line1"),
            col("address_line2"),
            col("postal_code"),
            col("city"),
            col("state_province").alias("state_prov"),
            col("country"),
            col("description"),
            col("founding_date"),
            col("ceo_name"),
            current_timestamp().alias("etl_timestamp"),
        )
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimSecurity(GoldLoaderBase):
    """Gold dimension table: DimSecurity (from silver_securities)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimSecurity from silver_securities.
        If silver_securities does not exist (e.g. FinWire not loaded), create empty gold_dim_security.
        """
        logger.info(f"Loading gold.DimSecurity from {silver_table}")
        try:
            silver_df = self.spark.table(silver_table)
        except Exception as e:
            err_msg = str(e).lower()
            if "table_or_view_not_found" in err_msg or "cannot be found" in err_msg:
                logger.warning(f"Silver table {silver_table} not found; creating empty {target_table}: {e}")
                schema = StructType([
                    StructField("sk_security_id", StringType()),
                    StructField("security_id", StringType()),
                    StructField("symbol", StringType()),
                    StructField("issue_type", StringType()),
                    StructField("status", StringType()),
                    StructField("name", StringType()),
                    StructField("exchange_id", StringType()),
                    StructField("shares_outstanding", LongType()),
                    StructField("first_trade_date", DateType()),
                    StructField("first_trade_exchange", StringType()),
                    StructField("dividend", DoubleType()),
                    StructField("company_id", StringType()),
                    StructField("etl_timestamp", TimestampType()),
                ])
                gold_df = self.spark.createDataFrame([], schema)
                return self._write_gold_table(gold_df, target_table, mode="overwrite")
            raise
        gold_df = silver_df.select(
            col("symbol").alias("sk_security_id"),
            col("symbol").alias("security_id"),
            col("symbol"),
            col("issue_type"),
            col("status"),
            col("name"),
            col("ex_id").alias("exchange_id"),
            col("sh_out").alias("shares_outstanding"),
            col("first_trade_date"),
            col("first_trade_exchg").alias("first_trade_exchange"),
            col("dividend"),
            col("co_name_or_cik").alias("company_id"),
            current_timestamp().alias("etl_timestamp"),
        )
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimTime(GoldLoaderBase):
    """Gold dimension table: DimTime (from silver_time)."""

    def load(self, silver_table: str, target_table: str, batch_id: int = 1) -> DataFrame:
        """Create DimTime from silver_time."""
        logger.info("Loading gold.DimTime from %s", silver_table)
        silver_df = self.spark.table(silver_table).filter(col("batch_id") == batch_id)
        gold_df = silver_df.select(
            col("sk_time_id"),
            col("sk_time_id").alias("time_id"),
            col("time_value"),
            col("hour_id"),
            col("hour_desc"),
            col("minute_id"),
            col("minute_desc"),
            col("second_id"),
            col("second_desc"),
            col("market_hours_flag"),
            col("office_hours_flag"),
            current_timestamp().alias("etl_timestamp"),
        )
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimBroker(GoldLoaderBase):
    """Gold dimension table: DimBroker (from bronze_hr, employee_job_code = 314 per TPC-DI)."""

    # TPC-DI standard code for Brokers (314); also accept legacy "1" and "%broker%" for backward compat.
    BROKER_JOB_CODE_314 = "314"

    def load(
        self,
        bronze_hr_table: str,
        target_table: str,
        batch_id: int = 1,
        dim_date_table: str | None = None,
    ) -> DataFrame:
        """Create DimBroker from bronze_hr: brokers (job_code = 314 per TPC-DI).
        HR.csv spec: 1=EmployeeID, 2=ManagerID, 3=FirstName, 4=LastName, 5=MI, 6=JobCode, 7=Branch, 8=Office, 9=Phone.
        Output matches v2 SQL: sk_broker_id, broker_id, manager_id, first_name, last_name, middle_initial,
        branch, office, phone, is_current, batch_id, start_date, end_date, etl_timestamp.
        start_date = MIN(date_value) from gold_dim_date when dim_date_table is provided; else current_date.
        """
        logger.info("Loading gold.DimBroker from %s", bronze_hr_table)
        bronze_df = self.spark.table(bronze_hr_table).filter(col("_batch_id") == batch_id)
        arr = split(col("raw_line"), ",")
        job_code = trim(element_at(arr, 6))
        is_broker = (
            (job_code == lit(self.BROKER_JOB_CODE_314))
            | lower(job_code).like("%broker%")
            | (job_code == lit("1"))
        )
        brokers_df = (
            bronze_df.filter(col("raw_line").isNotNull() & (size(arr) >= 9))
            .filter(is_broker)
            .select(
                element_at(arr, 1).alias("employee_id"),
                element_at(arr, 2).alias("manager_id"),
                element_at(arr, 3).alias("first_name"),
                element_at(arr, 4).alias("last_name"),
                element_at(arr, 5).alias("middle_initial"),
                element_at(arr, 7).alias("branch"),
                element_at(arr, 8).alias("office"),
                element_at(arr, 9).alias("phone"),
            )
            .distinct()
        )
        # Surrogate key: ROW_NUMBER() OVER (ORDER BY employee_id) to match SQL
        window_spec = Window.orderBy(col("employee_id"))
        brokers_with_sk = brokers_df.withColumn(
            "sk_broker_id", row_number().over(window_spec)
        )
        # start_date: MIN(date_value) from gold_dim_date when provided (matches v2 SQL)
        if dim_date_table:
            min_date_row = (
                self.spark.table(dim_date_table)
                .agg(spark_min("date_value").alias("min_date"))
                .first()
            )
            start_date_val = (
                min_date_row["min_date"]
                if min_date_row and min_date_row["min_date"] is not None
                else None
            )
        else:
            start_date_val = None
        start_date_col = (
            lit(start_date_val).cast("date")
            if start_date_val is not None
            else lit(None).cast("date")
        )
        end_date_col = lit("9999-12-31").cast("date")
        gold_df = brokers_with_sk.select(
            col("sk_broker_id"),
            col("employee_id").cast("bigint").alias("broker_id"),
            col("manager_id").cast("bigint"),
            col("first_name"),
            col("last_name"),
            col("middle_initial"),
            col("branch"),
            col("office"),
            col("phone"),
            lit(True).alias("is_current"),
            lit(batch_id).cast("int").alias("batch_id"),
            start_date_col.alias("start_date"),
            end_date_col.alias("end_date"),
            current_timestamp().alias("etl_timestamp"),
        )
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldProspect(GoldLoaderBase):
    """Gold dimension table: Prospect (from silver_prospect)."""

    def load(self, silver_table: str, target_table: str, batch_id: int = 1) -> DataFrame:
        """Create gold_prospect from silver_prospect."""
        logger.info("Loading gold.Prospect from %s", silver_table)
        silver_df = self.spark.table(silver_table).filter(col("batch_id") == batch_id)
        want = [
            "agency_id", "last_name", "first_name", "middle_initial", "gender",
            "address_line1", "address_line2", "postal_code", "city", "state", "country", "phone",
            "income", "number_cars", "number_children", "marital_status", "age", "credit_rating",
            "own_or_rent_flag", "employer", "is_customer", "net_worth", "marketing_nameplate",
        ]
        select_cols = [c for c in want if c in silver_df.columns]
        gold_df = silver_df.select(*select_cols).withColumn("etl_timestamp", current_timestamp())
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimDate(GoldLoaderBase):
    """Gold dimension table: DimDate (from silver_date)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimDate from silver_date.
        
        Args:
            silver_table: silver_date table name
            target_table: gold.DimDate table name
        """
        logger.info(f"Loading gold.DimDate from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        # Date dimension: all columns (silver_date already has sk_date_id)
        gold_df = silver_df.select(
            col("sk_date_id"),
            col("sk_date_id").alias("date_id"),  # date_id = sk_date_id
            col("date_value"),
            col("date_desc"),
            col("calendar_year_id"),
            col("calendar_year_desc"),
            col("calendar_qtr_id"),
            col("calendar_qtr_desc"),
            col("calendar_month_id"),
            col("calendar_month_desc"),
            col("calendar_week_id"),
            col("calendar_week_desc"),
            col("day_of_week_num"),
            col("day_of_week_desc"),
            col("fiscal_year_id"),
            col("fiscal_year_desc"),
            col("fiscal_qtr_id"),
            col("fiscal_qtr_desc"),
            col("holiday_flag"),
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimTradeType(GoldLoaderBase):
    """Gold dimension table: DimTradeType (from silver_trade_type)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimTradeType from silver_trade_type.
        
        Args:
            silver_table: silver_trade_type table name
            target_table: gold.DimTradeType table name
        """
        logger.info(f"Loading gold.DimTradeType from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        gold_df = silver_df.select(
            col("tt_id").alias("sk_trade_type_id"),
            col("tt_id").alias("trade_type_id"),
            col("tt_id").alias("trade_type_code"),
            col("tt_name").alias("trade_type_name"),
            col("tt_is_sell").alias("is_sell"),
            col("tt_is_mrkt").alias("is_market"),
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimStatusType(GoldLoaderBase):
    """Gold dimension table: DimStatusType (from silver_status_type)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimStatusType from silver_status_type.
        
        Args:
            silver_table: silver_status_type table name
            target_table: gold.DimStatusType table name
        """
        logger.info(f"Loading gold.DimStatusType from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        gold_df = silver_df.select(
            col("st_id").alias("sk_status_type_id"),
            col("st_id").alias("status_type_id"),
            col("st_id").alias("status_type_code"),
            col("st_name").alias("status_type_name"),
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")


class GoldDimIndustry(GoldLoaderBase):
    """Gold dimension table: DimIndustry (from silver_industry)."""
    
    def load(self, silver_table: str, target_table: str) -> DataFrame:
        """
        Create DimIndustry from silver_industry.
        
        Args:
            silver_table: silver_industry table name
            target_table: gold.DimIndustry table name
        """
        logger.info(f"Loading gold.DimIndustry from {silver_table}")
        
        silver_df = self.spark.table(silver_table)
        
        # Spec 3.2.13 Industry: IN_ID, IN_NAME, IN_SC_ID only (no sector name in source)
        gold_df = silver_df.select(
            col("in_id").alias("sk_industry_id"),
            col("in_id").alias("industry_id"),
            col("in_name").alias("industry_name"),
            col("in_sc_id").alias("sector_id"),
            lit(None).cast("string").alias("sector_name"),  # Not in spec; leave empty or extend from lookup
            current_timestamp().alias("etl_timestamp"),
        )
        
        return self._write_gold_table(gold_df, target_table, mode="overwrite")
