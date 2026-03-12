"""
Silver layer loader for Accounts.

Parses and cleans account data from bronze_customer_mgmt (Batch 1) or bronze_account (Batch 2+).
Implements SCD Type 2 for CDC (Change Data Capture) on incremental loads.

TPC-DI Format Differences:
- Batch 1: CustomerMgmt.xml (XML event log)
- Batch 2+: Account.txt (pipe-delimited state snapshot)
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, to_date, to_timestamp, explode, current_timestamp, coalesce, trim, expr
)
from pyspark.sql.types import LongType, IntegerType, StringType, DateType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase
from benchmark.etl.bronze.base import ensure_bronze_table_exists

logger = logging.getLogger(__name__)


class SilverAccounts(SilverLoaderBase):
    """
    Silver layer loader for Accounts.
    
    Handles two different source formats:
    - Batch 1: XML (CustomerMgmt.xml) - event log
    - Batch 2+: Pipe-delimited (Account.txt) - state snapshot
    
    Implements SCD Type 2 for incremental loads.
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load accounts from bronze table.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number (1 = XML, 2+ = pipe-delimited)
            
        Returns:
            DataFrame with cleaned account data
        """
        logger.info(f"Loading silver_accounts from {bronze_table} (batch {batch_id})")
        
        if batch_id == 1:
            # Batch 1: Load from XML
            return self._load_from_xml(bronze_table, target_table, batch_id)
        else:
            # Batch 2+: Load from pipe-delimited
            return self._load_from_pipe_delimited(bronze_table, target_table, batch_id)
    
    def _load_from_xml(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load accounts from XML format (Batch 1).
        
        CustomerMgmt.xml structure:
        - Root: <TPCDI:Actions>
        - Rows: <TPCDI:Action> or <Action>
        - Attributes: _ActionType, _ActionTS, _C_ID, _CA_ID
        - Elements: Account data nested within Action/Customer
        
        Args:
            bronze_table: bronze_customer_mgmt table
            target_table: Target silver table
            batch_id: Batch number (should be 1)
            
        Returns:
            DataFrame with cleaned account data
        """
        logger.info("Parsing accounts from XML format (Batch 1)")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Bronze schema: Customer (Account, Name, Address, ...), _ActionType, _ActionTS. No Action array.
        # Account fields per docs/CUSTOMERMGMT_STRUCTURE: Customer.Account._CA_ID, CA_B_ID, CA_NAME, _CA_TAX_ST
        extraction_errors = []
        account_df = None
        temp_view = f"_temp_accountmgmt_xml_{id(bronze_df)}"
        bronze_df.createOrReplaceTempView(temp_view)
        
        # Strategy 1: Nested struct paths (Customer.Account.*, Customer._C_ID)
        try:
            logger.info("Attempting XML parse with Customer.Account nested struct paths")
            sql_query = f"""
            SELECT 
                _ActionType as action_type,
                _ActionTS as action_ts,
                Customer.Account._CA_ID as ca_id,
                Customer.Account.CA_B_ID as ca_b_id,
                Customer._C_ID as c_id,
                Customer.Account.CA_NAME as ca_name,
                Customer.Account._CA_TAX_ST as ca_tax_st,
                CASE WHEN _ActionType = 'INACT' THEN 'INACT' ELSE 'ACTV' END as ca_st_id,
                _batch_id as batch_id,
                _load_timestamp as load_timestamp
            FROM {temp_view}
            WHERE _ActionType IN ('ADDACCT', 'UPDACCT', 'INACT') AND Customer IS NOT NULL AND Customer.Account IS NOT NULL
            """
            
            account_df = self.spark.sql(sql_query)
            if account_df.count() > 0:
                logger.info(f"Successfully parsed {account_df.count()} account records from XML")
            else:
                raise ValueError("No records extracted")
        except Exception as e:
            extraction_errors.append(f"Strategy 1 failed: {e}")
            logger.warning(f"XML parse (Account nested) failed: {e}")
            account_df = None
        
        # Strategy 2: Same paths with COALESCE for optional structs
        if account_df is None or account_df.count() == 0:
            temp_view = f"_temp_accountmgmt_xml2_{id(bronze_df)}"
            bronze_df.createOrReplaceTempView(temp_view)
            try:
                logger.info("Attempting XML parse with COALESCE for Account struct")
                sql_query = f"""
                SELECT 
                    _ActionType as action_type,
                    _ActionTS as action_ts,
                    Customer.Account._CA_ID as ca_id,
                    Customer.Account.CA_B_ID as ca_b_id,
                    Customer._C_ID as c_id,
                    TRIM(COALESCE(Customer.Account.CA_NAME, '')) as ca_name,
                    TRIM(COALESCE(CAST(Customer.Account._CA_TAX_ST AS STRING), '0')) as ca_tax_st,
                    CASE WHEN _ActionType = 'INACT' THEN 'INACT' ELSE 'ACTV' END as ca_st_id,
                    _batch_id as batch_id,
                    _load_timestamp as load_timestamp
                FROM {temp_view}
                WHERE _ActionType IN ('ADDACCT', 'UPDACCT', 'INACT') AND Customer IS NOT NULL AND Customer.Account IS NOT NULL
                """
                account_df = self.spark.sql(sql_query)
                if account_df.count() > 0:
                    logger.info(f"Successfully parsed {account_df.count()} account records (COALESCE fallback)")
                else:
                    raise ValueError("No records extracted")
            except Exception as e:
                extraction_errors.append(f"Strategy 2 failed: {e}")
                logger.warning(f"XML parse (Account COALESCE) failed: {e}")
                account_df = None
        
        # Note: We do not drop the temp view here because Spark uses lazy evaluation.
        # account_df may still reference the temp view when executed downstream (e.g. in
        # _transform_to_silver_schema / _write_silver_table). Dropping the view immediately
        # would cause TABLE_OR_VIEW_NOT_FOUND. Temp views are session-scoped and cleaned up
        # when the SparkSession ends.
        
        if account_df is None or account_df.count() == 0:
            raise RuntimeError(f"Failed to extract accounts: {extraction_errors}")
        
        # Transform to silver schema
        silver_df = self._transform_to_silver_schema(
            account_df,
            has_action_type=True,
            batch_id=batch_id
        )
        
        # Batch 1: Full historical load (overwrite)
        return self._write_silver_table(silver_df, target_table, batch_id)
    
    def _load_from_pipe_delimited(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load accounts from pipe-delimited format (Batch 2+).
        
        Account.txt format (pipe-delimited, state-at-time snapshot):
        record_type|CA_ID|CA_B_ID|CA_C_ID|UNKNOWN_ID|CA_NAME|CA_TAX_ST|CA_ST_ID
        - _c0: record_type (I/U/D), _c1: CA_ID, _c2: CA_B_ID, _c3: C_ID, _c4: skip, _c5: CA_NAME,
          _c6: CA_TAX_ST, _c7: CA_ST_ID. SCD2 driven by record_type: D = close only, no insert.
        """
        logger.info("Parsing accounts from pipe-delimited format (Batch 2+)")
        # Incremental: bronze_account may not exist yet; create empty table if needed so we can read
        ensure_bronze_table_exists(self.spark, self.platform, bronze_table)
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        num_cols = 10
        parsed_df = self._parse_pipe_delimited(bronze_df, num_cols)
        record_type_expr, data_offset = self._extract_record_type(parsed_df, batch_id, 0)
        
        def c(i: int):
            return coalesce(trim(col(f"_c{i}")), lit(""))
        
        # Map per sample: I|43490|30470|16206|15280|...|CA_NAME|CA_TAX_ST|CA_ST_ID
        # Use try_cast for numeric cols to tolerate empty strings from malformed input
        account_df = parsed_df.select(
            record_type_expr,
            expr("try_cast(trim(_c" + str(0 + data_offset) + ") AS BIGINT)").alias("ca_id"),
            expr("try_cast(trim(_c" + str(1 + data_offset) + ") AS BIGINT)").alias("ca_b_id"),
            expr("try_cast(trim(_c" + str(2 + data_offset) + ") AS BIGINT)").alias("c_id"),
            c(4 + data_offset).alias("ca_name"),   # skip _c(3+data_offset)
            expr("try_cast(trim(_c" + str(5 + data_offset) + ") AS INT)").alias("ca_tax_st"),
            c(6 + data_offset).alias("ca_st_id"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        # action_type from record_type for SCD2: I=ADDACCT, U=UPDACCT, D=INACT
        account_df = account_df.withColumn(
            "action_type",
            when(col("record_type") == "D", lit("INACT"))
            .when(col("record_type") == "U", lit("UPDACCT"))
            .when(col("record_type") == "I", lit("ADDACCT"))
            .otherwise(lit("UPDACCT"))
        ).withColumn("action_ts", current_timestamp())
        
        silver_df = self._transform_to_silver_schema(
            account_df,
            has_action_type=True,
            has_record_type=True,
            batch_id=batch_id
        )
        
        logger.info(f"Applying SCD Type 2 CDC for batch {batch_id} (record_type I/U/D)")
        return self._apply_scd_type2(
            incoming_df=silver_df,
            target_table=target_table,
            key_column="account_id",
            effective_date_col="effective_date",
            record_type_col="record_type",
            exclude_record_types=["D"],
        )
    
    def _transform_to_silver_schema(self, account_df: DataFrame, 
                                    has_action_type: bool, batch_id: int,
                                    has_record_type: bool = False) -> DataFrame:
        """
        Transform account DataFrame to silver schema.
        
        Args:
            account_df: Input DataFrame with account fields
            has_action_type: Whether action_type/action_ts columns exist
            batch_id: Batch number
            has_record_type: Whether record_type (I/U/D) exists (incremental); used for SCD2
        """
        select_cols = []
        
        select_cols.extend([
            col("ca_id").cast(LongType()).alias("account_id"),
            coalesce(col("ca_b_id"), lit(0)).cast(LongType()).alias("broker_id"),
            coalesce(col("c_id"), lit(0)).cast(LongType()).alias("customer_id"),
            coalesce(col("ca_name"), lit("")).alias("account_name"),
            coalesce(col("ca_tax_st"), lit(0)).cast(IntegerType()).alias("tax_status"),
            coalesce(col("ca_st_id"), lit("")).alias("status_id"),
        ])
        
        if has_action_type:
            select_cols.extend([
                col("action_type").alias("action_type"),
                to_timestamp(col("action_ts")).alias("action_timestamp"),
            ])
        else:
            select_cols.extend([
                lit("ADDACCT").alias("action_type"),
                current_timestamp().alias("action_timestamp"),
            ])
        
        if has_action_type:
            select_cols.append(
                to_date(to_timestamp(col("action_ts"))).alias("effective_date")
            )
        else:
            select_cols.append(
                to_date(current_timestamp()).alias("effective_date")
            )
        
        select_cols.extend([
            lit(None).cast(DateType()).alias("end_date"),
        ])
        
        # is_current: D = false (close only, no insert); I/U = true
        if has_record_type and "record_type" in account_df.columns:
            select_cols.append(
                when(col("record_type") == "D", lit(False)).otherwise(lit(True)).alias("is_current")
            )
        else:
            select_cols.append(lit(True).alias("is_current"))
        
        select_cols.extend([
            col("batch_id").cast(IntegerType()).alias("batch_id"),
            col("load_timestamp").alias("load_timestamp"),
        ])
        
        # record_type: same schema for batch 1 and incremental (SCD2 append expects it)
        if has_record_type and "record_type" in account_df.columns:
            select_cols.append(col("record_type"))
        else:
            select_cols.append(lit("I").alias("record_type"))
        
        return account_df.select(*select_cols)
