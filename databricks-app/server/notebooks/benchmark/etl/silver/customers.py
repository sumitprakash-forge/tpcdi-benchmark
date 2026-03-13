"""
Silver layer loader for Customers.

Parses and cleans customer data from bronze_customer_mgmt.
Implements SCD Type 2 for CDC (Change Data Capture) on incremental loads.

TPC-DI Format Differences:
- Batch 1: CustomerMgmt.xml (XML event log)
- Batch 2+: Customer.txt (pipe-delimited state snapshot)
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, to_date, to_timestamp, explode, current_timestamp, coalesce, trim, expr
)
from pyspark.sql.types import LongType, IntegerType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase
from benchmark.etl.bronze.base import ensure_bronze_table_exists

logger = logging.getLogger(__name__)


class SilverCustomers(SilverLoaderBase):
    """
    Silver layer loader for Customers.
    
    Handles two different formats per TPC-DI spec:
    - Batch 1: CustomerMgmt.xml (XML event log)
    - Batch 2+: Customer.txt (pipe-delimited state snapshot)
    
    CDC/SCD Type 2 Handling:
    - Batch 1 (Historical): Full load, overwrite from XML
    - Batch 2+ (Incremental): 
      - Parse pipe-delimited Customer.txt (state-at-time snapshot)
      - Apply SCD Type 2: Close existing records, insert new versions
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean customer data.
        
        Args:
            bronze_table: Source bronze table name
              - Batch 1: bronze_customer_mgmt (XML)
              - Batch 2+: bronze_customer (pipe-delimited)
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned customer data
        """
        logger.info(f"Loading silver_customers from {bronze_table} (batch {batch_id})")
        
        if batch_id == 1:
            # Batch 1: Parse XML from CustomerMgmt.xml
            return self._load_from_xml(bronze_table, target_table, batch_id)
        else:
            # Batch 2+: Parse pipe-delimited from Customer.txt
            return self._load_from_pipe_delimited(bronze_table, target_table, batch_id)
    
    def _load_from_xml(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load customers from XML format (Batch 1).
        
        Args:
            bronze_table: bronze_customer_mgmt table
            target_table: Target silver table
            batch_id: Batch number (should be 1)
            
        Returns:
            DataFrame with cleaned customer data
        """
        logger.info("Parsing customers from XML format (Batch 1)")
        
        # Read bronze table (contains nested XML structure)
        bronze_df = self.spark.table(bronze_table)
        
        # Filter for current batch
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Extract customer fields from XML structure
        customer_df = None
        extraction_errors = []
        
        # Pattern 1: Direct access (TPCDI:Action as row)
        try:
            customer_df = bronze_df.select(
                col("_ActionType").alias("action_type"),
                col("_ActionTS").alias("action_ts"),
                col("Customer._C_ID").alias("c_id"),
                col("Customer._C_TAX_ID").alias("c_tax_id"),
                col("Customer._C_GNDR").alias("c_gndr"),
                col("Customer._C_TIER").alias("c_tier"),
                col("Customer._C_DOB").alias("c_dob"),
                col("Customer.Name.C_L_NAME").alias("c_l_name"),
                col("Customer.Name.C_F_NAME").alias("c_f_name"),
                col("Customer.Name.C_M_NAME").alias("c_m_name"),
                col("Customer.Address.C_ADLINE1").alias("c_adline1"),
                col("Customer.Address.C_ADLINE2").alias("c_adline2"),
                col("Customer.Address.C_ZIPCODE").alias("c_zipcode"),
                col("Customer.Address.C_CITY").alias("c_city"),
                col("Customer.Address.C_STATE_PROV").alias("c_state_prov"),
                col("Customer.Address.C_CTRY").alias("c_ctry"),
                col("Customer.ContactInfo.C_PRIM_EMAIL").alias("c_prim_email"),
                col("Customer.ContactInfo.C_ALT_EMAIL").alias("c_alt_email"),
                col("Customer.TaxInfo.C_LCL_TX_ID").alias("c_lcl_tx_id"),
                col("Customer.TaxInfo.C_NAT_TX_ID").alias("c_nat_tx_id"),
                col("_batch_id").alias("batch_id"),
                col("_load_timestamp").alias("load_timestamp"),
            ).filter(col("Customer._C_ID").isNotNull())
            
            if customer_df.count() > 0:
                logger.info("Pattern 1 (direct access) succeeded")
        except Exception as e:
            extraction_errors.append(f"Pattern 1: {e}")
            customer_df = None
        
        # Pattern 2: Handle if Actions are in array
        if customer_df is None or customer_df.count() == 0:
            try:
                for col_name in bronze_df.columns:
                    if "array" in str(bronze_df.schema[col_name].dataType).lower():
                        exploded = bronze_df.select(
                            explode(col(col_name)).alias("Action"), 
                            col("_batch_id"), 
                            col("_load_timestamp")
                        )
                        customer_df = exploded.select(
                            col("Action._ActionType").alias("action_type"),
                            col("Action._ActionTS").alias("action_ts"),
                            col("Action.Customer._C_ID").alias("c_id"),
                            col("Action.Customer._C_TAX_ID").alias("c_tax_id"),
                            col("Action.Customer._C_GNDR").alias("c_gndr"),
                            col("Action.Customer._C_TIER").alias("c_tier"),
                            col("Action.Customer._C_DOB").alias("c_dob"),
                            col("Action.Customer.Name.C_L_NAME").alias("c_l_name"),
                            col("Action.Customer.Name.C_F_NAME").alias("c_f_name"),
                            col("Action.Customer.Name.C_M_NAME").alias("c_m_name"),
                            col("Action.Customer.Address.C_ADLINE1").alias("c_adline1"),
                            col("Action.Customer.Address.C_ADLINE2").alias("c_adline2"),
                            col("Action.Customer.Address.C_ZIPCODE").alias("c_zipcode"),
                            col("Action.Customer.Address.C_CITY").alias("c_city"),
                            col("Action.Customer.Address.C_STATE_PROV").alias("c_state_prov"),
                            col("Action.Customer.Address.C_CTRY").alias("c_ctry"),
                            col("Action.Customer.ContactInfo.C_PRIM_EMAIL").alias("c_prim_email"),
                            col("Action.Customer.ContactInfo.C_ALT_EMAIL").alias("c_alt_email"),
                            col("Action.Customer.TaxInfo.C_LCL_TX_ID").alias("c_lcl_tx_id"),
                            col("Action.Customer.TaxInfo.C_NAT_TX_ID").alias("c_nat_tx_id"),
                            col("_batch_id").alias("batch_id"),
                            col("_load_timestamp").alias("load_timestamp"),
                        ).filter(col("Action.Customer._C_ID").isNotNull())
                        
                        if customer_df.count() > 0:
                            logger.info("Pattern 2 (explode array) succeeded")
                            break
            except Exception as e:
                extraction_errors.append(f"Pattern 2: {e}")
        
        if customer_df is None or customer_df.count() == 0:
            raise RuntimeError(f"Failed to extract customers: {extraction_errors}")
        
        # Transform to silver schema
        silver_df = self._transform_to_silver_schema(
            customer_df, 
            has_action_type=True,  # XML has ActionType
            batch_id=batch_id
        )
        
        # Batch 1: Full historical load (overwrite)
        return self._write_silver_table(silver_df, target_table, batch_id)
    
    def _load_from_pipe_delimited(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load customers from pipe-delimited format (Batch 2+).
        
        Incremental Customer.txt format (pipe-delimited, state-at-time snapshot):
        record_type|C_ID|...|C_NAT_TX_ID
        - _c0: record_type (I=Insert, U=Update, D=Delete)
        - _c1: C_ID, _c2: (e.g. CA_ID, unused), _c3: C_TAX_ID, _c4: C_ST_ID,
        - _c5: C_L_NAME, _c6: C_F_NAME, _c7: C_M_NAME, _c8: C_GNDR, _c9: C_TIER,
        - _c10: C_DOB, _c11–12: C_ADLINE1/2, _c13: C_ZIPCODE, _c14–16: C_CITY, C_STATE_PROV, C_CTRY,
        - _c17–27: phones etc., _c28–29: C_PRIM_EMAIL, C_ALT_EMAIL, _c30–31: C_LCL_TX_ID, C_NAT_TX_ID
        """
        logger.info("Parsing customers from pipe-delimited format (Batch 2+)")
        # Incremental: bronze_customer may not exist yet; create empty table if needed so we can read
        ensure_bronze_table_exists(self.spark, self.platform, bronze_table)
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Incremental: record_type in first column, then 34 data columns (35 total)
        num_cols = 35
        parsed_df = self._parse_pipe_delimited(bronze_df, num_cols)
        record_type_expr, data_offset = self._extract_record_type(parsed_df, batch_id, 0)
        # data_offset = 1 for incremental: _c0=record_type, _c1..=data
        
        def c(i: int):
            return coalesce(trim(col(f"_c{i}")), lit(""))
        
        # Map columns per sample: I|21790|15280|732-50-0322|ACTV|Keung|Mahmut|T||2|1946-02-17|...
        # Data indices: 0=C_ID, 1=skip, 2=C_TAX_ID, 3=C_ST_ID, 4–10=name,dob,addr, 11–16=city..ctry, 27–30=emails,tax
        # Use try_cast for numeric cols to tolerate empty strings from malformed input
        customer_df = parsed_df.select(
            record_type_expr,
            expr("try_cast(trim(_c" + str(0 + data_offset) + ") AS BIGINT)").alias("c_id"),
            c(2 + data_offset).alias("c_tax_id"),
            c(3 + data_offset).alias("c_st_id"),
            c(4 + data_offset).alias("c_l_name"),
            c(5 + data_offset).alias("c_f_name"),
            c(6 + data_offset).alias("c_m_name"),
            c(7 + data_offset).alias("c_gndr"),
            expr("try_cast(trim(_c" + str(8 + data_offset) + ") AS INT)").alias("c_tier"),
            c(9 + data_offset).alias("c_dob"),
            c(10 + data_offset).alias("c_adline1"),
            c(11 + data_offset).alias("c_adline2"),
            c(12 + data_offset).alias("c_zipcode"),
            c(13 + data_offset).alias("c_city"),
            c(14 + data_offset).alias("c_state_prov"),
            c(15 + data_offset).alias("c_ctry"),
            c(27 + data_offset).alias("c_prim_email"),
            c(28 + data_offset).alias("c_alt_email"),
            c(29 + data_offset).alias("c_lcl_tx_id"),
            c(30 + data_offset).alias("c_nat_tx_id"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        # action_type from record_type for SCD2: I=NEW, U=UPDCUST, D=INACT
        customer_df = customer_df.withColumn(
            "action_type",
            when(col("record_type") == "D", lit("INACT"))
            .when(col("record_type") == "U", lit("UPDCUST"))
            .when(col("record_type") == "I", lit("NEW"))
            .otherwise(lit("UPDCUST"))
        ).withColumn("action_ts", current_timestamp())
        
        # Transform to silver schema (pass record_type for SCD2)
        silver_df = self._transform_to_silver_schema(
            customer_df,
            has_action_type=True,
            has_record_type=True,
            batch_id=batch_id
        )
        
        # Batch 2+: SCD Type 2 driven by record_type. D = close only, no insert.
        logger.info(f"Applying SCD Type 2 CDC for batch {batch_id} (record_type I/U/D)")
        return self._apply_scd_type2(
            incoming_df=silver_df,
            target_table=target_table,
            key_column="customer_id",
            effective_date_col="effective_date",
            record_type_col="record_type",
            exclude_record_types=["D"],
        )
    
    def _transform_to_silver_schema(self, customer_df: DataFrame, 
                                    has_action_type: bool, batch_id: int,
                                    has_record_type: bool = False) -> DataFrame:
        """
        Transform customer data to silver schema.
        
        Args:
            customer_df: Input DataFrame with customer fields
            has_action_type: Whether action_type/action_ts columns exist
            batch_id: Batch number
            has_record_type: Whether record_type (I/U/D) exists (incremental); used for SCD2
        """
        # Build select list conditionally (try_cast tolerates '' -> NULL for malformed input)
        select_cols = [
            expr("try_cast(c_id AS BIGINT)").alias("sk_customer_id"),
            expr("try_cast(c_id AS BIGINT)").alias("customer_id"),
            col("c_tax_id").alias("tax_id"),
        ]
        
        # Status column: action_type (NEW/UPDCUST/INACT from record_type when has_record_type)
        if has_action_type:
            select_cols.append(col("action_type").alias("status"))
        else:
            select_cols.append(lit("ACTIVE").alias("status"))
        
        select_cols.extend([
            col("c_l_name").alias("last_name"),
            col("c_f_name").alias("first_name"),
            col("c_m_name").alias("middle_name"),
            when(col("c_gndr").isin("M", "m"), "Male")
                .when(col("c_gndr").isin("F", "f"), "Female")
                .otherwise("Unknown").alias("gender"),
            expr("try_cast(c_tier AS INT)").alias("tier"),
            to_date(col("c_dob"), "yyyy-MM-dd").alias("dob"),
            col("c_adline1").alias("address_line1"),
            col("c_adline2").alias("address_line2"),
            col("c_zipcode").alias("postal_code"),
            col("c_city").alias("city"),
            col("c_state_prov").alias("state_prov"),
            col("c_ctry").alias("country"),
            col("c_prim_email").alias("email1"),
            col("c_alt_email").alias("email2"),
            col("c_lcl_tx_id").alias("local_tax_id"),
            col("c_nat_tx_id").alias("national_tax_id"),
            coalesce(col("batch_id"), lit(batch_id)).cast(IntegerType()).alias("batch_id"),
        ])
        
        # is_current: D = false (close only, no insert); I/U = true
        if has_record_type:
            select_cols.append(
                when(col("record_type") == "D", lit(False)).otherwise(lit(True)).alias("is_current")
            )
        elif has_action_type:
            select_cols.append(
                when(col("action_type") == "INACT", lit(False)).otherwise(lit(True)).alias("is_current")
            )
        else:
            select_cols.append(lit(True).alias("is_current"))
        
        # effective_date
        if has_action_type:
            select_cols.append(to_timestamp(col("action_ts")).alias("effective_date"))
        else:
            select_cols.append(current_timestamp().alias("effective_date"))
        
        select_cols.extend([
            lit(None).cast(TimestampType()).alias("end_date"),
            col("load_timestamp"),
        ])
        
        # record_type: same schema for batch 1 and incremental (SCD2 append expects it)
        if has_record_type and "record_type" in customer_df.columns:
            select_cols.append(col("record_type"))
        else:
            select_cols.append(lit("I").alias("record_type"))
        
        return customer_df.select(*select_cols)
