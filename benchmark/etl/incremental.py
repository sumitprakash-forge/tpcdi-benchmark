"""
Incremental ETL transformations for TPC-DI benchmark.
Handles incremental batch processing.
"""

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, lit, current_timestamp, explode

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

from benchmark.etl.table_timing import is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class IncrementalETL:
    """Incremental ETL processor for TPC-DI."""
    
    def __init__(self, platform):
        """
        Initialize incremental ETL processor.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
        logger.info("Initialized IncrementalETL processor")
    
    def _read_customermgmt_xml(self, batch_id: int) -> DataFrame:
        """
        Read CustomerMgmt.xml file and parse XML structure.
        
        TPC-DI structure (see docs/CUSTOMERMGMT_STRUCTURE.md):
        <TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">
          <TPCDI:Action ActionType="NEW" ActionTS="...">
            <Customer C_ID="..." ...><Account CA_ID="..." CA_TAX_ST="..."><CA_B_ID>...</CA_B_ID><CA_NAME>...</CA_NAME></Account></Customer>
          </TPCDI:Action>
        </TPCDI:Actions>
        
        Returns DataFrame with flattened structure.
        Requires spark-xml (com.databricks:spark-xml_2.12:0.15.0).
        """
        logger.info(f"Reading CustomerMgmt.xml from Batch{batch_id}")
        
        try:
            file_path = f"Batch{batch_id}/CustomerMgmt.xml"
            
            # Try TPCDI:Action + rootTag first (matches actual DIGen output)
            df = None
            for row_tag, root_tag in [("TPCDI:Action", "TPCDI:Actions"), ("Action", None)]:
                try:
                    opts = {"format": "xml", "rowTag": row_tag}
                    if root_tag:
                        opts["rootTag"] = root_tag
                    df = self.platform.read_raw_file(file_path, **opts)
                    if df.count() > 0:
                        break
                    df = None
                except Exception:
                    df = None
            
            if df is None or df.count() == 0:
                raise RuntimeError(
                    f"Could not read XML file - all options returned 0 rows.\n"
                    f"Expected: <TPCDI:Actions><Action ActionType=\"...\">...</Action></TPCDI:Actions>\n"
                    f"Please check:\n"
                    f"1. File exists and is not empty\n"
                    f"2. XML structure matches TPC-DI spec\n"
                    f"3. spark-xml library is installed (com.databricks:spark-xml_2.12:0.15.0)"
                )
            
            logger.info(f"Successfully read CustomerMgmt.xml: {df.count()} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            error_msg = str(e)
            if "xml" in error_msg.lower() or "datasource" in error_msg.lower() or "format" in error_msg.lower():
                logger.error(
                    f"Failed to read CustomerMgmt.xml. This requires the spark-xml library.\n"
                    f"Install it on your cluster:\n"
                    f"  Maven: com.databricks:spark-xml_2.12:0.15.0\n"
                    f"  Or add to cluster libraries via Cluster -> Libraries -> Install New -> Maven\n"
                    f"Original error: {error_msg}"
                )
            else:
                logger.error(f"Failed to read CustomerMgmt.xml: {error_msg}")
            raise
    
    def process_batch(self, batch_id: int, target_database: str, target_schema: str):
        """
        Process a single incremental batch (Direct/Gold architecture).
        
        This is the legacy/direct load method that writes to Gold layer tables directly.
        For Medallion architecture, use process_medallion_batch() instead.
        
        Args:
            batch_id: Batch number to process
            target_database: Target database name
            target_schema: Target schema name
        """
        logger.info(f"Processing incremental batch {batch_id} (direct to Gold)")
        
        # Load new/updated accounts
        self.load_dim_account_incremental(
            batch_id,
            f"{target_database}.{target_schema}.DimAccount"
        )
        
        # Load new trades
        self.load_fact_trade_incremental(
            batch_id,
            f"{target_database}.{target_schema}.FactTrade"
        )
        
        # Load new/updated customers
        self.load_dim_customer_incremental(
            batch_id,
            f"{target_database}.{target_schema}.DimCustomer"
        )
        
        logger.info(f"Completed processing batch {batch_id}")
    
    def process_medallion_batch(self, batch_id: int, target_database: str, target_schema: str):
        """
        Process a single incremental batch using Medallion Architecture.
        
        This follows the medallion pattern:
        1. Bronze: Raw data ingestion (appends to existing bronze tables)
        2. Silver: Cleaned/parsed data with SCD handling (appends to existing silver tables)
        
        Args:
            batch_id: Batch number to process (2+ for incremental)
            target_database: Target database/catalog name
            target_schema: Target schema name
        """
        from benchmark.etl.bronze import BronzeETL
        from benchmark.etl.silver import SilverETL
        
        logger.info(f"Processing incremental batch {batch_id} with Medallion Architecture")
        
        # Bronze layer: Raw data ingestion (append to existing tables)
        logger.info(f"=== BRONZE LAYER: Batch{batch_id} Raw Data Ingestion ===")
        bronze_etl = BronzeETL(self.platform)
        bronze_etl.run_bronze_batch_load(batch_id, target_database, target_schema)
        
        # Silver layer: Cleaned/parsed data (append to existing tables)
        logger.info(f"=== SILVER LAYER: Batch{batch_id} Data Cleaning & Parsing ===")
        silver_etl = SilverETL(self.platform)
        silver_etl.run_silver_batch_load(batch_id, target_database, target_schema)
        
        logger.info(f"Completed medallion processing for batch {batch_id}")
    
    def load_dim_account_incremental(self, batch_id: int, target_table: str) -> DataFrame:
        """
        Load Account dimension updates from CustomerMgmt.xml
        
        TPC-DI XML structure:
        <TPCDI:Actions>
          <Action ActionType="NEW" ActionTS="2024-05-20T12:00:00">
            <Customer C_ID="123">
              <Account CA_ID="987" CA_TAX_ST="1">
                ...
              </Account>
            </Customer>
          </Action>
        </TPCDI:Actions>
        
        Note: CustomerMgmt.xml is ALWAYS XML format per TPC-DI spec.
        """
        logger.info(f"Loading DimAccount incremental updates from batch {batch_id}")
        
        xml_df = self._read_customermgmt_xml(batch_id)
        
        if xml_df.count() == 0:
            raise RuntimeError(
                f"XML file was read but contains 0 rows.\n"
                f"Please check:\n"
                f"- File exists at: Batch{batch_id}/CustomerMgmt.xml\n"
                f"- File is not empty\n"
                f"- XML structure matches TPC-DI spec"
            )
        
        account_df = None
        extraction_errors = []
        
        # Pattern 1: Try direct access (Actions not in array)
        try:
            account_df = xml_df.select(
                col("_ActionType").alias("ActionType"),
                col("_ActionTS").alias("ActionTS"),
                col("Customer._C_ID").alias("CustomerID"),
                col("Customer.Account._CA_ID").alias("AccountID"),
                col("Customer.Account._CA_TAX_ST").alias("TaxStatus"),
                col("Customer.Account.CA_NAME").alias("AccountName"),
                col("Customer.Account.CA_B_ID").alias("BrokerID"),
            ).filter(col("Customer.Account._CA_ID").isNotNull())
        except Exception as e1:
            extraction_errors.append(f"Pattern 1 (direct access): {e1}")
        
        # Pattern 2: Explode Actions if they're in an array
        if account_df is None:
            try:
                action_col = None
                for col_name in ["Action", "Actions", "value"]:
                    try:
                        test_df = xml_df.select(col(col_name))
                        if "array" in str(test_df.schema[0].dataType).lower():
                            action_col = col_name
                            break
                    except:
                        continue
                
                if action_col:
                    exploded_df = xml_df.select(explode(col(action_col)).alias("Action"))
                    account_df = exploded_df.select(
                        col("Action._ActionType").alias("ActionType"),
                        col("Action._ActionTS").alias("ActionTS"),
                        col("Action.Customer._C_ID").alias("CustomerID"),
                        col("Action.Customer.Account._CA_ID").alias("AccountID"),
                        col("Action.Customer.Account._CA_TAX_ST").alias("TaxStatus"),
                        col("Action.Customer.Account.CA_NAME").alias("AccountName"),
                        col("Action.Customer.Account.CA_B_ID").alias("BrokerID"),
                    ).filter(col("Action.Customer.Account._CA_ID").isNotNull())
                else:
                    array_cols = []
                    for field in xml_df.schema.fields:
                        if "array" in str(field.dataType).lower():
                            array_cols.append(field.name)
                    
                    if array_cols:
                        exploded_df = xml_df.select(explode(col(array_cols[0])).alias("Action"))
                        account_df = exploded_df.select(
                            col("Action._ActionType").alias("ActionType"),
                            col("Action._ActionTS").alias("ActionTS"),
                            col("Action.Customer._C_ID").alias("CustomerID"),
                            col("Action.Customer.Account._CA_ID").alias("AccountID"),
                            col("Action.Customer.Account._CA_TAX_ST").alias("TaxStatus"),
                            col("Action.Customer.Account.CA_NAME").alias("AccountName"),
                            col("Action.Customer.Account.CA_B_ID").alias("BrokerID"),
                        ).filter(col("Action.Customer.Account._CA_ID").isNotNull())
            except Exception as e2:
                extraction_errors.append(f"Pattern 2 (explode Actions): {e2}")
        
        # Pattern 3: Try using SQL
        if account_df is None:
            try:
                temp_view = "_temp_customermgmt_xml"
                xml_df.createOrReplaceTempView(temp_view)
                
                sql_query_no_explode = f"""
                SELECT
                  _ActionType as ActionType,
                  _ActionTS as ActionTS,
                  Customer._C_ID as CustomerID,
                  Customer.Account._CA_ID as AccountID,
                  Customer.Account._CA_TAX_ST as TaxStatus,
                  Customer.Account.CA_NAME as AccountName,
                  Customer.Account.CA_B_ID as BrokerID
                FROM {temp_view}
                WHERE Customer.Account._CA_ID IS NOT NULL
                """
                
                try:
                    account_df = self.spark.sql(sql_query_no_explode)
                except Exception:
                    sql_query_with_explode = f"""
                    SELECT
                      Action._ActionType as ActionType,
                      Action._ActionTS as ActionTS,
                      Action.Customer._C_ID as CustomerID,
                      Action.Customer.Account._CA_ID as AccountID,
                      Action.Customer.Account._CA_TAX_ST as TaxStatus,
                      Action.Customer.Account.CA_NAME as AccountName,
                      Action.Customer.Account.CA_B_ID as BrokerID
                    FROM (
                      SELECT explode(Action) as Action FROM {temp_view}
                    )
                    WHERE Action.Customer.Account._CA_ID IS NOT NULL
                    """
                    account_df = self.spark.sql(sql_query_with_explode)
                # Note: We do not drop the temp view here; account_df is lazy and references it.
                # Dropping would cause TABLE_OR_VIEW_NOT_FOUND when account_df is used later.
            except Exception as e3:
                extraction_errors.append(f"Pattern 3 (SQL): {e3}")
        
        if account_df is None:
            error_summary = "\n".join(extraction_errors)
            raise RuntimeError(
                f"Failed to extract Account data from CustomerMgmt.xml.\n"
                f"Tried multiple patterns including explode().\n"
                f"Errors:\n{error_summary}\n\n"
                f"Expected structure (see docs/CUSTOMERMGMT_STRUCTURE.md):\n"
                f"  - _ActionType, _ActionTS; Customer._C_ID; Customer.Account._CA_ID, _CA_TAX_ST, CA_NAME, CA_B_ID\n\n"
                f"Please check spark-xml (com.databricks:spark-xml_2.12:0.15.0) and rowTag=TPCDI:Action, rootTag=TPCDI:Actions."
            )
        
        # Transform to DimAccount schema (CA_B_ID -> SK_BrokerID per TPC-DI)
        dim_account = account_df.select(
            col("AccountID").alias("SK_AccountID"),
            col("BrokerID").cast("bigint").alias("SK_BrokerID"),
            col("CustomerID").alias("SK_CustomerID"),
            col("ActionType").alias("Status"),
            col("AccountName").alias("AccountDesc"),
            col("TaxStatus"),
            when(col("ActionType") == "INACT", lit(False)).otherwise(lit(True)).alias("IsActive"),
            lit(batch_id).alias("BatchID"),
            current_timestamp().alias("EffectiveDate"),
            current_timestamp().alias("EndDate")
        )
        
        row_count = dim_account.count()
        logger.info(f"Loaded DimAccount incremental: {row_count} rows")
        
        # For incremental: merge with existing data (SCD Type 2)
        # This is simplified - actual implementation would handle SCD properly
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_account, target_table, mode="append")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: append")
        return dim_account
    
    def _read_pipe_delimited_txt(self, batch_id: int, file_pattern: str, expected_cols: int) -> DataFrame:
        """
        Read a pipe-delimited .txt file using pure SQL split.
        
        Returns:
            DataFrame with columns _c0, _c1, _c2, etc.
        """
        logger.info(f"Reading {file_pattern} as pipe-delimited text from Batch{batch_id}")
        
        text_df = self.platform.read_batch_files(
            batch_id,
            file_pattern,
            format="text"
        )
        
        temp_view_name = f"_temp_txt_{file_pattern.replace('.', '_').replace('/', '_')}_{batch_id}"
        text_df.createOrReplaceTempView(temp_view_name)

        select_parts = []
        for i in range(expected_cols):
            select_parts.append(f"TRIM(COALESCE(element_at(split(value, '|'), {i+1}), '')) AS _c{i}")
        
        sql_query = f"SELECT {', '.join(select_parts)} FROM {temp_view_name}"
        
        try:
            df = self.spark.sql(sql_query)
        except Exception as sql_error:
            logger.warning(f"SQL query failed with '|', trying with escaped '\\|': {sql_error}")
            select_parts = []
            for i in range(expected_cols):
                select_parts.append(f"TRIM(COALESCE(element_at(split(value, '\\\\|'), {i+1}), '')) AS _c{i}")
            sql_query = f"SELECT {', '.join(select_parts)} FROM {temp_view_name}"
            df = self.spark.sql(sql_query)
        
        # Note: We do not drop the temp view here because Spark uses lazy evaluation.
        # The DataFrame returned is lazy and will reference the temp view when executed.
        # Dropping the view immediately would cause "TABLE_OR_VIEW_NOT_FOUND" errors.
        # Temporary views are session-scoped and will be automatically cleaned up when
        # the SparkSession ends. On Databricks serverless, the session lifecycle is
        # managed by the platform, so manual cleanup is not necessary.
        
        return df
    
    def load_fact_trade_incremental(self, batch_id: int, target_table: str) -> DataFrame:
        """Load Trade fact table from batch files (pipe-delimited .txt)"""
        logger.info(f"Loading FactTrade from batch {batch_id}")
        
        # Read Trade.txt for this batch (pipe-delimited per TPC-DI spec)
        # Expected columns: T_ID|T_DTS|T_ST_ID|T_TT_ID|T_IS_CASH|T_S_SYMB|T_QTY|T_BID_PRICE|T_CA_ID|T_EXEC_NAME|T_TRADE_PRICE|T_CHRG|T_COMM|T_TAX
        df = self._read_pipe_delimited_txt(batch_id, "Trade.txt", expected_cols=14)
        
        # Transform to FactTrade schema
        fact_trade = df.select(
            col("_c0").alias("TradeID"),
            col("_c1").alias("SK_CreateDateID"),
            col("_c2").alias("SK_CreateTimeID"),
            col("_c3").alias("SK_CloseDateID"),
            col("_c4").alias("SK_CloseTimeID"),
            col("_c5").alias("Status"),
            col("_c6").alias("Type"),
            col("_c7").cast("boolean").alias("CashFlag"),
            col("_c8").alias("SK_SecurityID"),
            col("_c9").alias("SK_CompanyID"),
            col("_c10").cast("int").alias("Quantity"),
            col("_c11").cast("double").alias("BidPrice"),
            col("_c12").alias("SK_CustomerID"),
            col("_c13").alias("SK_AccountID"),
            lit(None).cast("bigint").alias("SK_BrokerID"),
            lit(None).cast("double").alias("Commission"),
            lit(None).cast("double").alias("Charge"),
            lit(batch_id).alias("BatchID")
        )
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(fact_trade, target_table, mode="append")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = fact_trade.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: append")
        logger.info(f"Loaded FactTrade: {row_count} rows")
        return fact_trade
    
    def load_dim_customer_incremental(self, batch_id: int, target_table: str) -> DataFrame:
        """
        Load Customer dimension updates from CustomerMgmt.xml
        
        TPC-DI XML structure:
        <TPCDI:Actions>
          <Action ActionType="NEW" ActionTS="2024-05-20T12:00:00">
            <Customer C_ID="123">
              <Name>...</Name>
              <Address>...</Address>
              ...
            </Customer>
          </Action>
        </TPCDI:Actions>
        
        Note: CustomerMgmt.xml is ALWAYS XML format per TPC-DI spec.
        """
        logger.info(f"Loading DimCustomer incremental updates from batch {batch_id}")
        
        xml_df = self._read_customermgmt_xml(batch_id)
        
        if xml_df.count() == 0:
            raise RuntimeError(
                f"XML file was read but contains 0 rows.\n"
                f"Please check:\n"
                f"- File exists at: Batch{batch_id}/CustomerMgmt.xml\n"
                f"- File is not empty\n"
                f"- XML structure matches TPC-DI spec"
            )
        
        customer_df = None
        extraction_errors = []
        
        # Pattern 1: Try direct access (Actions not in array)
        try:
            customer_df = xml_df.select(
                col("_ActionType").alias("ActionType"),
                col("_ActionTS").alias("ActionTS"),
                col("Customer._C_ID").alias("CustomerID"),
                col("Customer._C_TAX_ID").alias("TaxID"),
                col("Customer.Name._C_L_NAME").alias("LastName"),
                col("Customer.Name._C_F_NAME").alias("FirstName"),
                col("Customer.Name._C_M_NAME").alias("MiddleInitial"),
                col("Customer._C_GNDR").alias("Gender"),
                col("Customer._C_TIER").alias("Tier"),
                col("Customer._C_DOB").alias("DOB"),
                col("Customer.Address._C_ADLINE1").alias("AddressLine1"),
                col("Customer.Address._C_ADLINE2").alias("AddressLine2"),
                col("Customer.Address._C_ZIPCODE").alias("PostalCode"),
                col("Customer.Address._C_CITY").alias("City"),
                col("Customer.Address._C_STATE_PROV").alias("StateProv"),
                col("Customer.Address._C_CTRY").alias("Country"),
                col("Customer.ContactInfo._C_PRIM_EMAIL").alias("Email1"),
                col("Customer.ContactInfo._C_ALT_EMAIL").alias("Email2"),
                col("Customer.ContactInfo._C_PHONE_1").alias("Phone1"),
                col("Customer.ContactInfo._C_PHONE_2").alias("Phone2"),
                col("Customer.ContactInfo._C_PHONE_3").alias("Phone3")
            ).filter(col("Customer._C_ID").isNotNull())
        except Exception as e1:
            extraction_errors.append(f"Pattern 1 (direct access): {e1}")
        
        # Pattern 2: Explode Actions if they're in an array
        if customer_df is None:
            try:
                action_col = None
                for col_name in ["Action", "Actions", "value"]:
                    try:
                        test_df = xml_df.select(col(col_name))
                        if "array" in str(test_df.schema[0].dataType).lower():
                            action_col = col_name
                            break
                    except:
                        continue
                
                if action_col:
                    exploded_df = xml_df.select(explode(col(action_col)).alias("Action"))
                    customer_df = exploded_df.select(
                        col("Action._ActionType").alias("ActionType"),
                        col("Action._ActionTS").alias("ActionTS"),
                        col("Action.Customer._C_ID").alias("CustomerID"),
                        col("Action.Customer._C_TAX_ID").alias("TaxID"),
                        col("Action.Customer.Name._C_L_NAME").alias("LastName"),
                        col("Action.Customer.Name._C_F_NAME").alias("FirstName"),
                        col("Action.Customer.Name._C_M_NAME").alias("MiddleInitial"),
                        col("Action.Customer._C_GNDR").alias("Gender"),
                        col("Action.Customer._C_TIER").alias("Tier"),
                        col("Action.Customer._C_DOB").alias("DOB"),
                        col("Action.Customer.Address._C_ADLINE1").alias("AddressLine1"),
                        col("Action.Customer.Address._C_ADLINE2").alias("AddressLine2"),
                        col("Action.Customer.Address._C_ZIPCODE").alias("PostalCode"),
                        col("Action.Customer.Address._C_CITY").alias("City"),
                        col("Action.Customer.Address._C_STATE_PROV").alias("StateProv"),
                        col("Action.Customer.Address._C_CTRY").alias("Country"),
                        col("Action.Customer.ContactInfo._C_PRIM_EMAIL").alias("Email1"),
                        col("Action.Customer.ContactInfo._C_ALT_EMAIL").alias("Email2"),
                        col("Action.Customer.ContactInfo._C_PHONE_1").alias("Phone1"),
                        col("Action.Customer.ContactInfo._C_PHONE_2").alias("Phone2"),
                        col("Action.Customer.ContactInfo._C_PHONE_3").alias("Phone3")
                    ).filter(col("Action.Customer._C_ID").isNotNull())
                else:
                    # Check schema for array columns
                    array_cols = []
                    for field in xml_df.schema.fields:
                        if "array" in str(field.dataType).lower():
                            array_cols.append(field.name)
                    
                    if array_cols:
                        exploded_df = xml_df.select(explode(col(array_cols[0])).alias("Action"))
                        customer_df = exploded_df.select(
                            col("Action._ActionType").alias("ActionType"),
                            col("Action._ActionTS").alias("ActionTS"),
                            col("Action.Customer._C_ID").alias("CustomerID"),
                            col("Action.Customer._C_TAX_ID").alias("TaxID"),
                            col("Action.Customer.Name._C_L_NAME").alias("LastName"),
                            col("Action.Customer.Name._C_F_NAME").alias("FirstName"),
                            col("Action.Customer.Name._C_M_NAME").alias("MiddleInitial"),
                            col("Action.Customer._C_GNDR").alias("Gender"),
                            col("Action.Customer._C_TIER").alias("Tier"),
                            col("Action.Customer._C_DOB").alias("DOB"),
                            col("Action.Customer.Address._C_ADLINE1").alias("AddressLine1"),
                            col("Action.Customer.Address._C_ADLINE2").alias("AddressLine2"),
                            col("Action.Customer.Address._C_ZIPCODE").alias("PostalCode"),
                            col("Action.Customer.Address._C_CITY").alias("City"),
                            col("Action.Customer.Address._C_STATE_PROV").alias("StateProv"),
                            col("Action.Customer.Address._C_CTRY").alias("Country"),
                            col("Action.Customer.ContactInfo._C_PRIM_EMAIL").alias("Email1"),
                            col("Action.Customer.ContactInfo._C_ALT_EMAIL").alias("Email2"),
                            col("Action.Customer.ContactInfo._C_PHONE_1").alias("Phone1"),
                            col("Action.Customer.ContactInfo._C_PHONE_2").alias("Phone2"),
                            col("Action.Customer.ContactInfo._C_PHONE_3").alias("Phone3")
                        ).filter(col("Action.Customer._C_ID").isNotNull())
            except Exception as e2:
                extraction_errors.append(f"Pattern 2 (explode Actions): {e2}")
        
        # Pattern 3: Try using SQL
        if customer_df is None:
            try:
                temp_view = "_temp_customermgmt_xml_customer"
                xml_df.createOrReplaceTempView(temp_view)
                
                sql_query_no_explode = f"""
                SELECT
                  _ActionType as ActionType,
                  _ActionTS as ActionTS,
                  Customer._C_ID as CustomerID,
                  Customer._C_TAX_ID as TaxID,
                  Customer.Name._C_L_NAME as LastName,
                  Customer.Name._C_F_NAME as FirstName,
                  Customer.Name._C_M_NAME as MiddleInitial,
                  Customer._C_GNDR as Gender,
                  Customer._C_TIER as Tier,
                  Customer._C_DOB as DOB,
                  Customer.Address._C_ADLINE1 as AddressLine1,
                  Customer.Address._C_ADLINE2 as AddressLine2,
                  Customer.Address._C_ZIPCODE as PostalCode,
                  Customer.Address._C_CITY as City,
                  Customer.Address._C_STATE_PROV as StateProv,
                  Customer.Address._C_CTRY as Country,
                  Customer.ContactInfo._C_PRIM_EMAIL as Email1,
                  Customer.ContactInfo._C_ALT_EMAIL as Email2,
                  Customer.ContactInfo._C_PHONE_1 as Phone1,
                  Customer.ContactInfo._C_PHONE_2 as Phone2,
                  Customer.ContactInfo._C_PHONE_3 as Phone3
                FROM {temp_view}
                WHERE Customer._C_ID IS NOT NULL
                """
                
                try:
                    customer_df = self.spark.sql(sql_query_no_explode)
                except Exception:
                    sql_query_with_explode = f"""
                    SELECT
                      Action._ActionType as ActionType,
                      Action._ActionTS as ActionTS,
                      Action.Customer._C_ID as CustomerID,
                      Action.Customer._C_TAX_ID as TaxID,
                      Action.Customer.Name._C_L_NAME as LastName,
                      Action.Customer.Name._C_F_NAME as FirstName,
                      Action.Customer.Name._C_M_NAME as MiddleInitial,
                      Action.Customer._C_GNDR as Gender,
                      Action.Customer._C_TIER as Tier,
                      Action.Customer._C_DOB as DOB,
                      Action.Customer.Address._C_ADLINE1 as AddressLine1,
                      Action.Customer.Address._C_ADLINE2 as AddressLine2,
                      Action.Customer.Address._C_ZIPCODE as PostalCode,
                      Action.Customer.Address._C_CITY as City,
                      Action.Customer.Address._C_STATE_PROV as StateProv,
                      Action.Customer.Address._C_CTRY as Country,
                      Action.Customer.ContactInfo._C_PRIM_EMAIL as Email1,
                      Action.Customer.ContactInfo._C_ALT_EMAIL as Email2,
                      Action.Customer.ContactInfo._C_PHONE_1 as Phone1,
                      Action.Customer.ContactInfo._C_PHONE_2 as Phone2,
                      Action.Customer.ContactInfo._C_PHONE_3 as Phone3
                    FROM (
                      SELECT explode(Action) as Action FROM {temp_view}
                    )
                    WHERE Action.Customer._C_ID IS NOT NULL
                    """
                    customer_df = self.spark.sql(sql_query_with_explode)
                # Note: We do not drop the temp view here; customer_df is lazy and references it.
                # Dropping would cause TABLE_OR_VIEW_NOT_FOUND when customer_df is used later.
            except Exception as e3:
                extraction_errors.append(f"Pattern 3 (SQL): {e3}")
        
        if customer_df is None:
            error_summary = "\n".join(extraction_errors)
            raise RuntimeError(
                f"Failed to extract Customer data from CustomerMgmt.xml.\n"
                f"Tried multiple patterns including explode().\n"
                f"Errors:\n{error_summary}\n\n"
                f"Please check:\n"
                f"1. spark-xml library is installed (com.databricks:spark-xml_2.12:0.15.0)\n"
                f"2. XML file structure matches TPC-DI spec\n"
                f"3. Actions may need to be exploded if they're in an array"
            )
        
        # Transform to DimCustomer schema
        # Note: Tax rate fields may need to be joined from other tables or set to NULL
        dim_customer = customer_df.select(
            col("CustomerID").alias("SK_CustomerID"),
            col("TaxID"),
            col("ActionType").alias("Status"),
            col("LastName"),
            col("FirstName"),
            col("MiddleInitial"),
            col("Gender"),
            col("Tier"),
            col("DOB"),
            col("AddressLine1"),
            col("AddressLine2"),
            col("PostalCode"),
            col("City"),
            col("StateProv"),
            col("Country"),
            col("Phone1"),
            col("Phone2"),
            col("Phone3"),
            col("Email1"),
            col("Email2"),
            lit(None).alias("NationalTaxRateDesc"),
            lit(None).cast("double").alias("NationalTaxRate"),
            lit(None).alias("LocalTaxRateDesc"),
            lit(None).cast("double").alias("LocalTaxRate"),
            lit(batch_id).alias("BatchID"),
            current_timestamp().alias("EffectiveDate"),
            current_timestamp().alias("EndDate")
        )
        
        row_count = dim_customer.count()
        logger.info(f"Loaded DimCustomer incremental: {row_count} rows")
        
        # For incremental: merge with existing data (SCD Type 2)
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_customer, target_table, mode="append")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: append")
        return dim_customer
