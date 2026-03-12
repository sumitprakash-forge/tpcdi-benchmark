"""
Batch ETL transformations for TPC-DI benchmark.
Handles historical load and initial batch processing.
"""

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, lit, current_timestamp, split, element_at, size, explode
from pyspark.sql.types import StringType, StructType, StructField

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

from benchmark.etl.table_timing import is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class BatchETL:
    """Batch ETL processor for TPC-DI."""
    
    def __init__(self, platform):
        """
        Initialize batch ETL processor.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
        logger.info("Initialized BatchETL processor")
    
    def _read_pipe_delimited_txt(self, batch_id: int, file_pattern: str, expected_cols: int) -> DataFrame:
        """
        Read a pipe-delimited .txt file using pure SQL split.
        
        This method bypasses Spark's CSV reader which has issues with pipe delimiter
        on some Databricks runtimes. It uses spark.sql() with a temporary view to avoid
        query context tracking issues.
        
        Args:
            batch_id: Batch number
            file_pattern: File pattern (e.g., "Date.txt")
            expected_cols: Expected number of columns
        
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
    
    def _read_file_with_delimiter_detection(self, batch_id: int, file_pattern: str, 
                                           expected_cols: int, expected_format: str,
                                           preferred_delimiter: str = "|", **options) -> DataFrame:
        """
        Read a file trying different delimiters if the preferred one doesn't work.
        
        TPC-DI file formats:
        - Most .txt files: Pipe-delimited (|)
        - HR.csv, Prospect.csv: Comma-delimited (,)
        - FINWIRE files: Fixed-width (no delimiter)
        - CustomerMgmt.xml: XML format
        
        Args:
            batch_id: Batch number
            file_pattern: File pattern to read
            expected_cols: Expected minimum columns
            expected_format: Expected format description
            preferred_delimiter: Preferred delimiter (| or ,)
            **options: Additional read options
        
        Returns:
            DataFrame with the file data
        """
        # For .txt files (pipe-delimited per TPC-DI spec), use SQL-based split directly
        if file_pattern.endswith(".txt") and preferred_delimiter == "|":
            return self._read_pipe_delimited_txt(batch_id, file_pattern, expected_cols)
        
        # Try preferred delimiter first (for .csv files)
        try:
            read_options = {
                "sep": preferred_delimiter,
                "delimiter": preferred_delimiter,
                "header": False,
                "inferSchema": True,
                **options
            }
            
            df = self.platform.read_batch_files(
                batch_id,
                file_pattern,
                format="csv",
                **read_options
            )
            
            if len(df.columns) >= expected_cols:
                return df
            else:
                # Try with multiLine=false
                read_options_fixed = {
                    "sep": preferred_delimiter,
                    "delimiter": preferred_delimiter,
                    "header": False,
                    "inferSchema": True,
                    "multiLine": False,
                    "quote": "",
                    "escape": "",
                    **options
                }
                df = self.platform.read_batch_files(
                    batch_id,
                    file_pattern,
                    format="csv",
                    **read_options_fixed
                )
                
                if len(df.columns) >= expected_cols:
                    return df
        except Exception as e:
            logger.warning(f"Failed to read {file_pattern} with delimiter '{preferred_delimiter}': {e}")
        
        # Try alternative delimiter
        alt_delimiter = "," if preferred_delimiter == "|" else "|"
        try:
            df = self.platform.read_batch_files(
                batch_id,
                file_pattern,
                format="csv",
                sep=alt_delimiter,
                delimiter=alt_delimiter,
                header=False,
                inferSchema=True,
                **options
            )
            
            if len(df.columns) >= expected_cols:
                return df
        except Exception as e:
            logger.warning(f"Failed with alternative delimiter '{alt_delimiter}': {e}")
        
        return self._read_pipe_delimited_txt(batch_id, file_pattern, expected_cols)
    
    def _read_customermgmt_xml(self, batch_id: int) -> DataFrame:
        """
        Read CustomerMgmt.xml file and parse XML structure.
        
        TPC-DI structure (see docs/CUSTOMERMGMT_STRUCTURE.md):
        <TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">
          <TPCDI:Action ActionType="NEW" ActionTS="2007-07-07T02:56:25">
            <Customer C_ID="0" C_TAX_ID="..." ...>
              <Name><C_L_NAME/>...</Name>
              <Address>...</Address>
              <ContactInfo>...</ContactInfo>
              <TaxInfo>...</TaxInfo>
              <Account CA_ID="0" CA_TAX_ST="1">
                <CA_B_ID>17713</CA_B_ID>
                <CA_NAME>...</CA_NAME>
              </Account>
            </Customer>
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
    
    def _validate_and_debug_df(self, df: DataFrame, file_name: str, 
                               expected_cols: int, expected_format: str):
        """Validate DataFrame and log warnings if columns don't match."""
        if len(df.columns) < expected_cols:
            logger.warning(
                f"{file_name} has {len(df.columns)} columns but expected at least {expected_cols}. "
                f"Actual columns: {df.columns}. Expected format: {expected_format}"
            )
    
    def load_dim_date(self, target_table: str) -> DataFrame:
        """
        Load Date dimension table from Batch1/Date.txt (TPC-DI spec: historical data in Batch1)
        
        TPC-DI format: Pipe-delimited (.txt file)
        Format: DateValue|DayOfWeek|CalendarMonth|CalendarQuarter|CalendarYear|DayOfMonth
        """
        logger.info("Loading DimDate dimension table")
        
        # Read Date.txt from Batch1 - pipe-delimited per TPC-DI spec
        df = self._read_file_with_delimiter_detection(
            1,  # Batch1 contains historical load data
            "Date.txt",
            expected_cols=6,
            expected_format="DateValue|DayOfWeek|CalendarMonth|CalendarQuarter|CalendarYear|DayOfMonth",
            preferred_delimiter="|"
        )
        
        # Validate and debug
        self._validate_and_debug_df(
            df, 
            "Date.txt", 
            expected_cols=6,
            expected_format="DateValue|DayOfWeek|CalendarMonth|CalendarQuarter|CalendarYear|DayOfMonth"
        )
        
        # Transform to DimDate schema
        dim_date = df.select(
            col("_c0").alias("SK_DateID"),
            col("_c0").alias("DateValue"),
            col("_c1").alias("DateDesc"),
            col("_c2").alias("CalendarMonthID"),
            col("_c2").alias("CalendarMonthDesc"),
            col("_c3").alias("CalendarQuarterID"),
            col("_c3").alias("CalendarQuarterDesc"),
            col("_c4").alias("CalendarYear"),
            col("_c5").alias("DayOfWeek"),
            col("_c5").alias("DayOfWeekDesc"),
            current_timestamp().alias("BatchID")
        )
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_date, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = dim_date.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        return dim_date
    
    def load_dim_time(self, target_table: str) -> DataFrame:
        """
        Load Time dimension table from Batch1/Time.txt (TPC-DI spec: historical data in Batch1)
        
        TPC-DI format: Pipe-delimited (.txt file)
        Format: Time|TimeID|Hour|Minute|Second|AM|PM
        """
        logger.info("Loading DimTime dimension table")
        
        # Read Time.txt from Batch1 - pipe-delimited per TPC-DI spec
        df = self._read_file_with_delimiter_detection(
            1,  # Batch1 contains historical load data
            "Time.txt",
            expected_cols=7,
            expected_format="Time|TimeID|Hour|Minute|Second|AM|PM",
            preferred_delimiter="|"
        )
        
        # Validate and debug
        self._validate_and_debug_df(
            df,
            "Time.txt",
            expected_cols=7,
            expected_format="Time|TimeID|Hour|Minute|Second|AM|PM"
        )
        
        # Transform to DimTime schema
        dim_time = df.select(
            col("_c1").alias("SK_TimeID"),
            col("_c0").alias("TimeValue"),
            col("_c2").alias("HourID"),
            col("_c2").alias("HourDesc"),
            col("_c3").alias("MinuteID"),
            col("_c3").alias("MinuteDesc"),
            col("_c4").alias("SecondID"),
            col("_c4").alias("SecondDesc"),
            col("_c5").alias("MarketHoursFlag"),
            col("_c6").alias("OfficeHoursFlag"),
            current_timestamp().alias("BatchID")
        )
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_time, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = dim_time.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        return dim_time
    
    def load_dim_trade_type(self, target_table: str) -> DataFrame:
        """Load TradeType dimension table from Batch1/TradeType.txt"""
        logger.info("Loading DimTradeType dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,
            "TradeType.txt",
            expected_cols=2,
            expected_format="TT_ID|TT_NAME",
            preferred_delimiter="|"
        )
        
        self._validate_and_debug_df(df, "TradeType.txt", expected_cols=2, expected_format="TT_ID|TT_NAME")
        
        dim_trade_type = df.select(
            col("_c0").alias("TT_ID"),
            col("_c1").alias("TT_NAME"),
            col("_c1").alias("TT_IS_SELL"),
            col("_c1").alias("TT_IS_MRKT"),
            current_timestamp().alias("BatchID")
        )
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_trade_type, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = dim_trade_type.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        return dim_trade_type
    
    def load_dim_status_type(self, target_table: str) -> DataFrame:
        """Load StatusType dimension table from Batch1/StatusType.txt"""
        logger.info("Loading DimStatusType dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,
            "StatusType.txt",
            expected_cols=2,
            expected_format="ST_ID|ST_NAME",
            preferred_delimiter="|"
        )
        
        self._validate_and_debug_df(df, "StatusType.txt", expected_cols=2, expected_format="ST_ID|ST_NAME")
        
        dim_status_type = df.select(
            col("_c0").alias("ST_ID"),
            col("_c1").alias("ST_NAME"),
            current_timestamp().alias("BatchID")
        )
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_status_type, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = dim_status_type.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        return dim_status_type
    
    def load_dim_tax_rate(self, target_table: str) -> DataFrame:
        """Load TaxRate dimension table from Batch1/TaxRate.txt"""
        logger.info("Loading DimTaxRate dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,
            "TaxRate.txt",
            expected_cols=2,
            expected_format="TX_ID|TX_NAME",
            preferred_delimiter="|"
        )
        
        self._validate_and_debug_df(df, "TaxRate.txt", expected_cols=2, expected_format="TX_ID|TX_NAME")
        
        dim_tax_rate = df.select(
            col("_c0").alias("TX_ID"),
            col("_c1").alias("TX_NAME"),
            col("_c1").alias("TX_RATE"),
            current_timestamp().alias("BatchID")
        )
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_tax_rate, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = dim_tax_rate.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        return dim_tax_rate
    
    def load_dim_industry(self, target_table: str) -> DataFrame:
        """Load Industry dimension table from Batch1/Industry.txt"""
        logger.info("Loading DimIndustry dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,
            "Industry.txt",
            expected_cols=4,
            expected_format="IN_ID|IN_NAME|IN_SC_ID|IN_SC_NAME",
            preferred_delimiter="|"
        )
        
        self._validate_and_debug_df(df, "Industry.txt", expected_cols=4, expected_format="IN_ID|IN_NAME|IN_SC_ID|IN_SC_NAME")
        
        dim_industry = df.select(
            col("_c0").alias("IN_ID"),
            col("_c1").alias("IN_NAME"),
            col("_c2").alias("IN_SC_ID"),
            col("_c3").alias("IN_SC_NAME"),
            current_timestamp().alias("BatchID")
        )
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_industry, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = dim_industry.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        return dim_industry
    
    def load_dim_account(self, target_table: str) -> DataFrame:
        """
        Load Account dimension table from Batch1/CustomerMgmt.xml
        
        TPC-DI XML structure:
        <TPCDI:Actions>
          <Action ActionType="NEW" ActionTS="2024-05-20T12:00:00">
            <Customer C_ID="123">
              <Name>...</Name>
              <Account CA_ID="987" CA_TAX_ST="1">
                ...
              </Account>
            </Customer>
          </Action>
        </TPCDI:Actions>
        
        Note: CustomerMgmt.xml is ALWAYS XML format per TPC-DI spec. There is no .txt version.
        Account data is nested inside Customer elements.
        """
        logger.info("Loading DimAccount dimension table from CustomerMgmt.xml")
        
        xml_df = self._read_customermgmt_xml(1)
        
        if xml_df.count() == 0:
            raise RuntimeError(
                f"XML file was read but contains 0 rows.\n"
                f"Please check:\n"
                f"- File exists at: Batch1/CustomerMgmt.xml\n"
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
                    # Check schema for array columns
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
                f"  - _ActionType, _ActionTS (Action attributes)\n"
                f"  - Customer._C_ID, Customer.Account._CA_ID, _CA_TAX_ST\n"
                f"  - Customer.Account.CA_NAME, CA_B_ID (elements)\n\n"
                f"Please check:\n"
                f"1. spark-xml library (com.databricks:spark-xml_2.12:0.15.0)\n"
                f"2. XML matches TPC-DI spec\n"
                f"3. rowTag=TPCDI:Action, rootTag=TPCDI:Actions"
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
            lit(1).alias("BatchID"),  # Batch 1 for historical load
            current_timestamp().alias("EffectiveDate"),
            lit(None).cast("timestamp").alias("EndDate")  # NULL for current records
        )
        
        row_count = dim_account.count()
        logger.info(f"Loaded DimAccount: {row_count} rows")
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(dim_account, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        return dim_account
    
    def run_full_batch_load(self, target_database: str, target_schema: str):
        """
        Run the full batch load process (historical load from Batch1).
        
        This is the legacy/direct load method that writes to Gold layer tables directly.
        For Medallion architecture, use run_medallion_batch_load() instead.
        
        Args:
            target_database: Target database/catalog name
            target_schema: Target schema name
        """
        logger.info("Starting full batch load process (direct to Gold)")
        
        # Load dimension tables
        self.load_dim_date(f"{target_database}.{target_schema}.DimDate")
        self.load_dim_time(f"{target_database}.{target_schema}.DimTime")
        self.load_dim_trade_type(f"{target_database}.{target_schema}.DimTradeType")
        self.load_dim_status_type(f"{target_database}.{target_schema}.DimStatusType")
        self.load_dim_tax_rate(f"{target_database}.{target_schema}.DimTaxRate")
        self.load_dim_industry(f"{target_database}.{target_schema}.DimIndustry")
        self.load_dim_account(f"{target_database}.{target_schema}.DimAccount")
        
        logger.info("Full batch load completed")
    
    def run_medallion_batch_load(self, target_database: str, target_schema: str, 
                                  batch_id: int = 1):
        """
        Run Medallion Architecture batch load: Bronze -> Silver layers.
        
        This follows the medallion pattern:
        1. Bronze: Raw data ingestion (no transformations)
        2. Silver: Cleaned/parsed data with SCD handling
        
        Args:
            target_database: Target database/catalog name
            target_schema: Target schema name
            batch_id: Batch number (1 for historical, 2+ for incremental)
        """
        from benchmark.etl.bronze import BronzeETL
        from benchmark.etl.silver import SilverETL
        
        logger.info(f"Starting Medallion batch load for Batch{batch_id}: Bronze -> Silver")
        
        # Bronze layer: Raw data ingestion
        logger.info("=== BRONZE LAYER: Raw Data Ingestion ===")
        bronze_etl = BronzeETL(self.platform)
        bronze_etl.run_bronze_batch_load(batch_id, target_database, target_schema)
        
        # Silver layer: Cleaned/parsed data
        logger.info("=== SILVER LAYER: Data Cleaning & Parsing ===")
        silver_etl = SilverETL(self.platform)
        silver_etl.run_silver_batch_load(batch_id, target_database, target_schema)
        
        logger.info(f"Medallion batch load completed for Batch{batch_id}")
