"""
Base class for Silver layer ETL loaders.

Provides common functionality for data cleaning and transformation,
including CDC (Change Data Capture) and SCD Type 2 handling.
"""

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, current_timestamp, coalesce, min as spark_min

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed
from benchmark.config import LoadType

logger = logging.getLogger(__name__)


def _get_table_size_bytes(platform, table_name: str) -> Optional[int]:
    """Return table size in bytes from platform if available; else None."""
    try:
        get_mb = getattr(platform, "get_table_size_mb", None)
        if get_mb:
            mb = get_mb(table_name)
            if mb and mb > 0:
                bytes_val = int(mb * 1024 * 1024)
                logger.debug(f"Table {table_name} size: {mb:.2f} MB ({bytes_val:,} bytes)")
                return bytes_val
            else:
                logger.warning(f"Table {table_name} size returned 0 or None from platform.get_table_size_mb()")
        else:
            logger.debug(f"Platform does not have get_table_size_mb method")
    except Exception as e:
        logger.warning(f"Could not get table size for {table_name}: {e}", exc_info=True)
    return None


class SilverLoaderBase:
    """
    Base class for Silver layer data loaders.
    
    Provides common functionality:
    - Platform adapter access
    - Pipe-delimited parsing
    - CSV parsing
    - Standardized write operations
    """
    
    def __init__(self, platform):
        """
        Initialize Silver loader.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
    
    def _extract_record_type(self, parsed_df: DataFrame, batch_id: int, col_offset: int = 0) -> tuple:
        """
        Extract record_type from first column for incremental loads.
        
        Args:
            parsed_df: Parsed DataFrame with _c0, _c1, etc. columns
            batch_id: Batch number
            col_offset: Offset to apply to column indices (default 0)
            
        Returns:
            Tuple of (record_type_column, actual_col_offset)
            - record_type_column: Column expression for record_type
            - actual_col_offset: Column offset to use for data columns (0 or 1)
        """
        def col_exists(idx: int) -> bool:
            return f"_c{idx}" in parsed_df.columns
        
        if batch_id > 1 and col_exists(col_offset):
            # Incremental load: extract record_type from first column
            return (col(f"_c{col_offset}").alias("record_type"), col_offset + 1)
        else:
            # Batch 1: no record_type, use default
            return (lit("I").alias("record_type"), col_offset)
    
    def _parse_pipe_delimited(self, df: DataFrame, num_cols: int) -> DataFrame:
        """
        Parse pipe-delimited raw_line into columns _c0, _c1, etc.
        
        Args:
            df: DataFrame with 'raw_line' column
            num_cols: Number of expected columns
            
        Returns:
            DataFrame with parsed columns
        """
        temp_view = f"_temp_pipe_parse_{id(df)}"
        df.createOrReplaceTempView(temp_view)
        
        select_parts = []
        for i in range(num_cols):
            # Use get() to tolerate out-of-bounds (returns NULL); element_at() throws INVALID_ARRAY_INDEX
            select_parts.append(
                f"TRIM(COALESCE(get(split(raw_line, '\\\\|'), {i}), '')) AS _c{i}"
            )
        
        # Keep metadata columns
        select_parts.extend([
            "_load_timestamp",
            "_source_file", 
            "_batch_id"
        ])
        
        sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
        
        try:
            result = self.spark.sql(sql)
        except Exception:
            # Retry without escape if needed
            select_parts = []
            for i in range(num_cols):
                select_parts.append(
                    f"TRIM(COALESCE(element_at(split(raw_line, '|'), {i+1}), '')) AS _c{i}"
                )
            select_parts.extend(["_load_timestamp", "_source_file", "_batch_id"])
            sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
            result = self.spark.sql(sql)
        
        # Note: We do not drop the temp view here because Spark uses lazy evaluation.
        # The DataFrame returned is lazy and will reference the temp view when executed.
        # Dropping the view immediately would cause "TABLE_OR_VIEW_NOT_FOUND" errors.
        # Temporary views are session-scoped and will be automatically cleaned up when
        # the SparkSession ends. On Databricks serverless, the session lifecycle is
        # managed by the platform, so manual cleanup is not necessary.
        
        return result
    
    def _parse_csv_delimited(self, df: DataFrame, num_cols: int) -> DataFrame:
        """
        Parse comma-delimited raw_line into columns _c0, _c1, etc.
        
        Args:
            df: DataFrame with 'raw_line' column
            num_cols: Number of expected columns
            
        Returns:
            DataFrame with parsed columns
        """
        temp_view = f"_temp_csv_parse_{id(df)}"
        df.createOrReplaceTempView(temp_view)
        
        select_parts = []
        for i in range(num_cols):
            select_parts.append(
                f"TRIM(COALESCE(get(split(raw_line, ','), {i}), '')) AS _c{i}"
            )
        select_parts.extend(["_load_timestamp", "_source_file", "_batch_id"])
        
        sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
        result = self.spark.sql(sql)
        
        # Note: We do not drop the temp view here because Spark uses lazy evaluation.
        # The DataFrame returned is lazy and will reference the temp view when executed.
        # Dropping the view immediately would cause "TABLE_OR_VIEW_NOT_FOUND" errors.
        # Temporary views are session-scoped and will be automatically cleaned up when
        # the SparkSession ends. On Databricks serverless, the session lifecycle is
        # managed by the platform, so manual cleanup is not necessary.
        
        return result
    
    def _write_silver_table(self, df: DataFrame, target_table: str, 
                            batch_id: int) -> DataFrame:
        """
        Write DataFrame to Silver table.
        
        Args:
            df: Input DataFrame
            target_table: Full table name (catalog.schema.table)
            batch_id: Batch number
            
        Returns:
            DataFrame that was written
        """
        # Batch mode: always overwrite. Incremental: overwrite for batch_id 1, append for 2+
        load_type = getattr(self.platform, "_tpcdi_load_type", None)
        mode = "overwrite" if load_type == LoadType.BATCH else ("overwrite" if batch_id == 1 else "append")
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(df, target_table, mode=mode)
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: {mode}")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        
        return df
    
    def _apply_scd_type2(self, incoming_df: DataFrame, target_table: str,
                         key_column: str, effective_date_col: str = "effective_date",
                         record_type_col: Optional[str] = None,
                         exclude_record_types: Optional[List[str]] = None) -> DataFrame:
        """
        Apply SCD Type 2 logic for incremental loads.
        
        This method:
        1. Closes out existing current records that have updates (set is_current=False, end_date)
        2. Inserts new versions of updated records (and new records)
        3. When record_type_col/exclude_record_types are set (e.g. D=delete):
           - Close step uses all incoming keys (including D)
           - Append step inserts only rows whose record_type is NOT in exclude_record_types
           So D = close current row only, no insert.
        
        Args:
            incoming_df: DataFrame with incoming changes
            target_table: Target table name
            key_column: Business key column (e.g., 'customer_id', 'account_id')
            effective_date_col: Column containing the effective date
            record_type_col: Optional column name for record_type (I/U/D)
            exclude_record_types: Optional list of record_type values to exclude from append (e.g. ["D"])
            
        Returns:
            DataFrame with changes applied
        """
        # Log timing for entire SCD Type 2 operation (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting SCD Type 2 for {target_table} at {start_datetime}")
        
        logger.info(f"Applying SCD Type 2 to {target_table} on key {key_column}")
        
        # Check if target table exists
        try:
            existing_df = self.spark.table(target_table)
            table_exists = True
        except Exception:
            logger.info(f"Table {target_table} does not exist, will create")
            table_exists = False
        
        if not table_exists:
            # First load - just write directly
            write_start = time.time()
            self.platform.write_table(incoming_df, target_table, mode="overwrite")
            write_end = time.time()
            row_count = incoming_df.count()
            if table_timing_is_detailed():
                logger.info(f"[TIMING] {target_table} (initial create) - Duration: {write_end - write_start:.2f}s, Rows: {row_count}")
            logger.info(f"Created {target_table} with {row_count} rows")
            
            end_time = time.time()
            end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            duration = end_time - start_time
            if table_timing_is_detailed():
                logger.info(f"[TIMING] Completed SCD Type 2 for {target_table} at {end_datetime}")
                logger.info(f"[TIMING] {target_table} (SCD Type 2) - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s")
            table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
            return incoming_df
        
        # Align schema: Read existing table and ensure incoming schema matches
        # This handles cases where Batch 1 had different types (e.g., dob as string vs date)
        try:
            existing_schema = existing_df.schema
            incoming_schema = incoming_df.schema
            
            # Check for schema mismatches and align incoming data
            schema_fields = {}
            for field in existing_schema:
                schema_fields[field.name] = field.dataType
            
            # Build select with type casts to match existing schema
            select_cols = []
            for field in incoming_schema:
                col_name = field.name
                if col_name in schema_fields:
                    existing_type = schema_fields[col_name]
                    incoming_type = field.dataType
                    if str(existing_type) != str(incoming_type):
                        # Type mismatch - cast to match existing
                        logger.info(f"Aligning column {col_name}: {incoming_type} -> {existing_type}")
                        select_cols.append(col(col_name).cast(existing_type).alias(col_name))
                    else:
                        select_cols.append(col(col_name))
                else:
                    # New column - keep as is
                    select_cols.append(col(col_name))
            
            if select_cols:
                incoming_df = incoming_df.select(*select_cols)
                logger.info(f"Schema aligned for {target_table}")
        except Exception as e:
            logger.warning(f"Schema alignment failed: {e}. Proceeding with original schema.")
        
        # Get business keys from incoming data
        incoming_keys = incoming_df.select(key_column).distinct()
        
        # Create temp views for SQL
        incoming_df.createOrReplaceTempView("incoming_changes")
        incoming_keys.createOrReplaceTempView("incoming_keys")
        
        # MERGE requires target to have is_current and end_date (SCD2 schema)
        existing_field_names = existing_df.schema.fieldNames()
        has_scd2_schema = "is_current" in existing_field_names and "end_date" in existing_field_names
        
        # Step 1: Close out existing current records that have updates (or use fallback)
        merge_ok = False
        if has_scd2_schema:
            close_sql = f"""
            MERGE INTO {target_table} AS target
            USING (
                SELECT {key_column}, MIN({effective_date_col}) as new_effective_date
                FROM incoming_changes
                GROUP BY {key_column}
            ) AS updates
            ON target.{key_column} = updates.{key_column} AND target.is_current = true
            WHEN MATCHED THEN UPDATE SET
                target.is_current = false,
                target.end_date = updates.new_effective_date
            """
            try:
                self.spark.sql(close_sql)
                logger.info(f"Closed out existing records for updated keys in {target_table}")
                merge_ok = True
            except Exception as e:
                logger.warning(f"MERGE failed (may not be Delta/Parquet): {e}. Using read-modify-write fallback for SCD2.")
        else:
            logger.info(f"Target {target_table} has no is_current/end_date (legacy batch-1 schema). Using read-modify-write fallback for SCD2.")

        if not merge_ok:
            # Fallback: Same SCD2 logic using DataFrame ops (works for Parquet or legacy schema)
            updates_df = incoming_df.groupBy(key_column).agg(
                spark_min(effective_date_col).alias("new_effective_date")
            )
            existing_alias = existing_df.alias("e")
            updates_alias = updates_df.alias("u")
            # Use actual existing table schema (target may lack is_current/end_date from batch-1-only load)
            existing_col_names = set(existing_df.schema.fieldNames())
            has_scd2_cols = "is_current" in existing_col_names and "end_date" in existing_col_names
            existing_updated = None
            if has_scd2_cols:
                try:
                    existing_cols = [f for f in existing_field_names if f not in ("is_current", "end_date")]
                    existing_updated = existing_alias.join(updates_alias, col("e." + key_column) == col("u." + key_column), "left").select(
                        *[col("e." + f) for f in existing_cols],
                        when(col("e.is_current") & col("u.new_effective_date").isNotNull(), lit(False)).otherwise(col("e.is_current")).alias("is_current"),
                        when(col("e.is_current") & col("u.new_effective_date").isNotNull(), col("u.new_effective_date")).otherwise(col("e.end_date")).alias("end_date"),
                    )
                    # Trigger analysis so we catch UNRESOLVED_COLUMN (e.g. e.is_current missing) here
                    existing_updated.limit(0).count()
                except Exception:
                    has_scd2_cols = False
                    existing_updated = None
            if not has_scd2_cols or existing_updated is None:
                # Legacy schema: table from batch 1 without is_current/end_date; treat all existing rows as current
                existing_cols = list(existing_col_names)
                existing_updated = existing_alias.join(updates_alias, col("e." + key_column) == col("u." + key_column), "left").select(
                    *[col("e." + f) for f in existing_cols],
                    when(col("u.new_effective_date").isNotNull(), lit(False)).otherwise(lit(True)).alias("is_current"),
                    when(col("u.new_effective_date").isNotNull(), col("u.new_effective_date")).otherwise(lit(None).cast("timestamp")).alias("end_date"),
                )
            append_df = incoming_df
            if record_type_col and exclude_record_types:
                append_df = incoming_df.filter(~col(record_type_col).isin(*exclude_record_types))
            schema_names = [f.name for f in existing_updated.schema.fields]
            append_select = append_df.select(*[col(c) for c in schema_names if c in append_df.columns])
            combined = existing_updated.unionByName(append_select, allowMissingColumns=True)
            write_start = time.time()
            self.platform.write_table(combined, target_table, mode="overwrite")
            write_end = time.time()
            row_count = append_df.count()
            if table_timing_is_detailed():
                logger.info(f"[TIMING] {target_table} (SCD2 fallback) - Duration: {write_end - write_start:.2f}s, Rows inserted: {row_count}")
            try:
                self.spark.catalog.dropTempView("incoming_changes")
                self.spark.catalog.dropTempView("incoming_keys")
            except Exception:
                pass
            end_time = time.time()
            table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
            return incoming_df

        # Step 2: Insert new versions (exclude D: close-only, no insert)
        append_df = incoming_df
        if record_type_col and exclude_record_types:
            append_df = incoming_df.filter(~col(record_type_col).isin(*exclude_record_types))
            logger.info(f"Excluding record_type in {exclude_record_types} from append ({append_df.count()} rows to insert)")
        
        # Use mergeSchema=true for Delta append to handle schema evolution
        write_start = time.time()
        self.platform.write_table(append_df, target_table, mode="append")
        write_end = time.time()
        row_count = append_df.count()
        if table_timing_is_detailed():
            logger.info(f"[TIMING] {target_table} (append) - Duration: {write_end - write_start:.2f}s, Rows: {row_count}")
        
        # Cleanup temp views
        try:
            self.spark.catalog.dropTempView("incoming_changes")
            self.spark.catalog.dropTempView("incoming_keys")
        except:
            pass
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed SCD Type 2 for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} (SCD Type 2) - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows inserted: {row_count}")
        logger.info(f"SCD Type 2 applied to {target_table}: {row_count} new versions inserted")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return incoming_df
    
    def _upsert_fact_table(self, incoming_df: DataFrame, target_table: str,
                           key_column: str, update_columns: Optional[List[str]] = None) -> DataFrame:
        """
        Upsert fact table data using MERGE.
        
        For CDC on fact tables:
        - If record exists (by key), update specified columns
        - If record doesn't exist, insert new record
        
        Args:
            incoming_df: DataFrame with incoming data
            target_table: Target table name
            key_column: Primary key column (e.g., 'trade_id')
            update_columns: Columns to update on match (None = update all)
            
        Returns:
            DataFrame with changes applied
        """
        # Log timing for entire upsert operation (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting upsert for {target_table} at {start_datetime}")
        
        logger.info(f"Upserting {target_table} on key {key_column}")
        
        # Check if target table exists
        try:
            existing_df = self.spark.table(target_table)
            table_exists = True
        except Exception:
            logger.info(f"Table {target_table} does not exist, will create")
            table_exists = False
        
        if not table_exists:
            # First load - just write directly
            write_start = time.time()
            self.platform.write_table(incoming_df, target_table, mode="overwrite")
            write_end = time.time()
            row_count = incoming_df.count()
            if table_timing_is_detailed():
                logger.info(f"[TIMING] {target_table} (initial create) - Duration: {write_end - write_start:.2f}s, Rows: {row_count}")
            logger.info(f"Created {target_table} with {row_count} rows")
            
            end_time = time.time()
            end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            duration = end_time - start_time
            if table_timing_is_detailed():
                logger.info(f"[TIMING] Completed upsert for {target_table} at {end_datetime}")
                logger.info(f"[TIMING] {target_table} (upsert) - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s")
            table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
            return incoming_df
        
        # Create temp view for incoming data
        incoming_df.createOrReplaceTempView("incoming_data")
        
        # Build UPDATE SET clause
        if update_columns is None:
            # Update all columns except the key
            update_columns = [c for c in incoming_df.columns if c != key_column]
        
        update_set = ", ".join([f"target.{c} = source.{c}" for c in update_columns])
        
        # Build INSERT columns
        all_columns = incoming_df.columns
        insert_cols = ", ".join(all_columns)
        insert_vals = ", ".join([f"source.{c}" for c in all_columns])
        
        # MERGE statement
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING incoming_data AS source
        ON target.{key_column} = source.{key_column}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        try:
            merge_start = time.time()
            self.spark.sql(merge_sql)
            merge_end = time.time()
            if table_timing_is_detailed():
                logger.info(f"[TIMING] {target_table} (MERGE) - Duration: {merge_end - merge_start:.2f}s")
            logger.info(f"MERGE completed for {target_table}")
        except Exception as e:
            logger.warning(f"MERGE failed (may not be Delta): {e}. Falling back to append-only.")
            # Fallback: just append (works for non-Delta tables)
            write_start = time.time()
            self.platform.write_table(incoming_df, target_table, mode="append")
            write_end = time.time()
            row_count = incoming_df.count()
            if table_timing_is_detailed():
                logger.info(f"[TIMING] {target_table} (fallback append) - Duration: {write_end - write_start:.2f}s, Rows: {row_count}")
        
        # Cleanup temp view
        try:
            self.spark.catalog.dropTempView("incoming_data")
        except:
            pass
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = incoming_df.count()
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed upsert for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} (upsert) - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        
        return incoming_df
