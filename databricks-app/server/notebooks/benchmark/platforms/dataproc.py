"""
Dataproc platform adapter for TPC-DI benchmark.
Handles GCS paths and Dataproc-specific Spark configuration.
"""

import logging
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DataprocPlatform:
    """Platform adapter for Dataproc with GCS storage."""
    
    def __init__(self, spark: SparkSession, raw_data_path: str, 
                 gcs_bucket: str, project_id: str,
                 service_account_email: Optional[str] = None,
                 service_account_key_file: Optional[str] = None,
                 table_format: Optional[str] = None):
        """
        Initialize Dataproc platform adapter.
        
        Args:
            spark: SparkSession (should already be configured)
            raw_data_path: Base path to raw TPC-DI data in GCS (e.g., gs://bucket/tpcdi/sf=10)
            gcs_bucket: GCS bucket name
            project_id: GCP project ID
            service_account_email: Optional service account email for GCS access
            service_account_key_file: Optional path to service account JSON key file
            table_format: Table format (delta or parquet); default parquet when None
        """
        self.spark = spark
        self.raw_data_path = raw_data_path.rstrip("/")
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self.table_format = (table_format or "delta").lower()
        
        # Ensure GCS connector is available
        try:
            # Set Hadoop configuration for GCS
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.AbstractFileSystem.gs.impl", 
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.gs.project.id", project_id
            )
            
            # Configure service account authentication if provided.
            # GCS connector expects a local key file path (FileInputStream); gs:// path causes NPE.
            key_file = (service_account_key_file or "").strip()
            use_keyfile = (
                service_account_email
                and key_file
                and not key_file.startswith("gs://")
            )
            if use_keyfile:
                spark.sparkContext._jsc.hadoopConfiguration().set(
                    "fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE"
                )
                spark.sparkContext._jsc.hadoopConfiguration().set(
                    "fs.gs.auth.service.account.email", service_account_email
                )
                spark.sparkContext._jsc.hadoopConfiguration().set(
                    "fs.gs.auth.service.account.keyfile", key_file
                )
                logger.info(f"Configured GCS access with service account key file: {service_account_email}")
            elif service_account_email:
                # Use service account email only (assumes cluster has access to impersonate)
                spark.sparkContext._jsc.hadoopConfiguration().set(
                    "fs.gs.auth.service.account.email", service_account_email
                )
                logger.info(f"Configured GCS access with service account email: {service_account_email}")
            else:
                # Use default authentication (cluster's service account or Application Default Credentials)
                logger.info("Using default GCS authentication (cluster service account or ADC)")
        except Exception as e:
            logger.warning(f"Could not configure GCS connector: {e}")
        
        logger.info(f"Initialized Dataproc platform with raw_data_path: {self.raw_data_path}")

    def _resolve_path(self, relative_path: str) -> str:
        """Resolve relative path against raw_data_path (e.g. gs://bucket/tpcdi/sf=10)."""
        return f"{self.raw_data_path}/{relative_path}".replace("//", "/")
    
    def read_raw_file(self, file_path: str, schema: Optional[StructType] = None,
                     format: str = "csv", **options) -> DataFrame:
        """
        Read a raw data file from GCS.
        
        Args:
            file_path: Relative path from raw_data_path (e.g., "Batch1/CustomerMgmt.txt")
            schema: Optional schema for the data
            format: File format (csv, parquet, json, etc.)
            **options: Additional options for the reader
        
        Returns:
            DataFrame with the data
        """
        full_path = f"{self.raw_data_path}/{file_path}"
        logger.debug(f"Reading file: {full_path}")
        
        reader = self.spark.read.format(format)
        if schema:
            reader = reader.schema(schema)
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(full_path)
    
    def read_batch_files(self, batch_id: int, file_pattern: str,
                        schema: Optional[StructType] = None, **options) -> DataFrame:
        """
        Read files from a specific batch directory.
        
        Args:
            batch_id: Batch number (e.g., 1, 2, 3...)
            file_pattern: File pattern within batch (e.g., "CustomerMgmt*.txt")
            schema: Optional schema
            **options: Reader options
        
        Returns:
            DataFrame with the data
        """
        batch_path = f"Batch{batch_id}/{file_pattern}"
        return self.read_raw_file(batch_path, schema=schema, **options)
    
    def read_historical_files(self, file_pattern: str, 
                             schema: Optional[StructType] = None, **options) -> DataFrame:
        """
        Read files from Batch1 directory (TPC-DI spec: historical data is in Batch1).
        
        Note: This method is kept for backward compatibility. New code should use
        read_batch_files(1, file_pattern, ...) directly.
        
        Args:
            file_pattern: File pattern (e.g., "Date.txt")
            schema: Optional schema
            **options: Reader options
        
        Returns:
            DataFrame with the data
        """
        # TPC-DI spec: historical data is in Batch1, not HistoricalLoad
        return self.read_batch_files(1, file_pattern, schema=schema, **options)
    
    def write_table(self, df: DataFrame, table_name: str, mode: str = "overwrite",
                   partition_by: Optional[list] = None, format: Optional[str] = None):
        """
        Write DataFrame to a table in the target database.
        Uses --format (delta | parquet) from config; default parquet.
        Use delta only if Delta package is on cluster (e.g. --packages io.delta:delta-spark_2.12:3.0.0).
        
        Args:
            df: DataFrame to write
            table_name: Target table name (database.table on Dataproc)
            mode: Write mode (overwrite, append, etc.)
            partition_by: Optional list of columns to partition by
            format: Override table format (delta | parquet); when None, use platform default from config
        """
        fmt = (format or self.table_format).lower()
        logger.info(f"Writing table: {table_name} (mode={mode}, format={fmt})")
        
        # Check if table exists (for append path and for logging)
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        
        # For overwrite + Delta on Dataproc: write to path then CREATE TABLE to avoid
        # "does not support append in batch mode" (saveAsTable can use append internally).
        if mode == "overwrite" and fmt == "delta":
            self.drop_table_if_exists(table_name)
            parts = table_name.split(".")
            if len(parts) >= 2:
                db, tbl = parts[0], parts[-1]
                warehouse = self.spark.conf.get("spark.sql.warehouse.dir", "").rstrip("/")
                if not warehouse:
                    warehouse = f"gs://{self.gcs_bucket}/spark-warehouse"
                location = f"{warehouse}/{db}.db/{tbl}"
                writer = df.write.format("delta").mode("overwrite")
                if partition_by:
                    writer = writer.partitionBy(*partition_by)
                writer.save(location)
                self.spark.sql(f"CREATE TABLE {table_name} USING delta LOCATION '{location}'")
                logger.info(f"Created table {table_name} from path {location}")
                return
        
        # For append mode, if table doesn't exist, create it (treat as overwrite for first write)
        actual_mode = mode
        if mode == "append" and not table_exists:
            logger.info(f"Table {table_name} does not exist. Creating it with first write.")
            actual_mode = "overwrite"
        
        writer = df.write.format(fmt).mode(actual_mode)
        
        # For Delta append, enable schema merge in case of minor schema differences
        if fmt == "delta" and mode == "append" and table_exists:
            writer = writer.option("mergeSchema", "true")
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.saveAsTable(table_name)

    def drop_table_if_exists(self, table_name: str) -> None:
        """Drop the table if it exists. No-op if table does not exist."""
        try:
            if self.spark.catalog.tableExists(table_name):
                self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                logger.info("Dropped table %s", table_name)
        except Exception as e:
            logger.warning("Could not drop table %s: %s", table_name, e)

    def merge_upsert(self, df: DataFrame, table_name: str, key_columns: List[str],
                     format: Optional[str] = None) -> None:
        """
        MERGE (upsert) into Delta table. SCD Type 1: update existing rows, insert new.
        If table does not exist, create with overwrite. Requires Delta format.
        """
        fmt = (format or self.table_format).lower()
        if fmt != "delta":
            logger.warning("merge_upsert requires Delta; falling back to overwrite")
            self.write_table(df, table_name, mode="overwrite", format=fmt)
            return
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        if not table_exists:
            self.write_table(df, table_name, mode="overwrite", format="delta")
            return
        view_name = "_gold_merge_source_" + table_name.replace(".", "_")
        df.createOrReplaceTempView(view_name)
        cols = [c for c in df.columns]
        on_clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in key_columns)
        update_set = ", ".join(f"t.`{c}` = s.`{c}`" for c in cols)
        insert_cols = ", ".join(f"`{c}`" for c in cols)
        insert_vals = ", ".join(f"s.`{c}`" for c in cols)
        merge_sql = (
            f"MERGE INTO {table_name} AS t "
            f"USING {view_name} AS s ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        logger.info("Executing MERGE (upsert) into %s on keys %s", table_name, key_columns)
        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView(view_name)

    def merge_scd2(self, df: DataFrame, table_name: str, key_column: str,
                  effective_date_column: str = "effective_date",
                  end_date_column: str = "end_date",
                  is_current_column: str = "is_current",
                  format: Optional[str] = None) -> None:
        """
        MERGE into Delta table with SCD Type 2: expire old row (set is_current=false, end_date=effective_date), insert new.
        Source = new version rows (e.g. Silver current rows). If table does not exist, create with overwrite.
        """
        fmt = (format or self.table_format).lower()
        if fmt != "delta":
            logger.warning("merge_scd2 requires Delta; falling back to overwrite")
            self.write_table(df, table_name, mode="overwrite", format=fmt)
            return
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        if not table_exists:
            self.write_table(df, table_name, mode="overwrite", format="delta")
            return
        # If target was created before SCD2 columns existed, it won't have is_current/end_date/effective_date
        target_columns = [f.lower() for f in self.spark.table(table_name).schema.fieldNames()]
        scd2_required = {is_current_column.lower(), end_date_column.lower(), effective_date_column.lower()}
        if not scd2_required.issubset(set(target_columns)):
            logger.info(
                "Target table %s missing SCD2 columns (%s); overwriting to establish schema",
                table_name, scd2_required,
            )
            self.write_table(df, table_name, mode="overwrite", format="delta")
            return
        view_name = "_gold_scd2_source_" + table_name.replace(".", "_")
        df.createOrReplaceTempView(view_name)
        cols = [c for c in df.columns]
        on_clause = f"t.`{key_column}` = s.`{key_column}` AND t.`{is_current_column}` = true"
        insert_cols = ", ".join(f"`{c}`" for c in cols)
        insert_vals = ", ".join(f"s.`{c}`" for c in cols)
        merge_sql = (
            f"MERGE INTO {table_name} AS t "
            f"USING {view_name} AS s ON {on_clause} "
            f"WHEN MATCHED THEN UPDATE SET t.`{is_current_column}` = false, t.`{end_date_column}` = s.`{effective_date_column}` "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        logger.info("Executing MERGE (SCD2) into %s on key %s", table_name, key_column)
        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView(view_name)
    
    def check_database_path_exists(self, database_name: str) -> bool:
        """
        Check if the GCS folder for the target database exists.
        
        Returns:
            True if path exists, False otherwise
        """
        path_str = f"gs://{self.gcs_bucket}/spark-warehouse/{database_name}.db"
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path_str)
            fs = path.getFileSystem(hadoop_conf)
            return fs.exists(path)
        except Exception as e:
            logger.warning(f"Could not check if target folder {path_str} exists: {e}")
            return False
    
    def delete_target_database_path_if_exists(self, database_name: str) -> None:
        """
        Delete the GCS folder for the target database (spark-warehouse/{db}.db) if it exists.
        Call before create_database for a clean batch run.
        """
        path_str = f"gs://{self.gcs_bucket}/spark-warehouse/{database_name}.db"
        try:
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            path = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path_str)
            fs = path.getFileSystem(hadoop_conf)
            if fs.exists(path):
                fs.delete(path, True)
                logger.info(f"Deleted existing target folder: {path_str}")
            else:
                logger.debug(f"Target folder does not exist (no delete): {path_str}")
        except Exception as e:
            logger.warning(f"Could not delete target folder {path_str}: {e}")

    def create_database(self, database_name: str, if_not_exists: bool = True):
        """Create a database if it doesn't exist."""
        exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        self.spark.sql(f"CREATE DATABASE {exists_clause} {database_name}")
        logger.info(f"Created database: {database_name}")
    
    def get_spark(self) -> SparkSession:
        """Get the SparkSession."""
        return self.spark
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        result = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").first()
        return result.cnt if result else 0
    
    def get_table_size_mb(self, table_name: str) -> float:
        """Get approximate table size in MB. Uses table location + Hadoop FS to sum file sizes.
        Skips DESCRIBE DETAIL because Dataproc's Delta/Spark can raise PARSE_SYNTAX_ERROR when
        rewriting it to SELECT SUM(size) FROM (SELECT ... DESCRIBE DETAIL ...).
        Uses DESCRIBE EXTENDED (standard Spark SQL) to get table location."""
        try:
            # Check if table exists first
            if not self.spark.catalog.tableExists(table_name):
                logger.warning(f"Table {table_name} does not exist, cannot get size")
                return 0.0
            
            desc_df = self.spark.sql(f"DESCRIBE EXTENDED {table_name}")
            loc_row = desc_df.filter("col_name = 'Location'").first()
            if loc_row is None:
                logger.warning(f"Could not find Location in DESCRIBE EXTENDED for {table_name}")
                return 0.0
            
            # Try different attribute names for location
            location = None
            if hasattr(loc_row, "data_type"):
                location = loc_row.data_type
            elif hasattr(loc_row, "info_value"):
                location = loc_row.info_value
            elif len(loc_row) > 1:
                location = loc_row[1]
            
            if not location or str(location).startswith("view:"):
                logger.warning(f"Invalid location for {table_name}: {location}")
                return 0.0
            
            location_str = str(location).strip()
            logger.debug(f"Table {table_name} location: {location_str}")
            total = self._sum_path_size_bytes(location_str)
            mb = total / (1024 * 1024) if total else 0.0
            if mb > 0:
                logger.debug(f"Table {table_name} size: {mb:.2f} MB ({total:,} bytes)")
            else:
                logger.warning(f"Table {table_name} size calculation returned 0 bytes from path {location_str}")
            return mb
        except Exception as e:
            logger.warning(f"Could not get table size for {table_name}: {e}", exc_info=True)
            return 0.0

    def _sum_path_size_bytes(self, path: str) -> int:
        """Recursively sum file sizes under path via Hadoop FS."""
        try:
            jvm = self.spark.sparkContext._jvm
            hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
            fs = hadoop_path.getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration())
            total = 0
            for status in fs.listStatus(hadoop_path) or []:
                if status.isDirectory():
                    total += self._sum_path_size_bytes(status.getPath().toString())
                else:
                    total += status.getLen()
            return int(total)
        except Exception:
            return 0

    def get_raw_input_size_bytes(self, batch_id: int) -> int:
        """Sum file sizes under raw_data_path/Batch{batch_id}/ for throughput metrics."""
        try:
            batch_path = f"{self.raw_data_path}/Batch{batch_id}"
            jvm = self.spark.sparkContext._jvm
            path = jvm.org.apache.hadoop.fs.Path(batch_path)
            fs = path.getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration())
            total = 0
            for status in fs.listStatus(path) or []:
                if status.isDirectory():
                    for child in fs.listStatus(status.getPath()) or []:
                        if not child.isDirectory():
                            total += child.getLen()
                else:
                    total += status.getLen()
            return int(total)
        except Exception as e:
            logger.warning(f"Could not get raw input size for Batch{batch_id}: {e}")
            return 0
