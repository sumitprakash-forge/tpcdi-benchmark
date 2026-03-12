"""
Databricks platform adapter for TPC-DI benchmark.
"""

import logging
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DatabricksPlatform:
    """Platform adapter for Databricks. Reads raw data from configured raw_data_path."""

    def __init__(self, spark: SparkSession, raw_data_path: str):
        """
        Initialize Databricks platform adapter.

        Args:
            spark: SparkSession (should already be configured)
            raw_data_path: Base path to raw TPC-DI data including sf=X (e.g. dbfs:/mnt/tpcdi/sf=10 or gs://bucket/tpcdi/sf=10)
        """
        self.spark = spark
        self.raw_data_path = raw_data_path.rstrip("/")

    def _resolve_path(self, relative_path: str) -> str:
        full = f"{self.raw_data_path}/{relative_path}".replace("//", "/")
        if full.startswith("gs:/") and not full.startswith("gs://"):
            full = "gs://" + full[4:]
        return full

    def _get_dbutils(self):
        """Return dbutils when on Databricks; None otherwise. Used for serverless (Spark Connect) file listing."""
        # Spark Connect / serverless: no sparkContext; try session-attached or caller-injected dbutils
        dbutils = getattr(self.spark, "dbutils", None)
        if dbutils is not None:
            return dbutils
        try:
            import inspect
            for frame in inspect.stack():
                dbutils = frame.frame.f_globals.get("dbutils")
                if dbutils is not None:
                    return dbutils
        except Exception:
            pass
        # Non-Connect: use JVM-backed DBUtils
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark.sparkContext())
        except Exception:
            return None

    def _sum_path_size_bytes_dbutils(self, path: str) -> int:
        """Recursively sum file sizes under path using dbutils.fs (no JVM). For Spark Connect / serverless."""
        dbutils = self._get_dbutils()
        if dbutils is None:
            return 0
        total = 0
        try:
            for f in dbutils.fs.ls(path):
                is_dir = getattr(f, "isDir", None)
                if callable(is_dir):
                    is_dir = is_dir()
                if is_dir or getattr(f, "is_directory", False):
                    total += self._sum_path_size_bytes_dbutils(f.path)
                else:
                    total += int(getattr(f, "size", 0) or 0)
        except Exception as e:
            logger.debug("dbutils.fs.ls(%s): %s", path, e)
        return total

    def read_raw_file(self, file_path: str, schema: Optional[StructType] = None,
                      format: str = "csv", **options) -> DataFrame:
        full_path = self._resolve_path(file_path)
        reader = self.spark.read.format(format)
        if schema:
            reader = reader.schema(schema)
        for key, value in options.items():
            reader = reader.option(key, value)
        print(f"Reading file: {full_path}")
        return reader.load(full_path)

    def read_batch_files(self, batch_id: int, file_pattern: str,
                         schema: Optional[StructType] = None, **options) -> DataFrame:
        batch_path = f"Batch{batch_id}/{file_pattern}"
        return self.read_raw_file(batch_path, schema=schema, **options)

    def read_historical_files(self, file_pattern: str,
                              schema: Optional[StructType] = None, **options) -> DataFrame:
        return self.read_batch_files(1, file_pattern, schema=schema, **options)

    def drop_table_if_exists(self, table_name: str) -> None:
        """Drop the table if it exists. No-op if table does not exist."""
        try:
            if self.spark.catalog.tableExists(table_name):
                self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                logger.info("Dropped table %s", table_name)
        except Exception as e:
            logger.warning("Could not drop table %s: %s", table_name, e)

    def write_table(self, df: DataFrame, table_name: str, mode: str = "overwrite",
                    partition_by: Optional[list] = None, format: str = "delta"):
        logger.info(f"Writing table: {table_name} (mode={mode}, format={format})")
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        # For overwrite + Delta: drop first. Use path-based write only for 2-part names (Hive metastore).
        # Unity Catalog (3-part catalog.schema.table) does not allow CREATE TABLE with dbfs LOCATION.
        # Spark Connect (serverless) does not expose spark.sql.warehouse.dir — use saveAsTable only.
        if mode == "overwrite" and format == "delta":
            self.drop_table_if_exists(table_name)
            parts = table_name.split(".")
            if len(parts) == 2 and not self._is_spark_connect():
                try:
                    warehouse = self.spark.conf.get("spark.sql.warehouse.dir", "").rstrip("/")
                except Exception:
                    warehouse = ""
                if warehouse and not warehouse.startswith("dbfs:"):
                    # Path-based write only when warehouse is not dbfs (UC uses dbfs and rejects it)
                    location = f"{warehouse}/{parts[0]}.db/{parts[1]}"
                    writer = df.write.format("delta").mode("overwrite")
                    if partition_by:
                        writer = writer.partitionBy(*partition_by)
                    writer.save(location)
                    self.spark.sql(f"CREATE TABLE {table_name} USING delta LOCATION '{location}'")
                    logger.info(f"Created table {table_name} from path {location}")
                    return
        actual_mode = mode
        if mode == "append" and not table_exists:
            actual_mode = "overwrite"
        writer = df.write.format(format).mode(actual_mode)
        if format == "delta" and mode == "append" and table_exists:
            writer = writer.option("mergeSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(table_name)

    def merge_upsert(self, df: DataFrame, table_name: str, key_columns: List[str],
                     format: str = "delta") -> None:
        """
        MERGE (upsert) into Delta table. SCD Type 1: update existing rows, insert new.
        If table does not exist, create with overwrite.
        """
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        if not table_exists:
            self.write_table(df, table_name, mode="overwrite", format=format)
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
                   format: str = "delta") -> None:
        """
        MERGE into Delta table with SCD Type 2: expire old row (set is_current=false, end_date=effective_date), insert new.
        Source = new version rows. If table does not exist, create with overwrite.
        """
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        if not table_exists:
            self.write_table(df, table_name, mode="overwrite", format=format)
            return
        # If target was created before SCD2 columns existed, it won't have is_current/end_date/effective_date
        target_columns = [f.lower() for f in self.spark.table(table_name).schema.fieldNames()]
        scd2_required = {is_current_column.lower(), end_date_column.lower(), effective_date_column.lower()}
        if not scd2_required.issubset(set(target_columns)):
            logger.info(
                "Target table %s missing SCD2 columns (%s); overwriting to establish schema",
                table_name, scd2_required,
            )
            self.write_table(df, table_name, mode="overwrite", format=format)
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

    def create_database(
        self,
        database_name: str,
        if_not_exists: bool = True,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        """
        When catalog and schema are provided: only check that the catalog exists;
        do not create it. If the catalog does not exist, exit gracefully with an error.
        Then create schema if not exists.
        When catalog/schema not provided: create Hive database.
        """
        exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        if catalog and schema:
            # Only check if catalog exists; do not create it
            try:
                existing = [
                    row.catalog for row in self.spark.sql("SHOW CATALOGS").collect()
                ]
                if catalog not in existing:
                    msg = (
                        f"Catalog '{catalog}' does not exist. "
                        "Please create the catalog (e.g. in Data > Catalogs) and retry."
                    )
                    logger.error(msg)
                    raise ValueError(msg)
            except ValueError:
                raise
            except Exception as e:
                logger.error(f"Could not check catalog existence: {e}")
                raise RuntimeError(
                    f"Could not verify catalog '{catalog}'. "
                    "Please ensure the catalog exists and retry."
                ) from e
            logger.info(f"Catalog '{catalog}' exists; creating or verifying schema.")
            self.spark.sql(f"CREATE SCHEMA {exists_clause} {catalog}.{schema}")
            logger.info(f"Created or verified schema {catalog}.{schema}")
        else:
            self.spark.sql(f"CREATE DATABASE {exists_clause} {database_name}")
            logger.info(f"Created database: {database_name}")

    def get_spark(self) -> SparkSession:
        return self.spark

    def get_table_count(self, table_name: str) -> int:
        result = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").first()
        return result.cnt if result else 0

    def get_table_size_mb(self, table_name: str) -> float:
        """Get approximate table size in MB. Tries DESCRIBE DETAIL first, falls back to DESCRIBE EXTENDED + file system."""
        try:
            # Check if table exists first
            if not self.spark.catalog.tableExists(table_name):
                logger.warning(f"Table {table_name} does not exist, cannot get size")
                return 0.0
            
            quoted = ".".join(f"`{p}`" for p in table_name.split("."))
            
            # Try DESCRIBE DETAIL first (Databricks-specific, more efficient)
            try:
                detail_df = self.spark.sql(f"DESCRIBE DETAIL {quoted}")
                row = detail_df.first()
                if row is not None:
                    # DESCRIBE DETAIL returns columns. Access by column name or index.
                    size = None
                    
                    # Try accessing by column name (Row supports dict-like access)
                    columns = detail_df.columns
                    for col_name in ['size', 'Size', 'SIZE']:
                        if col_name in columns:
                            try:
                                size = row[col_name]
                                break
                            except (KeyError, IndexError):
                                continue
                    
                    # If not found by name, try by index (find size column)
                    if size is None:
                        for i, col in enumerate(columns):
                            if col.lower() == 'size':
                                try:
                                    size = row[i]
                                    break
                                except (IndexError, AttributeError):
                                    continue
                    
                    if size is not None and size > 0:
                        mb = size / (1024 * 1024)
                        logger.debug(f"Table {table_name} size (from DESCRIBE DETAIL): {mb:.2f} MB ({size:,} bytes)")
                        return mb
            except Exception as e:
                logger.debug(f"DESCRIBE DETAIL failed for {table_name}: {e}, trying DESCRIBE EXTENDED")
            
            # Fallback: Use DESCRIBE EXTENDED + file system (same as Dataproc)
            try:
                desc_df = self.spark.sql(f"DESCRIBE EXTENDED {quoted}")
                loc_row = desc_df.filter("col_name = 'Location'").first()
                if loc_row is None:
                    logger.warning(f"Could not find Location in DESCRIBE EXTENDED for {table_name}")
                    return 0.0
                
                # Get location from row (column index 1 typically has the value)
                location = None
                if len(loc_row) > 1:
                    location = loc_row[1]
                elif hasattr(loc_row, "data_type"):
                    location = loc_row.data_type
                
                if not location or str(location).startswith("view:"):
                    logger.warning(f"Invalid location for {table_name}: {location}")
                    return 0.0
                
                location_str = str(location).strip()
                logger.debug(f"Table {table_name} location: {location_str}")
                
                # Sum file sizes using Hadoop FS
                total = self._sum_path_size_bytes(location_str)
                mb = total / (1024 * 1024) if total else 0.0
                if mb > 0:
                    logger.debug(f"Table {table_name} size (from file system): {mb:.2f} MB ({total:,} bytes)")
                else:
                    logger.warning(f"Table {table_name} size calculation returned 0 bytes from path {location_str}")
                return mb
            except Exception as e:
                logger.warning(f"DESCRIBE EXTENDED fallback also failed for {table_name}: {e}")
                return 0.0
                
        except Exception as e:
            logger.warning(f"Could not get table size for {table_name}: {e}", exc_info=True)
            return 0.0
    
    def _is_spark_connect(self) -> bool:
        """True if session uses Spark Connect (e.g. serverless). On classic, accessing spark.client can raise."""
        try:
            return getattr(self.spark, "client", None) is not None
        except Exception:
            return False

    def _sum_path_size_bytes(self, path: str) -> int:
        """Recursively sum file sizes under path. On Spark Connect uses dbutils.fs; else Hadoop FS (JVM)."""
        if self._is_spark_connect():
            return self._sum_path_size_bytes_dbutils(path)
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
        """Sum file sizes under raw_data_path/Batch{batch_id}/ for throughput metrics.
        On Spark Connect (serverless) uses dbutils.fs; otherwise uses Hadoop FS (JVM)."""
        batch_path = f"{self.raw_data_path}/Batch{batch_id}"
        # Spark Connect (e.g. Databricks serverless): use dbutils.fs (no JVM)
        if self._is_spark_connect():
            total = self._sum_path_size_bytes_dbutils(batch_path)
            if total > 0:
                logger.debug("Raw input size Batch%d (dbutils): %d bytes", batch_id, total)
            return total
        try:
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
