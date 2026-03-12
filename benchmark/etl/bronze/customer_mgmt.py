"""
Bronze layer loader for CustomerMgmt.xml.

Ingests raw XML structure from CustomerMgmt.xml with no field extraction.
Uses spark-xml (Spark DataFrameReader).
Schema: Uses schema definition from customer_mgmt_schema_definition.py (preferred).
  Falls back to customer_mgmt_schema.json if definition import fails.
  Falls back to inference if both fail (prints and saves schema for next run).
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)

_SCHEMA_FILENAME = "customer_mgmt_schema.json"


def _schema_path() -> Path:
    """Path to schema JSON. Use writable dir: module dir when not running from zip, else cwd or env."""
    module_dir = Path(__file__).resolve().parent
    # When run from a zip (e.g. Dataproc --py-files=benchmark.zip), module_dir is inside the zip and not writable
    if module_dir.is_dir() and os.access(module_dir, os.W_OK):
        return module_dir / _SCHEMA_FILENAME
    fallback = os.environ.get("TPCDI_CUSTOMERMGMT_SCHEMA_DIR")
    if fallback:
        return Path(fallback).resolve() / _SCHEMA_FILENAME
    return Path.cwd() / _SCHEMA_FILENAME


def _load_customer_mgmt_schema() -> Optional[StructType]:
    """
    Load CustomerMgmt schema with priority:
    1. Schema definition from customer_mgmt_schema_definition.py (preferred)
    2. JSON file (customer_mgmt_schema.json) if definition import fails
    3. None (will infer schema)
    """
    # First try: use schema definition from Python module
    try:
        from benchmark.etl.bronze.customer_mgmt_schema_definition import get_customer_mgmt_schema
        schema = get_customer_mgmt_schema()
        logger.debug("Using CustomerMgmt schema from customer_mgmt_schema_definition.py")
        return schema
    except ImportError as e:
        logger.debug(f"Could not import schema definition module: {e}")
    except Exception as e:
        logger.warning(f"Could not load schema from definition module: {e}")
    
    # Second try: load from JSON file
    module_dir = Path(__file__).resolve().parent
    for path in [
        module_dir / _SCHEMA_FILENAME,  # repo/module dir when not from zip
        _schema_path(),  # writable fallback (cwd or env) when running from zip
    ]:
        if path.is_file():
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                # Pass dict; fromJson() in PySpark accepts dict (Databricks) or str (some versions)
                if isinstance(data, dict):
                    schema = StructType.fromJson(data)
                else:
                    schema = StructType.fromJson(json.dumps(data))
                logger.debug(f"Using CustomerMgmt schema from JSON file: {path}")
                return schema
            except Exception as e:
                logger.warning(f"Could not load schema from {path}: {e}")
                continue
    
    # No schema available - will infer
    return None


def _save_customer_mgmt_schema(schema_json: str) -> None:
    """Save schema JSON to file for next run (avoids inference)."""
    path = _schema_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(schema_json)
        logger.info(f"Saved CustomerMgmt schema to {path} for next run")
    except Exception as e:
        logger.warning(f"Could not save schema to {path}: {e}")


def _print_customer_mgmt_schema(df: DataFrame, always_print: bool = False) -> None:
    """Print CustomerMgmt.xml schema (JSON + DDL). Set always_print=True when we just inferred; else only if env var set."""
    if not always_print and os.environ.get("TPCDI_PRINT_CUSTOMERMGMT_SCHEMA") != "1":
        return
    try:
        schema_json = df.schema.json()
        schema_ddl = df.schema.simpleString()
        print("\n" + "=" * 80)
        print("CustomerMgmt.xml schema (saved to customer_mgmt_schema.json for next run)")
        print("=" * 80)
        print("\n# JSON (StructType.fromJson):")
        print(schema_json)
        print("\n# DDL (simpleString):")
        print(schema_ddl)
        print("=" * 80 + "\n")
    except Exception as e:
        logger.warning(f"Could not print schema: {e}")


class BronzeCustomerMgmt(BronzeLoaderBase):
    """
    Bronze layer loader for CustomerMgmt.xml.
    
    The XML is read with spark-xml (or UDTF on Databricks) and stored as nested struct.
    No field extraction - just raw capture of the XML structure.
    """
    
    def load(
        self,
        batch_id: int,
        target_table: str,
        xml_format: Optional[str] = None,
    ) -> DataFrame:
        """
        Ingest CustomerMgmt.xml as raw XML structure.
        
        Args:
            batch_id: Batch number (1 for historical, 2+ for incremental)
            target_table: Full target table name
            xml_format: Spark data source format for XML.
                - "org.apache.spark.sql.execution.datasources.xml": Databricks native XML reader (no custom JAR).
                - "xml" or "com.databricks.spark.xml": spark-xml library (use when attaching custom JAR).
                None = "xml".
            
        Returns:
            DataFrame with raw XML structure
            
        Raises:
            RuntimeError: If XML cannot be read
        """
        logger.info(f"Loading bronze_customer_mgmt from Batch{batch_id}")
        file_path = f"Batch{batch_id}/CustomerMgmt.xml"
        
        # Read XML with spark-xml. Use schema definition/JSON if available (skips inference); else infer, print, and save.
        fmt = (xml_format or "xml").strip() or "xml"
        logger.info(f"CustomerMgmt.xml reader format: {fmt}")
        schema = _load_customer_mgmt_schema()
        df = None
        success = False
        used_schema = False
        schema_source = None
        format_fallback = "xml"  # fallback if com.databricks.spark.xml fails with ServiceConfigurationError
        for row_tag, root_tag in [("TPCDI:Action", "TPCDI:Actions"), ("Action", None)]:
            try:
                opts = {"format": fmt, "rowTag": row_tag}
                if root_tag:
                    opts["rootTag"] = root_tag
                if schema is not None:
                    opts["schema"] = schema
                    used_schema = True
                    # Determine schema source for logging
                    try:
                        from benchmark.etl.bronze.customer_mgmt_schema_definition import get_customer_mgmt_schema
                        if schema == get_customer_mgmt_schema():
                            schema_source = "definition module"
                    except Exception:
                        schema_source = "JSON file"
                df = self.platform.read_raw_file(file_path, **opts)
                if df.count() > 0:
                    schema_msg = f" (using {schema_source})" if schema_source else " (inferred schema)"
                    logger.info(f"Successfully read XML with rowTag={row_tag}, format={fmt}{schema_msg}")
                    success = True
                    break
                df = None
            except Exception as e:
                err_msg = str(e)
                # com.databricks.spark.xml can fail with ServiceConfigurationError (e.g. no-arg constructor)
                if fmt == "com.databricks.spark.xml" and (
                    "ServiceConfigurationError" in err_msg or "Unable to get public no-arg constructor" in err_msg
                ):
                    logger.warning(
                        f"Format com.databricks.spark.xml failed ({e}); falling back to format 'xml'"
                    )
                    fmt = format_fallback
                    opts = {"format": fmt, "rowTag": row_tag}
                    if root_tag:
                        opts["rootTag"] = root_tag
                    if schema is not None:
                        opts["schema"] = schema
                    try:
                        df = self.platform.read_raw_file(file_path, **opts)
                        if df.count() > 0:
                            schema_msg = f" (using {schema_source})" if schema_source else " (inferred schema)"
                            logger.info(f"Successfully read XML with rowTag={row_tag}, format={fmt}{schema_msg}")
                            success = True
                            break
                    except Exception as e2:
                        logger.warning(f"Fallback format 'xml' also failed: {e2}")
                    if not success:
                        df = None
                    if success:
                        break
                    continue
                if schema is not None:
                    logger.warning(f"Read with schema failed, will infer: {e}")
                    schema = None
                    used_schema = False
                    schema_source = None
                    continue
                logger.warning(f"Failed to read XML with rowTag={row_tag}: {e}")
                df = None

        if not success or df is None:
            raise RuntimeError(
                f"Could not read CustomerMgmt.xml from Batch{batch_id}.\n"
                f"Ensure spark-xml library is installed (com.databricks:spark-xml_2.12:0.15.0)"
            )
        if used_schema:
            _print_customer_mgmt_schema(df, always_print=False)
        else:
            _print_customer_mgmt_schema(df, always_print=True)
            _save_customer_mgmt_schema(df.schema.json())
        return self._write_bronze_table(df, target_table, batch_id, "CustomerMgmt.xml")
