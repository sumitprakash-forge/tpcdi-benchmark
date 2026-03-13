"""
Configuration management for TPC-DI benchmark.
Supports both Databricks (DBFS) and Dataproc (GCS) platforms.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Platform(Enum):
    """Supported platforms for benchmarking."""
    DATABRICKS = "databricks"
    DATAPROC = "dataproc"


class LoadType(Enum):
    """Type of data load."""
    BATCH = "batch"
    INCREMENTAL = "incremental"


@dataclass
class BenchmarkConfig:
    """Configuration for TPC-DI benchmark run."""
    platform: Platform
    load_type: LoadType
    scale_factor: int
    raw_data_path: str  # GCS path for Dataproc; for Databricks use output_path when set
    target_database: str = "tpcdi_warehouse"  # Only used for Dataproc
    target_schema: str = "dw"
    target_catalog: Optional[str] = None  # Unity Catalog (required for Databricks)
    output_path: Optional[str] = None  # Databricks: raw data input location (DBFS or Volume base path)
    batch_id: Optional[int] = None  # For incremental loads
    spark_master: Optional[str] = None  # For Dataproc
    gcs_bucket: Optional[str] = None  # For Dataproc
    project_id: Optional[str] = None  # For Dataproc
    region: Optional[str] = None  # For Dataproc
    service_account_email: Optional[str] = None  # For Dataproc: service account email for GCS access
    service_account_key_file: Optional[str] = None  # For Dataproc: path to service account JSON key file
    table_format: Optional[str] = None  # For Dataproc: table format (delta or parquet); default parquet
    enable_metrics: bool = True
    metrics_output_path: Optional[str] = None
    log_detailed_stats: bool = True  # If True, log per-table timing and records; else only job start/end/total duration
    customer_mgmt_xml_format: Optional[str] = None  # "org.apache.spark.sql.execution.datasources.xml" (Databricks native), "xml"/"com.databricks.spark.xml" (spark-xml JAR); None = "xml"
    # Databricks workflow only: when True, workflow has a task that only loads bronze_customer_mgmt; main benchmark task skips that load and depends on it
    separate_customer_mgmt_bronze: bool = False
    # When True, run only bronze customer management table load then exit (used by the dedicated workflow task)
    bronze_only_customer_mgmt: bool = False
    # Optional cluster metadata for metrics (logged in metrics JSON and aggregate CSV)
    cluster_instance_type: Optional[str] = None  # Worker node type (e.g. n2d-standard-16, i3.xlarge)
    cluster_worker_count: Optional[int] = None   # Number of worker instances
    cluster_master_type: Optional[str] = None    # Master/driver node type (optional)
    cloud: Optional[str] = None  # For Databricks cost: AWS, Azure, or GCP
    def __post_init__(self):
        """Validate configuration."""
        if self.platform == Platform.DATAPROC:
            if not self.gcs_bucket:
                raise ValueError("gcs_bucket is required for Dataproc")
            if not self.project_id:
                raise ValueError("project_id is required for Dataproc")
            if not self.region:
                raise ValueError("region is required for Dataproc")
        
        if self.platform == Platform.DATABRICKS:
            if not self.target_catalog:
                raise ValueError("target_catalog is required for Databricks platform (Unity Catalog)")
        
        if self.load_type == LoadType.INCREMENTAL and self.batch_id is None:
            raise ValueError("batch_id is required for incremental loads")
        
        if self.enable_metrics and not self.metrics_output_path:
            # Default metrics path — use GCS bucket if available (GCP Databricks + Dataproc),
            # fall back to DBFS for AWS/Azure Databricks
            if self.gcs_bucket:
                self.metrics_output_path = f"gs://{self.gcs_bucket}/tpcdi/metrics"
            elif self.platform == Platform.DATABRICKS:
                self.metrics_output_path = f"dbfs:/mnt/tpcdi/metrics"
            else:
                self.metrics_output_path = f"gs://{self.gcs_bucket}/tpcdi/metrics"
