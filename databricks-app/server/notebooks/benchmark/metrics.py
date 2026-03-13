"""
Performance metrics collection and logging for TPC-DI benchmark.
"""

import json
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _write_string_via_hadoop_fs(spark: "SparkSession", path: str, content: str) -> bool:
    """Write a string to path using Spark's Hadoop FileSystem.

    Uses whatever credentials are in hadoopConfiguration():
    - For gs://: typically cluster GCS connector (not Unity Catalog credentials).
    - For /Volumes/ or dbfs:/Volumes/: on Databricks this uses UC credentials for the volume.
    On Databricks, save() tries dbutils first for gs:// or /Volumes/ so UC credentials are used when path is UC-managed.
    """
    try:
        sc = spark.sparkContext
        jvm = sc._jvm
        hadoop_conf = sc._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI.create(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        out = fs.create(hadoop_path, True)
        content_bytes = content.encode("utf-8")
        out.write(content_bytes)
        out.close()
        fs.close()
        return True
    except Exception as e:
        logger.debug("Spark FS write failed for %s: %s", path[:80], e)
        return False


def _write_string_to_gcs_via_spark(spark: "SparkSession", gcs_path: str, content: str) -> bool:
    """Write a string to a GCS path using Spark's Hadoop FileSystem.

    Uses cluster hadoopConfiguration() (GCS connector), not Unity Catalog credentials.
    For GCS locations registered in UC, use a UC Volume path or dbutils (auto-resolved on Databricks).
    """
    return _write_string_via_hadoop_fs(spark, gcs_path, content)


def _get_dbutils_if_databricks(spark: Optional["SparkSession"], platform: str):
    """Return dbutils when on Databricks and spark is available; else None. Used only for metrics path."""
    if spark is None or platform != "databricks":
        return None
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:
        return None


@dataclass
class StepMetrics:
    """Metrics for a single ETL step."""
    step_name: str
    start_time: float
    end_time: Optional[float] = None
    duration_seconds: Optional[float] = None
    rows_processed: Optional[int] = None
    bytes_processed: Optional[int] = None
    status: str = "running"  # running, completed, failed
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def finish(self, rows: Optional[int] = None, bytes: Optional[int] = None, 
               metadata: Optional[Dict[str, Any]] = None):
        """Mark step as completed."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        self.status = "completed"
        if rows is not None:
            self.rows_processed = rows
        if bytes is not None:
            self.bytes_processed = bytes
        if metadata:
            self.metadata = metadata
    
    def fail(self, error_message: str):
        """Mark step as failed."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        self.status = "failed"
        self.error_message = error_message


@dataclass
class BenchmarkMetrics:
    """Complete metrics for a benchmark run."""
    platform: str
    load_type: str
    scale_factor: int
    batch_id: Optional[int]
    start_time: float
    end_time: Optional[float] = None
    total_duration_seconds: Optional[float] = None
    steps: List[StepMetrics] = None
    summary: Optional[Dict[str, Any]] = None
    # Cluster metadata (instance type, worker count) for comparison across runs
    cluster_instance_type: Optional[str] = None
    cluster_worker_count: Optional[int] = None
    cluster_master_type: Optional[str] = None
    # Platform type for result/metrics: "databricks" | "dataproc" | "dataproc_serverless"
    platform_type: Optional[str] = None
    # Databricks only: "serverless" or "classic" (provisioned) compute
    databricks_compute_type: Optional[str] = None
    # Databricks only: job ID and run ID when running as a job (from clusterUsageTags or notebook context)
    databricks_job_id: Optional[str] = None
    databricks_run_id: Optional[str] = None
    # Path where metrics JSON was saved (set in save() before writing)
    metrics_saved_path: Optional[str] = None
    # DQ time per silver table: [{"table": str, "duration_seconds": float}, ...]
    dq_table_timings: Optional[List[Dict[str, Any]]] = None
    # Cost estimation (compute + software/DBU; list-price approximation)
    cost_breakdown: Optional[Dict[str, Any]] = None  # e.g. {"compute_usd": x, "software_usd": y, "dbu_usd": z, ...}
    total_cost_usd: Optional[float] = None
    dbu_cost_usd: Optional[float] = None  # Databricks only
    # Table override flag: True if tables/paths existed before loading (overridden), False otherwise
    table_override: Optional[bool] = None
    # Human-readable "TPC-DI BENCHMARK RESULTS - DATABRICKS/DATAPROC" summary (captured in saved JSON)
    benchmark_results_summary: Optional[str] = None
    # Per-table stats: table_name -> { duration_seconds, row_count, bytes_processed, throughput_rows_per_sec, throughput_mb_per_sec }
    per_table_details: Optional[Dict[str, Dict[str, Any]]] = None

    def __post_init__(self):
        if self.steps is None:
            self.steps = []

    def set_cluster_info(
        self,
        instance_type: Optional[str] = None,
        worker_count: Optional[int] = None,
        master_type: Optional[str] = None,
    ):
        """Set cluster metadata (from config or auto-detection)."""
        if instance_type is not None:
            self.cluster_instance_type = instance_type
        if worker_count is not None:
            self.cluster_worker_count = worker_count
        if master_type is not None:
            self.cluster_master_type = master_type
    
    def finish(self):
        """Mark benchmark as completed."""
        self.end_time = time.time()
        self.total_duration_seconds = self.end_time - self.start_time
        
        # Calculate summary
        completed_steps = [s for s in self.steps if s.status == "completed"]
        failed_steps = [s for s in self.steps if s.status == "failed"]
        
        total_rows = sum(s.rows_processed or 0 for s in completed_steps)
        total_bytes = sum(s.bytes_processed or 0 for s in completed_steps)
        total_duration = sum(s.duration_seconds or 0 for s in completed_steps)
        
        self.summary = {
            "total_steps": len(self.steps),
            "completed_steps": len(completed_steps),
            "failed_steps": len(failed_steps),
            "total_rows_processed": total_rows,
            "total_bytes_processed": total_bytes,
            "total_step_duration_seconds": total_duration,
            "throughput_rows_per_second": total_rows / total_duration if total_duration > 0 else 0,
            "throughput_mb_per_second": (total_bytes / (1024 * 1024)) / total_duration if total_duration > 0 else 0,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = {
            "platform": self.platform,
            "load_type": self.load_type,
            "scale_factor": self.scale_factor,
            "batch_id": self.batch_id,
            "start_time": self.start_time,
            "start_time_iso": datetime.fromtimestamp(self.start_time).isoformat(),
            "end_time": self.end_time,
            "end_time_iso": datetime.fromtimestamp(self.end_time).isoformat() if self.end_time else None,
            "total_duration_seconds": self.total_duration_seconds,
            "steps": [asdict(step) for step in self.steps],
            "summary": self.summary,
        }
        if self.platform_type is not None:
            d["platform_type"] = self.platform_type
        if self.cluster_instance_type is not None or self.cluster_worker_count is not None or self.cluster_master_type is not None:
            d["cluster_instance_type"] = self.cluster_instance_type
            d["cluster_worker_count"] = self.cluster_worker_count
            d["cluster_master_type"] = self.cluster_master_type
            # Cluster configuration (human-readable for result and metrics)
            d["cluster_configuration"] = {
                "worker_node_type": self.cluster_instance_type,
                "driver_node_type": self.cluster_master_type,
                "number_of_worker_nodes": self.cluster_worker_count,
            }
        if self.metrics_saved_path is not None:
            d["metrics_saved_path"] = self.metrics_saved_path
        if self.databricks_compute_type is not None:
            d["databricks_compute_type"] = self.databricks_compute_type
        if self.databricks_job_id is not None:
            d["databricks_job_id"] = self.databricks_job_id
        if self.databricks_run_id is not None:
            d["databricks_run_id"] = self.databricks_run_id
        if self.dq_table_timings is not None:
            d["dq_table_timings"] = self.dq_table_timings
        if self.cost_breakdown is not None:
            d["cost_breakdown"] = self.cost_breakdown
        if self.total_cost_usd is not None:
            d["total_cost_usd"] = self.total_cost_usd
        if self.dbu_cost_usd is not None:
            d["dbu_cost_usd"] = self.dbu_cost_usd
        if self.table_override is not None:
            d["table_override"] = self.table_override
        if self.benchmark_results_summary is not None:
            d["benchmark_results_summary"] = self.benchmark_results_summary
        if self.per_table_details is not None:
            d["per_table_details"] = self.per_table_details
        return d
    
    def save(
        self,
        output_path: str,
        service_account_key_file: Optional[str] = None,
        spark: Optional["SparkSession"] = None,
        dbutils: Optional[Any] = None,
    ):
        """Save metrics to file (JSON).

        - Local paths: pathlib/open.
        - gs:// or /Volumes/ (or dbfs:/Volumes/): try dbutils first on Databricks (UC credentials), then gsutil or Spark FS.
        - service_account_key_file (local path): gsutil uses that SA (GOOGLE_APPLICATION_CREDENTIALS).
        """
        timestamp = datetime.fromtimestamp(self.start_time).strftime("%Y%m%d_%H%M%S")
        # Use platform_type in filename when set (e.g. dataproc_serverless) so serverless has distinct file pattern
        platform_for_name = self.platform_type if self.platform_type is not None else self.platform
        filename = f"metrics_{platform_for_name}_{self.load_type}_sf{self.scale_factor}_{timestamp}.json"
        if self.batch_id is not None:
            filename = f"metrics_{platform_for_name}_{self.load_type}_sf{self.scale_factor}_batch{self.batch_id}_{timestamp}.json"

        json_content = json.dumps(self.to_dict(), indent=2)

        # On Databricks, try dbutils for gs:// or /Volumes/ so UC credentials are used when path is UC-managed
        if dbutils is None and (output_path.startswith("gs://") or output_path.startswith("/Volumes/") or output_path.startswith("dbfs:/Volumes/")):
            dbutils = _get_dbutils_if_databricks(spark, self.platform)

        def try_dbutils_put(full_path: str) -> bool:
            """Use UC credentials when path is UC-managed (external location or volume)."""
            if dbutils is None:
                return False
            try:
                dbutils.fs.put(full_path, json_content, overwrite=True)
                return True
            except Exception as e:
                logger.debug("dbutils.fs.put failed for %s: %s", full_path[:80], e)
                return False

        # UC Volume paths: /Volumes/... or dbfs:/Volumes/...
        if output_path.startswith("/Volumes/") or output_path.startswith("dbfs:/Volumes/"):
            base = output_path.rstrip("/")
            full_path = f"{base}/{filename}"
            self.metrics_saved_path = full_path
            if try_dbutils_put(full_path):
                logger.info(f"Metrics saved to {full_path} (via dbutils / UC)")
                return full_path
            if spark is not None and _write_string_via_hadoop_fs(spark, full_path, json_content):
                logger.info(f"Metrics saved to {full_path} (via Spark FS)")
                return full_path
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                f.write(json_content)
                tmp_path = f.name
            logger.warning(
                "Could not write to UC Volume %s. Metrics JSON written to %s.",
                full_path, tmp_path,
            )
            return tmp_path

        if output_path.startswith("gs://"):
            # pathlib.Path("gs://bucket/path") turns gs:// into gs:/ (one slash). Build path as string and upload.
            base = output_path.rstrip("/")
            full_gcs_path = f"{base}/{filename}"
            self.metrics_saved_path = full_gcs_path
            # Prefer dbutils so GCS locations registered in UC use UC credentials
            if try_dbutils_put(full_gcs_path):
                logger.info(f"Metrics saved to {full_gcs_path} (via dbutils / UC)")
                return full_gcs_path
            gsutil_cmd = shutil.which("gsutil")
            if not gsutil_cmd:
                # Spark Hadoop FileSystem uses cluster GCS config, not UC credentials
                if spark is not None and _write_string_to_gcs_via_spark(spark, full_gcs_path, json_content):
                    logger.info(f"Metrics saved to {full_gcs_path} (via Spark GCS)")
                    return full_gcs_path
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                    f.write(json_content)
                    tmp_path = f.name
                logger.warning(
                    "gsutil not found; could not upload metrics to GCS. "
                    "Metrics JSON written to %s. On Databricks, use /Volumes/... or pass dbutils for UC credentials.",
                    tmp_path,
                )
                return tmp_path
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                f.write(json_content)
                tmp_path = f.name
            try:
                env = os.environ.copy()
                # Use SA key for gsutil when a local key file is provided (Dataproc: same SA as Spark GCS access)
                if service_account_key_file and not service_account_key_file.startswith("gs://"):
                    if os.path.isfile(service_account_key_file):
                        env["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_file
                    else:
                        logger.warning(
                            "service_account_key_file=%s not found or not local; gsutil will use default credentials",
                            service_account_key_file,
                        )
                subprocess.run(
                    [gsutil_cmd, "-q", "cp", tmp_path, full_gcs_path],
                    check=True,
                    capture_output=True,
                    env=env,
                )
                logger.info(f"Metrics saved to {full_gcs_path}")
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                return full_gcs_path
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                logger.warning(
                    "Failed to upload metrics to GCS (%s): %s. Metrics JSON kept at %s. "
                    "On Databricks, use /Volumes/... or pass dbutils for UC credentials.",
                    full_gcs_path, e, tmp_path,
                )
                return tmp_path
        else:
            # Local path (or dbfs:/ on Databricks if mounted)
            output = Path(output_path)
            output.mkdir(parents=True, exist_ok=True)
            filepath = output / filename
            self.metrics_saved_path = str(filepath)
            with open(filepath, "w") as f:
                json.dump(self.to_dict(), f, indent=2)
            logger.info(f"Metrics saved to {filepath}")
            return str(filepath)


class MetricsCollector:
    """Context manager for collecting benchmark metrics."""

    def __init__(self, config):
        self.config = config
        self.metrics = BenchmarkMetrics(
            platform=config.platform.value,
            load_type=config.load_type.value,
            scale_factor=config.scale_factor,
            batch_id=config.batch_id,
            start_time=time.time(),
            cluster_instance_type=getattr(config, "cluster_instance_type", None),
            cluster_worker_count=getattr(config, "cluster_worker_count", None),
            cluster_master_type=getattr(config, "cluster_master_type", None),
        )
        self.current_step: Optional[StepMetrics] = None
    
    def start_step(self, step_name: str) -> StepMetrics:
        """Start a new step."""
        if self.current_step and self.current_step.status == "running":
            logger.warning(f"Previous step '{self.current_step.step_name}' not finished, marking as incomplete")
            self.current_step.fail("Step not properly finished")
        
        self.current_step = StepMetrics(
            step_name=step_name,
            start_time=time.time(),
        )
        self.metrics.steps.append(self.current_step)
        logger.info(f"Started step: {step_name}")
        return self.current_step
    
    def finish_step(self, rows: Optional[int] = None, bytes: Optional[int] = None,
                   metadata: Optional[Dict[str, Any]] = None):
        """Finish current step."""
        if self.current_step:
            self.current_step.finish(rows=rows, bytes=bytes, metadata=metadata)
            logger.info(f"Completed step: {self.current_step.step_name} "
                       f"({self.current_step.duration_seconds:.2f}s, "
                       f"{self.current_step.rows_processed or 0} rows)")
            self.current_step = None
        else:
            logger.warning("No current step to finish")
    
    def fail_step(self, error_message: str):
        """Mark current step as failed."""
        if self.current_step:
            self.current_step.fail(error_message)
            logger.error(f"Failed step: {self.current_step.step_name} - {error_message}")
            self.current_step = None
        else:
            logger.warning("No current step to fail")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Finish benchmark and save metrics."""
        if self.current_step and self.current_step.status == "running":
            if exc_type:
                self.fail_step(str(exc_val))
            else:
                self.finish_step()
        
        self.metrics.finish()
        
        # Save only if not deferred to caller (caller saves after printing result summary)
        if not getattr(self, "_save_deferred", False) and self.config.enable_metrics and self.config.metrics_output_path:
            try:
                self.metrics.save(
                    self.config.metrics_output_path,
                    service_account_key_file=getattr(
                        self.config, "service_account_key_file", None
                    ),
                    spark=getattr(self, "spark", None),
                )
            except Exception as e:
                logger.error(f"Failed to save metrics: {e}")
        
        return False  # Don't suppress exceptions
