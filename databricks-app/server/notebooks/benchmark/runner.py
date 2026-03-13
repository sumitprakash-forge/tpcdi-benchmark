"""
Main benchmark runner for TPC-DI benchmark.
Orchestrates ETL execution on Databricks or Dataproc platforms.
"""

import io
import logging
import re
import urllib.request
from typing import Optional, Tuple
from pyspark.sql import SparkSession

from benchmark.config import BenchmarkConfig, Platform, LoadType
from benchmark.metrics import MetricsCollector
from benchmark.etl.table_timing import (
    clear as clear_table_timing,
    configure as table_timing_configure,
    set_job_start as table_timing_job_start,
    set_job_end as table_timing_job_end,
    log_final_summary as table_timing_log_final,
)
from benchmark.platforms.databricks import DatabricksPlatform
from benchmark.platforms.dataproc import DataprocPlatform

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress py4j (PySpark JVM bridge) DEBUG/INFO logs
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)


def create_spark_session(config: BenchmarkConfig) -> SparkSession:
    """
    Create and configure SparkSession based on platform.
    
    Args:
        config: Benchmark configuration
    
    Returns:
        Configured SparkSession
    """
    if config.platform == Platform.DATABRICKS:
        # Databricks: SparkSession should already be available
        try:
            spark = SparkSession.builder.getOrCreate()
            logger.info("Using existing Databricks SparkSession")
            return spark
        except Exception:
            # Fallback: create new session
            logger.warning("Could not get existing SparkSession, creating new one")
            return SparkSession.builder.appName("TPC-DI-Benchmark").getOrCreate()
    
    elif config.platform == Platform.DATAPROC:
        # Dataproc: create SparkSession with GCS support.
        # Only set master when provided: use "yarn" on managed cluster; omit on serverless batches.
        builder = SparkSession.builder.appName("TPC-DI-Benchmark-Dataproc")
        
        if config.spark_master:
            builder = builder.master(config.spark_master)
            logger.info("Using Spark master: %s", config.spark_master)
        
        # Use GCS for Spark warehouse so CREATE DATABASE / tables use gs://, not file:/tmp/...
        warehouse_dir = f"gs://{config.gcs_bucket}/spark-warehouse"
        spark_config = builder.config("spark.sql.warehouse.dir", warehouse_dir)
        
        # Packages: spark-xml for CustomerMgmt.xml; delta when --format delta
        packages = ["com.databricks:spark-xml_2.12:0.18.0"]
        if getattr(config, "table_format", None) == "delta":
            packages.append("io.delta:delta-spark_2.12:3.0.0")
        spark_config = spark_config.config("spark.jars.packages", ",".join(packages))
        
        # Delta 3.x on Dataproc serverless (no spark_master) requires session extension and catalog.
        # Without these, DeltaDataSource fails with DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG.
        if getattr(config, "table_format", None) == "delta" and not config.spark_master:
            spark_config = spark_config.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        
        # Configure for GCS
        spark_config = spark_config.config("spark.hadoop.fs.gs.impl", 
                              "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                              "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
                      .config("spark.hadoop.fs.gs.project.id", config.project_id)
        
        # Configure service account authentication if provided.
        # GCS connector expects a *local* key file path; it opens it with FileInputStream.
        # If key file is gs:// or missing, do not set keyfile auth or SparkContext init fails (NPE).
        key_file = getattr(config, "service_account_key_file", None) or ""
        use_keyfile = (
            config.service_account_email
            and key_file
            and not key_file.strip().startswith("gs://")
        )
        if use_keyfile:
            spark_config = spark_config.config("spark.hadoop.fs.gs.auth.type",
                                              "SERVICE_ACCOUNT_JSON_KEYFILE") \
                                      .config("spark.hadoop.fs.gs.auth.service.account.email",
                                              config.service_account_email) \
                                      .config("spark.hadoop.fs.gs.auth.service.account.keyfile",
                                              key_file.strip())
            logger.info("Using service account key file for GCS (local path)")
        elif config.service_account_email:
            spark_config = spark_config.config("spark.hadoop.fs.gs.auth.service.account.email",
                                              config.service_account_email)
            logger.info("Using service account email for GCS (no local key file)")
        
        spark = spark_config.getOrCreate()
        
        logger.info("Created Dataproc SparkSession with GCS support")
        return spark
    
    else:
        raise ValueError(f"Unsupported platform: {config.platform}")


def create_platform_adapter(config: BenchmarkConfig, spark: SparkSession):
    """
    Create platform adapter based on configuration.
    
    Args:
        config: Benchmark configuration
        spark: SparkSession
    
    Returns:
        Platform adapter instance
    """
    if config.platform == Platform.DATABRICKS:
        base = (config.output_path or config.raw_data_path).rstrip("/")
        raw_root = f"{base}/sf={config.scale_factor}"

        # When reading from GCS on Databricks (classic), set bucket so connector does not throw "No bucket specified in GCS URI: null".
        # On serverless, spark.hadoop.fs.gs.bucket is not available (CONFIG_NOT_AVAILABLE, SQLSTATE: 42K0I). Skip setting it;
        # GCS works via Unity Catalog external locations or the default connector.
        if raw_root.startswith("gs://"):
            bucket_match = re.match(r"gs://([^/]+)", raw_root)
            if bucket_match:
                bucket = bucket_match.group(1)
                try:
                    spark.conf.set("spark.hadoop.fs.gs.bucket", bucket)
                    logger.info(f"Set spark.hadoop.fs.gs.bucket={bucket} for GCS reads on Databricks")
                except BaseException as e:
                    err_msg = str(e).strip()
                    if "CONFIG_NOT_AVAILABLE" in err_msg or "42K0I" in err_msg or "fs.gs.bucket" in err_msg:
                        logger.info(
                            "spark.hadoop.fs.gs.bucket is not available on this runtime (e.g. serverless). "
                            "GCS access will use Unity Catalog external locations or the default connector."
                        )
                    else:
                        logger.warning(
                            f"Could not set spark.hadoop.fs.gs.bucket: {e}. "
                            "GCS access may still work via Unity Catalog external locations or default connector."
                        )

        return DatabricksPlatform(spark, raw_root)
    elif config.platform == Platform.DATAPROC:
        # Build raw_root from base + /sf={scale_factor} (same pattern as Databricks)
        base = config.raw_data_path.rstrip("/")
        base = re.sub(r"/sf=\d+$", "", base) or base  # strip existing /sf=N so scale_factor is source of truth
        raw_root = f"{base}/sf={config.scale_factor}"
        return DataprocPlatform(spark, raw_root,
                               config.gcs_bucket, config.project_id,
                               service_account_email=config.service_account_email,
                               service_account_key_file=config.service_account_key_file,
                               table_format=getattr(config, "table_format", None))
    else:
        raise ValueError(f"Unsupported platform: {config.platform}")


def _get_gcp_machine_type() -> Optional[str]:
    """Get current VM machine type from GCP metadata (e.g. on Dataproc). Returns short name like n2d-standard-16."""
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/machine-type",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            path = resp.read().decode().strip()
            # path is like "projects/123456/machineTypes/n2d-standard-16"
            return path.split("/")[-1] if path else None
    except Exception:
        return None


def _get_dataproc_worker_count_from_metadata() -> Optional[int]:
    """Get number of worker nodes from Dataproc GCP instance metadata (attributes/dataproc-worker-count)."""
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/attributes/dataproc-worker-count",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            val = resp.read().decode().strip()
            return int(val) if val else None
    except Exception:
        return None


def _get_executor_count(spark: SparkSession) -> Optional[int]:
    """Get number of executors (worker nodes) from Spark. Excludes driver."""
    try:
        sc = spark.sparkContext
        # getExecutorMemoryStatus gives driver + executors; subtract 1 for driver
        status = sc._jsc.sc().getExecutorMemoryStatus()
        count = status.size() - 1
        return max(0, count) if count is not None else None
    except Exception:
        return None


def is_databricks_serverless(spark: SparkSession) -> bool:
    """
    Heuristic: True if this job appears to be running on Databricks serverless compute.
    
    On serverless, cluster usage tags (e.g. clusterNodeType) are not available
    (CONFIG_NOT_AVAILABLE, SQLSTATE: 42K0I). On classic clusters they are set.
    Use this when you need to branch behavior (e.g. skip unsupported config, log compute type).
    
    Returns:
        True if serverless (or tag read failed), False if classic cluster tags are present.
    """
    try:
        _ = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType")
        return False  # Config is available -> classic cluster
    except BaseException:
        return True  # Config not available -> likely serverless


def _get_databricks_job_run_ids(spark: SparkSession) -> Tuple[Optional[str], Optional[str]]:
    """
    Get (job_id, job_run_id) from Databricks runtime when running as a job.
    We want the job run ID (the top-level run when you trigger the job), not the task run ID.
    Tries spark.conf (clusterUsageTags); then dbutils notebook context, preferring rootRunId over currentRunId.
    Returns (None, None) when not on a job or not available (e.g. interactive run).
    """
    import json
    job_id, run_id = None, None
    for key in ("spark.databricks.clusterUsageTags.jobId", "spark.databricks.clusterUsageTags.JobId"):
        try:
            v = spark.conf.get(key)
            if v:
                job_id = v
                break
        except Exception:
            pass
    # Job run ID (not task run): clusterUsageTags may expose job-level run id
    for key in ("spark.databricks.clusterUsageTags.jobRunId", "spark.databricks.clusterUsageTags.JobRunId"):
        try:
            v = spark.conf.get(key)
            if v:
                run_id = v
                break
        except Exception:
            pass
    if job_id is None or run_id is None:
        try:
            tags_json = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
            tags = json.loads(tags_json) if tags_json else []
            tag_map = {t.get("key"): t.get("value") for t in tags if isinstance(t, dict)}
            if job_id is None:
                job_id = tag_map.get("JobId") or tag_map.get("jobId")
            if run_id is None:
                run_id = tag_map.get("JobRunId") or tag_map.get("jobRunId") or tag_map.get("RunId") or tag_map.get("runId")
        except Exception:
            pass
    # Notebook context: rootRunId = job run ID, currentRunId = task run ID (we want job run)
    if job_id is None or run_id is None:
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            ctx_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
            ctx = json.loads(ctx_str) if isinstance(ctx_str, str) else {}
            if run_id is None:
                # Prefer rootRunId (job run); fall back to currentRunId (task run) for single-task jobs
                root_run = ctx.get("rootRunId")
                current_run = ctx.get("currentRunId")
                run_id = None
                if isinstance(root_run, dict) and isinstance(root_run.get("id"), (str, int)):
                    run_id = str(root_run.get("id"))
                elif isinstance(root_run, (str, int)) and root_run:
                    run_id = str(root_run)
                if run_id is None and isinstance(current_run, dict) and isinstance(current_run.get("id"), (str, int)):
                    run_id = str(current_run.get("id"))
                elif run_id is None and isinstance(current_run, (str, int)) and current_run:
                    run_id = str(current_run)
            if job_id is None:
                job_id = (ctx.get("tags") or {}).get("jobId") if isinstance(ctx.get("tags"), dict) else None
        except Exception:
            pass
    return (job_id, run_id)


def _build_result_early(metrics, config: BenchmarkConfig) -> dict:
    """Build a minimal result dict for early exit (e.g. bronze_only_customer_mgmt task)."""
    m = metrics.metrics
    cluster_config = None
    if m.cluster_instance_type is not None or m.cluster_master_type is not None or m.cluster_worker_count is not None:
        cluster_config = {
            "worker_node_type": m.cluster_instance_type,
            "driver_node_type": m.cluster_master_type,
            "number_of_worker_nodes": m.cluster_worker_count,
        }
    result = {
        "status": "success",
        "platform_type": m.platform_type,
        "cluster_configuration": cluster_config,
        "metrics": m.to_dict(),
        "config": {
            "platform": config.platform.value,
            "load_type": config.load_type.value,
            "scale_factor": config.scale_factor,
            "batch_id": config.batch_id,
        },
    }
    result["_save_metrics"] = lambda: (
        metrics.metrics.save(
            config.metrics_output_path,
            service_account_key_file=getattr(config, "service_account_key_file", None),
            spark=getattr(metrics, "spark", None),
        )
        if config.enable_metrics and config.metrics_output_path
        else None
    )
    metrics._save_deferred = True
    return result


def _get_databricks_node_types(spark: SparkSession) -> Tuple[Optional[str], Optional[str]]:
    """
    Get (worker_node_type, driver_node_type) from Databricks Spark conf when available.
    Tries clusterNodeType (worker) and driverNodeType (driver); not all runtimes set both.
    On serverless these configs are not available (CONFIG_NOT_AVAILABLE, SQLSTATE: 42K0I); returns (None, None).
    """
    try:
        worker_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType")
    except BaseException:
        worker_type = None
    try:
        driver_type = spark.conf.get("spark.databricks.clusterUsageTags.driverNodeType")
    except BaseException:
        driver_type = None
    return (worker_type, driver_type)


def get_cluster_info(config: BenchmarkConfig, spark: SparkSession) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Return (cluster_instance_type, cluster_worker_count, cluster_master_type) from config or auto-detection.
    cluster_instance_type = worker node type; cluster_master_type = driver node type.
    """
    instance_type = getattr(config, "cluster_instance_type", None)
    worker_count = getattr(config, "cluster_worker_count", None)
    master_type = getattr(config, "cluster_master_type", None)

    if config.platform == Platform.DATAPROC:
        # GCP metadata is for the current VM = driver. Use it for driver type; use same for worker if not provided.
        driver_type = _get_gcp_machine_type()
        if master_type is None and driver_type:
            master_type = driver_type
            logger.info(f"Auto-detected cluster_master_type (driver): {master_type}")
        if instance_type is None and driver_type:
            instance_type = driver_type
            logger.info(f"Auto-detected cluster_instance_type (from driver; pass --cluster-instance-type if workers differ): {instance_type}")
        if worker_count is None:
            worker_count = _get_executor_count(spark)
            # Spark executor count can be 0 early or on single-node; use Dataproc metadata when available
            if (worker_count is None or worker_count == 0):
                meta_count = _get_dataproc_worker_count_from_metadata()
                if meta_count is not None:
                    worker_count = meta_count
                    logger.info(f"Auto-detected cluster_worker_count (Dataproc metadata): {worker_count}")
            elif worker_count is not None:
                logger.info(f"Auto-detected cluster_worker_count: {worker_count}")
    elif config.platform == Platform.DATABRICKS:
        worker_type, driver_type = _get_databricks_node_types(spark)
        if instance_type is None and worker_type:
            instance_type = worker_type
            logger.info(f"Auto-detected cluster_instance_type (worker): {instance_type}")
        if master_type is None and driver_type:
            master_type = driver_type
            logger.info(f"Auto-detected cluster_master_type (driver): {master_type}")
        if worker_count is None:
            worker_count = _get_executor_count(spark)
            if worker_count is not None:
                logger.info(f"Auto-detected cluster_worker_count: {worker_count}")

    return (instance_type, worker_count, master_type)


def _format_benchmark_results_summary(config: BenchmarkConfig, result: dict) -> str:
    """Build human-readable 'TPC-DI BENCHMARK RESULTS - DATABRICKS/DATAPROC' text for inclusion in saved metrics JSON."""
    out = io.StringIO()
    platform_label = "DATABRICKS" if config.platform == Platform.DATABRICKS else "DATAPROC"
    out.write("\n" + "=" * 80 + "\n")
    out.write(f"TPC-DI BENCHMARK RESULTS - {platform_label}\n")
    out.write("=" * 80 + "\n")
    out.write(f"Platform: {result['config']['platform']}\n")
    metrics_dict = result.get("metrics") or {}
    if metrics_dict.get("databricks_compute_type"):
        out.write(f"Compute: {metrics_dict['databricks_compute_type']}\n")
    out.write(f"Load Type: {result['config']['load_type']}\n")
    out.write(f"Scale Factor: {result['config']['scale_factor']}\n")
    if result["config"].get("batch_id"):
        out.write(f"Batch ID: {result['config']['batch_id']}\n")
    if metrics_dict.get("cluster_instance_type") or metrics_dict.get("cluster_worker_count") is not None or metrics_dict.get("cluster_master_type"):
        out.write("\nCluster Configuration:\n")
        if metrics_dict.get("cluster_instance_type"):
            out.write(f"  Worker Node Type: {metrics_dict['cluster_instance_type']}\n")
        if metrics_dict.get("cluster_master_type"):
            out.write(f"  Driver Node Type: {metrics_dict['cluster_master_type']}\n")
        if metrics_dict.get("cluster_worker_count") is not None:
            out.write(f"  Number of Worker Nodes: {metrics_dict['cluster_worker_count']}\n")
    if metrics_dict.get("table_override") is not None:
        out.write(f"\nTable Override: {metrics_dict['table_override']}\n")
    total_dur = metrics_dict.get("total_duration_seconds")
    out.write(f"\nTotal Duration: {total_dur:.2f} seconds\n" if total_dur is not None else "\nTotal Duration: N/A\n")
    summary = metrics_dict.get("summary") or {}
    if summary:
        out.write("\nSummary:\n")
        out.write(f"  Total Steps: {summary.get('total_steps', 0)}\n")
        out.write(f"  Completed Steps: {summary.get('completed_steps', 0)}\n")
        out.write(f"  Failed Steps: {summary.get('failed_steps', 0)}\n")
        out.write(f"  Total Rows Processed: {summary.get('total_rows_processed', 0):,}\n")
        total_bytes = summary.get("total_bytes_processed") or 0
        out.write(f"  Total Data Size: {total_bytes / (1024 * 1024):.2f} MB\n")
        out.write(f"  Throughput: {summary.get('throughput_rows_per_second', 0):.2f} rows/sec\n")
        out.write(f"  Data Throughput: {summary.get('throughput_mb_per_second', 0):.2f} MB/sec\n")
    dq_timings = metrics_dict.get("dq_table_timings")
    if dq_timings:
        n_tables = len(dq_timings)
        out.write(f"\nDQ time per table ({n_tables} tables):\n")
        for t in dq_timings:
            out.write(f"  {t.get('table', '?')}: {t.get('duration_seconds', 0):.2f}s\n")
        total_dq = sum(t.get("duration_seconds", 0) for t in dq_timings)
        out.write(f"  Total DQ: {total_dq:.2f}s\n")
    cb = metrics_dict.get("cost_breakdown")
    total_cost = metrics_dict.get("total_cost_usd")
    if cb is not None or total_cost is not None:
        out.write("\nCost (estimated):\n")
        if cb:
            if (cb.get("compute_usd") or 0) > 0:
                out.write(f"  Compute: ${cb.get('compute_usd', 0):.2f}\n")
            if cb.get("software_usd") is not None:
                out.write(f"  Software: ${cb.get('software_usd', 0):.2f}\n")
        if total_cost is not None:
            out.write(f"  Total cost: ${total_cost:.2f}\n")
    out.write("\nStep Details:\n")
    for step in metrics_dict.get("steps") or []:
        status_icon = "✓" if step.get("status") == "completed" else "✗" if step.get("status") == "failed" else "○"
        dur = step.get("duration_seconds")
        dur_str = f"{dur:.2f}s" if dur is not None else "N/A"
        out.write(f"  {status_icon} {step.get('step_name', '?')}: {dur_str}")
        if step.get("rows_processed") is not None:
            out.write(f" ({step['rows_processed']:,} rows)")
        if step.get("status") == "failed" and step.get("error_message"):
            out.write(f" - ERROR: {step['error_message']}")
        out.write("\n")
    # Per-table: table name, then inside element duration, rows, size, throughput (same as Results Summary)
    try:
        from benchmark.etl.table_timing import get_summary as get_table_summary
        tsum = get_table_summary()
        details = tsum.get("table_details") or []
        if details:
            total_rows = tsum.get("total_records_loaded") or 0
            total_bytes = tsum.get("total_bytes_processed") or 0
            total_dur = tsum.get("total_duration_seconds") or 0
            total_mb = total_bytes / (1024 * 1024)
            rows_per_sec = total_rows / total_dur if total_dur > 0 else 0
            mb_per_sec = total_mb / total_dur if total_dur > 0 and total_bytes else 0
            out.write("\nTable-level stats:\n")
            out.write(f"  Tables loaded:      {len(details)}\n")
            out.write(f"  Total records:      {total_rows:,}\n")
            out.write(f"  Total data size:    {total_mb:.2f} MB\n")
            out.write(f"  Overall throughput: {rows_per_sec:,.1f} rows/s, {mb_per_sec:.2f} MB/s\n")
            out.write("  Per-table (duration, rows, size, throughput):\n")
            for d in details:
                table_name = d.get("table", "?")
                dur = d.get("duration_seconds") or 0
                rows = d.get("row_count") or 0
                b = d.get("bytes_processed")
                row_s = rows / dur if dur > 0 else 0
                mb_s = (b / (1024 * 1024)) / dur if b and dur > 0 else None
                out.write(f"    {table_name}:\n")
                out.write(f"      duration: {dur:.2f}s\n")
                out.write(f"      rows: {rows:,}\n")
                if b is not None:
                    out.write(f"      size_mb: {b / (1024 * 1024):.2f}\n")
                out.write(f"      throughput_rows_per_sec: {row_s:,.1f}\n")
                if mb_s is not None:
                    out.write(f"      throughput_mb_per_sec: {mb_s:.2f}\n")
    except Exception as e:
        out.write(f"\n(Table-level stats unavailable: {e})\n")
    out.write("=" * 80 + "\n")
    return out.getvalue()


def run_benchmark(config: BenchmarkConfig) -> dict:
    """
    Run TPC-DI benchmark with the given configuration.

    Args:
        config: Benchmark configuration

    Returns:
        Dictionary with benchmark results and metrics
    """
    logger.info(f"Starting TPC-DI benchmark: {config.platform.value}, "
               f"{config.load_type.value}, SF={config.scale_factor}")

    # Create SparkSession
    with MetricsCollector(config) as metrics:
        metrics.start_step("spark_session_creation")
        spark = create_spark_session(config)
        metrics.finish_step()
        metrics.spark = spark  # used for GCS metrics upload when gsutil is not available (e.g. Databricks)
        
        if config.platform == Platform.DATABRICKS:
            serverless = is_databricks_serverless(spark)
            compute_type = "serverless" if serverless else "classic"
            metrics.metrics.databricks_compute_type = compute_type
            logger.info(f"Databricks compute: {compute_type}" + ("" if serverless else " (provisioned)"))
            job_id, run_id = _get_databricks_job_run_ids(spark)
            if job_id is not None or run_id is not None:
                metrics.metrics.databricks_job_id = job_id
                metrics.metrics.databricks_run_id = run_id
                logger.info(f"Databricks job_id={job_id} run_id={run_id}")
        
        # Create platform adapter
        metrics.start_step("platform_adapter_creation")
        platform = create_platform_adapter(config, spark)
        metrics.finish_step()

        # Expose load_type to ETL so bronze/silver can use overwrite when BATCH (gold gets load_type explicitly).
        setattr(platform, "_tpcdi_load_type", config.load_type)

        # Set cluster metadata for metrics (from config or auto-detection)
        instance_type, worker_count, master_type = get_cluster_info(config, spark)
        metrics.metrics.set_cluster_info(
            instance_type=instance_type,
            worker_count=worker_count,
            master_type=master_type,
        )

        # Platform type for result and metrics: databricks | dataproc | dataproc_serverless
        if config.platform == Platform.DATABRICKS:
            metrics.metrics.platform_type = "databricks"
        elif config.platform == Platform.DATAPROC:
            metrics.metrics.platform_type = "dataproc_serverless" if not config.spark_master else "dataproc"

        # Create target database (and catalog/schema for Databricks UC)
        # Append scale factor to database/schema names
        metrics.start_step("database_creation")
        db_name_with_sf = f"{config.target_database}_sf{config.scale_factor}"
        
        # Track if database/path exists before creation (for table override info)
        database_existed = False
        path_existed = False
        
        if config.platform == Platform.DATABRICKS:
            # Databricks requires Unity Catalog (target_catalog is validated in config)
            # For Unity Catalog, append SF to schema name
            schema_name_with_sf = f"{config.target_schema}_sf{config.scale_factor}"
            # Check if schema exists in Unity Catalog
            try:
                schemas = spark.sql(f"SHOW SCHEMAS IN {config.target_catalog}").collect()
                database_existed = any(row.databaseName == schema_name_with_sf for row in schemas)
            except Exception:
                database_existed = False
            
            platform.create_database(
                "",  # Not used for UC
                catalog=config.target_catalog,
                schema=schema_name_with_sf,  # Use schema name with SF
            )
            db_or_catalog = config.target_catalog
            effective_schema = schema_name_with_sf
        elif config.platform == Platform.DATAPROC:
            # spark_catalog expects two-part names (database.table). Use single DB = target_database_target_schema_sf{scale_factor}.
            spark_db = f"{db_name_with_sf}_{config.target_schema}"
            # Check if path exists before deletion
            if config.load_type == LoadType.BATCH:
                path_existed = platform.check_database_path_exists(spark_db)
                platform.delete_target_database_path_if_exists(spark_db)
            else:
                path_existed = platform.check_database_path_exists(spark_db)
            # Check if database exists
            try:
                database_existed = spark.catalog.databaseExists(spark_db)
            except Exception:
                database_existed = False
            platform.create_database(spark_db)
            db_or_catalog = spark_db
            effective_schema = ""
        else:
            raise ValueError(f"Unsupported platform: {config.platform}")
        
        # Store table override info in metrics
        table_override = database_existed or path_existed
        metrics.metrics.table_override = table_override
        metrics.finish_step()
        
        # Run ETL: Medallion only (Bronze -> Silver layers)
        clear_table_timing()
        table_timing_configure(log_detailed_stats=config.log_detailed_stats)
        table_timing_job_start()

        skip_bronze_customer_mgmt = getattr(config, "separate_customer_mgmt_bronze", False) and not getattr(config, "bronze_only_customer_mgmt", False)
        bronze_only_customer_mgmt = getattr(config, "bronze_only_customer_mgmt", False)

        if config.load_type == LoadType.BATCH:
            # When bronze_only_customer_mgmt (Task 1): do not drop tables; we only create ref + bronze_customer_mgmt
            # When skip_bronze_customer_mgmt (Task 2): do not drop bronze_customer_mgmt (created by Task 1)
            batch_tables = [
                "bronze_date", "bronze_time", "bronze_status_type", "bronze_tax_rate",
                "bronze_trade_type", "bronze_industry", "bronze_hr", "bronze_customer_mgmt",
                "bronze_trade", "bronze_daily_market", "bronze_prospect", "bronze_cash_transaction",
                "bronze_holding_history", "bronze_watch_history", "bronze_finwire",
                "silver_date", "silver_time", "silver_status_type", "silver_trade_type", "silver_industry",
                "silver_tax_rate", "silver_companies", "silver_securities", "silver_financials",
                "silver_customers", "silver_accounts", "silver_trades", "silver_daily_market",
                "silver_prospect", "silver_cash_transaction", "silver_watch_history", "silver_holding_history",
                "gold_dim_date", "gold_dim_time", "gold_dim_customer", "gold_dim_account", "gold_dim_broker",
                "gold_dim_company", "gold_dim_security", "gold_dim_trade_type", "gold_dim_status_type", "gold_dim_industry",
                "gold_financials", "gold_prospect", "gold_fact_trade", "gold_fact_market_history", "gold_dim_messages",
                "gold_fact_cash_balances", "gold_fact_holdings", "gold_fact_watches",
            ]
            if skip_bronze_customer_mgmt:
                batch_tables = [t for t in batch_tables if t != "bronze_customer_mgmt"]
            if hasattr(platform, "drop_table_if_exists") and not bronze_only_customer_mgmt:
                prefix = ".".join(p for p in (db_or_catalog, effective_schema) if p)
                for short_name in batch_tables:
                    full_name = f"{prefix}.{short_name}" if prefix else short_name
                    try:
                        platform.drop_table_if_exists(full_name)
                    except Exception as e:
                        logger.warning("Could not drop table %s: %s", full_name, e)
            metrics.start_step("bronze_etl")
            from benchmark.etl.bronze import BronzeETL
            bronze_etl = BronzeETL(platform)
            bronze_etl.run_bronze_batch_load(
                1, db_or_catalog, effective_schema,
                customer_mgmt_xml_format=getattr(config, "customer_mgmt_xml_format", None),
                skip_bronze_customer_mgmt=skip_bronze_customer_mgmt,
                only_bronze_customer_mgmt=bronze_only_customer_mgmt,
            )
            
            bronze_tables = ["bronze_customer_mgmt", "bronze_trade", "bronze_daily_market", 
                            "bronze_date", "bronze_status_type", "bronze_trade_type",
                            "bronze_industry", "bronze_finwire"]
            bronze_row_counts = {}
            for table in bronze_tables:
                table_name = ".".join(p for p in (db_or_catalog, effective_schema, table) if p)
                try:
                    bronze_row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            bronze_bytes = getattr(platform, "get_raw_input_size_bytes", lambda bid: 0)(1)
            metrics.finish_step(rows=sum(bronze_row_counts.values()),
                               bytes=bronze_bytes if bronze_bytes else None,
                               metadata={"table_counts": bronze_row_counts})

            if bronze_only_customer_mgmt:
                table_timing_job_end()
                table_timing_log_final()
                logger.info("Exiting early (bronze_only_customer_mgmt): bronze_customer_mgmt task completed")
                return _build_result_early(metrics, config)

            metrics.start_step("silver_etl")
            from benchmark.etl.silver import SilverETL
            silver_etl = SilverETL(platform)
            silver_etl.run_silver_batch_load(1, db_or_catalog, effective_schema, metrics=metrics)
            
            silver_tables = ["silver_customers", "silver_accounts", "silver_trades",
                            "silver_daily_market", "silver_date", "silver_time", "silver_status_type",
                            "silver_trade_type", "silver_industry", "silver_companies",
                            "silver_securities", "silver_financials"]
            silver_row_counts = {}
            for table in silver_tables:
                table_name = ".".join(p for p in (db_or_catalog, effective_schema, table) if p)
                try:
                    silver_row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            metrics.finish_step(rows=sum(silver_row_counts.values()),
                               metadata={"table_counts": silver_row_counts})
            
            # Gold layer: Transform Silver to Gold star schema
            metrics.start_step("gold_etl")
            from benchmark.etl.gold import GoldETL
            gold_etl = GoldETL(platform)
            gold_etl.run_gold_load(db_or_catalog, effective_schema, load_type=config.load_type, batch_id=1)
            
            gold_tables = ["gold_dim_customer", "gold_dim_account", "gold_dim_company",
                          "gold_dim_security", "gold_dim_date", "gold_dim_time", "gold_dim_broker",
                          "gold_dim_trade_type", "gold_dim_status_type", "gold_dim_industry",
                          "gold_prospect", "gold_fact_trade", "gold_fact_market_history",
                          "gold_fact_cash_balances", "gold_fact_holdings", "gold_fact_watches"]
            gold_row_counts = {}
            for table in gold_tables:
                table_name = ".".join(p for p in (db_or_catalog, effective_schema, table) if p)
                try:
                    gold_row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            metrics.finish_step(rows=sum(gold_row_counts.values()),
                               metadata={"table_counts": gold_row_counts})

            table_timing_job_end()
            table_timing_log_final()

        elif config.load_type == LoadType.INCREMENTAL:
            metrics.start_step(f"bronze_incremental_batch{config.batch_id}")
            from benchmark.etl.bronze import BronzeETL
            bronze_etl = BronzeETL(platform)
            bronze_etl.run_bronze_batch_load(
                config.batch_id, db_or_catalog, effective_schema,
                customer_mgmt_xml_format=getattr(config, "customer_mgmt_xml_format", None),
            )
            inc_batch_bytes = getattr(platform, "get_raw_input_size_bytes", lambda bid: 0)(config.batch_id)
            metrics.finish_step(bytes=inc_batch_bytes if inc_batch_bytes else None)
            
            metrics.start_step(f"silver_incremental_batch{config.batch_id}")
            from benchmark.etl.silver import SilverETL
            silver_etl = SilverETL(platform)
            silver_etl.run_silver_batch_load(config.batch_id, db_or_catalog, effective_schema, metrics=metrics)
            
            silver_tables = ["silver_customers", "silver_accounts", "silver_trades"]
            row_counts = {}
            for table in silver_tables:
                table_name = ".".join(p for p in (db_or_catalog, effective_schema, table) if p)
                try:
                    row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            metrics.finish_step(rows=sum(row_counts.values()), metadata={"table_counts": row_counts})

            # Gold OPTIMIZE ZORDER before incremental load (same as v2 SQL flow)
            _gold_optimize_tables = [
                ("gold_dim_company", "company_id"),
                ("gold_dim_customer", "customer_id"),
                ("gold_dim_account", "account_id"),
                ("gold_dim_security", "symbol"),
                ("gold_dim_broker", "broker_id"),
                ("gold_prospect", "agency_id"),
                ("gold_fact_trade", "sk_date_id, sk_account_id"),
                ("gold_fact_holdings", "sk_account_id, sk_security_id"),
                ("gold_financials", "co_name_or_cik"),
            ]
            metrics.start_step("gold_optimize")
            _prefix = ".".join(p for p in (db_or_catalog, effective_schema) if p)
            _spark = platform.get_spark()
            for _tbl, _zorder in _gold_optimize_tables:
                _full = f"{_prefix}.{_tbl}"
                try:
                    _spark.sql(f"OPTIMIZE {_full} ZORDER BY ({_zorder})")
                    logger.info("OPTIMIZE %s ZORDER BY (%s)", _full, _zorder)
                except Exception as e:
                    logger.warning("OPTIMIZE %s failed: %s", _full, e)
            metrics.finish_step()

            # Gold layer: Refresh Gold tables from updated Silver
            metrics.start_step(f"gold_incremental_batch{config.batch_id}")
            from benchmark.etl.gold import GoldETL
            gold_etl = GoldETL(platform)
            gold_etl.run_gold_load(db_or_catalog, effective_schema, load_type=config.load_type, batch_id=config.batch_id)
            metrics.finish_step()

            table_timing_job_end()
            table_timing_log_final()

        else:
            raise ValueError(f"Unsupported load type: {config.load_type}")
    
    # Cost estimation (compute + software/DBU; list-price approximation)
    try:
        from benchmark.cost import estimate_cost
        cost = estimate_cost(
            metrics.metrics,
            config.platform.value,
            getattr(config, "cloud", None),
        )
        if cost:
            metrics.metrics.cost_breakdown = {
                "compute_usd": cost.get("compute_usd", 0),
                "software_usd": cost.get("software_usd", cost.get("dbu_usd", 0)),
            }
            metrics.metrics.total_cost_usd = cost.get("total_usd")
            if config.platform == Platform.DATABRICKS and cost.get("dbu_usd") is not None:
                metrics.metrics.dbu_cost_usd = cost.get("dbu_usd")
    except Exception as e:
        logger.debug("Cost estimation skipped: %s", e)
    
    logger.info("Benchmark completed successfully")
    # Build cluster_configuration for result (same shape as in metrics JSON)
    m = metrics.metrics
    cluster_config = None
    if m.cluster_instance_type is not None or m.cluster_master_type is not None or m.cluster_worker_count is not None:
        cluster_config = {
            "worker_node_type": m.cluster_instance_type,
            "driver_node_type": m.cluster_master_type,
            "number_of_worker_nodes": m.cluster_worker_count,
        }
        logger.info(
            "Cluster Configuration: Worker Node Type: %s, Driver Node Type: %s, Number of Worker Nodes: %s",
            m.cluster_instance_type or "N/A",
            m.cluster_master_type or "N/A",
            m.cluster_worker_count if m.cluster_worker_count is not None else "N/A",
        )
    result = {
        "status": "success",
        "platform_type": m.platform_type,
        "cluster_configuration": cluster_config,
        "metrics": m.to_dict(),
        "config": {
            "platform": config.platform.value,
            "load_type": config.load_type.value,
            "scale_factor": config.scale_factor,
            "batch_id": config.batch_id,
        }
    }
    # Per-table details: table name -> { duration_seconds, row_count, bytes_processed, throughput_* } for metrics JSON
    try:
        from benchmark.etl.table_timing import get_summary as get_table_summary
        tsum = get_table_summary()
        details = tsum.get("table_details") or []
        if details:
            per_table = {}
            for d in details:
                table_name = d.get("table", "?")
                dur = d.get("duration_seconds") or 0
                rows = d.get("row_count") or 0
                b = d.get("bytes_processed")
                row_s = rows / dur if dur > 0 else 0
                mb_s = (b / (1024 * 1024)) / dur if b and dur > 0 else None
                per_table[table_name] = {
                    "duration_seconds": round(dur, 2),
                    "row_count": rows,
                    "bytes_processed": b,
                    "throughput_rows_per_sec": round(row_s, 2),
                    "throughput_mb_per_sec": round(mb_s, 2) if mb_s is not None else None,
                }
            metrics.metrics.per_table_details = per_table
    except Exception as e:
        logger.debug("Could not build per_table_details for metrics: %s", e)
    # Capture "TPC-DI BENCHMARK RESULTS - DATABRICKS/DATAPROC" text for inclusion in saved metrics JSON
    metrics.metrics.benchmark_results_summary = _format_benchmark_results_summary(config, result)
    # Defer save to caller so metrics are saved after the result summary block is printed
    def _save_metrics_after_summary():
        if not config.enable_metrics or not config.metrics_output_path:
            return None
        try:
            return metrics.metrics.save(
                config.metrics_output_path,
                service_account_key_file=getattr(config, "service_account_key_file", None),
                spark=getattr(metrics, "spark", None),
            )
        except Exception as e:
            logger.error("Failed to save metrics: %s", e)
            return None
    result["_save_metrics"] = _save_metrics_after_summary
    metrics._save_deferred = True
    return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run TPC-DI benchmark")
    parser.add_argument("--platform", choices=["databricks", "dataproc"], required=True)
    parser.add_argument("--load-type", choices=["batch", "incremental"], required=True)
    parser.add_argument("--scale-factor", type=int, required=True)
    parser.add_argument("--raw-data-path", help="GCS path for Dataproc; base path for Databricks if --output-path not set")
    parser.add_argument("--output-path", help="Databricks: raw data location (DBFS or Volume base); overrides raw-data-path")
    parser.add_argument("--target-database", default="tpcdi_warehouse")
    parser.add_argument("--target-schema", default="dw")
    parser.add_argument("--target-catalog", help="Unity Catalog (Databricks); when set, create catalog + schema")
    parser.add_argument("--batch-id", type=int, help="Required for incremental loads")
    parser.add_argument("--gcs-bucket", help="Required for Dataproc")
    parser.add_argument("--project-id", help="Required for Dataproc")
    parser.add_argument("--region", help="Required for Dataproc")
    parser.add_argument("--spark-master", help="Spark master URL for Dataproc")
    parser.add_argument("--metrics-output", help="Path to save metrics JSON")
    parser.add_argument("--log-detailed-stats", action="store_true",
                        help="Log per-table timing and records; default is only job start/end/total duration")
    
    args = parser.parse_args()
    
    raw_base = args.output_path or args.raw_data_path
    if not raw_base and args.platform == "dataproc":
        raw_base = args.raw_data_path
    if not raw_base:
        raise ValueError("Provide --raw-data-path or --output-path (Databricks)")
    
    config = BenchmarkConfig(
        platform=Platform(args.platform),
        load_type=LoadType(args.load_type),
        scale_factor=args.scale_factor,
        raw_data_path=raw_base,
        target_database=args.target_database,
        target_schema=args.target_schema,
        target_catalog=args.target_catalog,
        output_path=args.output_path,
        batch_id=args.batch_id,
        gcs_bucket=args.gcs_bucket,
        project_id=args.project_id,
        region=args.region,
        spark_master=args.spark_master,
        metrics_output_path=args.metrics_output,
        log_detailed_stats=args.log_detailed_stats,
    )
    
    result = run_benchmark(config)
    print(f"\nBenchmark Results:\n{result}")
