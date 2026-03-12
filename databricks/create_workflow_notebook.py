# Databricks notebook source
# MAGIC %md
# MAGIC # Create TPC-DI Benchmark Workflow
# MAGIC
# MAGIC This notebook creates a Databricks workflow/job that:
# MAGIC 1. Generates TPC-DI raw data
# MAGIC 2. Runs the benchmark ETL
# MAGIC
# MAGIC All parameters are configurable via workflow parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC 1. Select **Cloud** first (AWS, GCP, or Azure).
# MAGIC 2. **Re-run the next cell** to refresh Worker/Driver instance type dropdowns for that cloud (only types valid for the selected cloud are shown).

# COMMAND ----------

# Instance type options per cloud (only these are valid for each cloud)
CLOUD_NODE_OPTIONS = {
    "AWS": [
        "i3.xlarge", "i3.2xlarge", "i3.4xlarge",
        "m5d.xlarge", "m5d.2xlarge", "m5d.4xlarge",
        "r5d.xlarge", "r5d.2xlarge", "r5d.4xlarge",
    ],
    "GCP": [
        "c2-standard-4", "c2-standard-8", "c2-standard-16", "c2-standard-30",
        "n2d-standard-4", "n2d-standard-8", "n2d-standard-16", "n2d-standard-32",
        "n2d-standard-48", "n2d-standard-64", "n2d-standard-80", "n2d-standard-96",
        "n2d-highmem-4", "n2d-highmem-8", "n2d-highmem-16", "n2d-highmem-32",
        "n2d-highmem-48", "n2d-highmem-64", "n2d-highmem-80", "n2d-highmem-96",
    ],
    "Azure": [
        "Standard_E4s_v3", "Standard_E8s_v3", "Standard_E16s_v3", "Standard_E32s_v3",
        "Standard_D4s_v3", "Standard_D8s_v3", "Standard_D16s_v3", "Standard_D32s_v3",
        "Standard_L4s_v2", "Standard_L8s_v2", "Standard_L16s_v2", "Standard_L32s_v2",
    ],
}
DEFAULT_NODE_TYPES = {
    "AWS": ("m5d.4xlarge", "m5d.4xlarge"),  # GCP equivalent: n2d-standard-16 (16 vCPUs, 64 GB RAM)
    "GCP": ("n2d-standard-16", "n2d-standard-16"),
    "Azure": ("Standard_E16s_v3", "Standard_E16s_v3"),  # GCP equivalent: n2d-standard-16 (16 vCPUs, 128 GB RAM)
}

def get_worker_count_for_scale_factor(scale_factor: int) -> int:
    """Get recommended number of worker nodes based on scale factor."""
    if scale_factor == 10:
        return 2
    elif scale_factor == 100:
        return 3
    elif scale_factor == 1000:
        return 5
    else:
        # Default: scale_factor / 5, minimum 2, maximum 10
        return max(2, min(10, scale_factor // 5))

# Widgets: job name, paths, Spark version, cloud, num workers
dbutils.widgets.text("job_name", "TPC-DI-Benchmark", "Job Name")
dbutils.widgets.text("data_gen_notebook", "generate_tpcdi_data_notebook", "Data Generation Notebook Path")
dbutils.widgets.text("benchmark_notebook", "benchmark_databricks_notebook", "Benchmark Notebook Path")
dbutils.widgets.dropdown(
    "spark_version",
    "14.3.x-scala2.12",
    [
        "13.3.x-scala2.12",
        "13.3.x-photon-scala2.12",
        "14.3.x-scala2.12",
        "14.3.x-photon-scala2.12",
        "15.4.x-scala2.12",
        "15.4.x-photon-scala2.12",
        "16.4.x-scala2.13",
        "16.4.x-photon-scala2.13",
        "17.3.x-scala2.13",
        "17.3.x-photon-scala2.13",
    ],
    "Cluster Spark Version (DBR)"
)
dbutils.widgets.dropdown("cloud", "AWS", ["AWS", "GCP", "Azure"], "Cloud (pick first; then re-run next cell for instance types)")
dbutils.widgets.dropdown("workflow_type", "benchmark", ["data_gen", "benchmark", "full"], "Workflow type: data_gen = data generation only; benchmark = benchmark ETL only; full = both tasks in one job")
dbutils.widgets.dropdown("separate_customer_mgmt_bronze", "false", ["true", "false"], "Separate customer mgmt bronze task (benchmark/full): Task 01b loads only bronze_customer_mgmt; Task 02 depends on it and skips that load")
dbutils.widgets.text("scale_factor", "10", "Scale Factor (SF=10→2 workers, SF=100→3 workers, SF=1000→5 workers)")
dbutils.widgets.text("num_workers", "2", "Number of Workers (auto-set based on scale_factor if left blank)")
dbutils.widgets.text("existing_cluster_id", "", "Existing Cluster ID (optional)")

# COMMAND ----------

# Re-run this cell after changing Cloud to update Worker/Driver dropdowns to that cloud's instance types only
cloud = dbutils.widgets.get("cloud")
options = CLOUD_NODE_OPTIONS.get(cloud, CLOUD_NODE_OPTIONS["AWS"])
default_worker = DEFAULT_NODE_TYPES.get(cloud, ("m5d.4xlarge", "m5d.4xlarge"))[0]
default_driver = DEFAULT_NODE_TYPES.get(cloud, ("m5d.4xlarge", "m5d.4xlarge"))[1]
# Ensure defaults are in the options list
if default_worker not in options:
    default_worker = options[0]
if default_driver not in options:
    default_driver = options[0]

try:
    dbutils.widgets.remove("node_type_id")
except Exception:
    pass
try:
    dbutils.widgets.remove("driver_node_type_id")
except Exception:
    pass
dbutils.widgets.dropdown("node_type_id", default_worker, options, "Worker Node Type (" + cloud + ")")
dbutils.widgets.dropdown("driver_node_type_id", default_driver, options, "Driver Node Type (" + cloud + ")")
print(f"Instance type options updated for cloud: {cloud} ({len(options)} types)")

# COMMAND ----------

import json
from pathlib import Path

job_name = dbutils.widgets.get("job_name")
workflow_type = dbutils.widgets.get("workflow_type")
separate_customer_mgmt_bronze = dbutils.widgets.get("separate_customer_mgmt_bronze") == "true"
data_gen_notebook = dbutils.widgets.get("data_gen_notebook")
benchmark_notebook = dbutils.widgets.get("benchmark_notebook")
spark_version = dbutils.widgets.get("spark_version")
cloud = dbutils.widgets.get("cloud")
node_type_id = dbutils.widgets.get("node_type_id")
driver_node_type_id = dbutils.widgets.get("driver_node_type_id")
scale_factor_str = dbutils.widgets.get("scale_factor")
num_workers_str = dbutils.widgets.get("num_workers")
existing_cluster_id = dbutils.widgets.get("existing_cluster_id").strip()

# Parse scale_factor and auto-calculate num_workers if not provided
scale_factor = int(scale_factor_str) if scale_factor_str else 10
if num_workers_str and num_workers_str.strip():
    num_workers = int(num_workers_str)
else:
    # Auto-calculate based on scale factor
    num_workers = get_worker_count_for_scale_factor(scale_factor)
    print(f"Auto-setting num_workers={num_workers} based on scale_factor={scale_factor}")

# Get workspace path from current notebook path (parent directory)
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_path = str(Path(notebook_path).parent)
except Exception:
    workspace_path = "/Workspace/Repos"

# Construct full notebook paths
if not data_gen_notebook.startswith("/"):
    data_gen_notebook = f"{workspace_path}/{data_gen_notebook}"
if not benchmark_notebook.startswith("/"):
    benchmark_notebook = f"{workspace_path}/{benchmark_notebook}"

# Show resolved node types and workflow type
print(f"Workflow type: {workflow_type} | Cloud: {cloud} | Worker: {node_type_id} | Driver: {driver_node_type_id} | Scale Factor: {scale_factor} | Workers: {num_workers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Workflow Definition

# COMMAND ----------

# Build cluster config
cluster_config = {
    "spark_version": spark_version,
    "node_type_id": node_type_id,
    "num_workers": num_workers,
    "driver_node_type_id": driver_node_type_id,
    "runtime_engine": "PHOTON",
}

# Task and cluster definitions by workflow_type
data_gen_task = {
    "task_key": "01_data_generation",
    "description": "Generate TPC-DI raw data",
    "job_cluster_key": "01_data_generation_cluster",
    "notebook_task": {
        "notebook_path": data_gen_notebook,
        "base_parameters": {"scale_factor": "10", "tpcdi_raw_data_path": "dbfs:/mnt/tpcdi", "upload_threads": "8"},
        "source": "WORKSPACE"
    },
    "timeout_seconds": 0,
    "email_notifications": {},
    "webhook_notifications": {},
    "retry_on_timeout": False,
    "max_retries": 0,
    "min_retry_interval_millis": 0,
    "max_retry_interval_millis": 0,
}

benchmark_task = {
    "task_key": "02_benchmark_execution",
    "description": "Run TPC-DI benchmark ETL",
    "job_cluster_key": "02_benchmark_execution_cluster",
    "libraries": [{"maven": {"coordinates": "com.databricks:spark-xml_2.13:0.18.0"}}],
    "notebook_task": {
        "notebook_path": benchmark_notebook,
        "base_parameters": {
            "load_type": "batch",
            "scale_factor": "10",
            "tpcdi_raw_data_path": "dbfs:/mnt/tpcdi",
            "target_schema": "dw",
            "target_catalog": "",
            "batch_id": "",
            "metrics_output": "dbfs:/mnt/tpcdi/metrics",
            "log_detailed_stats": "false",
            "customer_mgmt_xml_format": "com.databricks.spark.xml"
        },
        "source": "WORKSPACE"
    },
    "timeout_seconds": 0,
    "email_notifications": {},
    "webhook_notifications": {},
    "retry_on_timeout": False,
    "max_retries": 0,
    "min_retry_interval_millis": 0,
    "max_retry_interval_millis": 0,
}
if workflow_type == "full":
    benchmark_task["depends_on"] = [{"task_key": "01_data_generation"}]
    benchmark_task["run_if"] = "ALL_SUCCESS"

# Optional: separate task that only loads bronze_customer_mgmt; benchmark task then skips it and depends on this task
if separate_customer_mgmt_bronze and workflow_type in ("benchmark", "full"):
    benchmark_task["notebook_task"]["base_parameters"]["separate_customer_mgmt_bronze"] = "true"
    benchmark_task["notebook_task"]["base_parameters"]["bronze_only_customer_mgmt"] = "false"
    customer_mgmt_bronze_task = {
        "task_key": "01b_customer_mgmt_bronze",
        "description": "Load bronze_customer_mgmt only (CustomerMgmt.xml)",
        "job_cluster_key": "01b_customer_mgmt_bronze_cluster",
        "libraries": [{"maven": {"coordinates": "com.databricks:spark-xml_2.13:0.18.0"}}],
        "notebook_task": {
            "notebook_path": benchmark_notebook,
            "base_parameters": {
                "load_type": "batch",
                "scale_factor": scale_factor_str,
                "tpcdi_raw_data_path": "dbfs:/mnt/tpcdi",
                "target_schema": "dw",
                "target_catalog": benchmark_task["notebook_task"]["base_parameters"].get("target_catalog", ""),
                "batch_id": "",
                "metrics_output": "dbfs:/mnt/tpcdi/metrics",
                "log_detailed_stats": "false",
                "customer_mgmt_xml_format": "com.databricks.spark.xml",
                "separate_customer_mgmt_bronze": "false",
                "bronze_only_customer_mgmt": "true",
            },
            "source": "WORKSPACE"
        },
        "timeout_seconds": 0,
        "email_notifications": {},
        "webhook_notifications": {},
        "retry_on_timeout": False,
        "max_retries": 0,
        "min_retry_interval_millis": 0,
        "max_retry_interval_millis": 0,
    }
    if workflow_type == "full":
        customer_mgmt_bronze_task["depends_on"] = [{"task_key": "01_data_generation"}]
        customer_mgmt_bronze_task["run_if"] = "ALL_SUCCESS"
        benchmark_task["depends_on"] = [{"task_key": "01b_customer_mgmt_bronze"}]
    else:
        benchmark_task["depends_on"] = [{"task_key": "01b_customer_mgmt_bronze"}]
    benchmark_task["run_if"] = "ALL_SUCCESS"

if workflow_type == "data_gen":
    tasks = [data_gen_task]
    job_clusters_def = [{"job_cluster_key": "01_data_generation_cluster", "new_cluster": cluster_config.copy()}]
    parameters = [
        {"name": "scale_factor", "default": "10", "description": "TPC-DI scale factor (e.g., 10, 100, 1000)"},
        {"name": "tpcdi_raw_data_path", "default": "dbfs:/mnt/tpcdi", "description": "TPC-DI raw data path; dbfs:/..., /Volumes/..., or gs://..."},
        {"name": "upload_threads", "default": "8", "description": "Number of parallel threads for uploads"},
        {"name": "tpcdi_local_gen_path", "default": "/local_disk0", "description": "Local path for datagen output (/local_disk0 on Databricks)"},
    ]
elif workflow_type == "benchmark":
    if separate_customer_mgmt_bronze:
        tasks = [customer_mgmt_bronze_task, benchmark_task]
        job_clusters_def = [
            {"job_cluster_key": "01b_customer_mgmt_bronze_cluster", "new_cluster": cluster_config.copy()},
            {"job_cluster_key": "02_benchmark_execution_cluster", "new_cluster": cluster_config.copy()},
        ]
    else:
        tasks = [benchmark_task]
        job_clusters_def = [{"job_cluster_key": "02_benchmark_execution_cluster", "new_cluster": cluster_config.copy()}]
    parameters = [
        {"name": "scale_factor", "default": "10", "description": "TPC-DI scale factor"},
        {"name": "tpcdi_raw_data_path", "default": "dbfs:/mnt/tpcdi", "description": "TPC-DI raw data path (dbfs:/..., /Volumes/..., gs://...)"},
        {"name": "load_type", "default": "batch", "description": "Load type: batch or incremental"},
        {"name": "target_schema", "default": "dw", "description": "Target schema name"},
        {"name": "target_catalog", "default": "", "description": "Unity Catalog name (required)"},
        {"name": "batch_id", "default": "", "description": "Batch ID for incremental (empty for batch)"},
        {"name": "metrics_output", "default": "dbfs:/mnt/tpcdi/metrics", "description": "Path to save metrics JSON"},
        {"name": "log_detailed_stats", "default": "false", "description": "Log per-table timing/records"},
        {"name": "customer_mgmt_xml_format", "default": "com.databricks.spark.xml", "description": "CustomerMgmt.xml reader format"},
    ]
else:
    # full
    if separate_customer_mgmt_bronze:
        tasks = [data_gen_task, customer_mgmt_bronze_task, benchmark_task]
        job_clusters_def = [
            {"job_cluster_key": "01_data_generation_cluster", "new_cluster": cluster_config.copy()},
            {"job_cluster_key": "01b_customer_mgmt_bronze_cluster", "new_cluster": cluster_config.copy()},
            {"job_cluster_key": "02_benchmark_execution_cluster", "new_cluster": cluster_config.copy()},
        ]
    else:
        tasks = [data_gen_task, benchmark_task]
        job_clusters_def = [
            {"job_cluster_key": "01_data_generation_cluster", "new_cluster": cluster_config.copy()},
            {"job_cluster_key": "02_benchmark_execution_cluster", "new_cluster": cluster_config.copy()},
        ]
    parameters = [
        {"name": "scale_factor", "default": "10", "description": "TPC-DI scale factor (e.g., 10, 100, 1000)"},
        {"name": "tpcdi_raw_data_path", "default": "dbfs:/mnt/tpcdi", "description": "TPC-DI raw data path (used by both tasks); dbfs:/..., /Volumes/..., or gs://..."},
        {"name": "load_type", "default": "batch", "description": "Load type: batch or incremental"},
        {"name": "target_schema", "default": "dw", "description": "Target schema name"},
        {"name": "target_catalog", "default": "", "description": "Unity Catalog name (optional); when set, create catalog + schema"},
        {"name": "batch_id", "default": "", "description": "Batch ID for incremental loads (leave empty for batch)"},
        {"name": "metrics_output", "default": "dbfs:/mnt/tpcdi/metrics", "description": "Path to save metrics JSON files"},
        {"name": "log_detailed_stats", "default": "false", "description": "Log per-table timing and records; false = only job start/end/total duration"},
        {"name": "customer_mgmt_xml_format", "default": "com.databricks.spark.xml", "description": "CustomerMgmt.xml reader: org.apache.spark... (Databricks native); xml or com.databricks.spark.xml"},
        {"name": "upload_threads", "default": "8", "description": "Number of parallel threads for DBFS uploads"},
    ]

# Create workflow definition
workflow = {
    "name": job_name,
    "email_notifications": {"on_start": [], "on_success": [], "on_failure": [], "no_alert_for_skipped_runs": False},
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "format": "MULTI_TASK",
    "performance_target": "PERFORMANCE_OPTIMIZED",
    "tasks": tasks,
    "parameters": parameters,
    "job_clusters": job_clusters_def,
    "tags": {"purpose": "tpcdi_benchmark", "component": "data_integration"}
}

# Use existing cluster or job_cluster_key
if existing_cluster_id:
    workflow["job_clusters"] = []
    for task in workflow["tasks"]:
        task["existing_cluster_id"] = existing_cluster_id
        task.pop("job_cluster_key", None)

print("Workflow definition created:")
print(json.dumps(workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Job via Databricks API

# COMMAND ----------

import requests

# Get Databricks host and token
databricks_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Create job
url = f"{databricks_host}/api/2.2/jobs/create"
headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers, json=workflow)

if response.status_code == 200:
    result = response.json()
    job_id = result.get("job_id")
    print(f"✓ Workflow created successfully!")
    print(f"  Job ID: {job_id}")
    print(f"  Job Name: {workflow['name']}")
    print(f"\nView job at: {databricks_host.replace('/api', '')}/#job/{job_id}")
    
    # Save job ID for reference
    dbutils.notebook.exit(json.dumps({"job_id": job_id, "job_name": workflow['name']}))
else:
    print(f"✗ Failed to create workflow")
    print(f"  Status Code: {response.status_code}")
    print(f"  Response: {response.text}")
    raise Exception(f"Failed to create workflow: {response.text}")

# COMMAND ----------
