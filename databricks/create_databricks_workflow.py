#!/usr/bin/env python3
"""
Create Databricks workflow/job for TPC-DI benchmark.
Supports separate workflows: data generation only, benchmark only, or both (full).
"""

import json
import argparse
from typing import Dict, Any, List


def create_workflow_definition(
    job_name: str,
    data_gen_notebook_path: str,
    benchmark_notebook_path: str,
    default_scale_factor: int = 10,
    default_output_path: str = "dbfs:/mnt/tpcdi",
    default_local_gen_path: str = "/local_disk0",
    default_load_type: str = "batch",
    default_target_schema: str = "dw",
    default_target_catalog: str = "main",
    default_metrics_output: str = "dbfs:/mnt/tpcdi/metrics",
    default_log_detailed_stats: bool = False,
    default_customer_mgmt_xml_format: str = "com.databricks.spark.xml",
    cluster_config: Dict[str, Any] = None,
    workflow_type: str = "benchmark",
    separate_customer_mgmt_bronze: bool = False,
) -> Dict[str, Any]:
    """
    Create Databricks workflow definition.

    workflow_type: "data_gen" = data generation only (single task);
                   "benchmark" = benchmark ETL only (single task);
                   "full" = both tasks in one job (data gen then benchmark);
                   "v2_batch" = single task running v2/databricks/run_tpcdi_batch (Bronze → Silver → Gold).
    separate_customer_mgmt_bronze: If True (benchmark/full only), add task 01b that only loads bronze_customer_mgmt;
                                   task 02 then depends on 01b and skips bronze_customer_mgmt load.
    """
    if cluster_config is None:
        cluster_config = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
            "driver_node_type_id": "i3.xlarge",
            "runtime_engine": "PHOTON",
        }

    data_gen_task = {
        "task_key": "01_data_generation",
        "description": "Generate TPC-DI raw data",
        "job_cluster_key": "01_data_generation_cluster",
        "notebook_task": {
            "notebook_path": data_gen_notebook_path,
            "base_parameters": {
                "scale_factor": str(default_scale_factor),
                "tpcdi_raw_data_path": default_output_path,
                "upload_threads": "8",
                "tpcdi_local_gen_path": default_local_gen_path or "/local_disk0"
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

    benchmark_task = {
        "task_key": "02_benchmark_execution",
        "description": "Run TPC-DI benchmark ETL",
        "job_cluster_key": "02_benchmark_execution_cluster",
        "libraries": [
            {"maven": {"coordinates": "com.databricks:spark-xml_2.13:0.18.0"}}
        ],
        "notebook_task": {
            "notebook_path": benchmark_notebook_path,
            "base_parameters": {
                "load_type": default_load_type,
                "scale_factor": str(default_scale_factor),
                "tpcdi_raw_data_path": default_output_path,
                "target_schema": default_target_schema,
                "target_catalog": default_target_catalog,
                "batch_id": "",
                "metrics_output": default_metrics_output,
                "log_detailed_stats": "true" if default_log_detailed_stats else "false",
                "customer_mgmt_xml_format": default_customer_mgmt_xml_format or "com.databricks.spark.xml"
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
        benchmark_task["base_parameters"]["separate_customer_mgmt_bronze"] = "true"
        benchmark_task["base_parameters"]["bronze_only_customer_mgmt"] = "false"
        customer_mgmt_bronze_task = {
            "task_key": "01b_customer_mgmt_bronze",
            "description": "Load bronze_customer_mgmt only (CustomerMgmt.xml)",
            "job_cluster_key": "01b_customer_mgmt_bronze_cluster",
            "libraries": [
                {"maven": {"coordinates": "com.databricks:spark-xml_2.13:0.18.0"}}
            ],
            "notebook_task": {
                "notebook_path": benchmark_notebook_path,
                "base_parameters": {
                    "load_type": default_load_type,
                    "scale_factor": str(default_scale_factor),
                    "tpcdi_raw_data_path": default_output_path,
                    "target_schema": default_target_schema,
                    "target_catalog": default_target_catalog,
                    "batch_id": "",
                    "metrics_output": default_metrics_output,
                    "log_detailed_stats": "true" if default_log_detailed_stats else "false",
                    "customer_mgmt_xml_format": default_customer_mgmt_xml_format or "com.databricks.spark.xml",
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

    if workflow_type == "v2_batch":
        v2_task = {
            "task_key": "run_tpcdi_batch",
            "description": "Run TPC-DI v2 batch pipeline (Bronze → Silver → Gold)",
            "job_cluster_key": "run_tpcdi_batch_cluster",
            "libraries": [
                {"maven": {"coordinates": "com.databricks:spark-xml_2.13:0.18.0"}}
            ],
            "notebook_task": {
                "notebook_path": benchmark_notebook_path,
                "base_parameters": {
                    "catalog": default_target_catalog,
                    "schema_name": "tpcdi_schema_sf" + str(default_scale_factor),
                    "raw_data_path": default_output_path,
                    "sf": str(default_scale_factor),
                    "batch_id": "1",
                    "xml_format": default_customer_mgmt_xml_format or "com.databricks.spark.xml",
                    "sql_base_path": "",
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
        tasks = [v2_task]
        job_clusters_def = [
            {"job_cluster_key": "run_tpcdi_batch_cluster", "new_cluster": cluster_config.copy()},
        ]
        parameters = [
            {"name": "catalog", "default": default_target_catalog, "description": "Unity Catalog name"},
            {"name": "schema_name", "default": "tpcdi_schema_sf" + str(default_scale_factor), "description": "Target schema name"},
            {"name": "raw_data_path", "default": default_output_path, "description": "Raw data path (e.g. gs://bucket/tpcdi or dbfs:/mnt/tpcdi)"},
            {"name": "sf", "default": str(default_scale_factor), "description": "Scale factor (10, 100, 1000)"},
            {"name": "batch_id", "default": "1", "description": "Batch ID"},
            {"name": "xml_format", "default": default_customer_mgmt_xml_format or "com.databricks.spark.xml", "description": "CustomerMgmt XML reader format"},
            {"name": "sql_base_path", "default": "", "description": "Optional path to sql/ folder (default = notebook dir)"},
        ]
    elif workflow_type == "data_gen":
        tasks = [data_gen_task]
        job_clusters_def = [
            {"job_cluster_key": "01_data_generation_cluster", "new_cluster": cluster_config.copy()},
        ]
        parameters = [
            {"name": "scale_factor", "default": str(default_scale_factor), "description": "TPC-DI scale factor (e.g., 10, 100, 1000)"},
            {"name": "tpcdi_raw_data_path", "default": default_output_path, "description": "TPC-DI raw data path; dbfs:/..., /Volumes/..., or gs://..."},
            {"name": "upload_threads", "default": "8", "description": "Number of parallel threads for uploads"},
            {"name": "tpcdi_local_gen_path", "default": default_local_gen_path or "/local_disk0", "description": "Local path for datagen output (/local_disk0 on Databricks)"},
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
            job_clusters_def = [
                {"job_cluster_key": "02_benchmark_execution_cluster", "new_cluster": cluster_config.copy()},
            ]
        parameters = [
            {"name": "scale_factor", "default": str(default_scale_factor), "description": "TPC-DI scale factor"},
            {"name": "tpcdi_raw_data_path", "default": default_output_path, "description": "TPC-DI raw data path (dbfs:/..., /Volumes/..., gs://...)"},
            {"name": "load_type", "default": default_load_type, "description": "Load type: batch or incremental"},
            {"name": "target_schema", "default": default_target_schema, "description": "Target schema name"},
            {"name": "target_catalog", "default": default_target_catalog, "description": "Unity Catalog name (required)"},
            {"name": "batch_id", "default": "", "description": "Batch ID for incremental (empty for batch)"},
            {"name": "metrics_output", "default": default_metrics_output, "description": "Path to save metrics JSON"},
            {"name": "log_detailed_stats", "default": "true" if default_log_detailed_stats else "false", "description": "Log per-table timing/records"},
            {"name": "customer_mgmt_xml_format", "default": default_customer_mgmt_xml_format or "com.databricks.spark.xml", "description": "CustomerMgmt.xml reader format"},
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
            {"name": "scale_factor", "default": str(default_scale_factor), "description": "TPC-DI scale factor (e.g., 10, 100, 1000)"},
            {"name": "tpcdi_raw_data_path", "default": default_output_path, "description": "TPC-DI raw data path (used by both tasks); dbfs:/..., /Volumes/..., or gs://..."},
            {"name": "load_type", "default": default_load_type, "description": "Load type: batch or incremental"},
            {"name": "target_schema", "default": default_target_schema, "description": "Target schema name"},
            {"name": "target_catalog", "default": default_target_catalog, "description": "Unity Catalog name (required for Databricks)"},
            {"name": "batch_id", "default": "", "description": "Batch ID for incremental loads (leave empty for batch)"},
            {"name": "metrics_output", "default": default_metrics_output, "description": "Path to save metrics JSON files"},
            {"name": "log_detailed_stats", "default": "true" if default_log_detailed_stats else "false", "description": "Log per-table timing and records; false = only job start/end/total duration"},
            {"name": "upload_threads", "default": "8", "description": "Number of parallel threads for DBFS uploads"},
            {"name": "tpcdi_local_gen_path", "default": default_local_gen_path or "/local_disk0", "description": "Local path for datagen output (e.g. /mnt/disks/ssd0 on GCP; /local_disk0 on Databricks; empty = use default)"},
            {"name": "customer_mgmt_xml_format", "default": default_customer_mgmt_xml_format or "com.databricks.spark.xml", "description": "CustomerMgmt.xml reader: org.apache.spark.sql.execution.datasources.xml (Databricks native); xml or com.databricks.spark.xml (when attaching custom spark-xml JAR)"}
        ]

    workflow = {
        "name": job_name,
        "email_notifications": {
            "on_start": [],
            "on_success": [],
            "on_failure": [],
            "no_alert_for_skipped_runs": False
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "tasks": tasks,
        "parameters": parameters,
        "job_clusters": job_clusters_def,
        "run_as": None,
        "tags": {
            "purpose": "tpcdi_benchmark",
            "component": "data_integration"
        }
    }
    return workflow


def create_workflow_via_api(
    workflow_def: Dict[str, Any],
    databricks_host: str,
    databricks_token: str,
    workspace_path: str = None
) -> Dict[str, Any]:
    """
    Create workflow using Databricks Jobs API.
    
    Args:
        workflow_def: Workflow definition dictionary
        databricks_host: Databricks workspace URL (e.g., https://workspace.cloud.databricks.com)
        databricks_token: Databricks personal access token
        workspace_path: Optional workspace path for notebooks
    
    Returns:
        API response with job details
    """
    import requests
    
    # Update notebook paths if workspace_path provided
    if workspace_path:
        for task in workflow_def["tasks"]:
            if "notebook_path" in task.get("notebook_task", {}):
                current_path = task["notebook_task"]["notebook_path"]
                if not current_path.startswith("/"):
                    task["notebook_task"]["notebook_path"] = f"{workspace_path}/{current_path}"
    
    url = f"{databricks_host.rstrip('/')}/api/2.2/jobs/create"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, json=workflow_def)
    response.raise_for_status()
    
    return response.json()


def main():
    parser = argparse.ArgumentParser(
        description="Create Databricks workflow for TPC-DI benchmark"
    )
    
    # Workflow configuration
    parser.add_argument("--job-name", default="TPC-DI-Benchmark",
                       help="Name of the Databricks job")
    parser.add_argument("--workflow-type", default="benchmark",
                       choices=["data_gen", "benchmark", "full", "v2_batch"],
                       help="data_gen = data generation only; benchmark = benchmark ETL only; full = both; v2_batch = single notebook run_tpcdi_batch (Bronze→Silver→Gold)")
    parser.add_argument("--data-gen-notebook", default="generate_tpcdi_data_notebook",
                       help="Path to data generation notebook (relative to workspace)")
    parser.add_argument("--benchmark-notebook", default="benchmark_databricks_notebook",
                       help="Path to benchmark notebook (relative to workspace); for v2_batch use v2/databricks/run_tpcdi_batch")
    parser.add_argument("--workspace-path", default="/Workspace/Repos",
                       help="Workspace path prefix for notebooks")
    
    # Default parameters
    parser.add_argument("--default-scale-factor", type=int, default=10,
                       help="Default scale factor")
    parser.add_argument("--default-output-path", default="dbfs:/mnt/tpcdi",
                       help="Default TPC-DI raw data path (used by both tasks)")
    parser.add_argument("--default-local-gen-path", default="/local_disk0",
                       help="Default local path for datagen output (/local_disk0 on Databricks; e.g. /mnt/disks/ssd0 on GCP)")
    parser.add_argument("--default-load-type", default="batch",
                       choices=["batch", "incremental"],
                       help="Default load type")
    parser.add_argument("--default-target-schema", default="dw",
                       help="Default target schema")
    parser.add_argument("--default-target-catalog", default="main",
                       help="Default Unity Catalog (required for Databricks)")
    parser.add_argument("--default-metrics-output", default="dbfs:/mnt/tpcdi/metrics",
                       help="Default metrics output path")
    parser.add_argument("--default-log-detailed-stats", action="store_true",
                       help="Default: log per-table timing/records; else only job start/end/total duration")
    parser.add_argument("--default-customer-mgmt-xml-format", default="xml",
                       choices=["xml", "com.databricks.spark.xml"],
                       help="CustomerMgmt.xml reader format: xml or com.databricks.spark.xml")
    parser.add_argument("--separate-customer-mgmt-bronze", action="store_true",
                       help="Add task 01b that only loads bronze_customer_mgmt; benchmark task depends on it and skips that load (benchmark/full only)")
    
    # Cluster configuration
    SPARK_VERSIONS = [
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
    ]
    parser.add_argument("--spark-version", default="14.3.x-scala2.12",
                       choices=SPARK_VERSIONS,
                       help="Databricks Runtime (Spark) version")
    # Instance type options per cloud (must match selected --cloud)
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
    
    parser.add_argument("--cloud", default="AWS", choices=["AWS", "GCP", "Azure"],
                       help="Cloud (instance types are restricted to this cloud)")
    parser.add_argument("--node-type-id", default=None,
                       help="Worker node type; must be valid for selected --cloud (use --list-node-types to see options)")
    parser.add_argument("--driver-node-type-id", default=None,
                       help="Driver node type; must be valid for selected --cloud")
    parser.add_argument("--list-node-types", action="store_true",
                       help="Print allowed instance types for each cloud and exit")
    parser.add_argument("--num-workers", type=int, default=None,
                       help="Number of worker nodes (auto-calculated from --default-scale-factor if not provided: SF=10→2, SF=100→3, SF=1000→5)")
    parser.add_argument("--use-existing-cluster", 
                       help="Use existing cluster ID instead of creating new")
    
    # API configuration
    parser.add_argument("--databricks-host",
                       help="Databricks workspace URL (e.g., https://workspace.cloud.databricks.com)")
    parser.add_argument("--databricks-token",
                       help="Databricks personal access token")
    parser.add_argument("--output-json", 
                       help="Output workflow definition to JSON file instead of creating via API")
    
    args = parser.parse_args()

    if args.list_node_types:
        for c in ["AWS", "GCP", "Azure"]:
            opts = CLOUD_NODE_OPTIONS[c]
            default = DEFAULT_NODE_TYPES[c][0]
            print(f"{c}: {', '.join(opts)} (default: {default})")
        return 0

    allowed = CLOUD_NODE_OPTIONS.get(args.cloud, CLOUD_NODE_OPTIONS["AWS"])
    if args.node_type_id and args.node_type_id not in allowed:
        print(f"Error: --node-type-id '{args.node_type_id}' is not valid for cloud '{args.cloud}'.")
        print(f"Allowed for {args.cloud}: {', '.join(allowed)}")
        return 1
    if args.driver_node_type_id and args.driver_node_type_id not in allowed:
        print(f"Error: --driver-node-type-id '{args.driver_node_type_id}' is not valid for cloud '{args.cloud}'.")
        print(f"Allowed for {args.cloud}: {', '.join(allowed)}")
        return 1

    node_type_id = args.node_type_id or DEFAULT_NODE_TYPES[args.cloud][0]
    driver_node_type_id = args.driver_node_type_id or DEFAULT_NODE_TYPES[args.cloud][1]
    
    # Auto-calculate num_workers from scale_factor if not provided
    num_workers = args.num_workers
    if num_workers is None:
        num_workers = get_worker_count_for_scale_factor(args.default_scale_factor)
        print(f"Auto-setting num_workers={num_workers} based on default_scale_factor={args.default_scale_factor}")

    # Build cluster config
    cluster_config = {
        "spark_version": args.spark_version,
        "node_type_id": node_type_id,
        "num_workers": num_workers,
        "driver_node_type_id": driver_node_type_id,
        "runtime_engine": "PHOTON",
    }
    
    # For v2_batch, default notebook to run_tpcdi_batch if still using generic default
    benchmark_notebook = args.benchmark_notebook
    if args.workflow_type == "v2_batch" and args.benchmark_notebook == "benchmark_databricks_notebook":
        benchmark_notebook = "v2/databricks/run_tpcdi_batch"

    # Create workflow definition
    workflow = create_workflow_definition(
        job_name=args.job_name,
        data_gen_notebook_path=args.data_gen_notebook,
        benchmark_notebook_path=benchmark_notebook,
        default_scale_factor=args.default_scale_factor,
        default_output_path=args.default_output_path,
        default_local_gen_path=args.default_local_gen_path or "/local_disk0",
        default_load_type=args.default_load_type,
        default_target_schema=args.default_target_schema,
        default_target_catalog=args.default_target_catalog,
        default_metrics_output=args.default_metrics_output,
        default_log_detailed_stats=args.default_log_detailed_stats,
        default_customer_mgmt_xml_format=getattr(args, "default_customer_mgmt_xml_format", "com.databricks.spark.xml") or "com.databricks.spark.xml",
        cluster_config=cluster_config,
        workflow_type=args.workflow_type,
        separate_customer_mgmt_bronze=getattr(args, "separate_customer_mgmt_bronze", False),
    )
    
    # Handle existing cluster: use existing_cluster_id on each task and clear job_clusters / job_cluster_key
    if args.use_existing_cluster:
        workflow["job_clusters"] = []
        for task in workflow["tasks"]:
            task["existing_cluster_id"] = args.use_existing_cluster
            task.pop("new_cluster", None)
            task.pop("job_cluster_key", None)
    
    # Output or create via API
    if args.output_json:
        # Save to JSON file
        with open(args.output_json, 'w') as f:
            json.dump(workflow, f, indent=2)
        print(f"Workflow definition saved to {args.output_json}")
        print("\nTo create the job, use:")
        print(f"  databricks jobs create --json-file {args.output_json}")
    elif args.databricks_host and args.databricks_token:
        # Create via API
        try:
            result = create_workflow_via_api(
                workflow,
                args.databricks_host,
                args.databricks_token,
                args.workspace_path
            )
            print(f"✓ Workflow created successfully!")
            print(f"  Job ID: {result.get('job_id')}")
            print(f"  Job Name: {workflow['name']}")
            print(f"\nView job at: {args.databricks_host}/#job/{result.get('job_id')}")
        except Exception as e:
            print(f"✗ Failed to create workflow: {e}")
            print("\nWorkflow definition:")
            print(json.dumps(workflow, indent=2))
            return 1
    else:
        # Just print the definition
        print("Workflow definition (JSON):")
        print(json.dumps(workflow, indent=2))
        print("\nTo create the job:")
        print("  1. Save this JSON to a file")
        print("  2. Use: databricks jobs create --json-file <file>")
        print("  3. Or use --databricks-host and --databricks-token to create via API")
    
    return 0


if __name__ == "__main__":
    exit(main())
