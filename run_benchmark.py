#!/usr/bin/env python3
"""
Unified wrapper script to run TPC-DI benchmarks from your laptop.
Supports submitting to Dataproc, Databricks, or running locally.

Automatic cluster sizing:
  - SF=10 → 2 workers, SF=100 → 3 workers, SF=1000 → 5 workers
  - GCP defaults to n2d-standard-16 instance type

Usage:
  # Submit to Dataproc
  python run_benchmark.py dataproc --cluster my-cluster --load-type batch --scale-factor 10 ...

  # Submit to Databricks (workflow, auto-creates job if missing)
  python run_benchmark.py databricks --load-type batch --scale-factor 100 --cloud GCP ...

  # Run locally (requires Spark installed)
  python run_benchmark.py local --load-type batch --scale-factor 10 ...
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional


def ensure_benchmark_zip():
    """Create benchmark.zip if it doesn't exist (next to this script in tpcdi/)."""
    script_dir = Path(__file__).resolve().parent
    zip_path = script_dir / "benchmark.zip"
    if zip_path.exists():
        return str(zip_path)
    benchmark_dir = script_dir / "benchmark"
    if not benchmark_dir.exists():
        print("ERROR: 'benchmark' directory not found. Run from project root or tpcdi/.", file=sys.stderr)
        sys.exit(1)
    print(f"Creating {zip_path}...")
    shutil.make_archive(str(zip_path.with_suffix("")), "zip", script_dir, "benchmark")
    return str(zip_path)


def check_dataproc_cluster_exists(cluster_name: str, project_id: str, region: str) -> bool:
    """Check if a Dataproc cluster exists."""
    try:
        result = subprocess.run(
            ["gcloud", "dataproc", "clusters", "describe", cluster_name,
             "--project", project_id, "--region", region],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0
    except Exception:
        return False


def create_dataproc_cluster(args) -> bool:
    """Create Dataproc cluster (and optionally network infrastructure). Returns True if created."""
    recommended_workers = get_worker_count_for_scale_factor(args.scale_factor)
    recommended_instance_type = "n2d-standard-16"
    
    # Check if cluster already exists
    if check_dataproc_cluster_exists(args.cluster, args.project_id, args.region):
        print(f"Cluster '{args.cluster}' already exists. Skipping creation.")
        return False
    
    print(f"\nCluster '{args.cluster}' not found. Creating cluster...")
    print(f"Configuration: {recommended_workers} workers, {recommended_instance_type} instance type\n")
    
    # Determine if we need to create network infrastructure
    create_network = args.create_network
    vpc_name = args.vpc_name or f"{args.cluster}-vpc"
    subnet_name = args.subnet_name or f"{args.cluster}-subnet"
    subnet_range = args.subnet_range or "10.10.0.0/24"
    zone = args.zone or f"{args.region}-b"
    
    if create_network:
        print(f"[1/4] Creating VPC: {vpc_name}")
        subprocess.run(
            ["gcloud", "compute", "networks", "create", vpc_name,
             "--project", args.project_id,
             "--subnet-mode=custom",
             "--bgp-routing-mode=regional"],
            check=True,
        )
        
        print(f"[2/4] Creating subnet: {subnet_name} (Private Google Access enabled)")
        subprocess.run(
            ["gcloud", "compute", "networks", "subnets", "create", subnet_name,
             "--project", args.project_id,
             "--network", vpc_name,
             "--region", args.region,
             "--range", subnet_range,
             "--enable-private-ip-google-access"],
            check=True,
        )
        
        firewall_rule_name = args.firewall_rule_name or f"allow-{subnet_name}-internal"
        print(f"[3/4] Creating firewall rule: {firewall_rule_name}")
        subprocess.run(
            ["gcloud", "compute", "firewall-rules", "create", firewall_rule_name,
             "--project", args.project_id,
             "--network", vpc_name,
             "--action=ALLOW",
             "--direction=INGRESS",
             "--rules=tcp:0-65535,udp:0-65535,icmp",
             "--source-ranges", subnet_range],
            check=True,
        )
        
        subnet_arg = f"--subnet={subnet_name}"
        no_address_arg = "--no-address"
    else:
        # Use default network or existing subnet
        subnet_arg = f"--subnet={args.subnet_name}" if args.subnet_name else ""
        no_address_arg = ""
        print(f"[1/1] Creating cluster (using existing network)")
    
    # Build cluster create command
    cmd = [
        "gcloud", "dataproc", "clusters", "create", args.cluster,
        "--project", args.project_id,
        "--region", args.region,
        "--zone", zone,
        "--image-version", "2.3-debian12",
        "--master-machine-type", recommended_instance_type,
        "--master-boot-disk-type", "hyperdisk-balanced",
        "--master-boot-disk-size", "100",
        "--num-workers", str(recommended_workers),
        "--worker-machine-type", recommended_instance_type,
        "--worker-boot-disk-type", "hyperdisk-balanced",
        "--worker-boot-disk-size", "200",
    ]
    
    if subnet_arg:
        cmd.append(subnet_arg)
    if no_address_arg:
        cmd.append(no_address_arg)
    
    # Add optional components
    if args.format == "delta":
        cmd.extend(["--optional-components", "DELTA"])
    
    cmd.extend([
        "--enable-component-gateway",
        "--scopes", "cloud-platform",
    ])
    
    print(f"[{'4' if create_network else '1'}/{'4' if create_network else '1'}] Creating Dataproc cluster: {args.cluster}")
    subprocess.run(cmd, check=True)
    
    print(f"\n✓ Cluster '{args.cluster}' created successfully!")
    return True


def run_dataproc(args):
    """Submit benchmark to Dataproc cluster. Creates cluster if missing."""
    zip_path = ensure_benchmark_zip()
    
    # Auto-set cluster metadata for metrics if not provided
    recommended_workers = get_worker_count_for_scale_factor(args.scale_factor)
    recommended_instance_type = "n2d-standard-16"  # GCP default
    
    if not args.cluster_worker_count:
        args.cluster_worker_count = recommended_workers
        print(f"Auto-setting cluster_worker_count={recommended_workers} based on scale_factor={args.scale_factor}")
    
    if not args.cluster_instance_type:
        args.cluster_instance_type = recommended_instance_type
        print(f"Auto-setting cluster_instance_type={recommended_instance_type} for GCP")
    
    if not args.cluster_master_type:
        args.cluster_master_type = recommended_instance_type
    
    # Check if cluster exists, create if missing
    cluster_exists = check_dataproc_cluster_exists(args.cluster, args.project_id, args.region)
    if not cluster_exists:
        if args.create_cluster or args.create_network:
            created = create_dataproc_cluster(args)
            if not created:
                # Cluster was created by another process or already exists
                print(f"Cluster '{args.cluster}' is now available.")
        else:
            print(f"\n⚠ ERROR: Cluster '{args.cluster}' not found!")
            print(f"Recommended configuration for SF={args.scale_factor}:")
            print(f"  Worker nodes: {recommended_workers}")
            print(f"  Instance type: {recommended_instance_type} (worker and master)")
            print(f"\nTo auto-create cluster, use:")
            print(f"  --create-cluster     (creates cluster using default network)")
            print(f"  --create-network     (creates VPC, subnet, firewall, and cluster)")
            print(f"\nOr create manually and ensure it matches the recommendations above.\n")
            sys.exit(1)
    
    # Build gcloud command (script and zip live in tpcdi/)
    script_path = Path(__file__).resolve().parent / "run_benchmark_dataproc.py"
    cmd = [
        "gcloud", "dataproc", "jobs", "submit", "pyspark",
        str(script_path),
        f"--cluster={args.cluster}",
        f"--region={args.region}",
        f"--project={args.project_id}",
        f"--py-files={zip_path}",
    ]
    
    # Add optional jars
    if args.jars:
        cmd.append(f"--jars={args.jars}")
    
    # Add benchmark arguments
    cmd.append("--")
    cmd.extend(["--load-type", args.load_type])
    cmd.extend(["--scale-factor", str(args.scale_factor)])
    cmd.extend(["--gcs-bucket", args.gcs_bucket])
    cmd.extend(["--project-id", args.project_id])
    cmd.extend(["--region", args.region])
    
    if args.raw_data_path:
        cmd.extend(["--raw-data-path", args.raw_data_path])
    if args.target_database:
        cmd.extend(["--target-database", args.target_database])
    if args.target_schema:
        cmd.extend(["--target-schema", args.target_schema])
    if args.batch_id:
        cmd.extend(["--batch-id", str(args.batch_id)])
    if args.spark_master:
        cmd.extend(["--spark-master", args.spark_master])
    if args.service_account_email:
        cmd.extend(["--service-account-email", args.service_account_email])
    if args.service_account_key_file:
        cmd.extend(["--service-account-key-file", args.service_account_key_file])
    if args.format:
        cmd.extend(["--format", args.format])
    if args.metrics_output:
        cmd.extend(["--metrics-output", args.metrics_output])
    if args.log_detailed_stats:
        cmd.append("--log-detailed-stats")
    if args.cluster_instance_type:
        cmd.extend(["--cluster-instance-type", args.cluster_instance_type])
    if args.cluster_worker_count:
        cmd.extend(["--cluster-worker-count", str(args.cluster_worker_count)])
    if args.cluster_master_type:
        cmd.extend(["--cluster-master-type", args.cluster_master_type])
    
    print(f"Submitting to Dataproc cluster: {args.cluster}")
    print(f"Command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def get_databricks_client():
    """Get Databricks host and token (from CLI config or env vars)."""
    # Try CLI first
    try:
        subprocess.run(["databricks", "--version"], capture_output=True, check=True)
        # CLI is available - try to get config
        try:
            result = subprocess.run(
                ["databricks", "configure", "--token", "--host"],
                capture_output=True,
                text=True,
            )
            # CLI config might be in ~/.databrickscfg
            import configparser
            config_path = Path.home() / ".databrickscfg"
            if config_path.exists():
                config = configparser.ConfigParser()
                config.read(config_path)
                if "DEFAULT" in config:
                    host = config["DEFAULT"].get("host")
                    token = config["DEFAULT"].get("token")
                    if host and token:
                        return host, token
        except Exception:
            pass
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    # Fall back to env vars
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        return None, None
    
    return host, token


def find_databricks_job_by_name(host: str, token: str, job_name: str) -> Optional[int]:
    """Find Databricks job by name. Returns job_id if found, None otherwise."""
    import urllib.request
    
    url = f"{host.rstrip('/')}/api/2.1/jobs/list"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    
    try:
        with urllib.request.urlopen(req) as resp:
            jobs = json.loads(resp.read())
            for job in jobs.get("jobs", []):
                if job.get("settings", {}).get("name") == job_name:
                    return job.get("job_id")
    except Exception:
        pass
    
    return None


def get_databricks_job(host: str, token: str, job_id: int) -> Optional[dict]:
    """Get Databricks job by ID. Returns job dict if found, None otherwise."""
    import urllib.request
    
    url = f"{host.rstrip('/')}/api/2.1/jobs/get?job_id={job_id}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except Exception:
        return None


def check_databricks_notebook_exists(host: str, token: str, notebook_path: str) -> bool:
    """Check if a notebook exists in Databricks workspace. Returns True if exists."""
    import urllib.request
    
    # Encode path for URL
    import urllib.parse
    encoded_path = urllib.parse.quote(notebook_path, safe='')
    
    url = f"{host.rstrip('/')}/api/2.0/workspace/get-status?path={encoded_path}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    
    try:
        with urllib.request.urlopen(req) as resp:
            status = json.loads(resp.read())
            return status.get("object_type") == "NOTEBOOK"
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        # Other errors might mean path doesn't exist
        return False
    except Exception:
        return False


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


def create_databricks_job(host: str, token: str, args) -> int:
    """Create a new Databricks job/workflow. Returns job_id."""
    # Import workflow creation functions
    sys.path.insert(0, str(Path(__file__).parent))
    try:
        from databricks.create_databricks_workflow import (
            create_workflow_definition,
            create_workflow_via_api,
        )
    except ImportError:
        print("ERROR: Cannot import workflow creation functions. Make sure databricks/create_databricks_workflow.py exists.", file=sys.stderr)
        sys.exit(1)
    
    # Determine worker count: use provided value or auto-calculate from scale factor
    num_workers = args.num_workers
    if num_workers is None:
        num_workers = get_worker_count_for_scale_factor(args.scale_factor)
        print(f"Auto-setting num_workers={num_workers} based on scale_factor={args.scale_factor}")
    
    # Determine node type: GCP defaults to n2d-standard-16, others use provided or default
    default_node_type = "n2d-standard-16" if args.cloud == "GCP" else "i3.xlarge"
    node_type_id = args.node_type_id or default_node_type
    driver_node_type_id = args.driver_node_type_id or args.node_type_id or default_node_type
    
    # Build cluster config
    cluster_config = {
        "spark_version": args.spark_version or "14.3.x-scala2.12",
        "node_type_id": node_type_id,
        "num_workers": num_workers,
        "driver_node_type_id": driver_node_type_id,
    }
    
    if args.cloud == "GCP":
        cluster_config["gcp_attributes"] = {
            "use_preemptible_executors": False,
        }
    
    if args.existing_cluster_id:
        cluster_config = None  # Will use existing_cluster_id in workflow
    
    # Create workflow definition
    workflow_def = create_workflow_definition(
        job_name=args.job_name or "TPC-DI-Benchmark",
        data_gen_notebook_path=args.data_gen_notebook or "generate_tpcdi_data_notebook",
        benchmark_notebook_path=args.benchmark_notebook or "benchmark_databricks_notebook",
        default_scale_factor=args.scale_factor,
        default_output_path=args.output_path or "dbfs:/mnt/tpcdi",
        default_local_gen_path=getattr(args, "local_gen_path", "") or "/local_disk0",
        default_load_type=args.load_type,
        default_target_schema=args.target_schema or "dw",
        default_target_catalog=args.target_catalog or "main",
        default_metrics_output=args.metrics_output or "dbfs:/mnt/tpcdi/metrics",
        default_log_detailed_stats=args.log_detailed_stats,
        default_customer_mgmt_xml_format=getattr(args, "customer_mgmt_xml_format", None) or "xml",
        cluster_config=cluster_config,
    )
    
    # Use existing cluster if specified
    if args.existing_cluster_id:
        for task in workflow_def["tasks"]:
            task["existing_cluster_id"] = args.existing_cluster_id
            task.pop("new_cluster", None)
    
    # Update notebook paths with workspace path if provided
    workspace_path = args.workspace_path
    notebook_paths = {}
    if workspace_path:
        for task in workflow_def["tasks"]:
            if "notebook_path" in task.get("notebook_task", {}):
                current_path = task["notebook_task"]["notebook_path"]
                if not current_path.startswith("/"):
                    full_path = f"{workspace_path}/{current_path}"
                    task["notebook_task"]["notebook_path"] = full_path
                    notebook_paths[task["task_key"]] = full_path
                else:
                    notebook_paths[task["task_key"]] = current_path
    else:
        # Extract paths from workflow definition
        for task in workflow_def["tasks"]:
            if "notebook_path" in task.get("notebook_task", {}):
                notebook_paths[task["task_key"]] = task["notebook_task"]["notebook_path"]
    
    # Check if notebooks exist (warn but don't fail - workflow creation will fail if missing)
    print("\nChecking if notebooks exist in Databricks workspace...")
    missing_notebooks = []
    for task_key, notebook_path in notebook_paths.items():
        exists = check_databricks_notebook_exists(host, token, notebook_path)
        if exists:
            print(f"  ✓ {task_key}: {notebook_path}")
        else:
            print(f"  ✗ {task_key}: {notebook_path} (NOT FOUND)")
            missing_notebooks.append((task_key, notebook_path))
    
    if missing_notebooks:
        print("\n⚠ WARNING: Some notebooks are missing in the Databricks workspace!")
        print("The workflow will be created, but it will fail when it tries to run these notebooks.")
        print("\nMissing notebooks:")
        for task_key, notebook_path in missing_notebooks:
            print(f"  - {task_key}: {notebook_path}")
        print("\nTo upload notebooks:")
        print("  1. Use Databricks UI: Workspace → Right-click folder → Import → Upload .py files")
        print("  2. Use Databricks CLI:")
        v1_dir = Path(__file__).resolve().parent
        for _, notebook_path in missing_notebooks:
            local_file = v1_dir / "databricks" / Path(notebook_path).name
            if local_file.exists():
                print(f"     databricks workspace import {local_file} {notebook_path} -l PYTHON")
        print("  3. Use Databricks Repos: Clone this repo to Databricks Repos")
        print("\nProceeding with workflow creation anyway...\n")
    
    # Create job via API
    result = create_workflow_via_api(workflow_def, host, token, workspace_path)
    job_id = result.get("job_id")
    
    print(f"✓ Created Databricks job: {workflow_def['name']} (ID: {job_id})")
    if missing_notebooks:
        print(f"\n⚠ Remember to upload the missing notebooks before running the job!")
    return job_id


def run_databricks(args):
    """Submit benchmark to Databricks workflow. Creates job if it doesn't exist."""
    # Validate target_catalog is provided
    if not args.target_catalog:
        print("ERROR: --target-catalog is required for Databricks platform (Unity Catalog)", file=sys.stderr)
        sys.exit(1)
    
    # Get Databricks client (host + token)
    host, token = get_databricks_client()
    
    if not host or not token:
        print(
            "ERROR: Databricks credentials not found. Either:\n"
            "  1. Install and configure databricks-cli: pip install databricks-cli && databricks configure --token\n"
            "  2. Or set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables",
            file=sys.stderr,
        )
        sys.exit(1)
    
    # Determine job_id
    job_id = args.job_id
    
    if not job_id:
        # Try to find by job name
        job_name = args.job_name or "TPC-DI-Benchmark"
        print(f"Job ID not provided. Looking for job by name: {job_name}")
        job_id = find_databricks_job_by_name(host, token, job_name)
        
        if not job_id:
            # Job doesn't exist - create it
            print(f"Job '{job_name}' not found. Creating new job...")
            job_id = create_databricks_job(host, token, args)
        else:
            print(f"Found existing job: {job_name} (ID: {job_id})")
    else:
        # Verify job exists
        job = get_databricks_job(host, token, job_id)
        if not job:
            print(f"Job ID {job_id} not found. Creating new job...")
            job_id = create_databricks_job(host, token, args)
        else:
            print(f"Using existing job: {job.get('settings', {}).get('name', 'Unknown')} (ID: {job_id})")
    
    # Build notebook params
    params = {
        "scale_factor": str(args.scale_factor),
        "load_type": args.load_type,
    }
    
    if args.output_path:
        params["tpcdi_raw_data_path"] = args.output_path
    if getattr(args, "local_gen_path", None):
        params["tpcdi_local_gen_path"] = args.local_gen_path
    if args.target_schema:
        params["target_schema"] = args.target_schema
    # target_catalog is required for Databricks
    params["target_catalog"] = args.target_catalog
    if args.batch_id:
        params["batch_id"] = str(args.batch_id)
    if args.metrics_output:
        params["metrics_output"] = args.metrics_output
    if args.log_detailed_stats:
        params["log_detailed_stats"] = "true"
    if args.cluster_instance_type:
        params["cluster_instance_type"] = args.cluster_instance_type
    if args.cluster_worker_count:
        params["cluster_worker_count"] = str(args.cluster_worker_count)
    if args.cluster_master_type:
        params["cluster_master_type"] = args.cluster_master_type
    if getattr(args, "customer_mgmt_xml_format", None):
        params["customer_mgmt_xml_format"] = args.customer_mgmt_xml_format
    
    # Submit run
    import urllib.request
    
    url = f"{host.rstrip('/')}/api/2.1/jobs/run-now"
    data = json.dumps({
        "job_id": int(job_id),
        "notebook_params": params,
    }).encode()
    
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    
    print(f"\nSubmitting run to Databricks job: {job_id}")
    print(f"Parameters: {json.dumps(params, indent=2)}")
    
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            run_id = result.get("run_id")
            print(f"✓ Run submitted successfully!")
            print(f"  Run ID: {run_id}")
            print(f"  View run at: {host.rstrip('/api')}/#job/{job_id}/run/{run_id}")
    except Exception as e:
        print(f"ERROR: Failed to submit job: {e}", file=sys.stderr)
        sys.exit(1)


def run_local(args):
    """Run benchmark locally (requires Spark installed)."""
    import sys
    from pathlib import Path
    
    # Ensure benchmark is importable
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    
    try:
        from benchmark.config import BenchmarkConfig, Platform, LoadType
        from benchmark.runner import run_benchmark
    except ImportError as e:
        print(f"ERROR: Cannot import benchmark module: {e}", file=sys.stderr)
        print("Make sure you're running from the project root and dependencies are installed.", file=sys.stderr)
        sys.exit(1)
    
    # Determine platform from data path
    raw_path = args.raw_data_path or args.output_path or ""
    if raw_path.startswith("gs://"):
        platform = Platform.DATAPROC
        if not args.gcs_bucket:
            # Extract bucket from path
            bucket_match = __import__("re").match(r"gs://([^/]+)", raw_path)
            if bucket_match:
                gcs_bucket = bucket_match.group(1)
            else:
                print("ERROR: Cannot infer gcs_bucket from raw_data_path. Pass --gcs-bucket.", file=sys.stderr)
                sys.exit(1)
        else:
            gcs_bucket = args.gcs_bucket
    else:
        platform = Platform.DATABRICKS
    
    # Build config
    config_kwargs = {
        "platform": platform,
        "load_type": LoadType(args.load_type),
        "scale_factor": args.scale_factor,
        "raw_data_path": args.raw_data_path or args.output_path or ".",
        "target_database": args.target_database or "tpcdi_warehouse",
        "target_schema": args.target_schema or "dw",
        "batch_id": args.batch_id,
        "enable_metrics": True,
        "metrics_output_path": args.metrics_output or "./metrics",
        "log_detailed_stats": args.log_detailed_stats,
    }
    
    if platform == Platform.DATAPROC:
        config_kwargs.update({
            "gcs_bucket": gcs_bucket,
            "project_id": args.project_id or os.environ.get("GOOGLE_CLOUD_PROJECT"),
            "region": args.region or "us-central1",
            "service_account_email": args.service_account_email,
            "service_account_key_file": args.service_account_key_file,
            "table_format": args.format or "parquet",
        })
        if not config_kwargs["project_id"]:
            print("ERROR: --project-id required for Dataproc platform.", file=sys.stderr)
            sys.exit(1)
    else:
        config_kwargs.update({
            "output_path": args.output_path,
            "target_catalog": args.target_catalog,
            "cloud": getattr(args, "cloud", None),
        })
    
    if args.cluster_instance_type:
        config_kwargs["cluster_instance_type"] = args.cluster_instance_type
    if args.cluster_worker_count:
        config_kwargs["cluster_worker_count"] = args.cluster_worker_count
    if args.cluster_master_type:
        config_kwargs["cluster_master_type"] = args.cluster_master_type
    
    config = BenchmarkConfig(**config_kwargs)
    
    print(f"Running benchmark locally (platform: {platform.value})")
    result = run_benchmark(config)
    
    print("\n" + "="*80)
    print("BENCHMARK RESULTS")
    print("="*80)
    print(f"Platform: {result['config']['platform']}")
    if result['metrics'].get('databricks_compute_type'):
        print(f"Compute: {result['metrics']['databricks_compute_type']}")
    print(f"Load Type: {result['config']['load_type']}")
    print(f"Scale Factor: {result['config']['scale_factor']}")
    if result['config']['batch_id']:
        print(f"Batch ID: {result['config']['batch_id']}")
    print(f"\nTotal Duration: {result['metrics']['total_duration_seconds']:.2f} seconds")
    if result['metrics']['summary']:
        print(f"Total Rows Processed: {result['metrics']['summary']['total_rows_processed']:,}")
        print(f"Throughput: {result['metrics']['summary']['throughput_rows_per_second']:.2f} rows/sec")
    dq_timings = result['metrics'].get('dq_table_timings')
    if dq_timings:
        n_tables = len(dq_timings)
        print(f"\nDQ time per table ({n_tables} tables):")
        for t in dq_timings:
            print(f"  {t['table']}: {t['duration_seconds']:.2f}s")
        total_dq = sum(t['duration_seconds'] for t in dq_timings)
        print(f"  Total DQ: {total_dq:.2f}s")
    # Cost (estimated; list-price approximation)
    cb = result['metrics'].get('cost_breakdown')
    total_cost = result['metrics'].get('total_cost_usd')
    if cb is not None or total_cost is not None:
        print("\nCost (estimated):")
        if cb:
            if cb.get('compute_usd') is not None and (cb.get('compute_usd') or 0) > 0:
                print(f"  Compute: ${cb['compute_usd']:.2f}")
            if cb.get('software_usd') is not None:
                print(f"  Software: ${cb['software_usd']:.2f}")
        if total_cost is not None:
            print(f"  Total cost: ${total_cost:.2f}")
    # Save metrics after result summary (so JSON is written after the summary is printed)
    save_fn = result.pop("_save_metrics", None)
    if save_fn:
        save_fn()


def main():
    parser = argparse.ArgumentParser(
        description="Unified wrapper to run TPC-DI benchmarks from your laptop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Submit to Dataproc
  python run_benchmark.py dataproc --cluster my-cluster --load-type batch --scale-factor 10 \\
    --gcs-bucket my-bucket --project-id my-project --region us-central1

  # Submit to Databricks workflow (creates job if missing)
  python run_benchmark.py databricks --load-type batch --scale-factor 10 \\
    --output-path dbfs:/mnt/tpcdi --workspace-path /Workspace/Repos/user/repo/databricks \\
    --cloud AWS --node-type-id i3.xlarge --num-workers 2

  # Run locally
  python run_benchmark.py local --load-type batch --scale-factor 10 \\
    --raw-data-path ./data --metrics-output ./metrics
        """,
    )
    
    subparsers = parser.add_subparsers(dest="platform", help="Platform to run on")
    
    # Common arguments
    def add_common_args(p):
        p.add_argument("--load-type", choices=["batch", "incremental"], required=True,
                      help="Type of load: batch or incremental")
        p.add_argument("--scale-factor", type=int, required=True,
                      help="TPC-DI scale factor (e.g., 10, 100, 1000)")
        p.add_argument("--target-database", default="tpcdi_warehouse",
                      help="Target database name (for Dataproc only)")
        p.add_argument("--target-schema", default="dw",
                      help="Target schema name")
        p.add_argument("--batch-id", type=int,
                      help="Batch ID for incremental loads")
        p.add_argument("--metrics-output",
                      help="Path to save metrics JSON")
        p.add_argument("--log-detailed-stats", action="store_true",
                      help="Log per-table timing and records")
        p.add_argument("--cluster-instance-type",
                      help="Worker instance type for metrics")
        p.add_argument("--cluster-worker-count", type=int,
                      help="Number of worker instances for metrics")
        p.add_argument("--cluster-master-type",
                      help="Driver/master instance type for metrics")
    
    # Dataproc subparser
    parser_dataproc = subparsers.add_parser("dataproc", help="Submit to Dataproc cluster (creates cluster if missing)")
    parser_dataproc.add_argument("--cluster", required=True,
                                 help="Dataproc cluster name")
    parser_dataproc.add_argument("--region", default="us-central1",
                                help="GCP region (default: us-central1)")
    parser_dataproc.add_argument("--project-id", required=True,
                                help="GCP project ID")
    parser_dataproc.add_argument("--gcs-bucket", required=True,
                                help="GCS bucket name")
    parser_dataproc.add_argument("--create-cluster", action="store_true",
                                 help="Create cluster if it doesn't exist (uses default network)")
    parser_dataproc.add_argument("--create-network", action="store_true",
                                 help="Create VPC, subnet, firewall, and cluster if missing (full infrastructure)")
    parser_dataproc.add_argument("--vpc-name",
                                 help="VPC name (used with --create-network, default: <cluster>-vpc)")
    parser_dataproc.add_argument("--subnet-name",
                                 help="Subnet name (used with --create-network or --create-cluster, default: <cluster>-subnet)")
    parser_dataproc.add_argument("--subnet-range",
                                 help="Subnet CIDR range (used with --create-network, default: 10.10.0.0/24)")
    parser_dataproc.add_argument("--zone",
                                 help="GCP zone (default: <region>-b)")
    parser_dataproc.add_argument("--firewall-rule-name",
                                 help="Firewall rule name (used with --create-network)")
    parser_dataproc.add_argument("--raw-data-path",
                                help="Base path to raw TPC-DI data in GCS (default: gs://<bucket>/tpcdi)")
    parser_dataproc.add_argument("--spark-master",
                                help="Spark master URL (default: yarn)")
    parser_dataproc.add_argument("--service-account-email",
                                help="Service account email for GCS access")
    parser_dataproc.add_argument("--service-account-key-file",
                                help="Path to service account JSON key file")
    parser_dataproc.add_argument("--format", choices=["delta", "parquet"], default="parquet",
                                 help="Table format (default: parquet)")
    parser_dataproc.add_argument("--jars",
                                help="Additional JAR files (comma-separated)")
    add_common_args(parser_dataproc)
    
    # Databricks subparser
    parser_databricks = subparsers.add_parser("databricks", help="Submit to Databricks workflow (creates job if missing)")
    parser_databricks.add_argument("--job-id", type=int,
                                  help="Databricks job/workflow ID (if not provided, searches by --job-name or creates new)")
    parser_databricks.add_argument("--job-name", default="TPC-DI-Benchmark",
                                   help="Job name (used to find existing job or name new job)")
    parser_databricks.add_argument("--output-path",
                                  help="Raw data location: DBFS, Volume, or GCS path")
    parser_databricks.add_argument("--local-gen-path", default="",
                                  help="Local path for datagen output (e.g. /mnt/disks/ssd0 on GCP; passed as tpcdi_local_gen_path)")
    parser_databricks.add_argument("--customer-mgmt-xml-format", default="xml",
                                  choices=["xml", "com.databricks.spark.xml"],
                                  help="CustomerMgmt.xml reader format: xml or com.databricks.spark.xml")
    parser_databricks.add_argument("--target-catalog", required=True,
                                  help="Unity Catalog name (required for Databricks)")
    parser_databricks.add_argument("--workspace-path",
                                  help="Workspace path prefix for notebooks (e.g., /Workspace/Repos/user/repo/databricks)")
    parser_databricks.add_argument("--data-gen-notebook", default="generate_tpcdi_data_notebook",
                                  help="Data generation notebook path (relative to workspace-path)")
    parser_databricks.add_argument("--benchmark-notebook", default="benchmark_databricks_notebook",
                                  help="Benchmark notebook path (relative to workspace-path)")
    parser_databricks.add_argument("--spark-version", default="14.3.x-scala2.12",
                                  help="Databricks Runtime version (for new jobs)")
    parser_databricks.add_argument("--cloud", choices=["AWS", "GCP", "Azure"], default="AWS",
                                  help="Cloud provider (for new jobs)")
    parser_databricks.add_argument("--node-type-id",
                                  help="Worker node type (for new jobs; GCP defaults to n2d-standard-16, AWS defaults to i3.xlarge)")
    parser_databricks.add_argument("--driver-node-type-id",
                                  help="Driver node type (for new jobs)")
    parser_databricks.add_argument("--num-workers", type=int, default=None,
                                  help="Number of worker nodes (for new jobs; auto-set based on scale_factor if not provided: SF=10→2, SF=100→3, SF=1000→5)")
    parser_databricks.add_argument("--existing-cluster-id",
                                  help="Use existing cluster ID instead of creating new cluster (for new jobs)")
    add_common_args(parser_databricks)
    
    # Local subparser
    parser_local = subparsers.add_parser("local", help="Run locally (requires Spark)")
    parser_local.add_argument("--raw-data-path",
                             help="Path to raw TPC-DI data (local or gs://)")
    parser_local.add_argument("--output-path",
                             help="Output path (for Databricks platform)")
    parser_local.add_argument("--gcs-bucket",
                             help="GCS bucket (required if raw-data-path is gs://)")
    parser_local.add_argument("--project-id",
                             help="GCP project ID (required for Dataproc platform)")
    parser_local.add_argument("--region", default="us-central1",
                             help="GCP region (default: us-central1)")
    parser_local.add_argument("--service-account-email",
                             help="Service account email for GCS")
    parser_local.add_argument("--service-account-key-file",
                             help="Path to service account JSON key file")
    parser_local.add_argument("--format", choices=["delta", "parquet"], default="parquet",
                             help="Table format (default: parquet)")
    parser_local.add_argument("--target-catalog",
                             help="Unity Catalog name (for Databricks platform)")
    parser_local.add_argument("--cloud", choices=["AWS", "GCP", "Azure"],
                             help="Cloud for Databricks cost estimation (when platform is Databricks)")
    add_common_args(parser_local)
    
    args = parser.parse_args()
    
    if not args.platform:
        parser.print_help()
        sys.exit(1)
    
    if args.platform == "dataproc":
        run_dataproc(args)
    elif args.platform == "databricks":
        run_databricks(args)
    elif args.platform == "local":
        run_local(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
