"""API routes for TPC-DI benchmark operations."""
import json
import traceback
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from server.config import get_workspace_client

router = APIRouter()


# ── Request Models ──────────────────────────────────────────────────────────

class StorageCredentialRequest(BaseModel):
    name: str
    sa_email: str
    sa_key_json: str


class CatalogRequest(BaseModel):
    name: str
    storage_root: str


class ExternalLocationRequest(BaseModel):
    name: str
    url: str
    credential_name: str


class SchemaRequest(BaseModel):
    catalog: str
    name: str


class VolumeRequest(BaseModel):
    catalog: str
    schema_name: str
    name: str


class DataGenJobRequest(BaseModel):
    job_name: str = "TPC-DI Data Generation"
    scale_factor: int = 10
    output_path: str = "gs://sumit_prakash_gcs/tpcdi"
    spark_version: str = "17.3.x-scala2.13"
    node_type: str = "n2-highmem-16"
    num_workers: int = 0
    photon: bool = False
    cloud: str = "GCP"
    local_gen_path: str = "/local_disk0"
    upload_threads: int = 8
    skip_if_exists: bool = True
    notebook_path: str = "/Workspace/Users/sumit.prakash@databricks.com/tpcdi-benchmark/databricks/generate_tpcdi_data_notebook"
    job_id: Optional[int] = None


class JobCreateRequest(BaseModel):
    job_name: str = "TPC-DI Benchmark"
    load_type: str = "batch"
    scale_factor: int = 10
    raw_data_path: str
    catalog: str
    schema_name: str = "dw"
    batch_id: str = ""
    cloud: str = "GCP"
    xml_format: str = "com.databricks.spark.xml"
    spark_version: str = "17.3.x-scala2.13"
    worker_node_type: str = "n2-highmem-16"
    driver_node_type: str = "n2-highmem-16"
    num_workers: int = 3
    jar_path: str = ""
    photon: bool = True
    log_detailed_stats: bool = True
    job_id: Optional[int] = None


class RunJobRequest(BaseModel):
    job_id: int


# ── Storage Credential ─────────────────────────────────────────────────────

@router.post("/databricks/create-storage-credential")
async def create_storage_credential(req: StorageCredentialRequest):
    try:
        w = get_workspace_client()
        key_data = json.loads(req.sa_key_json)

        from databricks.sdk.service.catalog import GcpServiceAccountKey
        cred = w.storage_credentials.create(
            name=req.name,
            gcp_service_account_key=GcpServiceAccountKey(
                email=req.sa_email,
                private_key_id=key_data["private_key_id"],
                private_key=key_data["private_key"],
            ),
            comment=f"TPC-DI benchmark storage credential",
        )
        return {"status": "success", "credential_id": str(cred.id), "name": cred.name}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Catalog ─────────────────────────────────────────────────────────────────

@router.post("/databricks/create-catalog")
async def create_catalog(req: CatalogRequest):
    try:
        w = get_workspace_client()
        catalog = w.catalogs.create(
            name=req.name,
            storage_root=req.storage_root,
            comment="TPC-DI benchmark catalog",
        )
        return {"status": "success", "name": catalog.name}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── External Location ──────────────────────────────────────────────────────

@router.post("/databricks/create-external-location")
async def create_external_location(req: ExternalLocationRequest):
    try:
        w = get_workspace_client()
        loc = w.external_locations.create(
            name=req.name,
            url=req.url,
            credential_name=req.credential_name,
            comment="TPC-DI benchmark external location for raw data",
        )
        return {"status": "success", "name": loc.name, "url": loc.url}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Schema ──────────────────────────────────────────────────────────────────

@router.post("/databricks/create-schema")
async def create_schema(req: SchemaRequest):
    try:
        w = get_workspace_client()
        schema = w.schemas.create(
            name=req.name,
            catalog_name=req.catalog,
            comment="TPC-DI benchmark schema",
        )
        return {"status": "success", "full_name": schema.full_name}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Volume ──────────────────────────────────────────────────────────────────

@router.post("/databricks/create-volume")
async def create_volume(req: VolumeRequest):
    try:
        w = get_workspace_client()
        from databricks.sdk.service.catalog import VolumeType
        vol = w.volumes.create(
            catalog_name=req.catalog,
            schema_name=req.schema_name,
            name=req.name,
            volume_type=VolumeType.MANAGED,
            comment="TPC-DI benchmark volume for JARs and libraries",
        )
        return {"status": "success", "full_name": vol.full_name}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Data Generation Job ───────────────────────────────────────────────────

@router.post("/databricks/create-datagen-job")
async def create_datagen_job(req: DataGenJobRequest):
    try:
        w = get_workspace_client()

        base_parameters = {
            "scale_factor": str(req.scale_factor),
            "tpcdi_raw_data_path": req.output_path,
            "upload_threads": str(req.upload_threads),
            "tpcdi_local_gen_path": req.local_gen_path,
        }

        new_cluster = {
            "spark_version": req.spark_version,
            "node_type_id": req.node_type,
            "num_workers": req.num_workers,
            "driver_node_type_id": req.node_type,
            "runtime_engine": "PHOTON" if req.photon else "STANDARD",
            "data_security_mode": "SINGLE_USER",
            "spark_conf": {},
        }

        # Add cloud-specific attributes
        if req.cloud == "GCP":
            new_cluster["gcp_attributes"] = {"use_preemptible_executors": False}

        job_settings = {
            "name": req.job_name,
            "job_clusters": [
                {
                    "job_cluster_key": "datagen_cluster",
                    "new_cluster": new_cluster,
                }
            ],
            "tasks": [
                {
                    "task_key": "generate_data",
                    "job_cluster_key": "datagen_cluster",
                    "notebook_task": {
                        "notebook_path": req.notebook_path,
                        "base_parameters": base_parameters,
                        "source": "WORKSPACE",
                    },
                }
            ],
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tags": {
                "purpose": "tpcdi_datagen",
                "scale_factor": str(req.scale_factor),
            },
        }

        if req.job_id:
            resp = w.api_client.do(
                "POST",
                "/api/2.1/jobs/reset",
                body={"job_id": req.job_id, "new_settings": job_settings},
            )
            return {"status": "success", "job_id": req.job_id, "action": "updated"}
        else:
            resp = w.api_client.do(
                "POST",
                "/api/2.1/jobs/create",
                body=job_settings,
            )
            return {"status": "success", "job_id": resp["job_id"], "action": "created"}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Job Create/Update ──────────────────────────────────────────────────────

@router.post("/databricks/create-job")
async def create_job(req: JobCreateRequest):
    try:
        w = get_workspace_client()

        jar_path = req.jar_path
        if not jar_path:
            jar_path = f"/Volumes/{req.catalog}/{req.schema_name}/libs/spark-xml_2.13-0.18.0.jar"

        base_parameters = {
            "load_type": req.load_type,
            "scale_factor": str(req.scale_factor),
            "tpcdi_raw_data_path": req.raw_data_path,
            "target_catalog": req.catalog,
            "target_schema": req.schema_name,
            "batch_id": req.batch_id,
            "cloud": req.cloud,
            "customer_mgmt_xml_format": req.xml_format,
            "log_detailed_stats": str(req.log_detailed_stats).lower(),
            "metrics_output": f"{req.raw_data_path}/metrics",
        }

        new_cluster = {
            "spark_version": req.spark_version,
            "node_type_id": req.worker_node_type,
            "num_workers": req.num_workers,
            "driver_node_type_id": req.driver_node_type,
            "runtime_engine": "PHOTON" if req.photon else "STANDARD",
            "data_security_mode": "SINGLE_USER",
            "spark_conf": {
                "spark.databricks.delta.optimizeWrite.enabled": "true",
            },
            "gcp_attributes": {
                "use_preemptible_executors": False,
            },
        }

        job_settings = {
            "name": req.job_name,
            "job_clusters": [
                {
                    "job_cluster_key": "benchmark_cluster",
                    "new_cluster": new_cluster,
                }
            ],
            "tasks": [
                {
                    "task_key": "benchmark_execution",
                    "job_cluster_key": "benchmark_cluster",
                    "libraries": [{"jar": jar_path}],
                    "notebook_task": {
                        "notebook_path": "/Workspace/Users/sumit.prakash@databricks.com/tpcdi-benchmark/databricks/benchmark_databricks_notebook",
                        "base_parameters": base_parameters,
                        "source": "WORKSPACE",
                    },
                }
            ],
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
        }

        if req.job_id:
            # Update existing job via REST API
            resp = w.api_client.do(
                "POST",
                "/api/2.1/jobs/reset",
                body={"job_id": req.job_id, "new_settings": job_settings},
            )
            return {"status": "success", "job_id": req.job_id, "action": "updated"}
        else:
            # Create new job via REST API
            resp = w.api_client.do(
                "POST",
                "/api/2.1/jobs/create",
                body=job_settings,
            )
            return {"status": "success", "job_id": resp["job_id"], "action": "created"}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Run Job ─────────────────────────────────────────────────────────────────

@router.post("/databricks/run-job")
async def run_job(req: RunJobRequest):
    try:
        w = get_workspace_client()
        run = w.jobs.run_now(job_id=req.job_id)
        return {"status": "success", "run_id": run.run_id}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Job Status ──────────────────────────────────────────────────────────────

@router.get("/databricks/job-status/{run_id}")
async def get_job_status(run_id: int):
    try:
        w = get_workspace_client()
        run = w.jobs.get_run(run_id=run_id)

        state = run.state
        result = {
            "run_id": run.run_id,
            "state": state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN",
            "result_state": state.result_state.value if state.result_state else None,
            "state_message": state.state_message or "",
            "run_page_url": run.run_page_url or "",
            "start_time": run.start_time,
            "end_time": run.end_time,
            "execution_duration": run.execution_duration,
            "setup_duration": run.setup_duration,
        }
        return result
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Job Runs ───────────────────────────────────────────────────────────────

@router.get("/databricks/job-runs/{job_id}")
async def list_job_runs(job_id: int, limit: int = Query(10)):
    """List recent runs for a job."""
    try:
        w = get_workspace_client()
        resp = w.api_client.do(
            "GET",
            "/api/2.1/jobs/runs/list",
            query={"job_id": job_id, "limit": limit, "expand_tasks": "false"},
        )
        runs = []
        for r in resp.get("runs", []):
            state = r.get("state", {})
            runs.append({
                "run_id": r.get("run_id"),
                "run_name": r.get("run_name", ""),
                "state": state.get("life_cycle_state", "UNKNOWN"),
                "result_state": state.get("result_state"),
                "state_message": state.get("state_message", ""),
                "run_page_url": r.get("run_page_url", ""),
                "start_time": r.get("start_time"),
                "end_time": r.get("end_time"),
                "execution_duration": r.get("execution_duration"),
                "setup_duration": r.get("setup_duration"),
            })
        return {"runs": runs, "has_more": resp.get("has_more", False)}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/databricks/list-jobs")
async def list_benchmark_jobs():
    """List recent TPC-DI benchmark jobs."""
    try:
        w = get_workspace_client()
        resp = w.api_client.do(
            "GET",
            "/api/2.1/jobs/list",
            query={"limit": 20, "name": "TPC-DI"},
        )
        jobs = []
        for j in resp.get("jobs", []):
            settings = j.get("settings", {})
            jobs.append({
                "job_id": j.get("job_id"),
                "name": settings.get("name", ""),
                "created_time": j.get("created_time"),
            })
        return {"jobs": jobs}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── List Resources ──────────────────────────────────────────────────────────

@router.get("/databricks/list-catalogs")
async def list_catalogs():
    try:
        w = get_workspace_client()
        catalogs = []
        for c in w.catalogs.list():
            catalogs.append({"name": c.name, "comment": c.comment or ""})
        return {"catalogs": catalogs}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/databricks/list-credentials")
async def list_credentials():
    try:
        w = get_workspace_client()
        creds = []
        for c in w.storage_credentials.list():
            creds.append({"name": c.name, "id": str(c.id) if c.id else ""})
        return {"credentials": creds}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Metrics / Results ──────────────────────────────────────────────────────

@router.get("/metrics/list")
async def list_metrics(metrics_path: str = Query("gs://sumit_prakash_gcs/tpcdi/metrics")):
    """List and return all metrics JSON files using Databricks SQL to read from GCS."""
    try:
        w = get_workspace_client()
        metrics = []

        # Use Databricks SQL Statement API to list and read JSON files via Spark
        # This works for GCS, DBFS, Volumes - any path Spark can access
        # Use SQL to read all metrics JSON files via Spark (works for GCS, DBFS, Volumes)
        # collect_list groups all lines per file, then we join them to get the full JSON
        sql = f"""
            SELECT _metadata.file_path as file_name,
                   concat_ws('', collect_list(value)) as content
            FROM read_files(
                '{metrics_path}',
                format => 'text',
                pathGlobFilter => 'metrics_*.json'
            )
            GROUP BY _metadata.file_path
        """

        # Find an available SQL warehouse
        warehouses = w.api_client.do("GET", "/api/2.0/sql/warehouses")
        warehouse_id = None
        for wh in warehouses.get("warehouses", []):
            if wh.get("state") in ("RUNNING", "STOPPED"):
                warehouse_id = wh["id"]
                break

        if not warehouse_id:
            if not metrics_path.startswith("gs://"):
                return await _list_metrics_dbfs(w, metrics_path)
            return {"metrics": [], "error": "No SQL warehouse available. Create one to read GCS metrics."}

        stmt_resp = w.api_client.do(
            "POST",
            "/api/2.0/sql/statements",
            body={
                "warehouse_id": warehouse_id,
                "statement": sql,
                "wait_timeout": "50s",
                "disposition": "INLINE",
                "format": "JSON_ARRAY",
            },
        )

        status = stmt_resp.get("status", {}).get("state", "")
        if status == "FAILED":
            error_msg = stmt_resp.get("status", {}).get("error", {}).get("message", "Unknown error")
            return {"metrics": [], "error": f"SQL failed: {error_msg}"}

        if status not in ("SUCCEEDED",):
            return {"metrics": [], "error": f"SQL status: {status}. Try again in a moment."}

        # Parse results - each row is [file_path, json_content_string]
        rows = stmt_resp.get("result", {}).get("data_array", [])

        for row in rows:
            if len(row) >= 2 and row[1]:
                try:
                    data = json.loads(row[1])
                    data["_source_file"] = row[0].split("/")[-1] if row[0] else "unknown"
                    metrics.append(data)
                except Exception as e:
                    print(f"Warning: skip {row[0]}: {e}")

        # Sort by start_time descending
        metrics.sort(key=lambda m: m.get("start_time", 0), reverse=True)

        return {"metrics": metrics, "count": len(metrics)}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def _list_metrics_dbfs(w, metrics_path: str):
    """Fallback: read metrics from DBFS."""
    metrics = []
    try:
        entries = w.dbfs.list(metrics_path)
        for entry in entries:
            if entry.path and entry.path.endswith(".json") and "metrics_" in entry.path:
                try:
                    with w.dbfs.open(entry.path) as f:
                        data = json.loads(f.read())
                        data["_source_file"] = entry.path.split("/")[-1]
                        metrics.append(data)
                except Exception as e:
                    print(f"Warning: skip {entry.path}: {e}")
    except Exception as e:
        return {"metrics": [], "error": str(e)}

    metrics.sort(key=lambda m: m.get("start_time", 0), reverse=True)
    return {"metrics": metrics, "count": len(metrics)}


# ── Health ──────────────────────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {"status": "healthy"}
