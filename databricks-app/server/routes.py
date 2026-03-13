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
    local_ssds: int = 1
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


class PrerequisitesRequest(BaseModel):
    catalog_name: str = "gcp_tpcdi_classic"
    schema_name: str = "dw"
    storage_root: str = "gs://"
    raw_data_path: str = "gs://sumit_prakash_gcs/tpcdi"
    sa_email: str = ""
    sa_key_json: str = ""


# ── Check Prerequisites (GET - read-only status) ─────────────────────────

@router.get("/databricks/check-prerequisites")
async def check_prerequisites(
    catalog_name: str = Query("tpcdi_benchmark"),
    schema_name: str = Query("dw"),
    raw_data_path: str = Query("gs://sumit_prakash_gcs/tpcdi"),
):
    """Check existence of catalog, credential, external location, schema, volume."""
    try:
        w = get_workspace_client()
        cred_name = f"{catalog_name}_cred"
        ext_loc_name = f"{catalog_name}_raw_data"
        volume_name = "libs"

        results = {
            "catalog": {"name": catalog_name, "exists": False},
            "external_location": {"name": ext_loc_name, "exists": False, "url": ""},
            "volume": {"name": f"{catalog_name}.{schema_name}.{volume_name}", "exists": False},
        }

        # Check catalog
        try:
            cat = w.catalogs.get(catalog_name)
            results["catalog"]["exists"] = True
        except Exception:
            pass

        # Check external location - try by name, then search by URL
        try:
            loc = w.external_locations.get(ext_loc_name)
            results["external_location"]["exists"] = True
            results["external_location"]["url"] = loc.url or ""
        except Exception:
            # Search for any external location matching the raw_data_path
            try:
                for loc in w.external_locations.list():
                    if loc.url and raw_data_path.rstrip("/").startswith(loc.url.rstrip("/")):
                        results["external_location"]["exists"] = True
                        results["external_location"]["name"] = loc.name or ext_loc_name
                        results["external_location"]["url"] = loc.url or ""
                        break
            except Exception:
                pass

        # Check volume
        try:
            vol = w.volumes.read(f"{catalog_name}.{schema_name}.{volume_name}")
            results["volume"]["exists"] = True
        except Exception:
            pass

        all_ready = all(v["exists"] for v in results.values())
        return {"status": "ready" if all_ready else "incomplete", "resources": results}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Create & Verify Prerequisites (POST - creates missing resources) ──────

@router.post("/databricks/verify-prerequisites")
async def verify_prerequisites(req: PrerequisitesRequest):
    """Check and create all prerequisites: credential, catalog, ext location, schema, volume."""
    try:
        w = get_workspace_client()
        cred_name = f"{req.catalog_name}_cred"
        ext_loc_name = f"{req.catalog_name}_raw_data"
        volume_name = "libs"

        steps = []

        # 1. Catalog
        catalog_exists = False
        try:
            w.catalogs.get(req.catalog_name)
            catalog_exists = True
            steps.append({"step": "catalog", "name": req.catalog_name, "status": "exists"})
        except Exception:
            pass

        if not catalog_exists:
            try:
                w.catalogs.create(
                    name=req.catalog_name,
                    storage_root=req.storage_root if req.storage_root != "gs://" else None,
                    comment="TPC-DI benchmark catalog",
                )
                catalog_exists = True
                steps.append({"step": "catalog", "name": req.catalog_name, "status": "created"})
            except Exception as e:
                steps.append({"step": "catalog", "name": req.catalog_name, "status": "error", "error": str(e)})

        # 3. External Location - check by name, then search by URL
        ext_loc_exists = False
        ext_loc_url = ""
        try:
            loc = w.external_locations.get(ext_loc_name)
            ext_loc_exists = True
            ext_loc_url = loc.url or ""
            steps.append({"step": "external_location", "name": ext_loc_name, "status": "exists", "url": ext_loc_url})
        except Exception:
            # Search for any external location covering the raw_data_path
            try:
                for loc in w.external_locations.list():
                    if loc.url and req.raw_data_path.rstrip("/").startswith(loc.url.rstrip("/")):
                        ext_loc_exists = True
                        ext_loc_name = loc.name or ext_loc_name
                        ext_loc_url = loc.url or ""
                        steps.append({"step": "external_location", "name": ext_loc_name, "status": "exists", "url": ext_loc_url})
                        break
            except Exception:
                pass

        if not ext_loc_exists:
            try:
                loc = w.external_locations.create(
                    name=ext_loc_name,
                    url=req.raw_data_path,
                    credential_name=cred_name,
                    comment="TPC-DI benchmark external location for raw data",
                )
                ext_loc_url = loc.url or req.raw_data_path
                steps.append({"step": "external_location", "name": ext_loc_name, "status": "created", "url": ext_loc_url})
            except Exception as e:
                steps.append({"step": "external_location", "name": ext_loc_name, "status": "error", "error": str(e)})

        # 4. Volume (libs)
        vol_full = f"{req.catalog_name}.{req.schema_name}.{volume_name}"
        vol_exists = False
        try:
            w.volumes.read(vol_full)
            vol_exists = True
            steps.append({"step": "volume", "name": vol_full, "status": "exists"})
        except Exception:
            pass

        if not vol_exists and catalog_exists:
            try:
                from databricks.sdk.service.catalog import VolumeType
                w.volumes.create(
                    catalog_name=req.catalog_name,
                    schema_name=req.schema_name,
                    name=volume_name,
                    volume_type=VolumeType.MANAGED,
                    comment="TPC-DI benchmark volume for JARs and libraries",
                )
                steps.append({"step": "volume", "name": vol_full, "status": "created"})
            except Exception as e:
                steps.append({"step": "volume", "name": vol_full, "status": "error", "error": str(e)})

        all_ok = all(s.get("status") in ("exists", "created") for s in steps)
        return {
            "status": "ready" if all_ok else "incomplete",
            "steps": steps,
            "summary": {
                "catalog": req.catalog_name,
                "external_location": ext_loc_name,
                "external_location_url": ext_loc_url,
                "volume": vol_full,
            },
        }

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
            gcp_attrs = {"use_preemptible_executors": False}
            if req.local_ssds > 0:
                gcp_attrs["local_ssd_count"] = req.local_ssds
            new_cluster["gcp_attributes"] = gcp_attrs

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
    """List recent TPC-DI benchmark jobs with latest run status."""
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
            job_id = j.get("job_id")
            job_info = {
                "job_id": job_id,
                "name": settings.get("name", ""),
                "created_time": j.get("created_time"),
                "latest_run": None,
            }
            # Fetch latest run for this job
            try:
                runs_resp = w.api_client.do(
                    "GET",
                    "/api/2.1/jobs/runs/list",
                    query={"job_id": job_id, "limit": 1, "expand_tasks": "false"},
                )
                runs = runs_resp.get("runs", [])
                if runs:
                    r = runs[0]
                    state = r.get("state", {})
                    job_info["latest_run"] = {
                        "run_id": r.get("run_id"),
                        "state": state.get("life_cycle_state", "UNKNOWN"),
                        "result_state": state.get("result_state"),
                        "start_time": r.get("start_time"),
                        "execution_duration": r.get("execution_duration"),
                    }
            except Exception:
                pass  # Skip if we can't fetch runs for this job
            jobs.append(job_info)
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
