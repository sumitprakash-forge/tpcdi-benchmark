"""API routes for TPC-DI benchmark operations."""
import json
import os
import traceback
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from server.config import get_workspace_client, get_user_email
from server import run_history

router = APIRouter()

# Notebooks live in the repo (not bundled with the app).
# Bootstrap verifies they exist at the workspace repo path.

# ── GCP Pricing Data ──────────────────────────────────────────────────────

GCP_VM_PRICING = {
    "n2d-standard-4":  {"vm_hr": 0.1754, "dbu_hr": 1.0},
    "n2d-standard-8":  {"vm_hr": 0.3508, "dbu_hr": 2.0},
    "n2d-standard-16": {"vm_hr": 0.7016, "dbu_hr": 4.0},
    "n2d-standard-32": {"vm_hr": 1.4032, "dbu_hr": 8.0},
    "n2d-standard-48": {"vm_hr": 2.1048, "dbu_hr": 12.0},
    "n2d-standard-64": {"vm_hr": 2.8064, "dbu_hr": 16.0},
    "n2d-standard-80": {"vm_hr": 3.5080, "dbu_hr": 20.0},
    "n2d-standard-96": {"vm_hr": 4.2096, "dbu_hr": 24.0},
    "n2d-highmem-4":   {"vm_hr": 0.2376, "dbu_hr": 1.0},
    "n2d-highmem-8":   {"vm_hr": 0.4752, "dbu_hr": 2.0},
    "n2d-highmem-16":  {"vm_hr": 0.9504, "dbu_hr": 4.0},
    "n2d-highmem-32":  {"vm_hr": 1.9008, "dbu_hr": 8.0},
    "n2d-highmem-48":  {"vm_hr": 2.8512, "dbu_hr": 12.0},
    "n2d-highmem-64":  {"vm_hr": 3.8016, "dbu_hr": 16.0},
    "n2d-highmem-80":  {"vm_hr": 4.7520, "dbu_hr": 20.0},
    "n2d-highmem-96":  {"vm_hr": 5.7024, "dbu_hr": 24.0},
    "n2-highmem-16":   {"vm_hr": 1.0282, "dbu_hr": 4.0},
    "c2-standard-4":   {"vm_hr": 0.2088, "dbu_hr": 1.0},
    "c2-standard-8":   {"vm_hr": 0.4176, "dbu_hr": 2.0},
    "c2-standard-16":  {"vm_hr": 0.8352, "dbu_hr": 4.0},
    "c2-standard-30":  {"vm_hr": 1.5660, "dbu_hr": 7.5},
}
DBU_LIST_PRICE = 0.15  # $/DBU for Jobs Compute (Photon)
LOCAL_SSD_COST_PER_HR = 0.0416  # per 375GB SSD per hour


def _get_instance_pricing(instance_type: str) -> dict:
    """Get pricing for an instance type, with fallback estimation."""
    if instance_type in GCP_VM_PRICING:
        return GCP_VM_PRICING[instance_type]
    # Fallback: estimate based on vCPU count from name
    try:
        parts = instance_type.rsplit("-", 1)
        vcpus = int(parts[-1])
        family = parts[0] if len(parts) > 1 else "n2d-standard"
        per_vcpu_vm = 0.0439  # n2d-standard rate
        per_vcpu_dbu = 0.25
        if "highmem" in family:
            per_vcpu_vm = 0.0594
        return {"vm_hr": vcpus * per_vcpu_vm, "dbu_hr": vcpus * per_vcpu_dbu}
    except Exception:
        return {"vm_hr": 0.5, "dbu_hr": 2.0}


def calculate_run_cost(
    driver_type: str,
    worker_type: str,
    num_workers: int,
    duration_ms: int,
    local_ssd_count: int = 0,
    photon: bool = True,
) -> dict:
    """Calculate cost breakdown for a job run."""
    hours = duration_ms / 3_600_000.0
    driver_p = _get_instance_pricing(driver_type)
    worker_p = _get_instance_pricing(worker_type)

    vm_cost = (driver_p["vm_hr"] + num_workers * worker_p["vm_hr"]) * hours
    total_dbu = (driver_p["dbu_hr"] + num_workers * worker_p["dbu_hr"]) * hours
    dbu_cost = total_dbu * DBU_LIST_PRICE
    total_nodes = 1 + num_workers
    ssd_cost = local_ssd_count * total_nodes * LOCAL_SSD_COST_PER_HR * hours

    return {
        "vm_cost_usd": round(vm_cost, 4),
        "dbu_cost_usd": round(dbu_cost, 4),
        "dbu_list_price": DBU_LIST_PRICE,
        "total_dbus": round(total_dbu, 2),
        "ssd_cost_usd": round(ssd_cost, 4),
        "total_cost_usd": round(vm_cost + dbu_cost + ssd_cost, 4),
        "duration_hours": round(hours, 4),
        "duration_ms": duration_ms,
        "cluster": {
            "driver_node_type": driver_type,
            "worker_node_type": worker_type,
            "num_workers": num_workers,
            "local_ssd_count": local_ssd_count,
        },
    }


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
    notebook_path: str = ""
    local_ssds: int = 2
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
    request: Request,
    catalog_name: str = Query("tpcdi_benchmark"),
    schema_name: str = Query("dw"),
    raw_data_path: str = Query("gs://sumit_prakash_gcs/tpcdi"),
):
    """Check existence of catalog, credential, external location, schema, volume."""
    try:
        w = get_workspace_client(request)
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
async def verify_prerequisites(req: PrerequisitesRequest, request: Request):
    """Check and create all prerequisites: SP permissions, catalog, schema, volume."""
    try:
        w = get_workspace_client(request)
        cred_name = req.catalog_name
        ext_loc_name = f"{req.catalog_name}_raw_data"
        volume_name = "libs"

        steps = []

        # 0a. Ensure SP has allow-cluster-create entitlement
        try:
            sp_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
            if sp_id:
                # Find SP numeric ID from application ID
                sp_list = w.service_principals.list(filter=f"applicationId eq '{sp_id}'")
                sp_obj = next(sp_list, None)
                if sp_obj:
                    has_cluster_create = any(
                        e.value == "allow-cluster-create"
                        for e in (sp_obj.entitlements or [])
                    )
                    if not has_cluster_create:
                        # Use raw API call — SDK SCIM types vary across versions
                        w.api_client.do(
                            "PATCH",
                            f"/api/2.0/preview/scim/v2/ServicePrincipals/{sp_obj.id}",
                            body={
                                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                                "Operations": [{
                                    "op": "add",
                                    "path": "entitlements",
                                    "value": [{"value": "allow-cluster-create"}],
                                }],
                            },
                        )
                        steps.append({"step": "sp_cluster_create", "name": "allow-cluster-create", "status": "created"})
                    else:
                        steps.append({"step": "sp_cluster_create", "name": "allow-cluster-create", "status": "exists"})
        except Exception as e:
            steps.append({"step": "sp_cluster_create", "name": "allow-cluster-create", "status": "skipped", "error": str(e)})

        # 0b. Grant SP permissions on catalog via raw API
        try:
            sp_client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
            if sp_client_id:
                w.api_client.do(
                    "PATCH",
                    f"/api/2.1/unity-catalog/permissions/catalog/{req.catalog_name}",
                    body={
                        "changes": [{
                            "principal": sp_client_id,
                            "add": ["USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"],
                        }],
                    },
                )
                steps.append({"step": "sp_catalog_grant", "name": req.catalog_name, "status": "created"})
        except Exception as e:
            steps.append({"step": "sp_catalog_grant", "name": req.catalog_name, "status": "skipped", "error": str(e)})

        # 0c. Grant SP CREATE EXTERNAL LOCATION on metastore (needed to create ext locations)
        try:
            sp_client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
            if sp_client_id:
                # Get current metastore assignment
                metastore = w.metastores.current()
                metastore_id = metastore.metastore_id if metastore else None
                if metastore_id:
                    w.api_client.do(
                        "PATCH",
                        f"/api/2.1/unity-catalog/permissions/metastore/{metastore_id}",
                        body={
                            "changes": [{
                                "principal": sp_client_id,
                                "add": ["CREATE_EXTERNAL_LOCATION"],
                            }],
                        },
                    )
                    steps.append({"step": "sp_metastore_grant", "name": "CREATE_EXTERNAL_LOCATION", "status": "created"})
                else:
                    steps.append({"step": "sp_metastore_grant", "name": "CREATE_EXTERNAL_LOCATION", "status": "skipped", "error": "No metastore assigned"})
        except Exception as e:
            error_msg = str(e)
            # Provide manual fix instructions
            try:
                metastore = w.metastores.current()
                mid = metastore.metastore_id if metastore else "UNKNOWN"
            except Exception:
                mid = "UNKNOWN"
            sp_id_str = os.environ.get("DATABRICKS_CLIENT_ID", "UNKNOWN")
            error_msg += (
                f". Manual fix: run as workspace admin: "
                f"databricks unity-catalog permissions update metastore {mid} "
                f'--json \'{{"changes":[{{"principal":"{sp_id_str}","add":["CREATE_EXTERNAL_LOCATION"]}}]}}\''
            )
            steps.append({"step": "sp_metastore_grant", "name": "CREATE_EXTERNAL_LOCATION", "status": "skipped", "error": error_msg})

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

        # 3. External Location for raw data - find or create one covering raw_data_path
        ext_loc_exists = False
        ext_loc_url = ""
        ext_loc_cred = ""

        # First check if any existing external location covers the raw_data_path
        try:
            for loc in w.external_locations.list():
                if loc.url and req.raw_data_path.rstrip("/").startswith(loc.url.rstrip("/")):
                    ext_loc_exists = True
                    ext_loc_name = loc.name or ext_loc_name
                    ext_loc_url = loc.url or ""
                    ext_loc_cred = loc.credential_name or ""
                    steps.append({"step": "external_location", "name": ext_loc_name, "status": "exists", "url": ext_loc_url})
                    break
        except Exception:
            pass

        # Try to get by name if not found by URL
        if not ext_loc_exists:
            try:
                loc = w.external_locations.get(ext_loc_name)
                ext_loc_exists = True
                ext_loc_url = loc.url or ""
                ext_loc_cred = loc.credential_name or ""
                steps.append({"step": "external_location", "name": ext_loc_name, "status": "exists", "url": ext_loc_url})
            except Exception:
                pass

        if not ext_loc_exists:
            # Try to create external location using the catalog's storage credential.
            # Don't check if credential exists first (SP may lack permission to read it).
            # Just attempt creation directly with the catalog-named credential.
            try:
                gcs_parts = req.raw_data_path.replace("gs://", "").split("/")
                gcs_bucket_url = f"gs://{gcs_parts[0]}"
                cred_to_use = req.catalog_name  # credential typically has same name as catalog

                loc = w.external_locations.create(
                    name=ext_loc_name,
                    url=gcs_bucket_url,
                    credential_name=cred_to_use,
                    comment="TPC-DI benchmark external location for raw data",
                )
                ext_loc_exists = True
                ext_loc_url = loc.url or gcs_bucket_url
                ext_loc_cred = cred_to_use
                steps.append({"step": "external_location", "name": ext_loc_name, "status": "created", "url": ext_loc_url, "credential": cred_to_use})
            except Exception as e:
                error_msg = str(e)
                sa_email = ""
                gcs_bucket = ""
                # Try to get the SA email and build actionable fix instructions
                try:
                    gcs_parts = req.raw_data_path.replace("gs://", "").split("/")
                    gcs_bucket = gcs_parts[0]
                except Exception:
                    pass
                # Try multiple ways to get the SA email
                try:
                    sc = w.storage_credentials.get(req.catalog_name)
                    if hasattr(sc, 'databricks_gcp_service_account') and sc.databricks_gcp_service_account:
                        sa_email = sc.databricks_gcp_service_account.email or ""
                except Exception:
                    pass
                if not sa_email:
                    # Try raw API to list storage credentials
                    try:
                        resp = w.api_client.do("GET", "/api/2.1/unity-catalog/storage-credentials")
                        for sc_item in (resp or {}).get("storage_credentials", []):
                            if sc_item.get("name") == req.catalog_name:
                                gcp_sa = sc_item.get("databricks_gcp_service_account", {})
                                sa_email = gcp_sa.get("email", "")
                                break
                    except Exception:
                        pass
                fix_steps = []
                if "CREATE EXTERNAL LOCATION" in error_msg.upper() or "CREATE_EXTERNAL_LOCATION" in error_msg.upper():
                    fix_steps.append(
                        "1) Grant SP metastore permission: Go to Catalog > gear icon > Permissions > "
                        "Grant 'CREATE EXTERNAL LOCATION' to the app's service principal"
                    )
                if sa_email and gcs_bucket:
                    fix_steps.append(
                        f"2) Grant GCS access: gsutil iam ch "
                        f"serviceAccount:{sa_email}:roles/storage.admin "
                        f"gs://{gcs_bucket}"
                    )
                elif sa_email:
                    fix_steps.append(f"2) Grant SA '{sa_email}' access to the GCS bucket")
                if fix_steps:
                    error_msg += ". FIX: " + " | ".join(fix_steps)
                steps.append({
                    "step": "external_location", "name": ext_loc_name,
                    "status": "skipped", "error": error_msg,
                    **({"sa_email": sa_email} if sa_email else {}),
                    **({"gcs_bucket": gcs_bucket} if gcs_bucket else {}),
                })

        # 3a. Grant SP READ_FILES and WRITE_FILES on the external location
        if ext_loc_exists:
            try:
                sp_client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
                if sp_client_id:
                    w.api_client.do(
                        "PATCH",
                        f"/api/2.1/unity-catalog/permissions/external_location/{ext_loc_name}",
                        body={
                            "changes": [{
                                "principal": sp_client_id,
                                "add": ["READ_FILES", "WRITE_FILES"],
                            }],
                        },
                    )
                    steps.append({"step": "sp_ext_loc_grant", "name": ext_loc_name, "status": "created"})
            except Exception as e:
                steps.append({"step": "sp_ext_loc_grant", "name": ext_loc_name, "status": "skipped", "error": str(e)})

        # 3b. Schema
        schema_full = f"{req.catalog_name}.{req.schema_name}"
        schema_exists = False
        if catalog_exists:
            try:
                w.schemas.get(schema_full)
                schema_exists = True
                steps.append({"step": "schema", "name": schema_full, "status": "exists"})
            except Exception:
                try:
                    w.schemas.create(
                        name=req.schema_name,
                        catalog_name=req.catalog_name,
                        comment="TPC-DI benchmark schema",
                    )
                    schema_exists = True
                    steps.append({"step": "schema", "name": schema_full, "status": "created"})
                except Exception as e:
                    steps.append({"step": "schema", "name": schema_full, "status": "error", "error": str(e)})

        # 4. Volume (libs)
        vol_full = f"{req.catalog_name}.{req.schema_name}.{volume_name}"
        vol_exists = False
        try:
            w.volumes.read(vol_full)
            vol_exists = True
            steps.append({"step": "volume", "name": vol_full, "status": "exists"})
        except Exception:
            pass

        if not vol_exists and catalog_exists and schema_exists:
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

        # 5. GCS bucket write access for compute SA (needed for datagen upload)
        gcs_bucket = ""
        compute_sa = ""
        try:
            gcs_parts = req.raw_data_path.replace("gs://", "").split("/")
            gcs_bucket = gcs_parts[0]
            if gcs_bucket:
                # Get the workspace's default compute SA via cluster API
                # On GCP Databricks, the compute SA is typically in the workspace config
                try:
                    # Try to get it from a recent cluster or workspace config
                    resp = w.api_client.do("GET", "/api/2.0/clusters/list")
                    for cl in (resp or {}).get("clusters", []):
                        gcp_attrs = cl.get("gcp_attributes", {})
                        sa = gcp_attrs.get("google_service_account", "")
                        if sa:
                            compute_sa = sa
                            break
                except Exception:
                    pass

                # Fallback: try workspace config API for default SA
                if not compute_sa:
                    try:
                        # The default compute SA follows the pattern: databricks-compute@<project>.iam.gserviceaccount.com
                        # Try to get from credential config
                        resp = w.api_client.do("GET", "/api/2.0/workspace-conf", data={"keys": "googleServiceAccount"})
                        compute_sa = (resp or {}).get("googleServiceAccount", "")
                    except Exception:
                        pass

                if compute_sa:
                    # Check if SA already has write access to the GCS bucket
                    from server.gcp_auth import get_credentials
                    creds = get_credentials()
                    if creds:
                        from googleapiclient.discovery import build
                        storage = build("storage", "v1", credentials=creds)
                        policy = storage.buckets().getIamPolicy(bucket=gcs_bucket).execute()
                        member = f"serviceAccount:{compute_sa}"
                        has_write = False
                        write_roles = {"roles/storage.admin", "roles/storage.objectAdmin", "roles/storage.objectCreator"}
                        for binding in policy.get("bindings", []):
                            if binding.get("role") in write_roles and member in binding.get("members", []):
                                has_write = True
                                break
                        if has_write:
                            steps.append({"step": "gcs_compute_sa", "name": f"{compute_sa} → gs://{gcs_bucket}", "status": "exists"})
                        else:
                            # Grant objectAdmin to the compute SA
                            role_to_grant = "roles/storage.objectAdmin"
                            bindings = policy.get("bindings", [])
                            role_binding = None
                            for b in bindings:
                                if b.get("role") == role_to_grant:
                                    role_binding = b
                                    break
                            if role_binding:
                                role_binding["members"].append(member)
                            else:
                                bindings.append({"role": role_to_grant, "members": [member]})
                            policy["bindings"] = bindings
                            storage.buckets().setIamPolicy(bucket=gcs_bucket, body=policy).execute()
                            steps.append({"step": "gcs_compute_sa", "name": f"{compute_sa} → gs://{gcs_bucket}", "status": "created"})
                    else:
                        steps.append({"step": "gcs_compute_sa", "name": f"{compute_sa} → gs://{gcs_bucket}",
                                      "status": "skipped", "error": "GCP auth not available. Sign in on Dataproc tab first."})
                else:
                    steps.append({"step": "gcs_compute_sa", "name": f"gs://{gcs_bucket}",
                                  "status": "skipped", "error": "Could not determine compute SA. Grant access manually."})
        except Exception as e:
            sa_display = f"{compute_sa} → gs://{gcs_bucket}" if compute_sa and gcs_bucket else f"gs://{gcs_bucket}"
            steps.append({"step": "gcs_compute_sa", "name": sa_display, "status": "skipped", "error": str(e)})

        all_ok = all(s.get("status") in ("exists", "created", "skipped") for s in steps)
        sa_email_summary = ""
        for s in steps:
            if s.get("step") == "gcs_compute_sa" and s.get("name"):
                sa_email_summary = s["name"].split(" → ")[0] if " → " in s["name"] else ""
                break
        return {
            "status": "ready" if all_ok else "incomplete",
            "steps": steps,
            "summary": {
                "catalog": req.catalog_name,
                "external_location": ext_loc_name,
                "external_location_url": ext_loc_url,
                "volume": vol_full,
                "sa_email": sa_email_summary,
            },
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Grant GCS IAM access to UC storage credential SA ────────────────────

class GcsIamGrantRequest(BaseModel):
    sa_email: str  # UC storage credential's GCP SA email
    gcs_bucket: str  # GCS bucket name (without gs:// prefix)
    role: str = "roles/storage.admin"


@router.post("/databricks/grant-gcs-iam")
async def grant_gcs_iam(req: GcsIamGrantRequest):
    """Grant a GCP service account IAM access to a GCS bucket.

    Uses the Dataproc GCP OAuth credentials (user's Google identity).
    This solves the 'storage.buckets.get' permission denied error for
    the UC storage credential's SA.
    """
    try:
        from server.gcp_auth import get_credentials
        creds = get_credentials()
        if not creds:
            raise HTTPException(
                status_code=401,
                detail="Not authenticated with Google Cloud. Sign in on the Dataproc tab first, "
                       "then retry this operation.",
            )

        from googleapiclient.discovery import build
        storage = build("storage", "v1", credentials=creds)

        # Get current IAM policy
        policy = storage.buckets().getIamPolicy(bucket=req.gcs_bucket).execute()

        # Check if binding already exists
        already_granted = False
        member = f"serviceAccount:{req.sa_email}"
        for binding in policy.get("bindings", []):
            if binding.get("role") == req.role and member in binding.get("members", []):
                already_granted = True
                break

        if already_granted:
            return {"status": "exists", "message": f"{req.sa_email} already has {req.role} on gs://{req.gcs_bucket}"}

        # Add binding
        bindings = policy.get("bindings", [])
        # Find existing binding for this role or create new one
        role_binding = None
        for b in bindings:
            if b.get("role") == req.role:
                role_binding = b
                break
        if role_binding:
            role_binding["members"].append(member)
        else:
            bindings.append({"role": req.role, "members": [member]})

        policy["bindings"] = bindings
        storage.buckets().setIamPolicy(bucket=req.gcs_bucket, body=policy).execute()

        return {
            "status": "created",
            "message": f"Granted {req.role} to {req.sa_email} on gs://{req.gcs_bucket}",
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Bootstrap: Deploy notebooks + JAR to workspace ──────────────────────

class BootstrapRequest(BaseModel):
    catalog_name: str = "tpcdi_benchmark"
    schema_name: str = "dw"


@router.post("/databricks/bootstrap")
async def bootstrap_workspace(req: BootstrapRequest, request: Request):
    """Verify notebooks exist in workspace repo and download spark-xml JAR to Volume.

    Notebooks and the benchmark module live in the Git repo (not bundled with the app).
    They are synced to the workspace via Git Repos or databricks sync.
    This endpoint verifies they are present and sets up the JAR dependency.
    """
    try:
        w = get_workspace_client(request)
        steps = []

        # 1. Determine workspace path for notebooks
        # Use SP's own workspace folder (SP always has write access there)
        try:
            me = w.current_user.me()
            sp_identity = me.user_name or str(me.id)
        except Exception:
            sp_identity = "shared"
        ws_base = f"/Workspace/Users/{sp_identity}/tpcdi-benchmark/databricks"

        # 2. Verify notebooks exist in workspace (deployed via Git/sync, not by this app)
        notebooks = [
            "benchmark_databricks_notebook",
            "generate_tpcdi_data_notebook",
        ]

        for nb_name in notebooks:
            nb_path = f"{ws_base}/{nb_name}"
            try:
                w.workspace.get_status(nb_path)
                steps.append({"step": f"notebook_{nb_name}", "name": nb_path, "status": "exists"})
            except Exception:
                steps.append({
                    "step": f"notebook_{nb_name}",
                    "name": nb_path,
                    "status": "error",
                    "error": f"Notebook not found at {nb_path}. Sync the repo to workspace first.",
                })

        # 2b. Verify benchmark Python module exists in workspace
        benchmark_ws_base = ws_base.rsplit("/databricks", 1)[0]
        benchmark_ws_path = f"{benchmark_ws_base}/benchmark"
        try:
            w.workspace.get_status(f"{benchmark_ws_path}/__init__.py")
            steps.append({"step": "benchmark_module", "name": benchmark_ws_path, "status": "exists"})
        except Exception:
            steps.append({
                "step": "benchmark_module",
                "name": benchmark_ws_path,
                "status": "error",
                "error": f"Benchmark module not found at {benchmark_ws_path}. Sync the repo to workspace first.",
            })

        # 3. Download spark-xml JAR to Volume
        jar_name = "spark-xml_2.13-0.18.0.jar"
        vol_path = f"/Volumes/{req.catalog_name}/{req.schema_name}/libs/{jar_name}"
        maven_url = "https://repo1.maven.org/maven2/com/databricks/spark-xml_2.13/0.18.0/spark-xml_2.13-0.18.0.jar"

        try:
            w.api_client.do("GET", "/api/2.0/fs/files", query={"path": vol_path.lstrip("/")})
            steps.append({"step": "spark_xml_jar", "name": vol_path, "status": "exists"})
        except Exception:
            try:
                import urllib.request
                import tempfile

                with tempfile.NamedTemporaryFile(suffix=".jar", delete=False) as tmp:
                    urllib.request.urlretrieve(maven_url, tmp.name)
                    tmp_path = tmp.name

                with open(tmp_path, "rb") as f:
                    jar_bytes = f.read()

                w.api_client.do(
                    "PUT",
                    f"/api/2.0/fs/files{vol_path}",
                    data=jar_bytes,
                    headers={"Content-Type": "application/octet-stream"},
                )
                steps.append({"step": "spark_xml_jar", "name": vol_path, "status": "created"})
                os.unlink(tmp_path)
            except Exception as e:
                steps.append({"step": "spark_xml_jar", "name": vol_path, "status": "error", "error": str(e)})

        all_ok = all(s.get("status") in ("exists", "created", "skipped") for s in steps)
        return {
            "status": "ready" if all_ok else "incomplete",
            "steps": steps,
            "notebook_base_path": ws_base,
            "benchmark_notebook": f"{ws_base}/benchmark_databricks_notebook",
            "datagen_notebook": f"{ws_base}/generate_tpcdi_data_notebook",
            "jar_path": vol_path,
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Data Generation Job ───────────────────────────────────────────────────

@router.post("/databricks/create-datagen-job")
async def create_datagen_job(req: DataGenJobRequest, request: Request):
    try:
        w = get_workspace_client(request)

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
async def create_job(req: JobCreateRequest, request: Request):
    try:
        w = get_workspace_client(request)

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
                "local_ssd_count": req.local_ssds,
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
                        "notebook_path": req.notebook_path or "/Workspace/Users/sumit.prakash@databricks.com/tpcdi-benchmark/databricks/benchmark_databricks_notebook",
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
async def run_job(req: RunJobRequest, request: Request):
    try:
        w = get_workspace_client(request)
        wait = w.jobs.run_now(job_id=req.job_id)
        run_id = wait.run_id
        # Build run_page_url from workspace host
        host = str(w.config.host).rstrip("/")
        run_page_url = f"{host}#job/{req.job_id}/run/{run_id}"
        return {"status": "success", "run_id": run_id, "run_page_url": run_page_url}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Job Status ──────────────────────────────────────────────────────────────

@router.get("/databricks/job-status/{run_id}")
async def get_job_status(run_id: int, request: Request):
    try:
        w = get_workspace_client(request)
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
async def list_job_runs(job_id: int, request: Request, limit: int = Query(10)):
    """List recent runs for a job."""
    try:
        w = get_workspace_client(request)
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
async def list_benchmark_jobs(request: Request):
    """List recent TPC-DI benchmark jobs with latest run status."""
    try:
        w = get_workspace_client(request)
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
async def list_catalogs(request: Request):
    try:
        w = get_workspace_client(request)
        catalogs = []
        for c in w.catalogs.list():
            catalogs.append({"name": c.name, "comment": c.comment or ""})
        return {"catalogs": catalogs}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/databricks/list-credentials")
async def list_credentials(request: Request):
    try:
        w = get_workspace_client(request)
        creds = []
        for c in w.storage_credentials.list():
            creds.append({"name": c.name, "id": str(c.id) if c.id else ""})
        return {"credentials": creds}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Metrics / Results ──────────────────────────────────────────────────────

@router.get("/metrics/list")
async def list_metrics(request: Request, metrics_path: str = Query("gs://sumit_prakash_gcs/tpcdi/metrics")):
    """List and return all metrics JSON files using Databricks SQL to read from GCS."""
    try:
        w = get_workspace_client(request)
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


# ── Run Cost Calculation ──────────────────────────────────────────────────

@router.get("/databricks/run-cost/{run_id}")
async def get_run_cost(run_id: int, request: Request):
    """Calculate cost breakdown for a completed job run."""
    try:
        w = get_workspace_client(request)
        # Get run details via REST API for raw JSON access
        run_data = w.api_client.do("GET", "/api/2.1/jobs/runs/get", query={"run_id": run_id})

        state = run_data.get("state", {})
        lifecycle = state.get("life_cycle_state", "")

        # Get durations
        exec_duration = run_data.get("execution_duration", 0) or 0
        setup_duration = run_data.get("setup_duration", 0) or 0
        total_duration = exec_duration + setup_duration

        # Extract cluster config from job_clusters
        driver_type = "unknown"
        worker_type = "unknown"
        num_workers = 0
        local_ssd_count = 0

        job_clusters = run_data.get("job_clusters", [])
        if job_clusters:
            cluster_spec = job_clusters[0].get("new_cluster", {})
            driver_type = cluster_spec.get("driver_node_type_id", cluster_spec.get("node_type_id", "unknown"))
            worker_type = cluster_spec.get("node_type_id", driver_type)
            num_workers = cluster_spec.get("num_workers", 0)
            gcp_attrs = cluster_spec.get("gcp_attributes", {})
            local_ssd_count = gcp_attrs.get("local_ssd_count", 1)  # GCP default: 1 local SSD per node

        photon = False
        if job_clusters:
            photon = job_clusters[0].get("new_cluster", {}).get("runtime_engine", "") == "PHOTON"

        cost = calculate_run_cost(
            driver_type=driver_type,
            worker_type=worker_type,
            num_workers=num_workers,
            duration_ms=total_duration,
            local_ssd_count=local_ssd_count,
            photon=photon,
        )

        cost["run_id"] = run_id
        cost["state"] = lifecycle
        cost["result_state"] = state.get("result_state")
        cost["exec_duration_ms"] = exec_duration
        cost["setup_duration_ms"] = setup_duration
        cost["run_page_url"] = run_data.get("run_page_url", "")

        return cost

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Cancel Run ────────────────────────────────────────────────────────────

@router.post("/databricks/cancel-run/{run_id}")
async def cancel_run(run_id: int, request: Request):
    """Cancel a running job."""
    try:
        w = get_workspace_client(request)
        w.jobs.cancel_run(run_id=run_id)
        return {"status": "cancelled", "run_id": run_id}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ── Health ──────────────────────────────────────────────────────────────────

class SaveRunRequest(BaseModel):
    run_data: dict


@router.post("/databricks/save-run")
async def save_run(req: SaveRunRequest, request: Request):
    result = run_history.save_run(request, req.run_data)
    return result


@router.get("/databricks/run-history")
async def list_run_history(request: Request, limit: int = Query(50)):
    return run_history.list_runs(request, limit)


@router.delete("/databricks/run-history/{filename}")
async def delete_run_history(filename: str, request: Request):
    return run_history.delete_run(request, filename)


@router.get("/health")
async def health():
    return {"status": "healthy"}
