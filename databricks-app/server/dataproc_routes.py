"""API routes for Dataproc benchmark operations.

Uses Google Cloud Python client libraries with user OAuth credentials.
Auth strategies: ADC (local dev), access token, or refresh token (deployed).
No gcloud CLI or service account key required.
"""
import asyncio
import json
import time
import traceback
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

import google.auth.transport.requests

from server.gcp_auth import (
    get_auth_status,
    get_credentials,
    set_access_token,
    set_refresh_token,
    set_adc_json,
    clear_credentials,
)

router = APIRouter()

# ── GCP VM Pricing (no DBU for Dataproc) ─────────────────────────────────

GCP_VM_PRICING = {
    "n2d-standard-4": 0.1754, "n2d-standard-8": 0.3508, "n2d-standard-16": 0.7016,
    "n2d-standard-32": 1.4032, "n2d-standard-48": 2.1048, "n2d-standard-64": 2.8064,
    "n2d-standard-80": 3.5080, "n2d-standard-96": 4.2096,
    "n2d-highmem-4": 0.2376, "n2d-highmem-8": 0.4752, "n2d-highmem-16": 0.9504,
    "n2d-highmem-32": 1.9008, "n2d-highmem-48": 2.8512, "n2d-highmem-64": 3.8016,
    "n2d-highmem-80": 4.7520, "n2d-highmem-96": 5.7024,
    "n2-highmem-16": 1.0282,
    "c2-standard-4": 0.2088, "c2-standard-8": 0.4176, "c2-standard-16": 0.8352,
    "c2-standard-30": 1.5660,
}
LOCAL_SSD_COST_PER_HR = 0.0416
DATAPROC_PREMIUM_PER_VCPU_HR = 0.01

VCPU_COUNTS = {
    "n2d-standard-4": 4, "n2d-standard-8": 8, "n2d-standard-16": 16,
    "n2d-standard-32": 32, "n2d-standard-48": 48, "n2d-standard-64": 64,
    "n2d-standard-80": 80, "n2d-standard-96": 96,
    "n2d-highmem-4": 4, "n2d-highmem-8": 8, "n2d-highmem-16": 16,
    "n2d-highmem-32": 32, "n2d-highmem-48": 48, "n2d-highmem-64": 64,
    "n2d-highmem-80": 80, "n2d-highmem-96": 96,
    "n2-highmem-16": 16, "c2-standard-4": 4, "c2-standard-8": 8,
    "c2-standard-16": 16, "c2-standard-30": 30,
}


def _get_vm_price(t: str) -> float:
    if t in GCP_VM_PRICING: return GCP_VM_PRICING[t]
    try: return int(t.rsplit("-", 1)[-1]) * 0.0439
    except Exception: return 0.5


def _get_vcpus(t: str) -> int:
    if t in VCPU_COUNTS: return VCPU_COUNTS[t]
    try: return int(t.rsplit("-", 1)[-1])
    except Exception: return 4


def calculate_dataproc_cost(master_type: str, worker_type: str, num_workers: int,
                            duration_ms: int, ssd_count: int = 0) -> dict:
    hours = duration_ms / 3_600_000.0
    vm_cost = (_get_vm_price(master_type) + num_workers * _get_vm_price(worker_type)) * hours
    total_vcpus = _get_vcpus(master_type) + num_workers * _get_vcpus(worker_type)
    dp_premium = total_vcpus * DATAPROC_PREMIUM_PER_VCPU_HR * hours
    ssd_cost = ssd_count * (1 + num_workers) * LOCAL_SSD_COST_PER_HR * hours
    return {
        "vm_cost_usd": round(vm_cost, 4),
        "dataproc_premium_usd": round(dp_premium, 4),
        "ssd_cost_usd": round(ssd_cost, 4),
        "total_cost_usd": round(vm_cost + dp_premium + ssd_cost, 4),
        "duration_hours": round(hours, 4),
        "duration_ms": duration_ms,
        "cluster": {"master_type": master_type, "worker_type": worker_type,
                    "num_workers": num_workers, "ssd_count": ssd_count},
    }


# ── Dataproc API helpers ─────────────────────────────────────────────────

def _get_dataproc_client():
    """Get an authenticated Dataproc ClusterController client."""
    from google.cloud import dataproc_v1
    creds = get_credentials()
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated with Google Cloud. Please sign in first.")
    return dataproc_v1.ClusterControllerClient(credentials=creds)


def _get_job_client():
    """Get an authenticated Dataproc JobController client."""
    from google.cloud import dataproc_v1
    creds = get_credentials()
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated with Google Cloud. Please sign in first.")
    return dataproc_v1.JobControllerClient(credentials=creds)


# ── Request Models ───────────────────────────────────────────────────────

class TokenRequest(BaseModel):
    token: str
    token_type: str = "access_token"  # "access_token", "refresh_token", "adc_json"


class DataprocPrereqRequest(BaseModel):
    project: str = "gcp-sandbox-field-eng"
    region: str = "us-central1"
    subnet: str = "tpcdi-benchmark-subnet"
    gcs_bucket: str = "sumit_prakash_gcs"
    metastore_mode: str = "spark_default"  # "spark_default", "create", "existing"
    metastore_name: str = "tpcdi-benchmark-metastore"
    metastore_path: str = ""  # Full path for existing metastore
    gcp_refresh_token: str = ""  # Auto-refresh: re-auth before prereq check


class DataprocFullRunRequest(BaseModel):
    cluster_name: str
    project: str = "gcp-sandbox-field-eng"
    region: str = "us-central1"
    zone: str = "us-central1-a"
    master_type: str = "n2d-standard-8"
    worker_type: str = "n2d-standard-8"
    num_workers: int = 3
    ssd_count: int = 2
    subnet: str = "tpcdi-benchmark-subnet"
    metastore: str = ""
    image_version: str = "2.3-debian12"
    load_type: str = "batch"
    batch_id: str = ""
    scale_factor: int = 1000
    gcs_bucket: str = "sumit_prakash_gcs"
    raw_data_path: str = "gs://sumit_prakash_gcs/tpcdi"
    target_database: str = "tpcdi_warehouse"
    metrics_output: str = "gs://sumit_prakash_gcs/tpcdi/metrics"
    gcp_refresh_token: str = ""  # Auto-refresh: re-auth before each run
    compute_mode: str = "classic"  # "classic" or "serverless"


# ── Active runs tracker ──────────────────────────────────────────────────

_active_runs: dict[str, dict] = {}
_run_counter = 0
_cancelled_runs: set[str] = set()


def _next_run_id() -> str:
    global _run_counter
    _run_counter += 1
    return f"dp-{_run_counter}"


# ── Auth Endpoints ───────────────────────────────────────────────────────

@router.get("/dataproc/auth/status")
async def auth_status():
    """Check Google Cloud authentication status."""
    return get_auth_status()


@router.post("/dataproc/auth/login")
async def auth_login(req: TokenRequest):
    """Authenticate with Google Cloud using a token."""
    if req.token_type == "access_token":
        return set_access_token(req.token)
    elif req.token_type == "refresh_token":
        return set_refresh_token(req.token)
    elif req.token_type == "adc_json":
        return set_adc_json(req.token)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown token_type: {req.token_type}")


@router.post("/dataproc/auth/logout")
async def auth_logout():
    """Clear cached Google credentials."""
    clear_credentials()
    return {"status": "logged_out"}


# ── Check Endpoints ──────────────────────────────────────────────────────

@router.get("/dataproc/check-network")
async def check_network(
    project: str = Query("gcp-sandbox-field-eng"),
    region: str = Query("us-central1"),
    subnet: str = Query("sumit-tpcdi-subnet"),
):
    """Check if VPC/subnet exists. Uses Compute API."""
    creds = get_credentials()
    if not creds:
        return {"status": "unauthenticated", "resources": {"subnet": {"name": subnet, "exists": False}}}

    try:
        from googleapiclient.discovery import build
        compute = build("compute", "v1", credentials=creds)
        result = compute.subnetworks().get(
            project=project, region=region, subnetwork=subnet
        ).execute()
        return {"status": "ready", "resources": {"subnet": {"name": subnet, "exists": True}}}
    except Exception as e:
        if "404" in str(e) or "notFound" in str(e):
            return {"status": "incomplete", "resources": {"subnet": {"name": subnet, "exists": False}}}
        return {"status": "error", "resources": {"subnet": {"name": subnet, "exists": False}},
                "error": str(e)}


# ── Verify / Auto-create Prerequisites ──────────────────────────────────

@router.post("/dataproc/verify-prerequisites")
async def verify_prerequisites(req: DataprocPrereqRequest):
    """Check and auto-create Dataproc prerequisites (VPC, subnet, firewall, Cloud NAT, GCS bucket).

    Steps attempted in order:
      1. vpc_network       – tpcdi-benchmark-vpc (or existing VPC)
      2. subnet            – req.subnet in req.region, CIDR 10.0.0.0/24, Private Google Access enabled
      3. firewall_internal – allow all internal traffic within the VPC
      4. cloud_nat         – Cloud Router + NAT so internal_ip_only nodes can reach the internet
      5. gcs_bucket        – verify req.gcs_bucket is accessible
    """
    # Auto-refresh: if a refresh token is provided, re-authenticate to ensure fresh creds
    if req.gcp_refresh_token:
        auth_result = set_refresh_token(req.gcp_refresh_token)
        if auth_result.get("status") != "authenticated":
            raise HTTPException(status_code=401, detail=f"Failed to refresh GCP token: {auth_result.get('message', 'Unknown error')}")

    creds = get_credentials()
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated with Google Cloud. Please sign in first.")

    from googleapiclient.discovery import build

    compute = build("compute", "v1", credentials=creds)
    storage = build("storage", "v1", credentials=creds)

    steps = []
    overall_ok = True

    vpc_name = "tpcdi-benchmark-vpc"
    firewall_name = "tpcdi-benchmark-allow-internal"
    router_name = "tpcdi-benchmark-router"
    nat_name = "tpcdi-benchmark-nat"

    # ── Helper: wait for a global operation ──────────────────────────────
    async def _wait_global_op(op_name: str):
        for _ in range(60):
            op = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda n=op_name: compute.globalOperations().get(
                    project=req.project, operation=n
                ).execute(),
            )
            if op.get("status") == "DONE":
                if "error" in op:
                    raise RuntimeError(op["error"]["errors"][0].get("message", str(op["error"])))
                return op
            await asyncio.sleep(5)
        raise TimeoutError(f"Global operation {op_name} did not complete in 5 minutes")

    # ── Helper: wait for a regional operation ────────────────────────────
    async def _wait_region_op(op_name: str):
        for _ in range(60):
            op = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda n=op_name: compute.regionOperations().get(
                    project=req.project, region=req.region, operation=n
                ).execute(),
            )
            if op.get("status") == "DONE":
                if "error" in op:
                    raise RuntimeError(op["error"]["errors"][0].get("message", str(op["error"])))
                return op
            await asyncio.sleep(5)
        raise TimeoutError(f"Regional operation {op_name} did not complete in 5 minutes")

    # ── Step 1: VPC network ───────────────────────────────────────────────
    vpc_self_link = None
    try:
        vpc = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: compute.networks().get(project=req.project, network=vpc_name).execute(),
        )
        vpc_self_link = vpc.get("selfLink")
        steps.append({"step": "vpc_network", "name": vpc_name, "status": "exists",
                       "self_link": vpc_self_link})
    except Exception as e:
        if "404" in str(e) or "notFound" in str(e):
            try:
                op = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: compute.networks().insert(
                        project=req.project,
                        body={"name": vpc_name, "autoCreateSubnetworks": False},
                    ).execute(),
                )
                await _wait_global_op(op["name"])
                vpc = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: compute.networks().get(project=req.project, network=vpc_name).execute(),
                )
                vpc_self_link = vpc.get("selfLink")
                steps.append({"step": "vpc_network", "name": vpc_name, "status": "created",
                               "self_link": vpc_self_link})
            except Exception as create_err:
                steps.append({"step": "vpc_network", "name": vpc_name, "status": "error",
                               "error": str(create_err)})
                overall_ok = False
        else:
            steps.append({"step": "vpc_network", "name": vpc_name, "status": "error",
                           "error": str(e)})
            overall_ok = False

    # ── Step 2: Subnet ────────────────────────────────────────────────────
    subnet_self_link = None
    try:
        sn = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: compute.subnetworks().get(
                project=req.project, region=req.region, subnetwork=req.subnet
            ).execute(),
        )
        subnet_self_link = sn.get("selfLink")
        steps.append({"step": "subnet", "name": req.subnet, "status": "exists",
                       "cidr": sn.get("ipCidrRange"), "self_link": subnet_self_link})
    except Exception as e:
        if ("404" in str(e) or "notFound" in str(e)) and vpc_self_link:
            try:
                subnet_body = {
                    "name": req.subnet,
                    "network": vpc_self_link,
                    "region": req.region,
                    "ipCidrRange": "10.0.0.0/24",
                    "privateIpGoogleAccess": True,
                }
                op = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: compute.subnetworks().insert(
                        project=req.project, region=req.region, body=subnet_body
                    ).execute(),
                )
                await _wait_region_op(op["name"])
                sn = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: compute.subnetworks().get(
                        project=req.project, region=req.region, subnetwork=req.subnet
                    ).execute(),
                )
                subnet_self_link = sn.get("selfLink")
                steps.append({"step": "subnet", "name": req.subnet, "status": "created",
                               "cidr": "10.0.0.0/24", "self_link": subnet_self_link})
            except Exception as create_err:
                steps.append({"step": "subnet", "name": req.subnet, "status": "error",
                               "error": str(create_err)})
                overall_ok = False
        else:
            steps.append({"step": "subnet", "name": req.subnet, "status": "error",
                           "error": str(e) if not vpc_self_link else "VPC not available"})
            overall_ok = False

    # ── Step 3: Firewall – allow internal traffic ─────────────────────────
    try:
        fw = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: compute.firewalls().get(
                project=req.project, firewall=firewall_name
            ).execute(),
        )
        steps.append({"step": "firewall_internal", "name": firewall_name, "status": "exists"})
    except Exception as e:
        if ("404" in str(e) or "notFound" in str(e)) and vpc_self_link:
            try:
                fw_body = {
                    "name": firewall_name,
                    "network": vpc_self_link,
                    "direction": "INGRESS",
                    "priority": 1000,
                    "sourceRanges": ["10.0.0.0/8"],
                    "allowed": [{"IPProtocol": "all"}],
                    "description": "Allow internal traffic within tpcdi-benchmark-vpc",
                }
                op = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: compute.firewalls().insert(
                        project=req.project, body=fw_body
                    ).execute(),
                )
                await _wait_global_op(op["name"])
                steps.append({"step": "firewall_internal", "name": firewall_name,
                               "status": "created"})
            except Exception as create_err:
                steps.append({"step": "firewall_internal", "name": firewall_name,
                               "status": "error", "error": str(create_err)})
                overall_ok = False
        else:
            steps.append({"step": "firewall_internal", "name": firewall_name, "status": "error",
                           "error": str(e) if vpc_self_link else "VPC not available"})
            overall_ok = False

    # ── Step 4: Cloud Router + NAT ────────────────────────────────────────
    if not vpc_self_link:
        steps.append({"step": "cloud_nat", "name": nat_name, "status": "skipped",
                       "reason": "VPC not available"})
    else:
        nat_status = None
        nat_error = None

        # Check if router already exists
        router_exists = False
        try:
            router = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: compute.routers().get(
                    project=req.project, region=req.region, router=router_name
                ).execute(),
            )
            router_exists = True
            # Check if NAT config is already on the router
            existing_nats = router.get("nats", [])
            nat_names = [n["name"] for n in existing_nats]
            if nat_name in nat_names:
                nat_status = "exists"
            else:
                # Patch NAT onto existing router
                try:
                    existing_nats.append({
                        "name": nat_name,
                        "natIpAllocateOption": "AUTO_ONLY",
                        "sourceSubnetworkIpRangesToNat": "ALL_SUBNETWORKS_ALL_IP_RANGES",
                    })
                    op = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: compute.routers().patch(
                            project=req.project, region=req.region, router=router_name,
                            body={**router, "nats": existing_nats},
                        ).execute(),
                    )
                    await _wait_region_op(op["name"])
                    nat_status = "created"
                except Exception as patch_err:
                    nat_status = "error"
                    nat_error = str(patch_err)
                    overall_ok = False
        except Exception as router_err:
            if "404" not in str(router_err) and "notFound" not in str(router_err):
                nat_status = "error"
                nat_error = str(router_err)
                overall_ok = False

        if nat_status is None:
            # Router does not exist – create it with NAT embedded
            try:
                router_body = {
                    "name": router_name,
                    "network": vpc_self_link,
                    "region": req.region,
                    "nats": [{
                        "name": nat_name,
                        "natIpAllocateOption": "AUTO_ONLY",
                        "sourceSubnetworkIpRangesToNat": "ALL_SUBNETWORKS_ALL_IP_RANGES",
                    }],
                }
                op = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: compute.routers().insert(
                        project=req.project, region=req.region, body=router_body
                    ).execute(),
                )
                await _wait_region_op(op["name"])
                nat_status = "created"
            except Exception as create_err:
                nat_status = "error"
                nat_error = str(create_err)
                overall_ok = False

        step_entry = {"step": "cloud_nat", "name": nat_name, "status": nat_status,
                      "router": router_name}
        if nat_error:
            step_entry["error"] = nat_error
        steps.append(step_entry)

    # ── Step 5: GCS bucket ────────────────────────────────────────────────
    try:
        bkt = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: storage.buckets().get(bucket=req.gcs_bucket).execute(),
        )
        steps.append({"step": "gcs_bucket", "name": f"gs://{req.gcs_bucket}",
                       "status": "exists", "location": bkt.get("location")})
    except Exception as e:
        if "404" in str(e) or "notFound" in str(e):
            try:
                bkt_body = {
                    "name": req.gcs_bucket,
                    "location": req.region.upper(),
                    "storageClass": "STANDARD",
                }
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: storage.buckets().insert(
                        project=req.project, body=bkt_body
                    ).execute(),
                )
                steps.append({"step": "gcs_bucket", "name": f"gs://{req.gcs_bucket}",
                               "status": "created"})
            except Exception as create_err:
                steps.append({"step": "gcs_bucket", "name": f"gs://{req.gcs_bucket}",
                               "status": "error", "error": str(create_err)})
                overall_ok = False
        else:
            steps.append({"step": "gcs_bucket", "name": f"gs://{req.gcs_bucket}",
                           "status": "error", "error": str(e)})
            overall_ok = False

    # ── Step 6 (optional): Dataproc Metastore ─────────────────────────────
    if req.metastore_mode == "create":
        metastore_service_id = req.metastore_name
        metastore_full_name = f"projects/{req.project}/locations/{req.region}/services/{metastore_service_id}"
        try:
            # Check if metastore already exists
            import requests as http_requests
            token = creds.token
            if not token:
                creds.refresh(google.auth.transport.requests.Request())
                token = creds.token
            ms_url = f"https://metastore.googleapis.com/v1/{metastore_full_name}"
            ms_resp = http_requests.get(ms_url, headers={"Authorization": f"Bearer {token}"})
            if ms_resp.status_code == 200:
                ms_data = ms_resp.json()
                ms_state = ms_data.get("state", "UNKNOWN")
                ms_network = ms_data.get("network", "")
                # Check if metastore is on the same VPC
                expected_vpc = f"projects/{req.project}/global/networks/{vpc_name}"
                if ms_network and expected_vpc not in ms_network:
                    steps.append({"step": "metastore", "name": metastore_service_id,
                                   "status": "warning",
                                   "error": f"Metastore exists but on different network ({ms_network}). Expected {expected_vpc}. Cluster creation may fail."})
                else:
                    steps.append({"step": "metastore", "name": metastore_service_id,
                                   "status": "exists", "state": ms_state,
                                   "full_name": metastore_full_name})
            elif ms_resp.status_code == 404:
                # Create the metastore
                if not vpc_self_link:
                    steps.append({"step": "metastore", "name": metastore_service_id,
                                   "status": "error", "error": "Cannot create metastore: VPC not available"})
                    overall_ok = False
                else:
                    create_url = f"https://metastore.googleapis.com/v1/projects/{req.project}/locations/{req.region}/services?serviceId={metastore_service_id}"
                    create_body = {
                        "hiveMetastoreConfig": {
                            "version": "3.1.2",
                        },
                        "network": f"projects/{req.project}/global/networks/{vpc_name}",
                        "port": 9083,
                        "tier": "DEVELOPER",
                    }
                    create_resp = http_requests.post(
                        create_url,
                        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                        json=create_body,
                    )
                    if create_resp.status_code in (200, 201):
                        op_data = create_resp.json()
                        op_name = op_data.get("name", "")
                        # Poll the operation (metastore creation can take several minutes)
                        steps.append({"step": "metastore", "name": metastore_service_id,
                                       "status": "creating",
                                       "full_name": metastore_full_name,
                                       "operation": op_name,
                                       "message": "Metastore creation started (takes 5-10 min). It will be ready before cluster needs it."})
                        # Poll for up to 10 minutes
                        for _poll in range(60):
                            await asyncio.sleep(10)
                            poll_resp = http_requests.get(
                                f"https://metastore.googleapis.com/v1/{op_name}",
                                headers={"Authorization": f"Bearer {token}"},
                            )
                            if poll_resp.status_code == 200:
                                poll_data = poll_resp.json()
                                if poll_data.get("done"):
                                    if "error" in poll_data:
                                        err_msg = poll_data["error"].get("message", str(poll_data["error"]))
                                        steps[-1] = {"step": "metastore", "name": metastore_service_id,
                                                      "status": "error", "error": err_msg}
                                        overall_ok = False
                                    else:
                                        steps[-1] = {"step": "metastore", "name": metastore_service_id,
                                                      "status": "created", "full_name": metastore_full_name}
                                    break
                        else:
                            # Timed out waiting
                            steps[-1] = {"step": "metastore", "name": metastore_service_id,
                                          "status": "creating_timeout",
                                          "full_name": metastore_full_name,
                                          "message": "Metastore still creating after 10 min. Check GCP console."}
                            overall_ok = False
                    else:
                        steps.append({"step": "metastore", "name": metastore_service_id,
                                       "status": "error",
                                       "error": f"Create failed ({create_resp.status_code}): {create_resp.text[:300]}"})
                        overall_ok = False
            else:
                steps.append({"step": "metastore", "name": metastore_service_id,
                               "status": "error",
                               "error": f"Check failed ({ms_resp.status_code}): {ms_resp.text[:300]}"})
                overall_ok = False
        except Exception as e:
            steps.append({"step": "metastore", "name": metastore_service_id,
                           "status": "error", "error": str(e)[:500]})
            overall_ok = False
    elif req.metastore_mode == "existing":
        # Validate the existing metastore is reachable
        metastore_path = req.metastore_path
        if not metastore_path:
            steps.append({"step": "metastore", "name": "Existing Metastore",
                           "status": "error", "error": "No metastore path provided"})
            overall_ok = False
        else:
            try:
                import requests as http_requests
                token = creds.token
                if not token:
                    creds.refresh(google.auth.transport.requests.Request())
                    token = creds.token
                ms_url = f"https://metastore.googleapis.com/v1/{metastore_path}"
                ms_resp = http_requests.get(ms_url, headers={"Authorization": f"Bearer {token}"})
                if ms_resp.status_code == 200:
                    ms_data = ms_resp.json()
                    ms_state = ms_data.get("state", "UNKNOWN")
                    steps.append({"step": "metastore", "name": metastore_path.split("/")[-1],
                                   "status": "exists", "state": ms_state, "full_name": metastore_path})
                else:
                    steps.append({"step": "metastore", "name": metastore_path.split("/")[-1],
                                   "status": "error",
                                   "error": f"Metastore not found or inaccessible ({ms_resp.status_code}): {ms_resp.text[:200]}"})
                    overall_ok = False
            except Exception as e:
                steps.append({"step": "metastore", "name": metastore_path.split("/")[-1],
                               "status": "error", "error": str(e)[:500]})
                overall_ok = False
    elif req.metastore_mode == "spark_default":
        steps.append({"step": "metastore", "name": "Spark Default",
                       "status": "skipped", "message": "Using Spark's built-in Derby metastore (no external Hive metastore)"})

    return {
        "status": "ready" if overall_ok else "incomplete",
        "steps": steps,
    }


# ── Full Cycle Run ───────────────────────────────────────────────────────

@router.post("/dataproc/run-full-cycle")
async def run_full_cycle(req: DataprocFullRunRequest):
    """Start full Dataproc cycle: create cluster → submit job → wait → delete cluster.
    Returns run_id for polling.
    """
    # Auto-refresh: if a refresh token is provided, re-authenticate to ensure fresh creds
    if req.gcp_refresh_token:
        auth_result = set_refresh_token(req.gcp_refresh_token)
        if auth_result.get("status") != "authenticated":
            raise HTTPException(status_code=401, detail=f"Failed to refresh GCP token: {auth_result.get('message', 'Unknown error')}")

    # Verify auth
    creds = get_credentials()
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated with Google Cloud. Please sign in first.")

    run_id = _next_run_id()
    is_serverless = req.compute_mode == "serverless"
    _active_runs[run_id] = {
        "run_id": run_id,
        "phase": "submitting_batch" if is_serverless else "creating_cluster",
        "state": "RUNNING",
        "cluster_name": "serverless" if is_serverless else req.cluster_name,
        "compute_mode": req.compute_mode,
        "job_id": None,
        "error": None,
        "start_time_ms": int(time.time() * 1000),
        "cluster_created_ms": None,
        "job_submitted_ms": None,
        "job_done_ms": None,
        "cluster_deleted_ms": None,
        "cluster_delete_error": None,
        "duration_ms": None,
        "cost": None,
        "config": {
            "master_type": "serverless" if is_serverless else req.master_type,
            "worker_type": "serverless" if is_serverless else req.worker_type,
            "num_workers": 0 if is_serverless else req.num_workers,
            "ssd_count": 0 if is_serverless else req.ssd_count,
            "load_type": req.load_type, "batch_id": req.batch_id,
            "scale_factor": req.scale_factor,
        },
    }

    if is_serverless:
        asyncio.create_task(_run_serverless_batch_task(run_id, req))
    else:
        asyncio.create_task(_run_full_cycle_task(run_id, req))
    return {"status": "started", "run_id": run_id}


async def _run_serverless_batch_task(run_id: str, req: DataprocFullRunRequest):
    """Background: submit Dataproc Serverless batch job (no cluster lifecycle)."""
    import uuid
    from google.cloud import dataproc_v1

    run = _active_runs[run_id]
    endpoint = f"{req.region}-dataproc.googleapis.com:443"

    def _fresh_batch_client():
        creds = get_credentials()
        if not creds:
            raise RuntimeError("Google credentials expired or missing")
        return dataproc_v1.BatchControllerClient(
            credentials=creds,
            client_options={"api_endpoint": endpoint},
        )

    try:
        creds = get_credentials()
        if not creds:
            run["state"] = "ERROR"
            run["error"] = "Google credentials expired"
            run["phase"] = "failed"
            return

        batch_client = _fresh_batch_client()

        # No cluster creation phase — go straight to job submission
        run["phase"] = "submitting_batch"

        # Build the same script args as classic
        script_args = [
            "--load-type", req.load_type,
            "--scale-factor", str(req.scale_factor),
            "--format", "delta",
            f"--gcs-bucket={req.gcs_bucket}",
            f"--project-id={req.project}",
            f"--region={req.region}",
            f"--raw-data-path={req.raw_data_path}",
            f"--target-database={req.target_database}",
            "--target-schema=dw",
            "--save-metrics",
            f"--metrics-output={req.metrics_output}",
            "--spark-master=yarn",
            "--log-detailed-stats",
        ]
        if req.batch_id:
            script_args.extend(["--batch-id", req.batch_id])

        batch = dataproc_v1.Batch(
            pyspark_batch=dataproc_v1.PySparkBatch(
                main_python_file_uri=f"gs://{req.gcs_bucket}/tpcdi/dataproc/run_benchmark_dataproc.py",
                python_file_uris=[f"gs://{req.gcs_bucket}/tpcdi/dataproc/benchmark.zip"],
                jar_file_uris=[f"gs://{req.gcs_bucket}/tpcdi/libs/spark-xml_2.12-0.18.0.jar"],
                args=script_args,
            ),
            runtime_config=dataproc_v1.RuntimeConfig(
                version="2.3",
            ),
            environment_config=dataproc_v1.EnvironmentConfig(
                execution_config=dataproc_v1.ExecutionConfig(
                    subnetwork_uri=f"projects/{req.project}/regions/{req.region}/subnetworks/{req.subnet}",
                ),
            ),
        )

        # Attach metastore if configured
        if req.metastore:
            batch.environment_config.peripherals_config = dataproc_v1.PeripheralsConfig(
                metastore_service=req.metastore,
            )

        # Generate a unique batch ID
        batch_id = f"tpcdi-{req.load_type}-sf{req.scale_factor}-{uuid.uuid4().hex[:8]}"

        run["phase"] = "running_job"
        run["job_submitted_ms"] = int(time.time() * 1000)
        # For serverless, cluster_created_ms = job_submitted_ms (no separate cluster phase)
        run["cluster_created_ms"] = run["job_submitted_ms"]

        operation = batch_client.create_batch(
            request={
                "parent": f"projects/{req.project}/locations/{req.region}",
                "batch": batch,
                "batch_id": batch_id,
            }
        )

        run["job_id"] = batch_id
        run["cluster_name"] = f"serverless-{batch_id}"

        # Poll for completion
        _last_client_refresh = time.time()
        _REFRESH_INTERVAL = 30 * 60

        while True:
            if run_id in _cancelled_runs:
                try:
                    batch_client = _fresh_batch_client()
                except Exception:
                    pass
                run["state"] = "CANCELLED"
                run["phase"] = "cancelled"
                return

            await asyncio.sleep(15)

            # Refresh client periodically
            if time.time() - _last_client_refresh > _REFRESH_INTERVAL:
                try:
                    batch_client = _fresh_batch_client()
                    _last_client_refresh = time.time()
                except Exception as e:
                    print(f"[dataproc-serverless] WARNING: token refresh failed: {e}")

            try:
                batch_status = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: batch_client.get_batch(
                        name=f"projects/{req.project}/locations/{req.region}/batches/{batch_id}"
                    ),
                )

                state_name = batch_status.state.name
                if state_name in ("SUCCEEDED",):
                    break
                elif state_name in ("FAILED", "CANCELLED"):
                    run["state"] = "ERROR"
                    run["error"] = batch_status.state_message or f"Batch {state_name}"
                    run["phase"] = "failed"
                    run["job_done_ms"] = int(time.time() * 1000)
                    run["cluster_deleted_ms"] = run["job_done_ms"]
                    total_ms = run["job_done_ms"] - run["start_time_ms"]
                    run["duration_ms"] = total_ms
                    return
            except Exception as poll_err:
                print(f"[dataproc-serverless] Poll error: {poll_err}")

        run["job_done_ms"] = int(time.time() * 1000)
        # For serverless, no cluster deletion phase
        run["cluster_deleted_ms"] = run["job_done_ms"]

        total_ms = run["cluster_deleted_ms"] - run["start_time_ms"]
        run["duration_ms"] = total_ms
        run["state"] = "DONE"
        run["phase"] = "completed"

        # Cost estimation for serverless (DCU-based)
        hours = total_ms / 3_600_000
        # Dataproc Serverless pricing: ~$0.065/DCU-hour, estimate ~4 DCUs for typical workload
        estimated_dcus = 4
        dcu_cost = estimated_dcus * 0.065 * hours
        run["cost"] = {
            "vm_cost_usd": 0,
            "dataproc_premium_usd": round(dcu_cost, 4),
            "ssd_cost_usd": 0,
            "total_cost_usd": round(dcu_cost, 4),
            "duration_hours": round(hours, 4),
            "duration_ms": total_ms,
            "cluster": {
                "master_type": "serverless",
                "worker_type": "serverless",
                "num_workers": 0,
                "ssd_count": 0,
            },
        }

    except Exception as e:
        run["state"] = "ERROR"
        run["error"] = str(e)[:500]
        run["phase"] = "failed"
        if not run.get("job_done_ms"):
            run["job_done_ms"] = int(time.time() * 1000)
        if not run.get("cluster_deleted_ms"):
            run["cluster_deleted_ms"] = int(time.time() * 1000)
        if run.get("start_time_ms"):
            run["duration_ms"] = run["cluster_deleted_ms"] - run["start_time_ms"]


async def _run_full_cycle_task(run_id: str, req: DataprocFullRunRequest):
    """Background: create cluster → submit job → poll → delete cluster."""
    from google.cloud import dataproc_v1
    from google.protobuf.duration_pb2 import Duration

    run = _active_runs[run_id]

    # Helper: get fresh clients with refreshed credentials before each phase.
    # gRPC clients cache the auth token internally and do NOT call refresh()
    # on the credentials object, so long-running tasks (>60 min) will hit 401
    # unless we re-create clients with a freshly-refreshed token.
    endpoint = f"{req.region}-dataproc.googleapis.com:443"

    def _fresh_cluster_client():
        creds = get_credentials()
        if not creds:
            raise RuntimeError("Google credentials expired or missing")
        return dataproc_v1.ClusterControllerClient(
            credentials=creds,
            client_options={"api_endpoint": endpoint},
        )

    def _fresh_job_client():
        creds = get_credentials()
        if not creds:
            raise RuntimeError("Google credentials expired or missing")
        return dataproc_v1.JobControllerClient(
            credentials=creds,
            client_options={"api_endpoint": endpoint},
        )

    try:
        creds = get_credentials()
        if not creds:
            run["state"] = "ERROR"
            run["error"] = "Google credentials expired"
            run["phase"] = "failed"
            return

        cluster_client = _fresh_cluster_client()
        job_client = _fresh_job_client()

        # ── Phase 1: Create Cluster ─────────────────────────────────
        run["phase"] = "creating_cluster"

        disk_config_master = dataproc_v1.DiskConfig(
            boot_disk_size_gb=100,
            num_local_ssds=4,
        )
        disk_config_worker = dataproc_v1.DiskConfig(
            boot_disk_size_gb=200,
            num_local_ssds=req.ssd_count,
        )

        cluster_config = {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": req.master_type,
                "disk_config": disk_config_master,
            },
            "worker_config": {
                "num_instances": req.num_workers,
                "machine_type_uri": req.worker_type,
                "disk_config": disk_config_worker,
            },
            "gce_cluster_config": {
                "zone_uri": f"projects/{req.project}/zones/{req.zone}",
                "subnetwork_uri": f"projects/{req.project}/regions/{req.region}/subnetworks/{req.subnet}",
                "internal_ip_only": True,
                "service_account_scopes": [
                    "https://www.googleapis.com/auth/cloud-platform",
                ],
            },
            "software_config": {
                "image_version": req.image_version,
                "optional_components": [dataproc_v1.Component.DELTA],
            },
            "endpoint_config": {
                "enable_http_port_access": True,
            },
        }

        if req.metastore:
            cluster_config["metastore_config"] = {
                "dataproc_metastore_service": req.metastore,
            }

        cluster = {
            "project_id": req.project,
            "cluster_name": req.cluster_name,
            "config": cluster_config,
        }

        operation = cluster_client.create_cluster(
            request={"project_id": req.project, "region": req.region, "cluster": cluster}
        )

        # Wait for cluster creation (blocking in thread pool)
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: operation.result(timeout=600)
        )

        run["cluster_created_ms"] = int(time.time() * 1000)

        if run_id in _cancelled_runs:
            run["state"] = "CANCELLED"
            run["phase"] = "cancelled"
            await _cleanup_cluster_api(cluster_client, req, run)
            return

        # ── Phase 2: Submit Job ──────────────────────────────────────
        run["phase"] = "running_job"
        run["job_submitted_ms"] = int(time.time() * 1000)

        # Refresh clients — cluster creation can take 5+ minutes
        job_client = _fresh_job_client()

        # Build PySpark job
        script_args = [
            "--load-type", req.load_type,
            "--scale-factor", str(req.scale_factor),
            "--format", "delta",
            f"--gcs-bucket={req.gcs_bucket}",
            f"--project-id={req.project}",
            f"--region={req.region}",
            f"--raw-data-path={req.raw_data_path}",
            f"--target-database={req.target_database}",
            "--target-schema=dw",
            "--save-metrics",
            f"--metrics-output={req.metrics_output}",
            "--no-spark-master",
            "--log-detailed-stats",
        ]
        if req.batch_id:
            script_args.extend(["--batch-id", req.batch_id])

        pyspark_job = {
            "main_python_file_uri": f"gs://{req.gcs_bucket}/tpcdi/dataproc/run_benchmark_dataproc.py",
            "python_file_uris": [f"gs://{req.gcs_bucket}/tpcdi/dataproc/benchmark.zip"],
            "jar_file_uris": [f"gs://{req.gcs_bucket}/tpcdi/libs/spark-xml_2.12-0.18.0.jar"],
            "args": script_args,
        }

        job = {
            "placement": {"cluster_name": req.cluster_name},
            "pyspark_job": pyspark_job,
        }

        job_response = job_client.submit_job(
            request={"project_id": req.project, "region": req.region, "job": job}
        )
        dp_job_id = job_response.reference.job_id
        run["job_id"] = dp_job_id

        # Poll job until terminal
        # Refresh clients every 30 min to avoid token expiry (tokens last ~60 min)
        _last_client_refresh = time.time()
        _REFRESH_INTERVAL = 30 * 60  # 30 minutes

        while True:
            if run_id in _cancelled_runs:
                try:
                    job_client = _fresh_job_client()
                    job_client.cancel_job(
                        request={"project_id": req.project, "region": req.region, "job_id": dp_job_id}
                    )
                except Exception:
                    pass
                run["state"] = "CANCELLED"
                run["phase"] = "cancelled"
                cluster_client = _fresh_cluster_client()
                await _cleanup_cluster_api(cluster_client, req, run)
                return

            await asyncio.sleep(15)

            # Refresh gRPC clients periodically to get a fresh OAuth token
            if time.time() - _last_client_refresh > _REFRESH_INTERVAL:
                try:
                    job_client = _fresh_job_client()
                    cluster_client = _fresh_cluster_client()
                    _last_client_refresh = time.time()
                except Exception as refresh_err:
                    print(f"[dataproc] WARNING: token refresh failed during poll: {refresh_err}")

            job_status = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: job_client.get_job(
                    request={"project_id": req.project, "region": req.region, "job_id": dp_job_id}
                ),
            )

            state = job_status.status.state.name
            if state in ("DONE", "ERROR", "CANCELLED"):
                break

        run["job_done_ms"] = int(time.time() * 1000)

        if state != "DONE":
            error_detail = job_status.status.details if job_status.status.details else state
            run["state"] = "ERROR"
            run["error"] = f"Job {state}: {error_detail}"
            run["phase"] = "deleting_cluster"
            cluster_client = _fresh_cluster_client()
            await _cleanup_cluster_api(cluster_client, req, run)
            run["phase"] = "failed"
            return

        # ── Phase 3: Delete Cluster ──────────────────────────────────
        run["phase"] = "deleting_cluster"
        cluster_client = _fresh_cluster_client()
        await _cleanup_cluster_api(cluster_client, req, run)
        # cluster_deleted_ms is set by the helper on success; fall back to now
        # if deletion failed so that duration and cost are still calculated.
        if not run.get("cluster_deleted_ms"):
            run["cluster_deleted_ms"] = int(time.time() * 1000)

        total_ms = run["cluster_deleted_ms"] - run["start_time_ms"]
        run["duration_ms"] = total_ms
        run["cost"] = calculate_dataproc_cost(
            master_type=req.master_type, worker_type=req.worker_type,
            num_workers=req.num_workers, duration_ms=total_ms, ssd_count=req.ssd_count,
        )
        # Report DONE even if cluster deletion failed – the job itself succeeded.
        run["state"] = "DONE"
        run["phase"] = "completed"

    except Exception as e:
        traceback.print_exc()
        run["state"] = "ERROR"
        run["error"] = str(e)
        run["phase"] = "failed"
        try:
            creds = get_credentials()
            if creds:
                from google.cloud import dataproc_v1
                endpoint = f"{req.region}-dataproc.googleapis.com:443"
                cc = dataproc_v1.ClusterControllerClient(
                    credentials=creds, client_options={"api_endpoint": endpoint}
                )
                await _cleanup_cluster_api(cc, req, run)
        except Exception:
            pass


async def _cleanup_cluster_api(cluster_client, req, run: dict | None = None):
    """Delete cluster and optionally record the outcome in *run*.

    On success sets run["cluster_deleted_ms"] (if run provided).
    On failure sets run["cluster_delete_error"] but does NOT raise, so the
    caller's overall status is unaffected.
    """
    try:
        op = cluster_client.delete_cluster(
            request={"project_id": req.project, "region": req.region, "cluster_name": req.cluster_name}
        )
        await asyncio.get_event_loop().run_in_executor(None, lambda: op.result(timeout=300))
        if run is not None:
            run["cluster_deleted_ms"] = int(time.time() * 1000)
    except Exception as e:
        err_msg = str(e)
        print(f"[dataproc] WARNING: cluster deletion failed for {req.cluster_name}: {err_msg}")
        if run is not None:
            run["cluster_delete_error"] = err_msg


@router.get("/dataproc/active-runs")
async def get_active_runs():
    """Return all runs that are still in RUNNING state (for UI reconnection after page reload)."""
    return [
        run for run in _active_runs.values()
        if run.get("state") == "RUNNING"
    ]


@router.get("/dataproc/run-status/{run_id}")
async def get_run_status(run_id: str):
    if run_id not in _active_runs:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return _active_runs[run_id]


@router.post("/dataproc/cancel-run/{run_id}")
async def cancel_dataproc_run(run_id: str):
    if run_id not in _active_runs:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    _cancelled_runs.add(run_id)
    return {"status": "cancel_requested", "run_id": run_id}


class DeleteClusterRequest(BaseModel):
    project: str
    region: str
    cluster_name: str


@router.post("/dataproc/delete-cluster")
async def delete_cluster(req: DeleteClusterRequest):
    """Delete a Dataproc cluster by name."""
    cluster_client = _get_dataproc_client()
    try:
        op = cluster_client.delete_cluster(
            request={"project_id": req.project, "region": req.region, "cluster_name": req.cluster_name}
        )
        await asyncio.get_event_loop().run_in_executor(None, lambda: op.result(timeout=300))
        return {"status": "deleted", "cluster_name": req.cluster_name}
    except Exception as e:
        err = str(e)
        if "404" in err or "NotFound" in err or "not found" in err.lower():
            return {"status": "not_found", "cluster_name": req.cluster_name, "message": "Cluster does not exist"}
        raise HTTPException(status_code=500, detail=f"Failed to delete cluster {req.cluster_name}: {err}")


@router.get("/dataproc/run-cost")
async def get_dataproc_run_cost(
    master_type: str = Query("n2d-standard-8"),
    worker_type: str = Query("n2d-standard-8"),
    num_workers: int = Query(3),
    duration_ms: int = Query(0),
    ssd_count: int = Query(2),
):
    return calculate_dataproc_cost(master_type, worker_type, num_workers, duration_ms, ssd_count)


@router.get("/dataproc/health")
async def dataproc_health():
    return {"status": "healthy"}
