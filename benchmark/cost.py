"""
Cost estimation for TPC-DI benchmark runs on Databricks and Dataproc.

Uses job total duration and cluster metadata (worker count, instance type) to produce
approximate cost breakdown (compute + software/DBU) and total. Rates are list-price
approximations and may not match your contract; use for comparison only.
"""

from typing import Any, Dict, Optional

# --- Databricks: DBU list price per cloud (USD per DBU, approximate list price)
# Jobs compute; serverless and classic use same SKU family in many regions.
# Source: Databricks pricing pages; override via environment or config if needed.
DATABRICKS_DBU_PRICE_PER_DBU: Dict[str, float] = {
    "AWS": 0.15,
    "Azure": 0.14,
    "GCP": 0.15,
}

# DBU consumption per node-hour for Jobs compute (approximate; varies by instance type)
DATABRICKS_DBU_PER_NODE_HOUR = 4.0

# Cloud instance type list price $/hour (on-demand, approximate; for compute cost)
# Source: AWS/GCP/Azure pricing pages; use for comparison only.
DATABRICKS_INSTANCE_USD_PER_HOUR: Dict[str, Dict[str, float]] = {
    "AWS": {
        "i3.xlarge": 0.312,
        "i3.2xlarge": 0.624,
        "i3.4xlarge": 1.248,
        "m5d.xlarge": 0.272,
        "m5d.2xlarge": 0.544,
        "m5d.4xlarge": 1.088,
        "r5d.xlarge": 0.288,
        "r5d.2xlarge": 0.576,
        "r5d.4xlarge": 1.152,
    },
    "GCP": {
        "n2d-standard-4": 0.193,
        "n2d-standard-8": 0.386,
        "n2d-standard-16": 0.77,
        "n2d-standard-32": 1.54,
        "n2d-standard-48": 2.31,
        "n2d-standard-64": 3.08,
        "n2d-standard-80": 3.85,
        "n2d-standard-96": 4.62,
        "n2d-highmem-4": 0.242,
        "n2d-highmem-8": 0.484,
        "n2d-highmem-16": 0.968,
        "n2d-highmem-32": 1.936,
        "c2-standard-4": 0.208,
        "c2-standard-8": 0.416,
        "c2-standard-16": 0.832,
        "c2-standard-30": 1.56,
    },
    "AZURE": {
        "Standard_E4s_v3": 0.20,
        "Standard_E8s_v3": 0.40,
        "Standard_E16s_v3": 0.80,
        "Standard_E32s_v3": 1.60,
    },
}
# Default $/hour when instance type unknown (e.g. 1 small node equivalent)
DATABRICKS_DEFAULT_USD_PER_NODE_HOUR = 0.50

# --- Dataproc: GCP list prices (USD)
# Dataproc fee: $0.01 per vCPU per hour (in addition to Compute Engine)
DATAPROC_FEE_PER_VCPU_HOUR = 0.01
# Compute: flat VM price per node-hour for a quick estimate (e.g. n2d-standard-16 ~$0.67/hr in us-central1)
DATAPROC_FLAT_VM_USD_PER_HOUR = 0.67
# vCPU/GB per instance (used for software fee and optional detailed compute; kept for compatibility)
DATAPROC_VCPU_HOUR: Dict[str, float] = {
    "n2d-standard-4": 4,
    "n2d-standard-8": 8,
    "n2d-standard-16": 16,
    "n2d-standard-32": 32,
    "n2d-standard-48": 48,
    "n2d-standard-64": 64,
    "n2d-highmem-4": 4,
    "n2d-highmem-8": 8,
    "n2d-highmem-16": 16,
    "n2d-highmem-32": 32,
    "n4-standard-4": 4,
    "n4-standard-8": 8,
    "n4-standard-16": 16,
    "n4-standard-32": 32,
    "e2-standard-4": 4,
    "e2-standard-8": 8,
    "e2-standard-16": 16,
}
DATAPROC_GB_PER_INSTANCE: Dict[str, float] = {
    "n2d-standard-4": 16,
    "n2d-standard-8": 32,
    "n2d-standard-16": 64,
    "n2d-standard-32": 128,
    "n2d-standard-48": 192,
    "n2d-standard-64": 256,
    "n2d-highmem-4": 32,
    "n2d-highmem-8": 64,
    "n2d-highmem-16": 128,
    "n2d-highmem-32": 256,
    "n4-standard-4": 16,
    "n4-standard-8": 32,
    "n4-standard-16": 64,
    "n4-standard-32": 128,
    "e2-standard-4": 16,
    "e2-standard-8": 32,
    "e2-standard-16": 64,
}
# Approximate $/vCPU-hour and $/GB-hour for n2d in us-central1 (Compute Engine only)
DATAPROC_USD_PER_VCPU_HOUR = 0.033
DATAPROC_USD_PER_GB_HOUR = 0.004
# Default when instance type unknown: assume 16 vCPU, 64 GB per node
DATAPROC_DEFAULT_VCPU_PER_NODE = 16
DATAPROC_DEFAULT_GB_PER_NODE = 64


def _hours(seconds: Optional[float]) -> float:
    if seconds is None or seconds <= 0:
        return 0.0
    return seconds / 3600.0


def _databricks_instance_usd_per_hour(cloud: str, instance_type: Optional[str]) -> float:
    """Return approximate $/hour for instance type in cloud; else default."""
    if not instance_type:
        return DATABRICKS_DEFAULT_USD_PER_NODE_HOUR
    cloud_key = (cloud or "AWS").upper()
    prices = DATABRICKS_INSTANCE_USD_PER_HOUR.get(cloud_key, {})
    return prices.get((instance_type or "").strip(), DATABRICKS_DEFAULT_USD_PER_NODE_HOUR)


def estimate_databricks_cost(
    total_duration_seconds: Optional[float],
    cluster_worker_count: Optional[int],
    cluster_instance_type: Optional[str],
    cluster_master_type: Optional[str],
    databricks_compute_type: Optional[str],
    cloud: str,
) -> Dict[str, Any]:
    """
    Estimate Databricks job cost from duration and cluster metadata.
    Compute cost = cloud instance-type $/hour × duration (driver + workers).
    DBU cost = DBU consumption × $/DBU. Total = compute + DBU.
    Cloud must be one of: AWS, Azure, GCP.
    Returns dict with keys: compute_usd, dbu_usd, total_usd, dbu_consumed, duration_hours.
    """
    duration_hours = _hours(total_duration_seconds)
    cloud_key = (cloud or "AWS").upper()
    if cloud_key not in ("AWS", "AZURE", "GCP"):
        cloud_key = "AWS"
    price_per_dbu = DATABRICKS_DBU_PRICE_PER_DBU.get(cloud_key, DATABRICKS_DBU_PRICE_PER_DBU["AWS"])
    # Compute cost: driver + workers from cloud instance-type pricing
    driver_price = _databricks_instance_usd_per_hour(cloud_key, cluster_master_type or cluster_instance_type)
    worker_price = _databricks_instance_usd_per_hour(cloud_key, cluster_instance_type)
    workers = cluster_worker_count if cluster_worker_count is not None and cluster_worker_count >= 0 else 0
    if (databricks_compute_type or "").lower() == "serverless" and cluster_worker_count is None:
        workers = 0
    compute_usd = round(duration_hours * (driver_price + workers * worker_price), 4)
    # DBU cost
    num_nodes = 1 + workers
    dbu_per_hour = num_nodes * DATABRICKS_DBU_PER_NODE_HOUR
    dbu_consumed = duration_hours * dbu_per_hour
    dbu_usd = round(dbu_consumed * price_per_dbu, 4)
    total_usd = round(compute_usd + dbu_usd, 4)
    return {
        "compute_usd": compute_usd,
        "dbu_usd": dbu_usd,
        "software_usd": dbu_usd,
        "total_usd": total_usd,
        "dbu_consumed": round(dbu_consumed, 2),
        "duration_hours": round(duration_hours, 4),
        "cloud": cloud_key,
    }


def estimate_dataproc_cost(
    total_duration_seconds: Optional[float],
    cluster_worker_count: Optional[int],
    cluster_instance_type: Optional[str],
    cluster_master_type: Optional[str],
) -> Dict[str, Any]:
    """
    Estimate Dataproc job cost from duration and cluster metadata.
    Compute = flat VM price per node-hour × (1 + workers) × duration.
    Software = Dataproc fee per vCPU-hour (from instance-type vCPU lookup).
    Returns dict with keys: compute_usd, software_usd, total_usd, duration_hours.
    """
    duration_hours = _hours(total_duration_seconds)
    workers = cluster_worker_count if cluster_worker_count is not None and cluster_worker_count >= 0 else 0
    num_nodes = 1 + workers
    # Compute: flat VM price per node-hour (quick estimate)
    compute_usd = round(duration_hours * num_nodes * DATAPROC_FLAT_VM_USD_PER_HOUR, 4)
    # Software: Dataproc fee per vCPU-hour (still uses instance-type for vCPU count)
    driver_vcpu = DATAPROC_VCPU_HOUR.get(
        (cluster_master_type or cluster_instance_type or "").strip(),
        DATAPROC_DEFAULT_VCPU_PER_NODE,
    )
    worker_vcpu = DATAPROC_VCPU_HOUR.get(
        (cluster_instance_type or "").strip(),
        DATAPROC_DEFAULT_VCPU_PER_NODE,
    )
    total_vcpu_hours = duration_hours * (driver_vcpu + workers * worker_vcpu)
    software_usd = round(total_vcpu_hours * DATAPROC_FEE_PER_VCPU_HOUR, 4)
    total_usd = round(compute_usd + software_usd, 4)
    return {
        "compute_usd": compute_usd,
        "software_usd": software_usd,
        "total_usd": total_usd,
        "duration_hours": round(duration_hours, 4),
    }


def estimate_cost(
    metrics: Any,
    platform: str,
    cloud: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Estimate run cost from metrics. platform is 'databricks' or 'dataproc'.
    For Databricks, cloud should be 'AWS', 'Azure', or 'GCP'.
    Returns None if duration is missing; otherwise returns a cost dict suitable for
    metrics.cost_breakdown and metrics.total_cost_usd / metrics.dbu_cost_usd.
    """
    duration = getattr(metrics, "total_duration_seconds", None)
    if duration is None or duration <= 0:
        return None
    workers = getattr(metrics, "cluster_worker_count", None)
    instance_type = getattr(metrics, "cluster_instance_type", None)
    master_type = getattr(metrics, "cluster_master_type", None)
    if platform == "databricks":
        compute_type = getattr(metrics, "databricks_compute_type", None)
        cloud_key = (cloud or "AWS").upper()
        if cloud_key not in ("AWS", "AZURE", "GCP"):
            cloud_key = "AWS"
        out = estimate_databricks_cost(
            duration,
            workers,
            instance_type,
            master_type,
            compute_type,
            cloud_key,
        )
        out["dbu_consumed"] = out.get("dbu_consumed")
        return out
    if platform == "dataproc":
        return estimate_dataproc_cost(duration, workers, instance_type, master_type)
    return None
