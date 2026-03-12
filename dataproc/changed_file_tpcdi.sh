#!/bin/bash


# nohup ./sumit_tpcdi_benchmark.sh </dev/null > benchmark_full.log 2>&1 &

# ==============================================================================
# 1. GLOBAL CONFIGURATION
# ==============================================================================
PROJECT="gcp-sandbox-field-eng"
REGION="us-central1"
ZONE="us-central1-a"
METASTORE="projects/gcp-sandbox-field-eng/locations/us-central1/services/sumit-bnchmark-metastore"
SA_EMAIL="sumit-wmt-workspace-creator@gcp-sandbox-field-eng.iam.gserviceaccount.com"
SA_KEY="gs://sumit_prakash_gcs/service_account_key_file/service_account.json"
GCS_BUCKET="sumit_prakash_gcs"

# --- GLOBAL TARGET DATABASE ---
TARGET_DATABASE="tpcdi_warehouse_dataproc_bnchmark_final"

# Networking Configuration
VNET_NAME="sumit-tpcdi-vnet"
SUBNET_NAME="sumit-tpcdi-subnet"
SUBNET_RANGE="10.10.0.0/24"
ROUTER_NAME="sumit-tpcdi-router"
NAT_NAME="sumit-tpcdi-nat"

# Ensure we stop on any error
set -e

echo "=== [$(date)] STARTING TPC-DI ORCHESTRATION SCRIPT ==="

# ==============================================================================
# 2. NETWORKING SETUP (VNET, Subnet, Router, and NAT)
# ==============================================================================

if ! gcloud compute networks describe "${VNET_NAME}" --project="${PROJECT}" > /dev/null 2>&1; then
    echo "Creating Network: ${VNET_NAME}..."
    gcloud compute networks create "${VNET_NAME}" --project="${PROJECT}" --subnet-mode=custom --bgp-routing-mode=regional
else
    echo "Network ${VNET_NAME} already exists."
fi

if ! gcloud compute networks subnets describe "${SUBNET_NAME}" --project="${PROJECT}" --region="${REGION}" > /dev/null 2>&1; then
    echo "Creating Subnet: ${SUBNET_NAME}..."
    gcloud compute networks subnets create "${SUBNET_NAME}" --project="${PROJECT}" --network="${VNET_NAME}" --region="${REGION}" --range="${SUBNET_RANGE}" --enable-private-ip-google-access
else
    echo "Subnet ${SUBNET_NAME} already exists."
fi

if ! gcloud compute routers describe "${ROUTER_NAME}" --project="${PROJECT}" --region="${REGION}" > /dev/null 2>&1; then
    echo "Creating Cloud Router: ${ROUTER_NAME}..."
    gcloud compute routers create "${ROUTER_NAME}" --project="${PROJECT}" --network="${VNET_NAME}" --region="${REGION}"
else
    echo "Cloud Router ${ROUTER_NAME} already exists."
fi

if ! gcloud compute routers nats describe "${NAT_NAME}" --router="${ROUTER_NAME}" --project="${PROJECT}" --region="${REGION}" > /dev/null 2>&1; then
    echo "Creating Cloud NAT: ${NAT_NAME}..."
    gcloud compute routers nats create "${NAT_NAME}" --project="${PROJECT}" --router="${ROUTER_NAME}" --region="${REGION}" --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
else
    echo "Cloud NAT gateway ${NAT_NAME} already exists."
fi

# ==============================================================================
# 3. REUSABLE BENCHMARK FUNCTION
# ==============================================================================

run_tpcdi_cycle() {
    local CLUSTER_NAME=$1
    local MASTER_TYPE=$2
    local NUM_WORKERS=$3
    local WORKER_TYPE=$4
    local SSD_COUNT=$5
    local LOAD_TYPE=$6
    local SF=$7
    local BATCH_ID=$8

    echo "------------------------------------------------------------"
    echo "CYCLE START: ${CLUSTER_NAME} | Master: ${MASTER_TYPE} | SF: ${SF}"
    echo "------------------------------------------------------------"

    # A. Cluster Creation
    gcloud dataproc clusters create "$CLUSTER_NAME" \
      --project="$PROJECT" --region="$REGION" --zone="$ZONE" \
      --dataproc-metastore="$METASTORE" \
      --image-version="2.3-debian12" \
      --master-machine-type="$MASTER_TYPE" \
      --master-boot-disk-size=100 --num-master-local-ssds=4 \
      --num-workers="$NUM_WORKERS" --worker-machine-type="$WORKER_TYPE" \
      --worker-boot-disk-size=200 --num-worker-local-ssds="$SSD_COUNT" \
      --subnet="$SUBNET_NAME" --no-address \
      --optional-components=DELTA --enable-component-gateway --scopes=cloud-platform

    # B. Job Submission
    local JOB_CMD="sh run_dataproc_job.sh \
        cluster=\"$CLUSTER_NAME\" \
        region=\"$REGION\" \
        project=\"$PROJECT\" \
        load-type=\"$LOAD_TYPE\" \
        batch-id=\"$BATCH_ID\" \
        scale-factor=\"$SF\" \
        target-database=\"$TARGET_DATABASE\" \
        gcs-bucket=\"$GCS_BUCKET\" \
        raw-data-path=\"gs://${GCS_BUCKET}/tpcdi\" \
        metrics-output=\"gs://${GCS_BUCKET}/tpcdi/metrics_new\" \
        service-account-email=\"$SA_EMAIL\" \
        service-account-key-file=\"$SA_KEY\""

    echo ">>> [$(date)] SUBMITTING JOB..."
    echo ">>> Executing: $JOB_CMD"
    eval "$JOB_CMD"

    # C. Cluster Deletion
    echo "Cleanup: Deleting Cluster ${CLUSTER_NAME}..."
    gcloud dataproc clusters delete "$CLUSTER_NAME" --region="$REGION" --project="$PROJECT" --quiet
}

# ==============================================================================
# 4. EXECUTION SEQUENCE
# ==============================================================================

# --- SF-10,000 WORKFLOW ---
# Format: name, master_type, num_workers, worker_type, ssd_count, load_type, sf, batch_id
run_tpcdi_cycle "sumit-dataproc-10000" "n2d-standard-8" 6 "n2d-standard-16" 4 "batch" 10000 ""
run_tpcdi_cycle "sumit-dataproc-inc-10000" "n2d-standard-8" 5 "n2d-standard-8" 2 "incremental" 10000 2
run_tpcdi_cycle "sumit-dataproc-inc-10000-b3" "n2d-standard-8" 5 "n2d-standard-8" 2 "incremental" 10000 3

# --- SF-1,000 WORKFLOW ---
run_tpcdi_cycle "sumit-dataproc-1000" "n2d-standard-8" 5 "n2d-standard-8" 2 "batch" 1000 ""
run_tpcdi_cycle "sumit-dataproc-inc-1000" "n2d-standard-8" 3 "n2d-standard-8" 2 "incremental" 1000 2
run_tpcdi_cycle "sumit-dataproc-inc-1000-b3" "n2d-standard-8" 3 "n2d-standard-8" 2 "incremental" 1000 3

# ==============================================================================
# 5. TEARDOWN
# ==============================================================================
echo "=== [$(date)] ALL BENCHMARKS COMPLETED. STARTING TEARDOWN ==="
gcloud compute routers nats delete "${NAT_NAME}" --router="${ROUTER_NAME}" --region="${REGION}" --project="${PROJECT}" --quiet || true
gcloud compute routers delete "${ROUTER_NAME}" --region="${REGION}" --project="${PROJECT}" --quiet || true
gcloud compute networks subnets delete "${SUBNET_NAME}" --region="${REGION}" --project="${PROJECT}" --quiet || true
gcloud compute networks delete "${VNET_NAME}" --project="${PROJECT}" --quiet || true
echo "=== [$(date)] TEARDOWN COMPLETE ==="