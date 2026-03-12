#!/usr/bin/env bash
#
# Create Dataproc infrastructure for TPC-DI benchmark:
#   1. Custom VPC
#   2. Subnet with Private Google Access (required for --no-address)
#   3. Internal firewall (allow nodes to talk to each other)
#   4. Dataproc cluster in the subnet
#
# Usage:
#   Set environment variables (or export them), then run:
#     ./dataproc/create_dataproc_infra.sh
#
#   Required: PROJECT_ID, REGION, VPC_NAME, SUBNET_NAME, SUBNET_RANGE, CLUSTER_NAME
#   Optional: ZONE (default: REGION-b), FIREWALL_RULE_NAME (default: allow-<SUBNET_NAME>-internal)
#
# Example:
#   export PROJECT_ID=gcp-sandbox-field-eng
#   export REGION=us-central1
#   export ZONE=us-central1-b
#   export VPC_NAME=tpcdi-vpc
#   export SUBNET_NAME=tpcdi-subnet
#   export SUBNET_RANGE=10.10.0.0/24
#   export FIREWALL_RULE_NAME=allow-tpcdi-internal
#   export CLUSTER_NAME=tpcdi-benchmark
#   ./dataproc/create_dataproc_infra.sh
#
# See dataproc/DataprocRun.md §0 for full documentation.

set -e

# Required
: "${PROJECT_ID:?Set PROJECT_ID (e.g. gcp-sandbox-field-eng)}"
: "${REGION:?Set REGION (e.g. us-central1)}"
: "${VPC_NAME:?Set VPC_NAME (e.g. tpcdi-vpc)}"
: "${SUBNET_NAME:?Set SUBNET_NAME (e.g. tpcdi-subnet)}"
: "${SUBNET_RANGE:?Set SUBNET_RANGE (e.g. 10.10.0.0/24)}"
: "${CLUSTER_NAME:?Set CLUSTER_NAME (e.g. tpcdi-benchmark)}"

# Optional
ZONE="${ZONE:-${REGION}-b}"
FIREWALL_RULE_NAME="${FIREWALL_RULE_NAME:-allow-${SUBNET_NAME}-internal}"

echo "Creating Dataproc infrastructure:"
echo "  PROJECT_ID=$PROJECT_ID REGION=$REGION ZONE=$ZONE"
echo "  VPC=$VPC_NAME SUBNET=$SUBNET_NAME RANGE=$SUBNET_RANGE"
echo "  FIREWALL=$FIREWALL_RULE_NAME CLUSTER=$CLUSTER_NAME"
echo ""

# 1. Create the custom VPC
echo "[1/4] Creating VPC: $VPC_NAME"
gcloud compute networks create "${VPC_NAME}" \
  --project="${PROJECT_ID}" \
  --subnet-mode=custom \
  --bgp-routing-mode=regional

# 2. Create the subnet with Private Google Access
echo "[2/4] Creating subnet: $SUBNET_NAME (Private Google Access enabled)"
gcloud compute networks subnets create "${SUBNET_NAME}" \
  --project="${PROJECT_ID}" \
  --network="${VPC_NAME}" \
  --region="${REGION}" \
  --range="${SUBNET_RANGE}" \
  --enable-private-ip-google-access

# 3. Create the internal firewall rule
echo "[3/4] Creating firewall rule: $FIREWALL_RULE_NAME"
gcloud compute firewall-rules create "${FIREWALL_RULE_NAME}" \
  --project="${PROJECT_ID}" \
  --network="${VPC_NAME}" \
  --action=ALLOW \
  --direction=INGRESS \
  --rules=tcp:0-65535,udp:0-65535,icmp \
  --source-ranges="${SUBNET_RANGE}"

# 4. Create the Dataproc cluster
echo "[4/4] Creating Dataproc cluster: $CLUSTER_NAME"
gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --zone="${ZONE}" \
  --image-version=2.3-debian12 \
  --master-machine-type=n4-standard-16 \
  --master-boot-disk-type=hyperdisk-balanced \
  --master-boot-disk-size=100 \
  --num-workers=2 \
  --worker-machine-type=n4-standard-16 \
  --worker-boot-disk-type=hyperdisk-balanced \
  --worker-boot-disk-size=200 \
  --subnet="${SUBNET_NAME}" \
  --no-address \
  --optional-components=DELTA \
  --enable-component-gateway \
  --scopes=cloud-platform

echo ""
echo "Done. Cluster $CLUSTER_NAME is in subnet $SUBNET_NAME (VPC $VPC_NAME)."
echo "Run the benchmark with: --cluster=$CLUSTER_NAME --region=$REGION --project=$PROJECT_ID"
