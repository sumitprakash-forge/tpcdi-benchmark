#!/bin/bash

# tail -100f /Users/sumit.prakash/Desktop/stuffs/WorkItems/dataproc_serverless_benchmark_full.log 2>&1 &

# ==============================================================================
# 1. GLOBAL CONFIGURATION
# ==============================================================================
PROJECT="gcp-sandbox-field-eng"
REGION="us-central1"
METASTORE="projects/$PROJECT/locations/$REGION/services/sumit-bnchmark-metastore"
SUBNET="projects/$PROJECT/regions/$REGION/subnetworks/sumit-tpcdi-subnet"
BUCKET="sumit_prakash_gcs"

# Target Database Variable
TARGET_DB="dataproc_serverless_final_run_2"

# Ensure script stops on any job failure
set -e

echo "=== [$(date)] STARTING DATAPROC SERVERLESS TPC-DI BENCHMARKS ==="

# ==============================================================================
# 2. REUSABLE SERVERLESS FUNCTION
# ==============================================================================
run_serverless_job() {
    local LOAD_TYPE=$1
    local SF=$2
    local BATCH_ID=$3
    
    local DISPLAY_NAME="BATCH LOAD"
    if [ "$LOAD_TYPE" == "incremental" ]; then
        DISPLAY_NAME="INCREMENTAL BATCH $BATCH_ID"
    fi

    echo ""
    echo ">>> [$(date)] Running SF-$SF $DISPLAY_NAME..."
    echo ">>> Database: $TARGET_DB"
    
    # Constructing the command into a variable for easy logging
    CMD="sh run_dataproc_job.sh \
      use-serverless=1 \
      properties=dataproc.tier=premium \
      region=$REGION \
      project=$PROJECT \
      deps-bucket=$BUCKET \
      load-type=$LOAD_TYPE \
      batch-id=$BATCH_ID \
      scale-factor=$SF \
      gcs-bucket=$BUCKET \
      target-database=$TARGET_DB \
      raw-data-path=gs://$BUCKET/tpcdi \
      metrics-output=gs://$BUCKET/tpcdi/metrics_new \
      metastore-service=$METASTORE \
      subnet=$SUBNET \
      BATCH_WAIT_LOG_FILE=/tmp/batch_wait.log"

    # Print the exact command being run
    echo ">>> Executing command:"
    echo "$CMD"
    echo "------------------------------------------------------------"

    # Execute the command
    eval "$CMD"

    echo ">>> [$(date)] SF-$SF $DISPLAY_NAME: SUCCESS"
}

# ==============================================================================
# 3. EXECUTION SEQUENCE
# ==============================================================================



# --- SCALE FACTOR 1,000 ---
echo "--- STAGE 1: SCALE FACTOR 1,000 ---"
run_serverless_job "batch" 1000 ""
run_serverless_job "incremental" 1000 2
run_serverless_job "incremental" 1000 3

# --- SCALE FACTOR 10,000 ---
echo ""
echo "--- STAGE 2: SCALE FACTOR 10,000 ---"
run_serverless_job "batch" 10000 ""
run_serverless_job "incremental" 10000 2
run_serverless_job "incremental" 10000 3

echo ""
echo "=== [$(date)] ALL SERVERLESS BENCHMARKS COMPLETED SUCCESSFULLY ==="
