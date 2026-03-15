#!/bin/bash
# E2E Orchestrator for TPC-DI Benchmark (Databricks)
# Monitors batch job, then triggers B2, B3 sequentially

set -e

PROFILE="fe-vm-gcp-tpcdi-new"
APP_URL="https://tpcdi-benchmark-8259553627480283.gcp.databricksapps.com"
NOTEBOOK="/Workspace/Users/d66536ef-bdcc-4d9a-9e63-f40cf49b4d44/tpcdi-benchmark/databricks/benchmark_databricks_notebook"
CATALOG="gcp_tpcdi_sandbox"
SCHEMA="dw"
RAW_DATA="gs://sumit_prakash_gcs/tpcdi"
JAR="/Volumes/gcp_tpcdi_sandbox/dw/libs/spark-xml_2.13-0.18.0.jar"
LOG_FILE="/Users/sumit.prakash/tpcdi-benchmark/e2e_run.log"

get_token() {
  databricks auth token --profile "$PROFILE" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])" 2>/dev/null
}

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

poll_run() {
  local run_id=$1
  local step_name=$2
  local token

  log "Polling $step_name (run_id=$run_id)..."

  while true; do
    token=$(get_token)
    local status_json
    status_json=$(curl -s -H "Authorization: Bearer $token" "$APP_URL/api/databricks/job-status/$run_id" 2>/dev/null)

    local state result_state
    state=$(echo "$status_json" | python3 -c "import json,sys; print(json.load(sys.stdin)['state'])" 2>/dev/null)
    result_state=$(echo "$status_json" | python3 -c "import json,sys; print(json.load(sys.stdin).get('result_state','') or '')" 2>/dev/null)

    if [ "$state" = "TERMINATED" ] || [ "$state" = "INTERNAL_ERROR" ] || [ "$state" = "SKIPPED" ]; then
      if [ "$result_state" = "SUCCESS" ]; then
        local duration
        duration=$(echo "$status_json" | python3 -c "
import json,sys
d=json.load(sys.stdin)
t=(d.get('execution_duration',0)+d.get('setup_duration',0))/1000
print(f'{int(t//60)}m {int(t%60)}s')
" 2>/dev/null)
        log "$step_name SUCCEEDED in $duration"
        # Extract cost info
        echo "$status_json" | python3 -c "
import json,sys
d=json.load(sys.stdin)
cost=d.get('cost',{})
if cost:
    print(f'  Cost: \${cost.get(\"total_cost\",0):.2f} (VM: \${cost.get(\"vm_cost\",0):.2f}, DBU: \${cost.get(\"dbu_cost\",0):.2f})')
" 2>/dev/null | tee -a "$LOG_FILE"
        return 0
      else
        log "$step_name FAILED with state=$state result=$result_state"
        echo "$status_json" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'  Message: {d.get(\"state_message\",\"\")}')" 2>/dev/null | tee -a "$LOG_FILE"
        return 1
      fi
    fi

    log "  $step_name: state=$state result=$result_state - waiting 60s..."
    sleep 60
  done
}

create_and_run_job() {
  local load_type=$1
  local batch_id=$2
  local job_name=$3
  local token

  token=$(get_token)

  log "Creating job: $job_name"
  local create_result
  create_result=$(curl -s -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -X POST "$APP_URL/api/databricks/create-job" \
    -d "{
      \"job_name\": \"$job_name\",
      \"load_type\": \"$load_type\",
      \"scale_factor\": 100,
      \"raw_data_path\": \"$RAW_DATA\",
      \"catalog\": \"$CATALOG\",
      \"schema_name\": \"$SCHEMA\",
      \"batch_id\": \"$batch_id\",
      \"cloud\": \"GCP\",
      \"xml_format\": \"com.databricks.spark.xml\",
      \"spark_version\": \"17.3.x-scala2.13\",
      \"worker_node_type\": \"n2d-standard-8\",
      \"driver_node_type\": \"n2d-standard-8\",
      \"num_workers\": 3,
      \"jar_path\": \"$JAR\",
      \"notebook_path\": \"$NOTEBOOK\",
      \"photon\": true,
      \"local_ssds\": 2,
      \"log_detailed_stats\": true
    }")

  local job_id
  job_id=$(echo "$create_result" | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)
  log "Job created: ID=$job_id"

  log "Starting run..."
  token=$(get_token)
  local run_result
  run_result=$(curl -s -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -X POST "$APP_URL/api/databricks/run-job" \
    -d "{\"job_id\": $job_id}")

  local run_id run_url
  run_id=$(echo "$run_result" | python3 -c "import json,sys; print(json.load(sys.stdin)['run_id'])" 2>/dev/null)
  run_url=$(echo "$run_result" | python3 -c "import json,sys; print(json.load(sys.stdin).get('run_page_url',''))" 2>/dev/null)
  log "Run started: ID=$run_id URL=$run_url"

  echo "$run_id"
}

# Main orchestration
log "=========================================="
log "Starting E2E Databricks TPC-DI SF100 Run"
log "=========================================="

# Step 1: Batch Load (already running)
BATCH_RUN_ID=${1:-""}
if [ -z "$BATCH_RUN_ID" ]; then
  BATCH_RUN_ID=$(create_and_run_job "batch" "" "TPC-DI Benchmark SF100 batch")
fi

log "Batch run ID: $BATCH_RUN_ID"
if poll_run "$BATCH_RUN_ID" "Batch Load"; then
  log "Batch succeeded, starting Incremental B2..."
else
  log "BATCH FAILED - aborting pipeline"
  exit 1
fi

# Step 2: Incremental B2
B2_RUN_ID=$(create_and_run_job "incremental" "2" "TPC-DI Benchmark SF100 incremental batch2")
if poll_run "$B2_RUN_ID" "Incremental B2"; then
  log "B2 succeeded, starting Incremental B3..."
else
  log "INCREMENTAL B2 FAILED - aborting pipeline"
  exit 1
fi

# Step 3: Incremental B3
B3_RUN_ID=$(create_and_run_job "incremental" "3" "TPC-DI Benchmark SF100 incremental batch3")
if poll_run "$B3_RUN_ID" "Incremental B3"; then
  log "B3 succeeded!"
else
  log "INCREMENTAL B3 FAILED - aborting pipeline"
  exit 1
fi

# Save run history
log "Saving run history..."
token=$(get_token)
curl -s -H "Authorization: Bearer $token" \
  -H "Content-Type: application/json" \
  -X POST "$APP_URL/api/databricks/save-run" \
  -d "{
    \"run_data\": {
      \"platform\": \"databricks\",
      \"scale_factor\": 100,
      \"batch_run_id\": \"$BATCH_RUN_ID\",
      \"b2_run_id\": \"$B2_RUN_ID\",
      \"b3_run_id\": \"$B3_RUN_ID\",
      \"status\": \"completed\",
      \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
    }
  }" 2>/dev/null | python3 -m json.tool 2>/dev/null | tee -a "$LOG_FILE"

log "=========================================="
log "E2E Databricks Pipeline COMPLETE"
log "=========================================="
