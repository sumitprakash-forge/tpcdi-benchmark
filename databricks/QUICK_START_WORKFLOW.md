# Quick Start: Databricks Workflow

## Create Workflow (One-Time Setup)

### Using Notebook (Easiest)
1. Open `databricks/create_workflow_notebook.py` in Databricks (or `create_workflow_notebook` in Repos under `databricks/`)
2. Set **Cluster Spark Version (DBR)** via dropdown (e.g. 14.3.x, 15.4.x)
3. Run all cells
4. Copy the job ID from the output

### Using Python Script
```bash
python databricks/create_databricks_workflow.py \
  --job-name "TPC-DI-Benchmark" \
  --workspace-path "/Workspace/Repos/your-org/your-repo/databricks" \
  --databricks-host "https://your-workspace.cloud.databricks.com" \
  --databricks-token "your-token"
```
Run from project root.

## Run Workflow

### Via UI
1. Go to **Workflows** → **Jobs**
2. Find "TPC-DI-Benchmark"
3. Click **Run now**
4. In **Parameters**, set:
   - `scale_factor`: `10` (or 100, 1000, etc.)
   - `tpcdi_raw_data_path`: `dbfs:/mnt/tpcdi` or `/Volumes/cat/schema/vol` (path type inferred: dbfs=DBFS, /Volumes/=Volume)
   - `load_type`: `batch` (or `incremental`)
   - `batch_id`: `2` (only for incremental)
5. Click **Run**

### Via CLI
```bash
# Batch load
databricks jobs run-now \
  --job-id <job-id> \
  --notebook-params '{
    "scale_factor": "10",
    "load_type": "batch"
  }'

# Incremental load
databricks jobs run-now \
  --job-id <job-id> \
  --notebook-params '{
    "scale_factor": "10",
    "load_type": "incremental",
    "batch_id": "2"
  }'
```

### Via API
```bash
curl -X POST \
  https://your-workspace.cloud.databricks.com/api/2.1/jobs/run-now \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": <job-id>,
    "notebook_params": {
      "scale_factor": "10",
      "load_type": "batch"
    }
  }'
```

## Workflow Tasks

1. **01_data_generation** - Generates TPC-DI raw data
2. **02_benchmark_execution** - Runs ETL and collects metrics (depends on task 1)

## Parameters

All parameters have defaults. Override at runtime:

- `scale_factor`: TPC-DI scale factor (default: `10`)
- `tpcdi_raw_data_path`: TPC-DI raw data path; type inferred from path: dbfs:/... (DBFS), /Volumes/... (Volume) (default: `dbfs:/mnt/tpcdi`)
- `load_type`: `batch` or `incremental` (default: `batch`)
- `target_database`: Target database (default: `tpcdi_warehouse`)
- `target_schema`: Target schema (default: `dw`)
- `batch_id`: Batch ID for incremental (default: `""`)
- `metrics_output`: Metrics output path (default: `dbfs:/mnt/tpcdi/metrics`)

## View Results

1. Go to **Workflows** → **Jobs** → Your job → Latest run
2. Check task status (both should be green ✓)
3. View metrics: `dbfs:/mnt/tpcdi/metrics/`
