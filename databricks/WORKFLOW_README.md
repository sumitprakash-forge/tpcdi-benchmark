# Databricks Workflow for TPC-DI Benchmark

This guide explains how to create and use a Databricks workflow that sequences data generation and benchmark execution.

## Overview

The workflow consists of two sequential tasks:
1. **Data Generation** - Generates TPC-DI raw data using DIGen
2. **Benchmark Execution** - Runs ETL transformations and collects performance metrics

All parameters are configurable via workflow parameters, allowing you to run the same workflow with different configurations.

## Creating the Workflow

### Option 1: Using the Notebook (Recommended)

1. Open `databricks/create_workflow_notebook.py` in Databricks (or `create_workflow_notebook` when in Repos under `databricks/`)
2. Configure the widgets:
   - **Job Name**: Name for your workflow
   - **Data Generation Notebook**: Path to `generate_tpcdi_data_notebook` (same dir; default)
   - **Benchmark Notebook**: Path to `benchmark_databricks_notebook` (same dir; default)
   - **Cluster Spark Version (DBR)**: Dropdown (13.3.x–16.4.x, standard and Photon)
   - **Cloud**: Choose **AWS**, **GCP**, or **Azure** first. Then **re-run the next cell** so the Worker/Driver dropdowns show only instance types valid for that cloud.
   - **Worker Node Type** / **Driver Node Type**: Dropdowns are populated from the selected cloud (e.g. GCP shows only n2d/c2 types; AWS shows i3/m5d/r5d; Azure shows Standard_E/D/L).
   - **Number of workers**, **Existing Cluster ID** (optional)
3. Run all cells (after changing Cloud, re-run the instance-type cell to refresh the dropdowns)
4. The workflow will be created and you'll get a job ID

### Option 2: Using Python Script

```bash
python databricks/create_databricks_workflow.py \
  --job-name "TPC-DI-Benchmark" \
  --data-gen-notebook "generate_tpcdi_data_notebook" \
  --benchmark-notebook "benchmark_databricks_notebook" \
  --workspace-path "/Workspace/Repos/user/repo/databricks" \
  --default-scale-factor 10 \
  --spark-version "14.3.x-scala2.12" \
  --default-output-path "dbfs:/mnt/tpcdi" \
  --node-type-id "i3.xlarge" \
  --num-workers 2 \
  --databricks-host "https://workspace.cloud.databricks.com" \
  --databricks-token "<your-token>" \
  --output-json workflow.json
```

Run from project root. Instance types are restricted by `--cloud`: use `--cloud GCP` and `--node-type-id`/`--driver-node-type-id` only from the allowed list for that cloud. Use `--list-node-types` to print allowed types per cloud:

```bash
python databricks/create_databricks_workflow.py --list-node-types
```

Use `--workspace-path` as the Databricks path to the `databricks/` folder (e.g. `/Workspace/Repos/<org>/<repo>/databricks`).

Or create directly via API:
```bash
python databricks/create_databricks_workflow.py \
  --databricks-host "https://workspace.cloud.databricks.com" \
  --databricks-token "<your-token>" \
  --job-name "TPC-DI-Benchmark" \
  --workspace-path "/Workspace/Repos/user/repo/databricks"
```

### Option 3: Using Databricks CLI

1. Generate workflow JSON:
```bash
python databricks/create_databricks_workflow.py --output-json workflow.json \
  --workspace-path "/Workspace/Repos/user/repo/databricks"
```

2. Create job:
```bash
databricks jobs create --json-file workflow.json
```

## Workflow Parameters

The workflow supports the following parameters (all have defaults):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `scale_factor` | `10` | TPC-DI scale factor (10, 100, 1000, etc.) |
| `tpcdi_raw_data_path` | `dbfs:/mnt/tpcdi` | TPC-DI raw data path (used by both 01_data_generation and 02_benchmark_execution); dbfs:/..., /Volumes/..., or gs://... |
| `load_type` | `batch` | Load type: `batch` or `incremental` |
| `target_database` | `tpcdi_warehouse` | Target database name |
| `target_schema` | `dw` | Target schema name |
| `batch_id` | `""` | Batch ID for incremental loads (empty for batch) |
| `metrics_output` | `dbfs:/mnt/tpcdi/metrics` | Path to save metrics JSON |
| `upload_threads` | `8` | Parallel threads for DBFS uploads |
| `customer_mgmt_xml_format` | `com.databricks.spark.xml` | See [CustomerMgmt XML format](#customermgmt-xml-format) below. |

Load type for 01_data_generation is inferred from path: **dbfs:/...** (DBFS), **/Volumes/...** (Volume), **gs://...** (GCS).

### CustomerMgmt XML format

The benchmark task reads `CustomerMgmt.xml` using a Spark XML data source. You can choose:

- **Databricks native XML reader** (no custom JAR): set the job parameter  
  `customer_mgmt_xml_format` to **`org.apache.spark.sql.execution.datasources.xml`**.  
  This uses the built-in Databricks/Spark native XML reader. Do **not** attach the spark-xml Maven library to the benchmark task when using this.

- **Custom spark-xml library**: when you attach the Maven library `com.databricks:spark-xml_2.13:0.18.0` (or similar) to the benchmark task, set `customer_mgmt_xml_format` to **`xml`** or **`com.databricks.spark.xml`**.  
  If `com.databricks.spark.xml` fails with a `ServiceConfigurationError`, the code falls back to the `xml` alias automatically.

In short: use **`org.apache.spark.sql.execution.datasources.xml`** to use the Databricks native reader; use **`xml`** or **`com.databricks.spark.xml`** when attaching the custom spark-xml JAR.

## Running the Workflow

### Via Databricks UI

1. Go to **Workflows** → **Jobs**
2. Find your job (e.g., "TPC-DI-Benchmark")
3. Click **Run now**
4. Override parameters if needed:
   - `scale_factor`: `100`
   - `load_type`: `batch`
   - etc.
5. Click **Run**

### Via Databricks CLI

```bash
databricks jobs run-now \
  --job-id <job-id> \
  --jar-params '{"scale_factor": "100", "load_type": "batch"}'
```

### Via API

```bash
curl -X POST \
  https://workspace.cloud.databricks.com/api/2.1/jobs/run-now \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": <job-id>,
    "notebook_params": {
      "scale_factor": "100",
      "load_type": "batch"
    }
  }'
```

### Via Python

```python
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient

client = ApiClient(host="https://workspace.cloud.databricks.com", token="<token>")
jobs_api = JobsApi(client)

run = jobs_api.run_now(
    job_id=<job-id>,
    notebook_params={
        "scale_factor": "100",
        "load_type": "batch",
        "target_database": "tpcdi_warehouse"
    }
)
print(f"Run ID: {run['run_id']}")
```

## Workflow Task Details

### Task 1: Data Generation
- **Task Key**: `01_data_generation`
- **Notebook**: `generate_tpcdi_data_notebook`
- **Purpose**: Generate TPC-DI raw data files
- **Output**: Raw data files at `{tpcdi_raw_data_path}/sf={scale_factor}/` (DBFS, Volume, or GCS; inferred from path)

### Task 2: Benchmark Execution
- **Task Key**: `02_benchmark_execution`
- **Notebook**: `benchmark_databricks_notebook` (in `databricks/`)
- **Depends On**: `01_data_generation`
- **Libraries**: By default the task attaches `com.databricks:spark-xml_2.13:0.18.0`. To use the **Databricks native XML reader** instead, set job parameter `customer_mgmt_xml_format` to `org.apache.spark.sql.execution.datasources.xml` and remove the spark-xml library from the task if desired (see [CustomerMgmt XML format](#customermgmt-xml-format)).
- **Purpose**: Run ETL transformations and collect metrics
- **Output**: 
  - Data warehouse tables in `{target_database}.{target_schema}`
  - Metrics JSON at `{metrics_output}/`

## Example Workflow Runs

### Batch Load (Scale Factor 10)
```json
{
  "scale_factor": "10",
  "load_type": "batch",
  "tpcdi_raw_data_path": "dbfs:/mnt/tpcdi",
  "target_database": "tpcdi_warehouse",
  "target_schema": "dw"
}
```

### Batch Load (Scale Factor 100)
```json
{
  "scale_factor": "100",
  "load_type": "batch",
  "tpcdi_raw_data_path": "dbfs:/mnt/tpcdi",
  "target_database": "tpcdi_warehouse",
  "target_schema": "dw"
}
```

### Incremental Load (Batch 2)
```json
{
  "scale_factor": "10",
  "load_type": "incremental",
  "batch_id": "2",
  "tpcdi_raw_data_path": "dbfs:/mnt/tpcdi",
  "target_database": "tpcdi_warehouse",
  "target_schema": "dw"
}
```

## Monitoring Workflow Runs

1. **View in UI**: Go to **Workflows** → **Jobs** → Select job → View runs
2. **Check Task Status**: Each task shows success/failure status
3. **View Logs**: Click on task to see notebook execution logs
4. **Download Metrics**: Metrics JSON files are saved to `metrics_output` path

## Troubleshooting

### Workflow Fails at Data Generation
- Check that TPC-DI Tools (DIGen.jar, pdgf/) are available in the repo
- Verify cluster has sufficient disk space for scale factor
- Check driver node has enough memory

### Workflow Fails at Benchmark
- Ensure data generation completed successfully
- Verify raw data exists at expected path
- Check cluster has sufficient resources for ETL

### Parameter Not Working
- Ensure parameter name matches exactly (case-sensitive)
- Check that notebook reads from `dbutils.widgets.get()`
- Verify parameter is defined in workflow definition

## Updating the Workflow

To update an existing workflow:

1. **Via UI**: Edit job → Update tasks/parameters → Save
2. **Via API**: Use `jobs/update` endpoint with new definition
3. **Via CLI**: 
   ```bash
   databricks jobs reset --job-id <id> --json-file updated_workflow.json
   ```

## Best Practices

1. **Use Existing Clusters**: For repeated runs, use `existing_cluster_id` to avoid cluster startup time
2. **Parameterize Everything**: Make all configurable values workflow parameters
3. **Monitor Metrics**: Review metrics JSON files to track performance over time
4. **Scale Appropriately**: Adjust cluster size based on scale factor
5. **Version Control**: Keep workflow definitions in version control

## Example: Complete Workflow Run

```python
# Create workflow
python databricks/create_databricks_workflow.py \
  --job-name "TPC-DI-Benchmark-SF100" \
  --default-scale-factor 100 \
  --num-workers 4 \
  --workspace-path "/Workspace/Repos/user/repo/databricks" \
  --databricks-host "https://workspace.cloud.databricks.com" \
  --databricks-token "<token>"

# Run workflow
databricks jobs run-now --job-id <job-id>

# Check status
databricks runs get --run-id <run-id>

# Download metrics
databricks fs cp -r dbfs:/mnt/tpcdi/metrics ./metrics/
```
