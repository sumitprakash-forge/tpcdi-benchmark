# How to run the TPC-DI benchmark on Dataproc

This guide describes how to run the benchmark on **Dataproc**: prerequisites, all parameters (mandatory vs optional, what each means), setup, run commands, and running with a service account. See **[BENCHMARK_README.md](../BENCHMARK_README.md)** for overview, architecture, and Databricks usage.

---

## Prerequisites

1. **Dataproc cluster** with GCS connector installed. To create a custom VPC, subnet (with Private Google Access), internal firewall, and a cluster in that subnet, see **§0. Infrastructure: VPC, Subnet, Firewall, and Cluster**.
2. **TPC-DI raw data** must already exist in GCS. `run_benchmark_dataproc.py` does **not** generate data. Generate separately (e.g. run `generate_tpcdi_data.py` on a GCP VM or the Dataproc master, then upload to GCS). For **large scale factors (e.g. 1TB)** on GCP, use **local SSDs** so DIGen does not fill the boot disk: run with `--local-gen-path /mnt/disks/ssd0` (or set `TPCDI_LOCAL_GEN_PATH=/mnt/disks/ssd0`). See `generate_tpcdi_data.py --help`.
3. Data path: `gs://<bucket>/tpcdi/sf=<scale_factor>/` (or your `--raw-data-path`).
4. GCP **project ID** and **region**.
5. **Metastore (optional):** Without a [Dataproc Metastore](https://cloud.google.com/dataproc-metastore/docs), Spark uses the default metastore. The benchmark sets the warehouse to GCS (`gs://<bucket>/spark-warehouse`) and uses two-part table names (`database.table`) and Parquet by default. See **../docs/DATAPROC_METASTORE.md**.

**How this guide is structured:** §0 documents optional infrastructure (VPC, subnet, firewall, cluster). §1 lists all parameters (gcloud + script). §2 explains which are mandatory vs optional. §3 describes what each parameter means. §4–§5 cover setup and run commands; **§5 includes steps to submit a job to Dataproc Serverless** (no cluster). §6 explains running with a service account. §7 gives a full example with SA. The end section covers Dataproc-specific troubleshooting.

---

## 0. Infrastructure: VPC, Subnet, Firewall, and Cluster (optional)

If you want a **dedicated network** for TPC-DI (e.g. a private-style cluster with `--no-address`), create a custom VPC, a subnet with **Private Google Access**, an internal firewall, then create the Dataproc cluster in that subnet. Run these in order.

**Variables** — set these or substitute in the commands below:

| Variable | Example | Description |
|----------|---------|-------------|
| `PROJECT_ID` | `gcp-sandbox-field-eng` | GCP project ID |
| `REGION` | `us-central1` | GCP region for subnet and cluster |
| `ZONE` | `us-central1-b` | GCP zone for cluster (e.g. `REGION-a` or `REGION-b`) |
| `VPC_NAME` | `tpcdi-vpc` | Custom VPC name |
| `SUBNET_NAME` | `tpcdi-subnet` | Subnet name |
| `SUBNET_RANGE` | `10.10.0.0/24` | CIDR for the subnet |
| `FIREWALL_RULE_NAME` | `allow-tpcdi-internal` | Firewall rule name (internal traffic) |
| `CLUSTER_NAME` | `tpcdi-benchmark` | Dataproc cluster name |

### Step 1: Create the custom VPC

```bash
gcloud compute networks create "${VPC_NAME}" \
  --project="${PROJECT_ID}" \
  --subnet-mode=custom \
  --bgp-routing-mode=regional
```

- `--subnet-mode=custom`: you create subnets manually (required for a single regional subnet).
- `--bgp-routing-mode=regional`: recommended for regional subnets.

### Step 2: Create the subnet with Private Google Access

**Private Google Access** is required when using `--no-address` on the cluster so that nodes can reach Google APIs (e.g. GCS, BigQuery) without public IPs.

```bash
gcloud compute networks subnets create "${SUBNET_NAME}" \
  --project="${PROJECT_ID}" \
  --network="${VPC_NAME}" \
  --region="${REGION}" \
  --range="${SUBNET_RANGE}" \
  --enable-private-ip-google-access
```

- `--range`: CIDR for the subnet (e.g. `10.10.0.0/24`).
- `--enable-private-ip-google-access`: allows private IPs in this subnet to reach Google APIs via Private Google Access.

### Step 3: Create the internal firewall rule

Allow traffic between nodes in the subnet (required for Spark/YARN and cluster communication).

```bash
gcloud compute firewall-rules create "${FIREWALL_RULE_NAME}" \
  --project="${PROJECT_ID}" \
  --network="${VPC_NAME}" \
  --action=ALLOW \
  --direction=INGRESS \
  --rules=tcp:0-65535,udp:0-65535,icmp \
  --source-ranges="${SUBNET_RANGE}"
```

- `--source-ranges`: restrict to your subnet so only internal traffic is allowed.

### Step 4: Create the Dataproc cluster in the subnet

Create the cluster in the new subnet. Use `--no-address` so worker/driver nodes do not get external IPs (they use Private Google Access for GCS etc.).

```bash
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
```

| Option | Purpose |
|--------|--------|
| `--subnet` | Subnet name (must be in `REGION` and belong to your VPC). |
| `--no-address` | No external IPs; requires Private Google Access on the subnet. |
| `--optional-components=DELTA` | Install Delta Lake (omit if you use `--format parquet` only). |
| `--enable-component-gateway` | Web UIs accessible via secure gateway. |
| `--scopes=cloud-platform` | Full cloud scope for GCS and other APIs. |

Adjust `--master-machine-type`, `--worker-machine-type`, `--num-workers`, and disk sizes as needed. Then run the benchmark with `--cluster=${CLUSTER_NAME}` as in §5 and §7.

**One-shot example** (replace placeholders):

```bash
export PROJECT_ID=gcp-sandbox-field-eng
export REGION=us-central1
export ZONE=us-central1-b
export VPC_NAME=sumit-tpcdi-vpc
export SUBNET_NAME=sumit-tpcdi-subnet
export SUBNET_RANGE=10.10.0.0/24
export FIREWALL_RULE_NAME=allow-tpcdi-internal
export CLUSTER_NAME=sumitbnchmark4

# 1. VPC
gcloud compute networks create "${VPC_NAME}" \
  --project="${PROJECT_ID}" --subnet-mode=custom --bgp-routing-mode=regional

# 2. Subnet (Private Google Access)
gcloud compute networks subnets create "${SUBNET_NAME}" \
  --project="${PROJECT_ID}" --network="${VPC_NAME}" --region="${REGION}" \
  --range="${SUBNET_RANGE}" --enable-private-ip-google-access

# 3. Internal firewall
gcloud compute firewall-rules create "${FIREWALL_RULE_NAME}" \
  --project="${PROJECT_ID}" --network="${VPC_NAME}" \
  --action=ALLOW --direction=INGRESS \
  --rules=tcp:0-65535,udp:0-65535,icmp --source-ranges="${SUBNET_RANGE}"

# 4. Cluster
gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --project="${PROJECT_ID}" --region="${REGION}" --zone="${ZONE}" \
  --image-version=2.3-debian12 \
  --master-machine-type=n4-standard-16 --master-boot-disk-type=hyperdisk-balanced --master-boot-disk-size=100 \
  --num-workers=2 --worker-machine-type=n4-standard-16 --worker-boot-disk-type=hyperdisk-balanced --worker-boot-disk-size=200 \
  --subnet="${SUBNET_NAME}" --no-address \
  --optional-components=DELTA --enable-component-gateway --scopes=cloud-platform
```

A script that runs these steps with the same variables is in **dataproc/create_dataproc_infra.sh** (see script header for usage).

---

## 1. All parameters

You run the benchmark with `gcloud dataproc jobs submit pyspark`. Options **before** `--` are gcloud/Spark; arguments **after** `--` go to the benchmark script.

### 1.1 gcloud options (before `--`)

| Option | Description |
|--------|-------------|
| `--cluster` | Dataproc cluster name |
| `--region` | GCP region (e.g. `us-central1`) |
| `--project` | GCP project ID |
| `--py-files=benchmark.zip` | Benchmark package; create with `zip -r benchmark.zip benchmark` |
| `--jars=dataproc/libs/spark-xml_2.12-0.18.0.jar` | Optional; bundled spark-xml JAR (e.g. when no Maven access) |

### 1.2 Script parameters (after `--`)

| Argument | Default | Description |
|----------|---------|-------------|
| `--load-type` | — | `batch` or `incremental` |
| `--scale-factor` | — | TPC-DI scale factor (e.g. 10, 100, 1000) |
| `--gcs-bucket` | — | GCS bucket for raw data and metrics |
| `--project-id` | — | GCP project ID |
| `--region` | `us-central1` | GCP region |
| `--raw-data-path` | `gs://<gcs-bucket>/tpcdi/sf=<scale-factor>` | Path to raw TPC-DI data in GCS |
| `--target-database` | `tpcdi_warehouse` | Target database name |
| `--target-schema` | `dw` | Target schema name |
| `--batch-id` | — | Batch ID for incremental loads |
| `--spark-master` | `yarn` | Spark master URL |
| `--service-account-email` | — | Service account email for GCS access |
| `--service-account-key-file` | — | Path to SA JSON key file (local or `gs://`) |
| `--save-metrics` | `true` | Save benchmark metrics to GCS |
| `--no-save-metrics` | — | Do not save metrics |
| `--metrics-output` | `gs://<gcs-bucket>/tpcdi/metrics` | Directory to save metrics JSON |
| `--log-detailed-stats` | `false` | Log per-table timing and row counts |
| `--format` | `parquet` | Table format: `delta` or `parquet` |

**Detailed stats (per-table timing):** To print per-table timing, row counts, and throughput in the job logs, pass the **script argument** `--log-detailed-stats` (or `--log-detailed-stats true`) after the `--` in your `gcloud dataproc jobs submit pyspark` or `gcloud dataproc batches submit pyspark` command. Example:

```bash
# Cluster
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> --region=us-central1 --project=<project> --py-files=benchmark.zip \
  -- \
  --load-type batch --scale-factor 10 --gcs-bucket=<bucket> --project-id=<project> --region=us-central1 \
  --log-detailed-stats

# Serverless
gcloud dataproc batches submit pyspark run_benchmark_dataproc.py \
  --region=us-central1 --deps-bucket=gs://<bucket> --py-files=benchmark.zip --project=<project> \
  -- \
  --load-type batch --scale-factor 10 --gcs-bucket=<bucket> --project-id=<project> --region=us-central1 \
  --log-detailed-stats
```

---

## 2. Mandatory vs optional parameters

### 2.1 Mandatory (always)

- **gcloud:** `--cluster`, `--region`, `--project`, `--py-files=benchmark.zip`
- **Script:** `--load-type`, `--scale-factor`, `--gcs-bucket`, `--project-id`

### 2.2 Mandatory only for incremental

- **Script:** `--batch-id` — required when `--load-type incremental`.

### 2.3 Optional (have defaults)

If omitted, these use the defaults in §1:

- **gcloud:** `--jars` (omit unless using bundled spark-xml)
- **Script:** `--region`, `--raw-data-path`, `--target-database`, `--target-schema`, `--spark-master`, `--save-metrics` / `--no-save-metrics`, `--metrics-output`, `--log-detailed-stats`, `--format`

### 2.4 Optional (no default)

- **Script:** `--service-account-email`, `--service-account-key-file` — use both when running with a service account for GCS access.

---

## 3. What each parameter means

### gcloud options (before `--`)

| Option | Meaning |
|--------|--------|
| `--cluster` | Dataproc cluster that runs the PySpark job. |
| `--region` | GCP region where the cluster lives (e.g. `us-central1`). |
| `--project` | GCP project that owns the cluster and job. |
| `--py-files=benchmark.zip` | Zipped `benchmark` package. The script imports `benchmark`; without this, the job fails. Create with `zip -r benchmark.zip benchmark` from the project root. |
| `--jars=dataproc/libs/spark-xml_2.12-0.18.0.jar` | Spark-xml JAR for reading `CustomerMgmt.xml`. Optional if the cluster can resolve Maven packages; use when air-gapped or without Maven. Run from project root. |

### Script parameters (after `--`)

| Argument | Meaning |
|----------|--------|
| `--load-type` | **batch** = full historical + Batch1; **incremental** = specific batch (use with `--batch-id`). |
| `--scale-factor` | TPC-DI scale factor. Must match raw data under `--raw-data-path`. |
| `--gcs-bucket` | GCS bucket for raw data, metrics, and Spark warehouse. |
| `--project-id` | GCP project ID for Spark/Hadoop GCS config. |
| `--region` | GCP region; default `us-central1`. |
| `--raw-data-path` | Full GCS path to raw TPC-DI data. Default `gs://<gcs-bucket>/tpcdi/sf=<scale-factor>`. Must exist. |
| `--target-database` | Spark database name. On Dataproc, used as `tpcdi_warehouse_dw` with schema. |
| `--target-schema` | Schema name; combined with `--target-database` for table naming. |
| `--batch-id` | Batch number for **incremental** loads. Required only when `--load-type incremental`. |
| `--spark-master` | Spark master URL; default `yarn`. |
| `--service-account-email` | Service account email for GCS. Use with `--service-account-key-file`. |
| `--service-account-key-file` | Path to SA JSON key. **Local** path → Spark uses SA for GCS. **`gs://`** path → Spark uses default credentials. |
| `--save-metrics` | Save metrics JSON to GCS (default). |
| `--no-save-metrics` | Do not save metrics; results still printed to stdout. |
| `--metrics-output` | GCS directory for metrics JSON. Default `gs://<gcs-bucket>/tpcdi/metrics`. |
| `--log-detailed-stats` | Log per-table timing and row counts; otherwise job-level summary only. Same detailed stats as Databricks (per-table start/end, duration, rows; final [JOB SUMMARY] with tables and time spent). Output appears in the job driver logs (e.g. Cloud Console job output or `gcloud dataproc jobs wait <job-id>`). |
| `--format` | **parquet** (default) or **delta**. With `--format delta`, the benchmark adds the Delta package. |

---

## 4. Setup before running

**Package the benchmark module:** Only the main script is uploaded by default. Provide the `benchmark` package via `--py-files`. From the project root:

```bash
zip -r benchmark.zip benchmark
```

Pass `--py-files=benchmark.zip` (or `--py-files=gs://<bucket>/benchmark.zip` if uploaded to GCS) to every `gcloud dataproc jobs submit pyspark` command.

**Metrics:** By default, metrics are saved to GCS. Use `--no-save-metrics` to skip; use `--metrics-output=gs://bucket/path/metrics` to override the default path.

**Table format:** Use `--format delta` or `--format parquet` (default). With `--format delta`, the benchmark adds the Delta package automatically.

**Spark packages:** The benchmark adds `spark-xml` (for CustomerMgmt.xml) and, when `--format delta`, Delta automatically. The driver needs Maven access. For air-gapped setups, use `--jars=dataproc/libs/spark-xml_2.12-0.18.0.jar` (see **dataproc/libs/README.md**).

---

## 5. Run commands

### Batch load

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --project=<your-project> \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1
```

### With per-table timing (`--log-detailed-stats`)

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --project=<your-project> \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1 \
  --log-detailed-stats
```

### Incremental load

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --project=<your-project> \
  --py-files=benchmark.zip \
  -- \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1
```

---

### Submit a job to Dataproc Serverless

[Dataproc Serverless](https://cloud.google.com/dataproc-serverless/docs) runs Spark batch jobs without managing a cluster. Use `gcloud dataproc batches submit pyspark` (no `--cluster`). The same benchmark script and args apply; dependencies are uploaded to a **deps bucket**.

**Prerequisites**

1. **Deps bucket** — A GCS bucket for workload dependencies (e.g. `gs://<your-bucket>/dataproc-deps` or the same bucket you use for data).
2. **TPC-DI raw data** in GCS (same as for cluster runs).
3. **Enable the API** — `gcloud services enable dataproc.googleapis.com` (if not already).

**Sample command (run from gcloud)**

From the project root, set your project and bucket, then submit a batch load. Replace `YOUR_PROJECT` and `YOUR_BUCKET` with your GCP project ID and GCS bucket name.

```bash
# Set once; run from project root (where run_benchmark_dataproc.py and benchmark/ live)
export PROJECT_ID=YOUR_PROJECT
export BUCKET=YOUR_BUCKET
export REGION=us-central1

# Package and upload deps (first time or after code changes)
zip -r benchmark.zip benchmark
gsutil cp run_benchmark_dataproc.py gs://${BUCKET}/deps/
gsutil cp benchmark.zip gs://${BUCKET}/deps/
gsutil cp dataproc/libs/spark-xml_2.12-0.18.0.jar gs://${BUCKET}/deps/  # if using bundled JAR

# Submit batch load to Dataproc Serverless
gcloud dataproc batches submit pyspark gs://${BUCKET}/deps/run_benchmark_dataproc.py \
  --region=${REGION} \
  --deps-bucket=gs://${BUCKET} \
  --py-files=gs://${BUCKET}/deps/benchmark.zip \
  --jars=gs://${BUCKET}/deps/spark-xml_2.12-0.18.0.jar \
  --project=${PROJECT_ID} \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=${BUCKET} \
  --project-id=${PROJECT_ID} \
  --region=${REGION}
```

To use a **local** main file instead of uploading it (deps still from GCS):

```bash
gcloud dataproc batches submit pyspark run_benchmark_dataproc.py \
  --region=${REGION} \
  --deps-bucket=gs://${BUCKET} \
  --py-files=benchmark.zip \
  --project=${PROJECT_ID} \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=${BUCKET} \
  --project-id=${PROJECT_ID} \
  --region=${REGION}
```

**Steps**

1. **Package the benchmark** (from project root):

   ```bash
   zip -r benchmark.zip benchmark
   ```

2. **Upload the main script and package to GCS** (optional but recommended so the batch can resolve them from one place):

   ```bash
   gsutil cp run_benchmark_dataproc.py gs://<your-bucket>/deps/
   gsutil cp benchmark.zip gs://<your-bucket>/deps/
   ```

   If you prefer to submit a **local** main file, you can pass it directly; dependencies are still uploaded to `--deps-bucket`.

3. **Submit a batch (batch load)** — use `gcloud dataproc batches submit pyspark`, `--deps-bucket`, and `--region`. Arguments after `--` are passed to the benchmark script:

   ```bash
   gcloud dataproc batches submit pyspark gs://<your-bucket>/deps/run_benchmark_dataproc.py \
     --region=us-central1 \
     --deps-bucket=gs://<your-bucket> \
     --py-files=gs://<your-bucket>/deps/benchmark.zip \
     --jars=gs://<your-bucket>/deps/spark-xml_2.12-0.18.0.jar \
     --project=<your-project> \
     -- \
     --load-type batch \
     --scale-factor 10 \
     --gcs-bucket=<your-bucket> \
     --project-id=<your-project> \
     --region=us-central1
   ```

   If the main file is **local**:

   ```bash
   gcloud dataproc batches submit pyspark run_benchmark_dataproc.py \
     --region=us-central1 \
     --deps-bucket=gs://<your-bucket> \
     --py-files=benchmark.zip \
     --project=<your-project> \
     -- \
     --load-type batch \
     --scale-factor 10 \
     --gcs-bucket=<your-bucket> \
     --project-id=<your-project> \
     --region=us-central1
   ```

4. **Submit a batch (incremental load)**:

   ```bash
   gcloud dataproc batches submit pyspark run_benchmark_dataproc.py \
     --region=us-central1 \
     --deps-bucket=gs://<your-bucket> \
     --py-files=benchmark.zip \
     --project=<your-project> \
     -- \
     --load-type incremental \
     --scale-factor 10 \
     --batch-id 2 \
     --gcs-bucket=<your-bucket> \
     --project-id=<your-project> \
     --region=us-central1
   ```

5. **Optional: Service account** — To run the batch as a specific identity (e.g. for GCS access), add:

   ```bash
   --service-account=projects/<your-project>/serviceAccounts/tpcdi-dataproc@<your-project>.iam.gserviceaccount.com
   ```

6. **Optional: Metastore** — To use a Dataproc Metastore with serverless, add:

   ```bash
   --metastore-service=projects/<your-project>/locations/<region>/services/<metastore-name>
   ```

7. **Monitor the batch** — List and describe batches in the same region:

   ```bash
   gcloud dataproc batches list --region=us-central1
   gcloud dataproc batches describe <BATCH_ID> --region=us-central1
   ```

8. **Post-run: fetch batch usage and merge into metrics** — After the batch has **succeeded**, run the post script from your machine (or CI) to get `runtimeInfo.approximateUsage` (milliDcuSeconds, shuffleStorageGbSeconds) for cost calculation and merge it into your metrics JSON:

   ```bash
   # After: gcloud dataproc batches wait <BATCH_ID> --region=us-central1
   python tpcdi/dataproc/fetch_dataproc_batch_usage.py \
     --batch-id <BATCH_ID> \
     --region us-central1 \
     --project <your-project> \
     --metrics-output gs://<your-bucket>/tpcdi/metrics
   ```

   This writes a standalone `dataproc_batch_<id>_usage.json` and, when `--metrics-output` is a directory, merges a `dataproc_batch` block into the latest `metrics_*.json` in that directory. Requires `gcloud` and `gsutil` in PATH.

**Notes**

- **No `--cluster`** — Serverless batches do not use a cluster; only `--region` and `--deps-bucket` are required (plus your project).
- **JARs** — Upload `dataproc/libs/spark-xml_2.12-0.18.0.jar` to GCS and pass it with `--jars=gs://...` if the runtime does not have Maven access.
- **Metrics** — Metrics are written to GCS when `--metrics-output=gs://...` is set (or the default). The batch’s service account (or project default) must have write access to that path.

---

## 6. Running with a service account (SA)

When the cluster’s default identity should not be used for GCS (e.g. different project, stricter IAM), run with a **service account** and its **JSON key file**.

**1. Create a service account and key**

- Create a SA (e.g. `tpcdi-dataproc@<project>.iam.gserviceaccount.com`).
- Grant the SA roles to read/write your GCS bucket (e.g. **Storage Object Viewer** on raw data, **Storage Object Admin** or **Creator** where you write).
- Create a JSON key:
  ```bash
  gcloud iam service-accounts keys create sa-key.json \
    --iam-account=tpcdi-dataproc@<project>.iam.gserviceaccount.com
  ```

**2. Pass SA and key to the benchmark**

- `--service-account-email <sa-email>`
- `--service-account-key-file <path-to-json>`

Use **both** for key-file auth.

**3. Run locally (driver on your machine)**

```bash
python run_benchmark_dataproc.py \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1 \
  --service-account-email=tpcdi-dataproc@<project>.iam.gserviceaccount.com \
  --service-account-key-file=./sa-key.json
```

**4. Submit as Dataproc job**

**A. Key file in GCS**  
Upload the key to a restricted GCS path (e.g. `gs://<bucket>/secrets/tpcdi-sa-key.json`). Ensure the cluster’s default SA can read it. Then:

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --project=<your-project> \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1 \
  --service-account-email=tpcdi-dataproc@<project>.iam.gserviceaccount.com \
  --service-account-key-file=gs://<bucket>/secrets/tpcdi-sa-key.json
```

**B. Key file on cluster disk**  
Copy the key to the driver (e.g. via init action to `/var/lib/tpcdi/sa-key.json`). Use the same `gcloud` command as above with `--service-account-key-file=/var/lib/tpcdi/sa-key.json` after `--`.

**5. Using only the cluster’s service account**

If the cluster is created with the desired SA (`gcloud dataproc clusters create ... --service-account=...`), you do **not** need `--service-account-email` or `--service-account-key-file`. The job uses the cluster’s default identity for GCS.

**6. Security**

- Do **not** commit the JSON key. Add it to `.gitignore`.
- Prefer **Secret Manager** or a restricted GCS path; restrict access to the key.
- Use a dedicated SA with minimal roles; rotate keys regularly.

For more detail, see **../docs/DATAPROC_SERVICE_ACCOUNT.md**.

---

## 7. Full example: Dataproc run with service account

Complete `gcloud` command with SA, optional parameters set explicitly:

```bash
zip -r benchmark.zip benchmark

gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=sumitbnchmark \
  --region=us-central1 \
  --project=gcp-sandbox-field-eng \
  --py-files=benchmark.zip \
  --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --format delta \
  --gcs-bucket=sumit_prakash_gcs \
  --project-id=gcp-sandbox-field-eng \
  --region=us-central1 \
  --raw-data-path=gs://sumit_prakash_gcs/benchmark/__unitystorage/catalogs/059afd06-0390-412b-9f4b-af8a5511fd35/volumes/f91c4ca5-899e-4ce2-8989-bf7627b9dda2/sf=10 \
  --target-database=tpcdi_warehouse \
  --target-schema=dw \
  --spark-master=yarn \
  --save-metrics \
  --metrics-output=gs://sumit_prakash_gcs/tpcdi/metrics \
  --service-account-email=tsumit-wmt-workspace-creator@gcp-sandbox-field-eng.iam.gserviceaccount.com \
  --service-account-key-file=gs://sumit_prakash_gcs/service_account_key_file/service_account.json
```

**Notes:** `--jars=dataproc/libs/...` uses the bundled spark-xml JAR; run from project root. `--format delta` adds the Delta package. With `--service-account-key-file=gs://...`, Spark uses **default** GCS credentials (key not local). For Spark to use the SA, use a **local** key path on the driver. Add `--log-detailed-stats` for per-table timing.

**Incremental (batch 2):**

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=sumitbnchmark \
  --region=us-central1 \
  --project=gcp-sandbox-field-eng \
  --py-files=benchmark.zip \
  --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
  -- \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --format delta \
  --gcs-bucket=sumit_prakash_gcs \
  --project-id=gcp-sandbox-field-eng \
  --region=us-central1 \
  --raw-data-path=gs://sumit_prakash_gcs/benchmark/__unitystorage/catalogs/059afd06-0390-412b-9f4b-af8a5511fd35/volumes/f91c4ca5-899e-4ce2-8989-bf7627b9dda2/sf=10 \
  --target-database=tpcdi_warehouse \
  --target-schema=dw \
  --save-metrics \
  --metrics-output=gs://sumit_prakash_gcs/tpcdi/metrics \
  --service-account-email=tsumit-wmt-workspace-creator@gcp-sandbox-field-eng.iam.gserviceaccount.com \
  --service-account-key-file=gs://sumit_prakash_gcs/service_account_key_file/service_account.json
```

---

## Troubleshooting (Dataproc)

- **GCS connector:** Ensure it is installed on the cluster.
- **GCS permissions:** Verify bucket (and paths) access for the identity used (cluster SA or SA key).
- **Raw data:** Confirm `--raw-data-path` exists and is readable from the cluster.
- **Project / region:** Check `--project-id` and `--region` match the cluster.
- **Tables/database gone after job or cluster ends:** Without a Dataproc Metastore, metadata and often data are ephemeral. See **../docs/DATAPROC_METASTORE.md**.
