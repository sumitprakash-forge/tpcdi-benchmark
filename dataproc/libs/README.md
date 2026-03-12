# libs/

Pre-bundled JARs used by the TPC-DI benchmark when running on **Dataproc** (or other Spark runtimes).

| JAR | Purpose |
|-----|--------|
| `spark-xml_2.12-0.18.0.jar` | [spark-xml](https://github.com/databricks/spark-xml) – read/write XML (e.g. `CustomerMgmt.xml`). Scala 2.12, Spark 3.x. |
| `spark-xml_2.13-0.18.0.jar` | Same, for Scala 2.13 (e.g. Dataproc serverless default runtime). |
| `delta-spark_2.13-3.0.0.jar` | [Delta Lake](https://docs.delta.io/) – Delta table format. Scala 2.13, Spark 3.x. Required when using `--format delta` on serverless. |
| `delta-storage-3.0.0.jar` | [Delta Storage](https://docs.delta.io/) – LogStore for GCS/S3. **Required** when writing Delta to `gs://` (e.g. Dataproc serverless with `--format delta`). Match version to delta-spark (3.0.x). |

## Usage

The benchmark adds `spark-xml` and (when `--format delta`) Delta via `spark.jars.packages` by default (Maven). If your runtime does not resolve packages (e.g. Dataproc serverless), pass the JARs with `--jars` when submitting.

**Scala 2.13 (e.g. Dataproc serverless):**
- `--format parquet`: pass only spark-xml: `--jars=gs://your-bucket/tpcdi/libs/spark-xml_2.13-0.18.0.jar`
- `--format delta`: pass **spark-xml**, **delta-spark**, and **delta-storage** (comma-separated). delta-storage is required for writing Delta to GCS (`NoClassDefFoundError: io/delta/storage/LogStore` otherwise):  
  `--jars=gs://your-bucket/tpcdi/libs/spark-xml_2.13-0.18.0.jar,gs://your-bucket/tpcdi/libs/delta-spark_2.13-3.0.0.jar,gs://your-bucket/tpcdi/libs/delta-storage-3.0.0.jar`

**Scala 2.12 (managed cluster):** use the 2.12 spark-xml JAR; for delta, the runner’s packages usually resolve.

Example:

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=... --region=... \
  --py-files=benchmark.zip \
  --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
  -- \
  --load-type batch --scale-factor 10 --gcs-bucket=... --project-id=...
```

Run from the project root so the `dataproc/libs/` path resolves. The JAR is uploaded with the job.

## Source

- `spark-xml_2.12-0.18.0.jar`: [Maven Central](https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar)
- `spark-xml_2.13-0.18.0.jar`: [Maven Central](https://repo1.maven.org/maven2/com/databricks/spark-xml_2.13/0.18.0/spark-xml_2.13-0.18.0.jar)
- `delta-spark_2.13-3.0.0.jar`: [Maven Central](https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/3.0.0/delta-spark_2.13-3.0.0.jar)
- `delta-storage-3.0.0.jar`: [Maven Central](https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar)
