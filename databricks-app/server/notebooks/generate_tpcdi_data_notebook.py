# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Data Generation (Databricks)
# MAGIC
# MAGIC Generates TPC-DI benchmark raw data per the [TPC-DI v1.1.0 spec](https://www.tpc.org/tpcdi/).
# MAGIC Uses the official DIGen (Java) tool. **Prerequisites:** TPC-DI Tools v1.1.0 in `tools/datagen/` (DIGen.jar, pdgf/).
# MAGIC
# MAGIC - **Scale factor** controls data size (e.g. 10 ≈ 1GB, 100 ≈ 10GB).
# MAGIC - Output can go to **DBFS** or a **Unity Catalog Volume**.
# MAGIC - Generation runs on the **driver**; use a driver with sufficient local storage for large scale factors.
# MAGIC - **Upload threads** controls parallel file upload speed to DBFS (more threads = faster uploads).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets (optional)

# COMMAND ----------

try:
    dbutils.widgets.drop("scale_factor")
except Exception:
    pass
try:
    dbutils.widgets.drop("output_path")
except Exception:
    pass
try:
    dbutils.widgets.drop("raw_output_path")
except Exception:
    pass
try:
    dbutils.widgets.drop("tpcdi_raw_data_path")
except Exception:
    pass
try:
    dbutils.widgets.drop("upload_threads")
except Exception:
    pass
try:
    dbutils.widgets.drop("tpcdi_local_gen_path")
except Exception:
    pass

# Create widgets with defaults (for interactive use)
# When run as workflow task, these will be overridden by workflow parameters
dbutils.widgets.text("scale_factor", "10", "Scale factor (e.g. 10 ~ 1GB)")
dbutils.widgets.text("tpcdi_raw_data_path", "dbfs:/mnt/tpcdi", "TPC-DI raw data path: dbfs:/... (DBFS), /Volumes/... (Volume), gs://... (GCS), or local")
dbutils.widgets.text("upload_threads", "8", "Upload threads for DBFS (parallel file uploads)")
dbutils.widgets.text("tpcdi_local_gen_path", "/local_disk0", "Local path for datagen output (e.g. /mnt/disks/ssd0 on GCP; /local_disk0 on Databricks; empty = use default)")

# COMMAND ----------

# Get parameters (from widgets or workflow parameters)
# Workflow parameters override widget defaults
# Support both tpcdi_raw_data_path (workflow parameter) and raw_output_path (backward compatibility)
scale_factor = int(dbutils.widgets.get("scale_factor"))
try:
    tpcdi_raw_data_path = dbutils.widgets.get("tpcdi_raw_data_path").strip()
except Exception:
    # Fallback to raw_output_path for backward compatibility
    try:
        tpcdi_raw_data_path = dbutils.widgets.get("raw_output_path").strip()
    except Exception:
        tpcdi_raw_data_path = "dbfs:/mnt/tpcdi"
raw_output_path = tpcdi_raw_data_path  # Use for generate_tpcdi_data() call
upload_threads = int(dbutils.widgets.get("upload_threads").strip() or "8")
try:
    tpcdi_local_gen_path = (dbutils.widgets.get("tpcdi_local_gen_path") or "").strip()
except Exception:
    tpcdi_local_gen_path = ""
local_gen_path = tpcdi_local_gen_path if tpcdi_local_gen_path else None

print(f"Data Generation Parameters:")
print(f"  Scale Factor: {scale_factor}")
print(f"  TPC-DI Raw Data Path: {tpcdi_raw_data_path} (dbfs=DBFS, /Volumes/=Volume, gs://=GCS)")
print(f"  Upload Threads: {upload_threads}")
print(f"  Local Gen Path: {local_gen_path or '(default)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run generation

# COMMAND ----------

import os
import sys
import importlib
from pathlib import Path

# Ensure project root is on path (notebook in databricks/ → repo root = parent; else notebook dir)
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_path = Path(notebook_path).parent
    # Repo may be repo/tpcdi/databricks (then parent of databricks = tpcdi) or repo/databricks (then parent = repo)
    parent = workspace_path.parent
    if (parent / "tpcdi").is_dir():
        project_root = parent  # repo root (has tpcdi/)
    elif (parent / "benchmark").is_dir():
        project_root = parent  # tpcdi dir (has benchmark/)
    else:
        project_root = workspace_path
except Exception:
    project_root = Path(os.getcwd())
sys.path.insert(0, str(project_root.resolve()))
# When repo root is on path, add tpcdi dir so "import generate_tpcdi_data" finds tpcdi/generate_tpcdi_data.py
_tpcdi_dir = project_root / "tpcdi" if (project_root / "tpcdi").is_dir() else project_root
if str(_tpcdi_dir.resolve()) not in sys.path:
    sys.path.insert(0, str(_tpcdi_dir.resolve()))

# Import and reload to pick up any code changes (works whether path has tpcdi/ or repo root)
try:
    import generate_tpcdi_data as _gen_mod
    importlib.reload(_gen_mod)
    from generate_tpcdi_data import generate_tpcdi_data
except ModuleNotFoundError:
    import tpcdi.generate_tpcdi_data as _gen_mod
    importlib.reload(_gen_mod)
    from tpcdi.generate_tpcdi_data import generate_tpcdi_data

out = generate_tpcdi_data(
    scale_factor=scale_factor,
    raw_output_path=raw_output_path,
    digen_path=None,
    skip_if_exists=True,
    upload_threads=upload_threads,
    local_gen_path=local_gen_path,
)
print("Output location:", out)

# COMMAND ----------
