#!/usr/bin/env python3
"""
TPC-DI Data Generation for Databricks and GCP

Generates TPC-DI benchmark raw data per the TPC-DI v1.1.0 specification
(https://www.tpc.org/tpcdi/). Uses the official DIGen (Java) tool invoked
via a Python wrapper. Designed to run on Databricks (driver node) or on
GCP (VM/Dataproc driver) with optional local SSD for large scale factors.

Usage:
    # Databricks notebook: use widgets or set variables, then call main()
    # CLI (local or Databricks job):
    python generate_tpcdi_data.py --scale-factor 10 --output dbfs:/mnt/tpcdi

    # GCP with local SSDs (e.g. n2-standard-16 + 4×375GB): use --local-gen-path
    # so DIGen writes to the SSD instead of /tmp (avoids "No space left on device"):
    python generate_tpcdi_data.py -s 1000 -o gs://BUCKET/tpcdi --local-gen-path /mnt/disks/ssd0
    # Or set env: TPCDI_LOCAL_GEN_PATH=/mnt/disks/ssd0

    # Databricks: driver /local_disk0 is small (~100–200GB). For 1TB SF set tpcdi_local_gen_path
    # to a larger path, e.g. DBFS: /dbfs/tmp/tpcdi_gen (workflow param or widget).

Prerequisites:
    - Java 7+ (available on Databricks runtime)
    - TPC-DI Tools v1.1.0 extracted in tools/datagen/ (DIGen.jar, pdgf/, etc.)
    - See tools/datagen/README.txt and README.md for setup.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Optional Databricks imports (only when running on Databricks)
try:
    import dbutils  # type: ignore
    IN_DATABRICKS = True
except ImportError:
    IN_DATABRICKS = False
    dbutils = None

try:
    from pyspark.sql import SparkSession
    spark: Optional[SparkSession] = None
    if IN_DATABRICKS:
        spark = SparkSession.builder.getOrCreate()
except Exception:
    spark = None


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DRIVER_ROOT = "/local_disk0"  # Databricks driver local disk
# GCP/Dataproc: local SSDs are at /mnt/disks/ssd0, ssd1, ... (375GB each). Use for large SF to avoid "No space left on device".
LOCAL_SSD_GCP = "/mnt/disks/ssd0"  # typical first local SSD on GCP VMs/Dataproc
DEFAULT_SCALE_FACTOR = 10
DEFAULT_DIGEN_PATH = "tools/datagen"
DEFAULT_UPLOAD_THREADS = 8
TPCDI_TMP = "tpcdi_tmp"
# Env var to override local generation dir (e.g. TPCDI_LOCAL_GEN_PATH=/mnt/disks/ssd0 on GCP with local SSDs)
ENV_LOCAL_GEN_PATH = "TPCDI_LOCAL_GEN_PATH"


def _repo_root() -> Path:
    """Infer repo root (script location or workspace). When in tpcdi/, use parent so tools/datagen is found."""
    if IN_DATABRICKS and dbutils:
        try:
            ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            ws = ctx.workspacePath().get()
            if ws:
                return Path(ws)
        except Exception:
            pass
        return Path.cwd()
    script_dir = Path(__file__).resolve().parent
    # If this script lives in tpcdi/, tools/datagen is at project root (parent of tpcdi)
    parent = script_dir.parent
    if (parent / "tools" / "datagen").exists():
        return parent
    return script_dir


def _default_digen_path() -> Path:
    return _repo_root() / DEFAULT_DIGEN_PATH


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _copy_directory(src: Path, dst: Path, overwrite: bool = True) -> None:
    if dst.exists() and overwrite:
        shutil.rmtree(dst)
    if not dst.exists():
        shutil.copytree(src, dst)


def _run_digen(digen_path: Path, scale_factor: int, output_path: Path) -> None:
    """Run DIGen.jar. Sets PDG_AGREE=YES to accept EULA automatically."""
    jar = digen_path / "DIGen.jar"
    if not jar.exists():
        raise FileNotFoundError(
            f"DIGen.jar not found at {jar}. "
            "Download TPC-DI Tools v1.1.0 and extract into tools/datagen/. See README.md."
        )
    pdgf = digen_path / "pdgf"
    if not pdgf.is_dir():
        raise FileNotFoundError(
            f"pdgf/ not found under {digen_path}. "
            "TPC-DI Tools must include the pdgf directory (use lowercase 'pdgf')."
        )

    output_path.mkdir(parents=True, exist_ok=True)
    cmd = f"java -jar {jar} -sf {scale_factor} -o {output_path}"
    args = shlex.split(cmd)
    
    # Set PDG_AGREE=YES to automatically accept EULA
    env = os.environ.copy()
    env['PDG_AGREE'] = 'YES'
    
    proc = subprocess.Popen(
        args,
        cwd=str(digen_path),
        env=env,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    if proc.stdout:
        for line in iter(proc.stdout.readline, ""):
            line = line.rstrip()
            if line:
                print(line)
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"DIGen exited with code {proc.returncode}")


def _copy_file_to_dbfs(src: Path, dest: Path) -> str:
    """Copy a single file to DBFS destination."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return f"Copied {src} -> {dest}"


def _upload_to_dbfs(local_dir: Path, dbfs_path: str, max_workers: int = DEFAULT_UPLOAD_THREADS) -> None:
    """Copy generated files from local_dir to DBFS using parallel threads.
    
    Args:
        local_dir: Source directory containing files to upload.
        dbfs_path: DBFS destination path (dbfs:/... or /dbfs/...).
        max_workers: Number of parallel threads for file uploads. Default: 8.
    """
    # Normalize: dbfs:/path -> /dbfs/path
    if dbfs_path.startswith("dbfs:"):
        os_path = "/dbfs" + dbfs_path[5:]
    elif dbfs_path.startswith("/dbfs"):
        os_path = dbfs_path
    else:
        os_path = "/dbfs/" + dbfs_path.lstrip("/")

    dest = Path(os_path)
    dest.mkdir(parents=True, exist_ok=True)
    
    # First, create all directories
    for root, dirs, files in os.walk(local_dir, topdown=True):
        for d in dirs:
            (dest / os.path.relpath(os.path.join(root, d), local_dir)).mkdir(
                parents=True, exist_ok=True
            )
    
    # Collect all files to copy
    file_pairs = []
    for root, _, files in os.walk(local_dir):
        for f in files:
            src = Path(root) / f
            rel = os.path.relpath(src, local_dir)
            dst = dest / rel
            file_pairs.append((src, dst))
    
    # Copy files in parallel
    if file_pairs:
        print(f"Uploading {len(file_pairs)} files to DBFS using {max_workers} threads...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_copy_file_to_dbfs, src, dst)
                for src, dst in file_pairs
            ]
            completed = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    completed += 1
                    if len(file_pairs) <= 20:  # Only print for small batches
                        print(result)
                    elif completed % 100 == 0:  # Progress update for large batches
                        print(f"Progress: {completed}/{len(file_pairs)} files uploaded...")
                except Exception as e:
                    print(f"Error copying file: {e}")
        print(f"Successfully uploaded {len(file_pairs)} files to {dbfs_path}")


def _upload_to_volume(local_dir: Path, volume_path: str) -> None:
    """Copy generated files to a Unity Catalog volume. Volume path: /Volumes/catalog/schema/vol/."""
    # /Volumes/... is mounted under /Volumes/... on the driver
    if not volume_path.startswith("/Volumes/"):
        raise ValueError("Volume path must start with /Volumes/")
    dest = Path(volume_path)
    dest.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(local_dir, topdown=True):
        for d in dirs:
            (dest / os.path.relpath(os.path.join(root, d), local_dir)).mkdir(parents=True, exist_ok=True)
        for f in files:
            src = Path(root) / f
            rel = os.path.relpath(src, local_dir)
            (dest / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest / rel)


def _upload_local(local_dir: Path, output_path: str) -> None:
    """Copy to a local path (e.g. /tmp/out)."""
    dest = Path(output_path)
    dest.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(local_dir, topdown=True):
        for d in dirs:
            (dest / os.path.relpath(os.path.join(root, d), local_dir)).mkdir(parents=True, exist_ok=True)
        for f in files:
            src = Path(root) / f
            rel = os.path.relpath(src, local_dir)
            (dest / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest / rel)


def _parse_gs_path(gs_path: str) -> tuple:
    """Return (bucket_name, prefix) for gs://bucket/prefix/path. prefix may be empty."""
    if not gs_path.startswith("gs://"):
        raise ValueError("GCS path must start with gs://")
    rest = gs_path[5:].lstrip("/")  # after gs://
    if "/" in rest:
        bucket, prefix = rest.split("/", 1)
        return bucket, prefix.rstrip("/")
    return rest, ""


def _upload_to_gcs_with_python(local_dir: Path, gs_path: str) -> None:
    """Upload directory to GCS using google-cloud-storage (when gsutil is not available, e.g. Databricks)."""
    try:
        from google.cloud import storage
    except ImportError:
        raise RuntimeError(
            "GCS upload requires gsutil or the google-cloud-storage package. "
            "Install with: pip install google-cloud-storage"
        ) from None
    bucket_name, prefix = _parse_gs_path(gs_path)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    base = str(local_dir) + os.sep
    uploaded = 0
    for root_dir, _dirs, files in os.walk(local_dir, topdown=True):
        for f in files:
            src = Path(root_dir) / f
            rel = os.path.relpath(src, local_dir)
            dest_key = f"{prefix}/{rel}" if prefix else rel
            dest_key = dest_key.replace("\\", "/")
            blob = bucket.blob(dest_key)
            blob.upload_from_filename(str(src), content_type="application/octet-stream")
            uploaded += 1
    print(f"Successfully uploaded {uploaded} file(s) to {gs_path}/")


def _upload_to_gcs(local_dir: Path, gs_path: str) -> None:
    """Upload generated files to GCS. Uses gsutil if available; else google-cloud-storage (e.g. on Databricks)."""
    if not gs_path.startswith("gs://"):
        raise ValueError("GCS path must start with gs://")
    gs_path = gs_path.rstrip("/")
    print(f"Uploading to GCS: {gs_path}/ ...")
    # Try gsutil first (common on GCE/Dataproc)
    try:
        cmd = ["gsutil", "-m", "cp", "-r", str(local_dir) + "/*", gs_path + "/"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"gsutil upload failed (exit {result.returncode}): {result.stderr or result.stdout}"
            )
        print(f"Successfully uploaded to {gs_path}/")
        return
    except FileNotFoundError:
        pass  # gsutil not found (e.g. Databricks), fall back to Python client
    _upload_to_gcs_with_python(local_dir, gs_path)


def _gcs_path_exists(gs_path: str) -> bool:
    """Return True if the GCS path exists and has objects (e.g. Batch1/). Uses gsutil or google-cloud-storage."""
    if not gs_path.startswith("gs://"):
        return False
    gs_path = gs_path.rstrip("/") + "/"
    try:
        result = subprocess.run(
            ["gsutil", "ls", gs_path],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return True
        if result.returncode != 0:
            return False
    except FileNotFoundError:
        pass  # gsutil not found, try Python client
    try:
        from google.cloud import storage
        bucket_name, prefix = _parse_gs_path(gs_path.rstrip("/"))
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        it = bucket.list_blobs(prefix=prefix, max_results=1)
        return next(iter(it), None) is not None
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def generate_tpcdi_data(
    scale_factor: int = DEFAULT_SCALE_FACTOR,
    raw_output_path: str = "dbfs:/mnt/tpcdi",
    digen_path: Optional[str] = None,
    skip_if_exists: bool = True,
    upload_threads: int = DEFAULT_UPLOAD_THREADS,
    local_gen_path: Optional[str] = None,
) -> str:
    """
    Generate TPC-DI raw data and optionally upload to DBFS, UC Volume, or GCS.

    Destination is inferred from raw_output_path:
    - dbfs:/... or /dbfs/... -> DBFS load
    - /Volumes/... -> Unity Catalog Volume load
    - gs://... -> GCS load
    - otherwise -> local directory

    Args:
        scale_factor: TPC-DI scale factor (e.g. 10 ~ 1GB, 100 ~ 10GB).
        raw_output_path: DBFS path (dbfs:/...), UC Volume (/Volumes/catalog/schema/vol), GCS (gs://...), or local dir.
        digen_path: Path to folder containing DIGen.jar and pdgf/. Default: tools/datagen.
        skip_if_exists: If output already exists, skip generation.
        upload_threads: Number of parallel threads for DBFS file uploads. Default: 8.
        local_gen_path: Local directory for generation (DIGen writes here first; then upload to raw_output_path).
            When set, used on both Databricks and GCP (driver /local_disk0 is small ~100–200GB; use this for large SF).
            GCP: e.g. /mnt/disks/ssd0. Databricks: e.g. /dbfs/tmp/tpcdi_gen (DBFS has more space). Env: TPCDI_LOCAL_GEN_PATH.

    Returns:
        Final path where data was written (DBFS, Volume, or GCS path).
    """
    root = _repo_root()
    digen = Path(digen_path) if digen_path else _default_digen_path()
    if not digen.is_absolute():
        digen = root / digen

    # Determine where to generate. Explicit local_gen_path/env is always honored (driver disk is small on Databricks).
    explicit = local_gen_path or os.environ.get(ENV_LOCAL_GEN_PATH)
    if explicit:
        base_tmp = Path(explicit).resolve() / TPCDI_TMP
        print(f"Using local generation path (large capacity): {base_tmp}")
    elif IN_DATABRICKS and Path(DRIVER_ROOT).exists():
        base_tmp = Path(DRIVER_ROOT) / "tmp" / TPCDI_TMP
        print(f"Using driver local disk (limited size; for large SF set tpcdi_local_gen_path e.g. /dbfs/tmp/tpcdi_gen): {base_tmp}")
    else:
        base_tmp = Path("/tmp") / TPCDI_TMP
    driver_tmp = base_tmp / "datagen"
    driver_out = base_tmp / f"sf={scale_factor}"

    # Copy DIGen tools to driver temp (DIGen expects to run from its directory)
    _ensure_dir(base_tmp)
    _copy_directory(digen, driver_tmp, overwrite=True)
    digen_run = driver_tmp  # run from copied location

    # Output destination: infer from path (dbfs -> DBFS, /Volumes/ -> Volume, gs:// -> GCS)
    final_dest = (raw_output_path.rstrip("/") + f"/sf={scale_factor}").replace("//", "/")
    # restore gs:// (replace("//","/") above turns gs:// into gs:/)
    if final_dest.startswith("gs:/") and not final_dest.startswith("gs://"):
        final_dest = "gs://" + final_dest[4:]

    # If Volume path, ensure catalog/schema/volume exist
    if final_dest.startswith("/Volumes/"):
        parts = final_dest.split("/")
        # /Volumes/catalog/schema/vol_name/sf=N -> catalog, schema, vol_name
        if len(parts) >= 5:
            _catalog, _schema, _vol_name = parts[2], parts[3], parts[4]
            create_volume_if_needed(_catalog, _schema, _vol_name, spark)

    # Skip if exists
    if skip_if_exists:
        if final_dest.startswith("dbfs:"):
            check_path = "/dbfs" + final_dest[5:]
            if os.path.exists(check_path) and os.listdir(check_path):
                print(f"Output {final_dest} already exists; skipping generation.")
                return final_dest
        elif final_dest.startswith("/Volumes/"):
            if Path(final_dest).exists() and any(Path(final_dest).iterdir()):
                print(f"Output {final_dest} already exists; skipping generation.")
                return final_dest
        elif final_dest.startswith("gs://") or final_dest.startswith("gs:/"):
            gs_check = ("gs://" + final_dest[4:]) if (final_dest.startswith("gs:/") and not final_dest.startswith("gs://")) else final_dest
            if _gcs_path_exists(gs_check):
                print(f"Output {gs_check} already exists; skipping generation.")
                return gs_check
        elif os.path.isdir(final_dest) and os.listdir(final_dest):
            print(f"Output {final_dest} already exists; skipping generation.")
            return final_dest

    print(f"Generating TPC-DI data (scale factor={scale_factor}) into {driver_out}")
    _run_digen(digen_run, scale_factor, driver_out)
    print("Generation complete. Uploading to destination...")

    if final_dest.startswith("/Volumes/"):
        _upload_to_volume(driver_out, final_dest)
    elif final_dest.startswith("dbfs:") or final_dest.startswith("/dbfs"):
        dbfs_arg = ("dbfs:" + final_dest[5:]) if final_dest.startswith("/dbfs") else final_dest
        _upload_to_dbfs(driver_out, dbfs_arg, max_workers=upload_threads)
    elif final_dest.startswith("gs://") or final_dest.startswith("gs:/"):
        # Normalize gs:/ to gs:// so we never treat GCS as a local path (e.g. under Workspace)
        if final_dest.startswith("gs:/") and not final_dest.startswith("gs://"):
            final_dest = "gs://" + final_dest[4:]
        _upload_to_gcs(driver_out, final_dest)
    else:
        _upload_local(driver_out, final_dest)

    print(f"Done. Data written to {final_dest}")
    return final_dest


def create_volume_if_needed(
    catalog: str, schema: str, volume_name: str, spark_session: Optional[SparkSession]
) -> None:
    """Ensure catalog exists; create schema and volume if they do not exist.
    Does not create the catalog — if it does not exist, exits gracefully with an error.
    """
    if not IN_DATABRICKS or spark_session is None:
        return
    # Check if catalog exists; do not create it
    try:
        existing = [row.catalog for row in spark_session.sql("SHOW CATALOGS").collect()]
        if catalog not in existing:
            msg = (
                f"Catalog '{catalog}' does not exist. "
                "Please create the catalog (e.g. in Data > Catalogs) and retry."
            )
            print(f"ERROR: {msg}")
            raise RuntimeError(msg)
    except RuntimeError:
        raise
    except Exception as e:
        print(f"ERROR: Could not check catalog existence: {e}")
        raise RuntimeError(
            f"Could not verify catalog '{catalog}'. Please ensure the catalog exists."
        ) from e
    spark_session.sql(
        f"CREATE DATABASE IF NOT EXISTS {catalog}.{schema} "
        "COMMENT 'Schema for TPC-DI Raw Files Volume'"
    )
    spark_session.sql(
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name} "
        "COMMENT 'TPC-DI Raw Files'"
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate TPC-DI benchmark data (Databricks-ready).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument(
        "-s", "--scale-factor",
        type=int,
        default=DEFAULT_SCALE_FACTOR,
        help="TPC-DI scale factor (e.g. 10 ~ 1GB)",
    )
    ap.add_argument(
        "-o", "--output", "--raw-output-path",
        dest="raw_output_path",
        default="dbfs:/mnt/tpcdi",
        help="Raw output path: dbfs:/..., /Volumes/cat/schema/vol, gs://bucket/path, or local dir",
    )
    ap.add_argument(
        "-d", "--digen-path",
        default=None,
        help="Path to DIGen (DIGen.jar + pdgf/). Default: tools/datagen",
    )
    ap.add_argument(
        "--upload-threads",
        type=int,
        default=DEFAULT_UPLOAD_THREADS,
        help=f"Number of parallel threads for DBFS file uploads (default: {DEFAULT_UPLOAD_THREADS})",
    )
    ap.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Always regenerate even if output exists",
    )
    ap.add_argument(
        "--local-gen-path",
        dest="local_gen_path",
        default=None,
        help=f"Local directory for datagen output (avoids filling /tmp). On GCP with local SSDs use e.g. /mnt/disks/ssd0. Env: {ENV_LOCAL_GEN_PATH}",
    )
    args = ap.parse_args()

    generate_tpcdi_data(
        scale_factor=args.scale_factor,
        raw_output_path=args.raw_output_path,
        digen_path=args.digen_path,
        skip_if_exists=not args.no_skip_existing,
        upload_threads=args.upload_threads,
        local_gen_path=args.local_gen_path,
    )


if __name__ == "__main__":
    main()
    sys.exit(0)
