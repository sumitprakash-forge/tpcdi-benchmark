#!/usr/bin/env python3
"""
Aggregate TPC-DI benchmark metrics from multiple runs (Databricks, Dataproc) into one CSV or JSON.

Usage:
  python scripts/aggregate_metrics.py --input ./metrics --output ./comparison.csv
  python scripts/aggregate_metrics.py --input /path/to/metrics --output ./comparison.json --format json

Input: directory containing metrics_*.json files (e.g. from metrics_output_path).
Output: single CSV (one row per run) or JSON array for easy comparison.
"""

import argparse
import csv
import json
import sys
from pathlib import Path


def load_metrics_file(path: Path) -> dict:
    """Load a single metrics JSON file."""
    with open(path, "r") as f:
        return json.load(f)


def row_from_metrics(data: dict, source_file: str) -> dict:
    """Build a flat row for CSV from a metrics dict."""
    summary = data.get("summary") or {}
    return {
        "source_file": source_file,
        "platform": data.get("platform", ""),
        "load_type": data.get("load_type", ""),
        "scale_factor": data.get("scale_factor", ""),
        "batch_id": data.get("batch_id", ""),
        "start_time_iso": data.get("start_time_iso", ""),
        "total_duration_seconds": data.get("total_duration_seconds"),
        "total_steps": summary.get("total_steps"),
        "completed_steps": summary.get("completed_steps"),
        "failed_steps": summary.get("failed_steps"),
        "total_rows_processed": summary.get("total_rows_processed"),
        "total_bytes_processed": summary.get("total_bytes_processed"),
        "throughput_rows_per_second": summary.get("throughput_rows_per_second"),
        "throughput_mb_per_second": summary.get("throughput_mb_per_second"),
        "cluster_instance_type": data.get("cluster_instance_type", ""),
        "cluster_worker_count": data.get("cluster_worker_count", ""),
        "cluster_master_type": data.get("cluster_master_type", ""),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate TPC-DI benchmark metrics (Databricks + Dataproc) into one file."
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Directory containing metrics_*.json files",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output file path (CSV or JSON)",
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=["csv", "json"],
        default="csv",
        help="Output format: csv (one row per run) or json (array of run dicts)",
    )
    args = parser.parse_args()

    input_dir = Path(args.input)
    if not input_dir.is_dir():
        print(f"Error: not a directory: {input_dir}", file=sys.stderr)
        sys.exit(1)

    files = sorted(input_dir.glob("metrics_*.json"))
    if not files:
        print(f"No metrics_*.json files in {input_dir}", file=sys.stderr)
        sys.exit(1)

    rows = []
    for path in files:
        try:
            data = load_metrics_file(path)
            rows.append(row_from_metrics(data, path.name))
        except Exception as e:
            print(f"Warning: skip {path}: {e}", file=sys.stderr)

    if not rows:
        print("No valid metrics loaded.", file=sys.stderr)
        sys.exit(1)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "csv":
        fieldnames = list(rows[0].keys())
        with open(output_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} runs to {output_path}")
    else:
        with open(output_path, "w") as f:
            json.dump(rows, f, indent=2)
        print(f"Wrote {len(rows)} runs to {output_path}")


if __name__ == "__main__":
    main()
