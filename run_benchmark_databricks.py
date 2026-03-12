#!/usr/bin/env python3
"""
Databricks benchmark runner for TPC-DI.
Run this script on Databricks to benchmark ETL performance.
"""

import sys
from pathlib import Path

# Add benchmark module to path
sys.path.insert(0, str(Path(__file__).parent))

from benchmark.config import BenchmarkConfig, Platform, LoadType
from benchmark.runner import run_benchmark

# Example configuration for Databricks
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run TPC-DI benchmark on Databricks")
    parser.add_argument("--load-type", choices=["batch", "incremental"],
                       help="Type of load: batch or incremental (required unless --list-node-types)")
    parser.add_argument("--scale-factor", type=int,
                       help="TPC-DI scale factor (e.g., 10, 100, 1000) (required unless --list-node-types)")
    parser.add_argument("--output-path", default="dbfs:/mnt/tpcdi",
                       help="Raw data location: DBFS or Volume base path (default: dbfs:/mnt/tpcdi)")
    parser.add_argument("--cloud", choices=["AWS", "GCP", "Azure"],
                       help="Cloud provider (used to show allowed instance types)")
    parser.add_argument("--list-node-types", action="store_true",
                       help="Print allowed instance types per cloud and exit (use with or without --cloud)")
    parser.add_argument("--target-schema", default="dw",
                       help="Target schema name (default: dw)")
    parser.add_argument("--target-catalog", required=True,
                       help="Unity Catalog name (required for Databricks)")
    parser.add_argument("--batch-id", type=int,
                       help="Batch ID for incremental loads (required for incremental)")
    parser.add_argument("--gcs-bucket",
                       help="GCS bucket for metrics/data (optional; used on GCP Databricks)")
    parser.add_argument("--metrics-output",
                       help="Path to save metrics JSON (default: auto-detected based on platform)")
    parser.add_argument("--log-detailed-stats", action="store_true", default=True,
                       help="Log per-table timing and records (default: True)")
    parser.add_argument("--no-log-detailed-stats", dest="log_detailed_stats", action="store_false",
                       help="Disable per-table timing; only job start/end/total duration")
    parser.add_argument("--cluster-instance-type",
                       help="Worker node type for metrics (e.g. i3.xlarge). If omitted, auto-detected from cluster tags when available.")
    parser.add_argument("--cluster-worker-count", type=int,
                       help="Number of worker instances for metrics. If omitted, auto-detected from Spark executors.")
    parser.add_argument("--cluster-master-type",
                       help="Driver node type for metrics (optional).")

    args = parser.parse_args()

    # Allowed instance types per cloud (must match create_workflow_notebook / create_databricks_workflow)
    CLOUD_NODE_OPTIONS = {
        "AWS": ["i3.xlarge", "i3.2xlarge", "i3.4xlarge", "m5d.xlarge", "m5d.2xlarge", "m5d.4xlarge", "r5d.xlarge", "r5d.2xlarge", "r5d.4xlarge"],
        "GCP": ["c2-standard-4", "c2-standard-8", "c2-standard-16", "c2-standard-30"] + [f"n2d-standard-{n}" for n in [4, 8, 16, 32, 48, 64, 80, 96]] + [f"n2d-highmem-{n}" for n in [4, 8, 16, 32, 48, 64, 80, 96]],
        "Azure": ["Standard_E4s_v3", "Standard_E8s_v3", "Standard_E16s_v3", "Standard_E32s_v3", "Standard_D4s_v3", "Standard_D8s_v3", "Standard_D16s_v3", "Standard_D32s_v3", "Standard_L4s_v2", "Standard_L8s_v2", "Standard_L16s_v2", "Standard_L32s_v2"],
    }
    DEFAULT_NODE_TYPES = {"AWS": ("i3.xlarge", "i3.xlarge"), "GCP": ("c2-standard-16", "c2-standard-16"), "Azure": ("Standard_E8s_v3", "Standard_E8s_v3")}

    if args.list_node_types:
        clouds = [args.cloud] if args.cloud else ["AWS", "GCP", "Azure"]
        for c in clouds:
            opts = CLOUD_NODE_OPTIONS[c]
            default = DEFAULT_NODE_TYPES[c][0]
            print(f"{c}: {', '.join(opts)} (default: {default})")
        return 0

    if not args.load_type or args.scale_factor is None:
        parser.error("--load-type and --scale-factor are required (unless using --list-node-types)")
    if args.cloud:
        print(f"Cloud: {args.cloud} | Recommended: Worker/Driver = {DEFAULT_NODE_TYPES[args.cloud][0]} | Allowed: {', '.join(CLOUD_NODE_OPTIONS[args.cloud])}")
    
    # output_path = raw data base; runner appends /sf={scale_factor}
    if not args.target_catalog:
        print("ERROR: --target-catalog is required for Databricks platform (Unity Catalog)", file=sys.stderr)
        sys.exit(1)
    
    config = BenchmarkConfig(
        platform=Platform.DATABRICKS,
        load_type=LoadType(args.load_type),
        scale_factor=args.scale_factor,
        raw_data_path=args.output_path,
        target_schema=args.target_schema,
        target_catalog=args.target_catalog,
        output_path=args.output_path,
        batch_id=args.batch_id,
        gcs_bucket=args.gcs_bucket,
        metrics_output_path=args.metrics_output,
        log_detailed_stats=args.log_detailed_stats,
        cluster_instance_type=args.cluster_instance_type,
        cluster_worker_count=args.cluster_worker_count,
        cluster_master_type=args.cluster_master_type,
        cloud=args.cloud,
    )
    
    result = run_benchmark(config)
    
    print("\n" + "="*80)
    print("BENCHMARK RESULTS")
    print("="*80)
    print(f"Platform: {result['config']['platform']}")
    if result['metrics'].get('databricks_compute_type'):
        print(f"Compute: {result['metrics']['databricks_compute_type']}")
    print(f"Load Type: {result['config']['load_type']}")
    print(f"Scale Factor: {result['config']['scale_factor']}")
    if result['config']['batch_id']:
        print(f"Batch ID: {result['config']['batch_id']}")

    # Cluster configuration
    metrics_dict = result['metrics']
    if metrics_dict.get('cluster_instance_type') or metrics_dict.get('cluster_worker_count') or metrics_dict.get('cluster_master_type'):
        print(f"\nCluster Configuration:")
        if metrics_dict.get('cluster_instance_type'):
            print(f"  Worker Node Type: {metrics_dict['cluster_instance_type']}")
        if metrics_dict.get('cluster_master_type'):
            print(f"  Driver Node Type: {metrics_dict['cluster_master_type']}")
        if metrics_dict.get('cluster_worker_count') is not None:
            print(f"  Number of Worker Nodes: {metrics_dict['cluster_worker_count']}")
    
    # Table override information
    if metrics_dict.get('table_override') is not None:
        print(f"\nTable Override: {metrics_dict['table_override']}")

    print(f"\nTotal Duration: {result['metrics']['total_duration_seconds']:.2f} seconds")
    if result['metrics']['summary']:
        print(f"Total Rows Processed: {result['metrics']['summary']['total_rows_processed']:,}")
        print(f"Throughput: {result['metrics']['summary']['throughput_rows_per_second']:.2f} rows/sec")
        print(f"Data Size: {result['metrics']['summary']['total_bytes_processed'] / (1024*1024):.2f} MB")
    dq_timings = result['metrics'].get('dq_table_timings')
    if dq_timings:
        n_tables = len(dq_timings)
        print(f"\nDQ time per table ({n_tables} tables):")
        for t in dq_timings:
            print(f"  {t['table']}: {t['duration_seconds']:.2f}s")
        total_dq = sum(t['duration_seconds'] for t in dq_timings)
        print(f"  Total DQ: {total_dq:.2f}s")
    # Cost (estimated; list-price approximation)
    cb = result['metrics'].get('cost_breakdown')
    total_cost = result['metrics'].get('total_cost_usd')
    if cb is not None or total_cost is not None:
        print("\nCost (estimated):")
        if cb:
            if cb.get('compute_usd') is not None and (cb.get('compute_usd') or 0) > 0:
                print(f"  Compute: ${cb['compute_usd']:.2f}")
            if cb.get('software_usd') is not None:
                print(f"  Software: ${cb['software_usd']:.2f}")
        if total_cost is not None:
            print(f"  Total cost: ${total_cost:.2f}")
    if args.log_detailed_stats:
        try:
            from benchmark.etl.table_timing import get_summary as get_table_summary
            tsum = get_table_summary()
            details = tsum.get("table_details") or []
            if details:
                total_rows = tsum.get("total_records_loaded") or 0
                total_bytes = tsum.get("total_bytes_processed") or 0
                total_dur = tsum.get("total_duration_seconds") or 0
                total_mb = total_bytes / (1024 * 1024)
                rows_per_sec = total_rows / total_dur if total_dur > 0 else 0
                mb_per_sec = total_mb / total_dur if total_dur > 0 and total_bytes else 0
                print("\nTable-level stats:")
                print(f"  Tables loaded:      {len(details)}")
                print(f"  Total records:      {total_rows:,}")
                print(f"  Total data size:    {total_mb:.2f} MB")
                print(f"  Overall throughput: {rows_per_sec:,.1f} rows/s, {mb_per_sec:.2f} MB/s")
                print("  Per-table (duration, rows, size, throughput):")
                for d in details:
                    dur = d.get("duration_seconds") or 0
                    rows = d.get("row_count") or 0
                    b = d.get("bytes_processed")
                    row_s = rows / dur if dur > 0 else 0
                    mb_s = (b / (1024 * 1024)) / dur if b and dur > 0 else None
                    size_str = f", {b / (1024 * 1024):.2f} MB" if b else ""
                    tp_str = f", {row_s:,.1f} rows/s" + (f", {mb_s:.2f} MB/s" if mb_s is not None else "")
                    print(f"    - {d.get('table', '?')}: {dur:.2f}s, {rows:,} rows{size_str}{tp_str}")
        except Exception as e:
            print(f"\n  (Table-level stats unavailable: {e})")
    # Save metrics after result summary (so JSON is written after the summary is printed)
    save_fn = result.pop("_save_metrics", None)
    if save_fn:
        save_fn()
    print("="*80)
