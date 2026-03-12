"""
Table-level timing for ETL load tracking.

Tracks start/end time, row count, and optional bytes per table. When log_detailed_stats
is True, emits per-table and job-level performance: duration, rows, bytes, rows/sec, MB/sec.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# In-memory collector (single process)
_job_start_time: Optional[float] = None
_job_end_time: Optional[float] = None
_table_starts: Dict[str, float] = {}  # table_name -> start_time
_table_records: List[Dict] = []  # list of {table, start_time, end_time, duration_sec, row_count, bytes_processed?}
_log_detailed_stats: bool = False  # When False, only log job start/end/total duration for performance comparison


def configure(log_detailed_stats: bool = False):
    """Set whether to log detailed per-table stats. Call before ETL (e.g. from runner)."""
    global _log_detailed_stats
    _log_detailed_stats = log_detailed_stats


def is_detailed() -> bool:
    """True if detailed stats (per-table timing, records) should be logged."""
    return _log_detailed_stats


def clear():
    """Reset collector for a new job."""
    global _job_start_time, _job_end_time, _table_starts, _table_records
    _job_start_time = None
    _job_end_time = None
    _table_starts = {}
    _table_records = []


def set_job_start():
    """Record job start time. Call at the beginning of ETL. Always logged for performance comparison."""
    global _job_start_time
    _job_start_time = time.time()
    logger.info(f"[JOB] Job started at {datetime.fromtimestamp(_job_start_time).strftime('%Y-%m-%d %H:%M:%S')}")


def set_job_end():
    """Record job end time. Call at the end of ETL. Always logged for performance comparison."""
    global _job_end_time
    _job_end_time = time.time()
    logger.info(f"[JOB] Job ended at {datetime.fromtimestamp(_job_end_time).strftime('%Y-%m-%d %H:%M:%S')}")


def start_table(table_name: str):
    """Record start of table-specific work. Call when processing for this table begins."""
    _table_starts[table_name] = time.time()
    if _log_detailed_stats:
        start_dt = datetime.fromtimestamp(_table_starts[table_name]).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"[TIMING] Table load started: {table_name} at {start_dt}")


def end_table(table_name: str, row_count: int, bytes_processed: Optional[int] = None):
    """Record end of table load (after write). Optionally pass bytes written for throughput stats."""
    end_time = time.time()
    start_time = _table_starts.get(table_name)
    if start_time is None:
        logger.warning(f"[TIMING] end_table called for {table_name} without start_table; using write duration only")
        start_time = end_time  # will show 0 duration for this table
    duration = end_time - start_time
    start_dt = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
    end_dt = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
    _table_records.append({
        "table": table_name,
        "start_time": start_time,
        "end_time": end_time,
        "start_datetime": start_dt,
        "end_datetime": end_dt,
        "duration_seconds": duration,
        "row_count": row_count,
        "bytes_processed": bytes_processed,
    })
    if _log_detailed_stats:
        extra = f", Bytes: {bytes_processed / (1024 * 1024):.2f} MB" if bytes_processed else ""
        rows_sec = row_count / duration if duration > 0 else 0
        mb_sec = (bytes_processed / (1024 * 1024)) / duration if bytes_processed and duration > 0 else None
        throughput = f", Throughput: {rows_sec:.1f} rows/s"
        if mb_sec is not None:
            throughput += f", {mb_sec:.2f} MB/s"
        logger.info(f"[TIMING] Table load completed: {table_name} - Start: {start_dt}, End: {end_dt}, Duration: {duration:.2f}s, Rows: {row_count}{extra}{throughput}")
    # Remove so we don't leak if same table loaded again
    _table_starts.pop(table_name, None)


def get_summary() -> Dict:
    """Return job and table summary (for programmatic use)."""
    job_start = _job_start_time
    job_end = _job_end_time or time.time()
    total_duration = (job_end - job_start) if job_start else 0.0
    total_bytes = sum(r.get("bytes_processed") or 0 for r in _table_records)
    return {
        "job_start_time": job_start,
        "job_end_time": job_end,
        "job_start_datetime": datetime.fromtimestamp(job_start).strftime("%Y-%m-%d %H:%M:%S") if job_start else None,
        "job_end_datetime": datetime.fromtimestamp(job_end).strftime("%Y-%m-%d %H:%M:%S") if job_end else None,
        "total_duration_seconds": total_duration,
        "tables_loaded": [r["table"] for r in _table_records],
        "table_details": _table_records,
        "total_records_loaded": sum(r["row_count"] for r in _table_records),
        "total_bytes_processed": total_bytes,
    }


def log_final_summary():
    """
    Emit final job summary log.
    - Always: job start time, end time, total duration (for performance comparison).
    - If log_detailed_stats is True: per-table duration, rows, bytes (MB), rows/s, MB/s;
      job-level totals and overall throughput.
    """
    summary = get_summary()
    job_start_dt = summary["job_start_datetime"] or "N/A"
    job_end_dt = summary["job_end_datetime"] or "N/A"
    total_dur = summary["total_duration_seconds"]

    logger.info("=" * 60)
    logger.info("[JOB SUMMARY]")
    logger.info(f"  Job start time:     {job_start_dt}")
    logger.info(f"  Job end time:       {job_end_dt}")
    logger.info(f"  Total duration:    {total_dur:.2f}s")

    if _log_detailed_stats:
        tables = summary["tables_loaded"]
        details = summary["table_details"]
        total_rows = summary["total_records_loaded"]
        total_bytes = summary.get("total_bytes_processed") or 0
        total_mb = total_bytes / (1024 * 1024)
        rows_per_sec = total_rows / total_dur if total_dur > 0 else 0
        mb_per_sec = total_mb / total_dur if total_dur > 0 and total_bytes else 0

        logger.info(f"  Tables loaded:      {len(tables)}")
        logger.info(f"  Total records:      {total_rows:,}")
        logger.info(f"  Total data size:    {total_mb:.2f} MB")
        logger.info(f"  Overall throughput: {rows_per_sec:,.1f} rows/s, {mb_per_sec:.2f} MB/s")
        logger.info("  Per-table (duration, rows, size, throughput):")
        for d in details:
            dur = d["duration_seconds"]
            rows = d["row_count"]
            b = d.get("bytes_processed")
            row_s = rows / dur if dur > 0 else 0
            mb_s = (b / (1024 * 1024)) / dur if b and dur > 0 else None
            size_str = f", {b / (1024 * 1024):.2f} MB" if b else ""
            tp_str = f", {row_s:,.1f} rows/s" + (f", {mb_s:.2f} MB/s" if mb_s is not None else "")
            logger.info(f"    - {d['table']}: {dur:.2f}s, {rows:,} rows{size_str}{tp_str}")

    logger.info("=" * 60)
