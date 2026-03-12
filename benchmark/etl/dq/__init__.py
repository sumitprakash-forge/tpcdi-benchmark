"""
Data Quality (DQ) module for TPC-DI benchmark.

- TPC-DI validation rules at Silver (nulls, value ranges, RI, formats).
- Failures logged to gold.dim_messages (DimMessages audit table).
- Late-arriving dimension handling in Gold (placeholder rows + flag).
"""

from benchmark.etl.dq.dim_messages import ensure_dim_messages_exists, log_message, log_messages
from benchmark.etl.dq.silver_rules import SilverDQRunner

__all__ = [
    "ensure_dim_messages_exists",
    "log_message",
    "log_messages",
    "SilverDQRunner",
]
