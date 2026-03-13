"""
Spark version compatibility shims.

Use these instead of importing version-specific functions directly, so the
benchmark runs on older Dataproc/Spark (e.g. 3.3) and newer runtimes (3.5+).
"""

try:
    from pyspark.sql.functions import try_to_date
except ImportError:
    # try_to_date was added in Spark 3.5; on older Spark use to_date (returns NULL on invalid input)
    from pyspark.sql.functions import to_date as try_to_date

__all__ = ["try_to_date"]
