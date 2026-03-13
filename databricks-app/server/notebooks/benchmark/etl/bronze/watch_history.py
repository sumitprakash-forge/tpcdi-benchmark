"""
Bronze layer loader for WatchHistory.txt.

Ingests raw pipe-delimited watch list data without parsing.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeWatchHistory(BronzeLoaderBase):
    """
    Bronze layer loader for WatchHistory.txt.
    
    WatchHistory.txt is pipe-delimited watch list data.
    
    At Bronze layer, we store each line as a raw string.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest WatchHistory.txt as raw pipe-delimited data.
        
        Args:
            batch_id: Batch number
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if file not found
        """
        logger.info(f"Loading bronze_watch_history from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/WatchHistory.txt"
        
        try:
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "WatchHistory.txt")
            
        except Exception as e:
            logger.warning(f"WatchHistory.txt not found for Batch{batch_id}: {e}")
            return None
