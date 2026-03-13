"""
Bronze layer loader for HoldingHistory.txt.

Ingests raw pipe-delimited holding history data without parsing.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeHoldingHistory(BronzeLoaderBase):
    """
    Bronze layer loader for HoldingHistory.txt.
    
    HoldingHistory.txt is pipe-delimited holdings data.
    
    At Bronze layer, we store each line as a raw string.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest HoldingHistory.txt as raw pipe-delimited data.
        
        Args:
            batch_id: Batch number
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if file not found
        """
        logger.info(f"Loading bronze_holding_history from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/HoldingHistory.txt"
        
        try:
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "HoldingHistory.txt")
            
        except Exception as e:
            logger.warning(f"HoldingHistory.txt not found for Batch{batch_id}: {e}")
            return None
