"""
Bronze layer loader for Trade.txt.

Ingests raw pipe-delimited trade data without parsing.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeTrade(BronzeLoaderBase):
    """
    Bronze layer loader for Trade.txt.
    
    Trade.txt is pipe-delimited with 14 columns per TPC-DI spec.
    At Bronze layer, we store each line as a raw string.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest Trade.txt as raw pipe-delimited data.
        
        Args:
            batch_id: Batch number
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if file not found
        """
        logger.info(f"Loading bronze_trade from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/Trade.txt"
        
        try:
            # Read as text (raw lines)
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "Trade.txt")
            
        except Exception as e:
            logger.warning(f"Trade.txt not found for Batch{batch_id}: {e}")
            return None
