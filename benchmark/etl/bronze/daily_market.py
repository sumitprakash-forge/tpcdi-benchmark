"""
Bronze layer loader for DailyMarket.txt.

Ingests raw pipe-delimited daily market data without parsing.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeDailyMarket(BronzeLoaderBase):
    """
    Bronze layer loader for DailyMarket.txt.
    
    DailyMarket.txt is pipe-delimited with 6 columns:
    DM_DATE|DM_S_SYMB|DM_CLOSE|DM_HIGH|DM_LOW|DM_VOL
    
    At Bronze layer, we store each line as a raw string.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest DailyMarket.txt as raw pipe-delimited data.
        
        Args:
            batch_id: Batch number
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if file not found
        """
        logger.info(f"Loading bronze_daily_market from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/DailyMarket.txt"
        
        try:
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "DailyMarket.txt")
            
        except Exception as e:
            logger.warning(f"DailyMarket.txt not found for Batch{batch_id}: {e}")
            return None
