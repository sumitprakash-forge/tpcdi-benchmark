"""
Bronze layer loader for HR.csv.

Ingests raw CSV employee data without parsing.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeHR(BronzeLoaderBase):
    """
    Bronze layer loader for HR.csv.
    
    HR.csv is comma-delimited employee data.
    Only present in Batch1 (historical load).
    
    At Bronze layer, we store each line as a raw string.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest HR.csv as raw CSV data.
        
        Args:
            batch_id: Batch number
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if file not found
        """
        logger.info(f"Loading bronze_hr from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/HR.csv"
        
        try:
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "HR.csv")
            
        except Exception as e:
            logger.warning(f"HR.csv not found for Batch{batch_id}: {e}")
            return None
