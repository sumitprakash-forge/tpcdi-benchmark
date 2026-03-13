"""
Bronze layer loader for Prospect.csv.

Ingests raw CSV prospect/marketing data without parsing.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeProspect(BronzeLoaderBase):
    """
    Bronze layer loader for Prospect.csv.
    
    Prospect.csv is comma-delimited marketing data.
    
    At Bronze layer, we store each line as a raw string.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest Prospect.csv as raw CSV data.
        
        Args:
            batch_id: Batch number
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if file not found
        """
        logger.info(f"Loading bronze_prospect from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/Prospect.csv"
        
        try:
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "Prospect.csv")
            
        except Exception as e:
            logger.warning(f"Prospect.csv not found for Batch{batch_id}: {e}")
            return None
