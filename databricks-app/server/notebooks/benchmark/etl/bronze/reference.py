"""
Bronze layer loaders for reference data files.

Includes: Date, Time, StatusType, TaxRate, TradeType, Industry
"""

import logging
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeDate(BronzeLoaderBase):
    """Bronze layer loader for Date.txt (reference data, Batch1 only)."""
    
    def load(self, target_table: str) -> DataFrame:
        """Ingest Date.txt as raw pipe-delimited data."""
        logger.info("Loading bronze_date from Batch1")
        
        df = self.platform.read_raw_file("Batch1/Date.txt", format="text")
        bronze_df = df.withColumnRenamed("value", "raw_line")
        
        return self._write_bronze_table(bronze_df, target_table, 1, "Date.txt")


class BronzeTime(BronzeLoaderBase):
    """Bronze layer loader for Time.txt (reference data, Batch1 only)."""
    
    def load(self, target_table: str) -> DataFrame:
        """Ingest Time.txt as raw pipe-delimited data."""
        logger.info("Loading bronze_time from Batch1")
        
        df = self.platform.read_raw_file("Batch1/Time.txt", format="text")
        bronze_df = df.withColumnRenamed("value", "raw_line")
        
        return self._write_bronze_table(bronze_df, target_table, 1, "Time.txt")


class BronzeStatusType(BronzeLoaderBase):
    """Bronze layer loader for StatusType.txt (reference data, Batch1 only)."""
    
    def load(self, target_table: str) -> DataFrame:
        """Ingest StatusType.txt as raw pipe-delimited data."""
        logger.info("Loading bronze_status_type from Batch1")
        
        df = self.platform.read_raw_file("Batch1/StatusType.txt", format="text")
        bronze_df = df.withColumnRenamed("value", "raw_line")
        
        return self._write_bronze_table(bronze_df, target_table, 1, "StatusType.txt")


class BronzeTaxRate(BronzeLoaderBase):
    """Bronze layer loader for TaxRate.txt (reference data, Batch1 only)."""
    
    def load(self, target_table: str) -> DataFrame:
        """Ingest TaxRate.txt as raw pipe-delimited data."""
        logger.info("Loading bronze_tax_rate from Batch1")
        
        df = self.platform.read_raw_file("Batch1/TaxRate.txt", format="text")
        bronze_df = df.withColumnRenamed("value", "raw_line")
        
        return self._write_bronze_table(bronze_df, target_table, 1, "TaxRate.txt")


class BronzeTradeType(BronzeLoaderBase):
    """Bronze layer loader for TradeType.txt (reference data, Batch1 only)."""
    
    def load(self, target_table: str) -> DataFrame:
        """Ingest TradeType.txt as raw pipe-delimited data."""
        logger.info("Loading bronze_trade_type from Batch1")
        
        df = self.platform.read_raw_file("Batch1/TradeType.txt", format="text")
        bronze_df = df.withColumnRenamed("value", "raw_line")
        
        return self._write_bronze_table(bronze_df, target_table, 1, "TradeType.txt")


class BronzeIndustry(BronzeLoaderBase):
    """Bronze layer loader for Industry.txt (reference data, Batch1 only)."""
    
    def load(self, target_table: str) -> DataFrame:
        """Ingest Industry.txt as raw pipe-delimited data."""
        logger.info("Loading bronze_industry from Batch1")
        
        df = self.platform.read_raw_file("Batch1/Industry.txt", format="text")
        bronze_df = df.withColumnRenamed("value", "raw_line")
        
        return self._write_bronze_table(bronze_df, target_table, 1, "Industry.txt")
