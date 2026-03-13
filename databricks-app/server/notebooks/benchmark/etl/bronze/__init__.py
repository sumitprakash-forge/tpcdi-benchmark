"""
Bronze Layer ETL - Raw Data Ingestion for TPC-DI Benchmark.

The Bronze layer captures raw source data exactly as-is, with minimal transformation:
- Add _load_timestamp (when data was ingested)
- Add _source_file (which file the data came from)
- Add _batch_id (which batch the data belongs to)

No parsing, no type conversions - just raw capture.
"""

import logging
from typing import TYPE_CHECKING, Optional

# Import individual loaders
from benchmark.etl.bronze.customer_mgmt import BronzeCustomerMgmt
from benchmark.etl.bronze.customer import BronzeCustomer
from benchmark.etl.bronze.account import BronzeAccount
from benchmark.etl.bronze.finwire import BronzeFinwire
from benchmark.etl.bronze.trade import BronzeTrade
from benchmark.etl.bronze.daily_market import BronzeDailyMarket
from benchmark.etl.bronze.hr import BronzeHR
from benchmark.etl.bronze.prospect import BronzeProspect
from benchmark.etl.bronze.cash_transaction import BronzeCashTransaction
from benchmark.etl.bronze.holding_history import BronzeHoldingHistory
from benchmark.etl.bronze.watch_history import BronzeWatchHistory
from benchmark.etl.bronze.reference import (
    BronzeDate, BronzeTime, BronzeStatusType, 
    BronzeTaxRate, BronzeTradeType, BronzeIndustry
)
from benchmark.etl.table_timing import start_table as table_timing_start

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

logger = logging.getLogger(__name__)

# Export all loader classes
__all__ = [
    "BronzeETL",
    "BronzeCustomerMgmt",
    "BronzeCustomer",
    "BronzeAccount",
    "BronzeFinwire",
    "BronzeTrade",
    "BronzeDailyMarket",
    "BronzeHR",
    "BronzeProspect",
    "BronzeCashTransaction",
    "BronzeHoldingHistory",
    "BronzeWatchHistory",
    "BronzeDate",
    "BronzeTime",
    "BronzeStatusType",
    "BronzeTaxRate",
    "BronzeTradeType",
    "BronzeIndustry",
]


class BronzeETL:
    """
    Bronze Layer ETL orchestrator for TPC-DI.
    
    Coordinates all Bronze layer loaders to ingest raw source data.
    """
    
    def __init__(self, platform):
        """
        Initialize Bronze ETL orchestrator.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        
        # Initialize all loaders
        self.customer_mgmt = BronzeCustomerMgmt(platform)  # Batch 1 only (XML)
        self.customer = BronzeCustomer(platform)  # Batch 2+ only (pipe-delimited)
        self.account = BronzeAccount(platform)  # Batch 2+ only (pipe-delimited)
        self.finwire = BronzeFinwire(platform)
        self.trade = BronzeTrade(platform)
        self.daily_market = BronzeDailyMarket(platform)
        self.hr = BronzeHR(platform)
        self.prospect = BronzeProspect(platform)
        self.cash_transaction = BronzeCashTransaction(platform)
        self.holding_history = BronzeHoldingHistory(platform)
        self.watch_history = BronzeWatchHistory(platform)
        self.date = BronzeDate(platform)
        self.time = BronzeTime(platform)
        self.status_type = BronzeStatusType(platform)
        self.tax_rate = BronzeTaxRate(platform)
        self.trade_type = BronzeTradeType(platform)
        self.industry = BronzeIndustry(platform)
        
        logger.info("Initialized BronzeETL orchestrator")
    
    def run_bronze_batch_load(
        self,
        batch_id: int,
        target_database: str,
        target_schema: str,
        customer_mgmt_xml_format: Optional[str] = None,
        skip_bronze_customer_mgmt: bool = False,
        only_bronze_customer_mgmt: bool = False,
    ):
        """
        Run full Bronze layer load for a batch.

        Args:
            batch_id: Batch number (1 for historical, 2+ for incremental)
            target_database: Target database/catalog name
            target_schema: Target schema name
            customer_mgmt_xml_format: Spark XML format: "org.apache.spark.sql.execution.datasources.xml" (Databricks native), or "xml"/"com.databricks.spark.xml" (spark-xml JAR). None = "xml".
            skip_bronze_customer_mgmt: If True, do not load bronze_customer_mgmt (table already created by a prior task).
            only_bronze_customer_mgmt: If True, run only bronze_customer_mgmt (Batch 1) then return; no reference tables, no trade/daily_market/etc. (for Databricks workflow Task 1).
        """
        prefix = ".".join(p for p in (target_database, target_schema) if p)

        logger.info(f"Starting Bronze layer load for Batch{batch_id} (skip_customer_mgmt={skip_bronze_customer_mgmt}, only_customer_mgmt={only_bronze_customer_mgmt})")

        # Reference data (Batch1 only) — skip when only_bronze_customer_mgmt (Task 1 creates only bronze_customer_mgmt)
        if batch_id == 1 and not only_bronze_customer_mgmt:
            table_timing_start(f"{prefix}.bronze_date")
            self.date.load(f"{prefix}.bronze_date")
            table_timing_start(f"{prefix}.bronze_time")
            self.time.load(f"{prefix}.bronze_time")
            table_timing_start(f"{prefix}.bronze_status_type")
            self.status_type.load(f"{prefix}.bronze_status_type")
            table_timing_start(f"{prefix}.bronze_tax_rate")
            self.tax_rate.load(f"{prefix}.bronze_tax_rate")
            table_timing_start(f"{prefix}.bronze_trade_type")
            self.trade_type.load(f"{prefix}.bronze_trade_type")
            table_timing_start(f"{prefix}.bronze_industry")
            self.industry.load(f"{prefix}.bronze_industry")
            table_timing_start(f"{prefix}.bronze_hr")
            self.hr.load(batch_id, f"{prefix}.bronze_hr")

        # Customer/Account data: Different formats for Batch 1 vs Batch 2+
        # Batch 1: CustomerMgmt.xml (XML event log)
        # Batch 2+: Customer.txt and Account.txt (pipe-delimited state snapshots)
        if batch_id == 1 and not skip_bronze_customer_mgmt:
            table_timing_start(f"{prefix}.bronze_customer_mgmt")
            xml_fmt = (customer_mgmt_xml_format or "xml").strip() or "xml"
            self.customer_mgmt.load(
                batch_id, f"{prefix}.bronze_customer_mgmt",
                xml_format=xml_fmt,
            )
        elif batch_id != 1:
            # Incremental batches: pipe-delimited flat files
            table_timing_start(f"{prefix}.bronze_customer")
            self.customer.load(batch_id, f"{prefix}.bronze_customer")
            table_timing_start(f"{prefix}.bronze_account")
            self.account.load(batch_id, f"{prefix}.bronze_account")

        if only_bronze_customer_mgmt:
            logger.info("Bronze layer load completed (only_customer_mgmt mode: stopping after bronze_customer_mgmt)")
            return

        # Other data files (all batches)
        table_timing_start(f"{prefix}.bronze_trade")
        self.trade.load(batch_id, f"{prefix}.bronze_trade")
        table_timing_start(f"{prefix}.bronze_daily_market")
        self.daily_market.load(batch_id, f"{prefix}.bronze_daily_market")
        table_timing_start(f"{prefix}.bronze_prospect")
        self.prospect.load(batch_id, f"{prefix}.bronze_prospect")
        table_timing_start(f"{prefix}.bronze_cash_transaction")
        self.cash_transaction.load(batch_id, f"{prefix}.bronze_cash_transaction")
        table_timing_start(f"{prefix}.bronze_holding_history")
        self.holding_history.load(batch_id, f"{prefix}.bronze_holding_history")
        table_timing_start(f"{prefix}.bronze_watch_history")
        self.watch_history.load(batch_id, f"{prefix}.bronze_watch_history")
        
        # FINWIRE (Batch1 only per TPC-DI spec)
        if batch_id == 1:
            table_timing_start(f"{prefix}.bronze_finwire")
            self.finwire.load(batch_id, f"{prefix}.bronze_finwire")
        
        logger.info(f"Bronze layer load completed for Batch{batch_id}")
