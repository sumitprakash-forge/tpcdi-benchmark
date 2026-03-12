"""
ETL transformations for TPC-DI benchmark.

Medallion Architecture (modular structure):
- bronze/: Raw data ingestion (no transformations)
  - BronzeETL: Orchestrator for all Bronze loaders
  - Individual loaders: BronzeCustomerMgmt, BronzeFinwire, BronzeTrade, etc.
- silver/: Cleaned/parsed data with SCD handling
  - SilverETL: Orchestrator for all Silver loaders
  - Individual loaders: SilverCustomers, SilverAccounts, SilverCompanies, etc.

Direct Architecture (legacy):
- BatchETL: Direct load to Gold/Dim tables
- IncrementalETL: Incremental updates to Gold/Dim tables
"""

# Import from modular Bronze layer
from benchmark.etl.bronze import (
    BronzeETL,
    BronzeCustomerMgmt,
    BronzeFinwire,
    BronzeTrade,
    BronzeDailyMarket,
    BronzeHR,
    BronzeProspect,
    BronzeCashTransaction,
    BronzeHoldingHistory,
    BronzeWatchHistory,
    BronzeDate,
    BronzeTime,
    BronzeStatusType,
    BronzeTaxRate,
    BronzeTradeType,
    BronzeIndustry,
)

# Import from modular Silver layer
from benchmark.etl.silver import (
    SilverETL,
    SilverCustomers,
    SilverAccounts,
    SilverCompanies,
    SilverSecurities,
    SilverFinancials,
    SilverTrades,
    SilverDailyMarket,
    SilverDate,
    SilverStatusType,
    SilverTradeType,
    SilverIndustry,
)

# Import legacy direct architecture
from benchmark.etl.batch import BatchETL
from benchmark.etl.incremental import IncrementalETL

__all__ = [
    # Bronze layer
    "BronzeETL",
    "BronzeCustomerMgmt",
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
    # Silver layer
    "SilverETL",
    "SilverCustomers",
    "SilverAccounts",
    "SilverCompanies",
    "SilverSecurities",
    "SilverFinancials",
    "SilverTrades",
    "SilverDailyMarket",
    "SilverDate",
    "SilverStatusType",
    "SilverTradeType",
    "SilverIndustry",
    # Legacy direct architecture
    "BatchETL",
    "IncrementalETL",
]
