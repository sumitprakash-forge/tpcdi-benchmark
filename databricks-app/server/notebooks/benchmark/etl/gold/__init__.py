"""
Gold Layer ETL - Business-Ready Analytics Tables for TPC-DI Benchmark.

The Gold layer provides query-optimized, business-ready tables:
- Dimensions: Current versions only (overwrite mode)
- Fact tables: Append only on incremental (after $SK$ lookups)
- Financials: SCD Type 1 MERGE (upsert)

Transforms Silver cleaned data into Gold star schema tables.
"""

import logging
from typing import TYPE_CHECKING, Optional

# Import dimension loaders
from benchmark.etl.gold.dimensions import (
    GoldDimCustomer,
    GoldDimAccount,
    GoldDimCompany,
    GoldDimSecurity,
    GoldDimDate,
    GoldDimTime,
    GoldDimBroker,
    GoldProspect,
    GoldDimTradeType,
    GoldDimStatusType,
    GoldDimIndustry,
)

# Import fact loaders
from benchmark.etl.gold.facts import (
    GoldFactTrade,
    GoldFactMarketHistory,
    GoldFactCashBalances,
    GoldFactHoldings,
    GoldFactWatches,
)
from benchmark.etl.gold.financials import GoldFinancials
from benchmark.etl.table_timing import start_table as table_timing_start
from benchmark.etl.dq.dim_messages import ensure_dim_messages_exists

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

logger = logging.getLogger(__name__)

# Export all loader classes
__all__ = [
    "GoldETL",
    "GoldDimCustomer",
    "GoldDimAccount",
    "GoldDimCompany",
    "GoldDimSecurity",
    "GoldDimDate",
    "GoldDimTime",
    "GoldDimBroker",
    "GoldProspect",
    "GoldDimTradeType",
    "GoldDimStatusType",
    "GoldDimIndustry",
    "GoldFactTrade",
    "GoldFactMarketHistory",
    "GoldFactCashBalances",
    "GoldFactHoldings",
    "GoldFactWatches",
    "GoldFinancials",
]


class GoldETL:
    """
    Gold Layer ETL orchestrator for TPC-DI.
    
    Coordinates all Gold layer loaders to transform Silver data
    into business-ready star schema tables.
    """
    
    def __init__(self, platform):
        """
        Initialize Gold ETL orchestrator.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        
        # Initialize dimension loaders
        self.dim_customer = GoldDimCustomer(platform)
        self.dim_account = GoldDimAccount(platform)
        self.dim_company = GoldDimCompany(platform)
        self.dim_security = GoldDimSecurity(platform)
        self.dim_date = GoldDimDate(platform)
        self.dim_time = GoldDimTime(platform)
        self.dim_broker = GoldDimBroker(platform)
        self.prospect = GoldProspect(platform)
        self.dim_trade_type = GoldDimTradeType(platform)
        self.dim_status_type = GoldDimStatusType(platform)
        self.dim_industry = GoldDimIndustry(platform)
        
        # Initialize fact loaders
        self.fact_trade = GoldFactTrade(platform)
        self.fact_market_history = GoldFactMarketHistory(platform)
        self.fact_cash_balances = GoldFactCashBalances(platform)
        self.fact_holdings = GoldFactHoldings(platform)
        self.fact_watches = GoldFactWatches(platform)
        self.financials = GoldFinancials(platform)
        
        logger.info("Initialized GoldETL orchestrator")

    def run_gold_load(self, target_database: str, target_schema: str,
                      load_type: Optional["LoadType"] = None, batch_id: Optional[int] = None):
        """
        Run Gold layer load.
        
        Reads from Silver tables and creates Gold star schema tables.
        Dimensions must be loaded before facts (for joins).
        Fact tables: append only when load_type is INCREMENTAL (TPC-DI spec).
        
        Args:
            target_database: Target database/catalog name
            target_schema: Target schema name
            load_type: BATCH or INCREMENTAL; when INCREMENTAL, fact tables use append
            batch_id: Batch number (1 for batch, 2+ for incremental)
        """
        from benchmark.config import LoadType
        prefix = ".".join(p for p in (target_database, target_schema) if p)
        fact_mode = "append" if load_type == LoadType.INCREMENTAL else "overwrite"
        bid = batch_id if batch_id is not None else (2 if load_type == LoadType.INCREMENTAL else 1)

        logger.info("Starting Gold layer load (fact mode=%s)", fact_mode)
        
        # Ensure DimMessages exists (Silver DQ may have already created it)
        ensure_dim_messages_exists(self.platform.get_spark(), f"{prefix}.gold_dim_messages", self.platform)
        
        # Step 1: Load dimension tables (must be done first)
        logger.info("Loading Gold dimension tables...")
        
        table_timing_start(f"{prefix}.gold_dim_date")
        self.dim_date.load(
            f"{prefix}.silver_date",
            f"{prefix}.gold_dim_date"
        )

        table_timing_start(f"{prefix}.gold_dim_time")
        self.dim_time.load(
            f"{prefix}.silver_time",
            f"{prefix}.gold_dim_time",
            batch_id=bid,
        )

        table_timing_start(f"{prefix}.gold_dim_customer")
        self.dim_customer.load(
            f"{prefix}.silver_customers",
            f"{prefix}.gold_dim_customer",
            load_type=load_type,
            batch_id=bid,
        )
        
        table_timing_start(f"{prefix}.gold_dim_account")
        self.dim_account.load(
            f"{prefix}.silver_accounts",
            f"{prefix}.gold_dim_account",
            load_type=load_type,
            batch_id=bid,
            dim_customer_table=f"{prefix}.gold_dim_customer",
        )

        table_timing_start(f"{prefix}.gold_dim_broker")
        self.dim_broker.load(
            f"{prefix}.bronze_hr",
            f"{prefix}.gold_dim_broker",
            batch_id=bid,
            dim_date_table=f"{prefix}.gold_dim_date",
        )

        table_timing_start(f"{prefix}.gold_dim_company")
        self.dim_company.load(
            f"{prefix}.silver_companies",
            f"{prefix}.gold_dim_company"
        )

        table_timing_start(f"{prefix}.gold_dim_security")
        self.dim_security.load(
            f"{prefix}.silver_securities",
            f"{prefix}.gold_dim_security"
        )
        
        table_timing_start(f"{prefix}.gold_dim_trade_type")
        self.dim_trade_type.load(
            f"{prefix}.silver_trade_type",
            f"{prefix}.gold_dim_trade_type"
        )
        
        table_timing_start(f"{prefix}.gold_dim_status_type")
        self.dim_status_type.load(
            f"{prefix}.silver_status_type",
            f"{prefix}.gold_dim_status_type"
        )
        
        table_timing_start(f"{prefix}.gold_dim_industry")
        self.dim_industry.load(
            f"{prefix}.silver_industry",
            f"{prefix}.gold_dim_industry"
        )
        
        # Financials: batch = overwrite, incremental = MERGE (upsert)
        try:
            table_timing_start(f"{prefix}.gold_financials")
            self.financials.load(
                f"{prefix}.silver_financials",
                f"{prefix}.gold_financials",
                load_type=load_type,
            )
        except Exception as e:
            logger.warning("Gold financials skipped: %s", e)

        try:
            table_timing_start(f"{prefix}.gold_prospect")
            self.prospect.load(
                f"{prefix}.silver_prospect",
                f"{prefix}.gold_prospect",
                batch_id=bid,
            )
        except Exception as e:
            logger.warning("Gold prospect skipped: %s", e)

        logger.info("Gold dimension tables loaded")
        
        # Step 2: Load fact tables (join with dimensions)
        logger.info("Loading Gold fact tables...")
        
        table_timing_start(f"{prefix}.gold_fact_trade")
        self.fact_trade.load(
            f"{prefix}.silver_trades",
            f"{prefix}.gold_fact_trade",
            dim_customer_table=f"{prefix}.gold_dim_customer",
            dim_account_table=f"{prefix}.gold_dim_account",
            dim_security_table=f"{prefix}.gold_dim_security",
            dim_date_table=f"{prefix}.gold_dim_date",
            dim_trade_type_table=f"{prefix}.gold_dim_trade_type",
            fact_write_mode=fact_mode,
            batch_id=bid if load_type == LoadType.INCREMENTAL else None,
        )

        table_timing_start(f"{prefix}.gold_fact_market_history")
        self.fact_market_history.load(
            f"{prefix}.silver_daily_market",
            f"{prefix}.gold_fact_market_history",
            dim_date_table=f"{prefix}.gold_dim_date",
            dim_security_table=f"{prefix}.gold_dim_security",
            fact_write_mode=fact_mode,
            batch_id=bid if load_type == LoadType.INCREMENTAL else None,
        )
        
        # Optional fact tables (may not exist yet)
        try:
            table_timing_start(f"{prefix}.gold_fact_cash_balances")
            self.fact_cash_balances.load(
                f"{prefix}.silver_cash_transaction",
                f"{prefix}.gold_fact_cash_balances",
                dim_date_table=f"{prefix}.gold_dim_date",
                dim_account_table=f"{prefix}.gold_dim_account",
                fact_write_mode=fact_mode,
                batch_id=bid if load_type == LoadType.INCREMENTAL else None,
            )
        except Exception as e:
            logger.warning(f"FactCashBalances skipped: {e}")
        
        try:
            table_timing_start(f"{prefix}.gold_fact_holdings")
            self.fact_holdings.load(
                f"{prefix}.silver_holding_history",
                f"{prefix}.gold_fact_holdings",
                dim_date_table=f"{prefix}.gold_dim_date",
                dim_account_table=f"{prefix}.gold_dim_account",
                dim_security_table=f"{prefix}.gold_dim_security",
                fact_write_mode=fact_mode,
                batch_id=bid if load_type == LoadType.INCREMENTAL else None,
            )
        except Exception as e:
            logger.warning(f"FactHoldings skipped: {e}")

        try:
            table_timing_start(f"{prefix}.gold_fact_watches")
            self.fact_watches.load(
                f"{prefix}.silver_watch_history",
                f"{prefix}.gold_fact_watches",
                dim_customer_table=f"{prefix}.gold_dim_customer",
                dim_security_table=f"{prefix}.gold_dim_security",
                batch_id=bid,
                fact_write_mode=fact_mode,
            )
        except Exception as e:
            logger.warning("FactWatches skipped: %s", e)

        logger.info("Gold layer load completed")
