"""
Gold layer fact table loaders.

Fact tables join Silver facts with Gold dimensions to create denormalized star schema.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, current_timestamp, coalesce, lit, sum as spark_sum, count

from benchmark.etl.gold.base import GoldLoaderBase

logger = logging.getLogger(__name__)


class GoldFactTrade(GoldLoaderBase):
    """Gold fact table: FactTrade (denormalized trades with dimension keys)."""
    
    def load(self, silver_trade_table: str, target_table: str,
             dim_customer_table: str, dim_account_table: str,
             dim_security_table: str, dim_date_table: str,
             dim_trade_type_table: str,
             fact_write_mode: str = "overwrite") -> DataFrame:
        """
        Create FactTrade by joining silver_trades with dimension tables.
        TPC-DI spec: append only after $SK$ lookups (fact_write_mode=append for incremental).
        
        Args:
            silver_trade_table: silver_trades table name
            target_table: gold.FactTrade table name
            dim_*_table: gold dimension table names
            fact_write_mode: "overwrite" (batch) or "append" (incremental)
        """
        logger.info(f"Loading gold.FactTrade from {silver_trade_table}")
        
        # Get all trades from Silver (no SCD filtering)
        silver_trades = self.spark.table(silver_trade_table)
        
        # Read dimension tables (no SCD filtering - use all records)
        dim_customer = self.spark.table(dim_customer_table)
        dim_account = self.spark.table(dim_account_table)
        dim_security = self.spark.table(dim_security_table)
        dim_date = self.spark.table(dim_date_table)
        dim_trade_type = self.spark.table(dim_trade_type_table)
        
        # Join with dimensions to get surrogate keys (qualify columns to avoid ambiguity)
        # Note: silver_trades has account_id, join to account to get customer_id
        # Late-arriving dim: use placeholder -1 when account/customer not yet loaded; flag the row
        fact_df = silver_trades \
            .join(dim_date,
                  to_date(silver_trades["trade_dts"]) == dim_date["date_value"],
                  "left") \
            .join(dim_account,
                  silver_trades["account_id"] == dim_account["account_id"],
                  "left") \
            .join(dim_customer,
                  dim_account["customer_id"] == dim_customer["customer_id"],
                  "left") \
            .join(dim_security,
                  silver_trades["symbol"] == dim_security["symbol"],
                  "left") \
            .join(dim_trade_type,
                  silver_trades["trade_type_id"] == dim_trade_type["trade_type_id"],
                  "left") \
            .select(
                # Surrogate keys (placeholder -1 or "UNKNOWN" when dimension not yet arrived)
                coalesce(dim_date["sk_date_id"], lit(-1)).alias("sk_date_id"),
                coalesce(dim_customer["sk_customer_id"], lit(-1)).alias("sk_customer_id"),
                coalesce(dim_account["sk_account_id"], lit(-1)).alias("sk_account_id"),
                coalesce(dim_security["sk_security_id"], lit("UNKNOWN")).alias("sk_security_id"),
                coalesce(dim_trade_type["sk_trade_type_id"], lit("UNKNOWN")).alias("sk_trade_type_id"),
                # Fact measures (from silver_trades)
                silver_trades["trade_id"],
                silver_trades["trade_dts"],
                silver_trades["trade_price"],
                silver_trades["quantity"].alias("trade_quantity"),
                (silver_trades["trade_price"] * silver_trades["quantity"]).alias("trade_amount"),
                silver_trades["commission"],
                silver_trades["charge"],
                silver_trades["tax"],
                silver_trades["status_id"],
                silver_trades["is_cash"],
                silver_trades["exec_name"],
                silver_trades["batch_id"],
                # Late-arriving fact: trade arrived before account/customer in a later batch
                (dim_account["sk_account_id"].isNull() | dim_customer["sk_customer_id"].isNull()).alias("late_arriving_flag"),
                current_timestamp().alias("etl_timestamp"),
            )
        
        return self._write_gold_table(fact_df, target_table, mode=fact_write_mode)


class GoldFactMarketHistory(GoldLoaderBase):
    """Gold fact table: FactMarketHistory (daily market data with dimension keys)."""
    
    def load(self, silver_daily_market_table: str, target_table: str,
             dim_date_table: str, dim_security_table: str,
             fact_write_mode: str = "overwrite") -> DataFrame:
        """
        Create FactMarketHistory by joining silver_daily_market with dimensions.
        TPC-DI spec: append only (fact_write_mode=append for incremental).
        """
        logger.info(f"Loading gold.FactMarketHistory from {silver_daily_market_table}")
        
        # Get all daily market data from Silver (no SCD filtering)
        silver_dm = self.spark.table(silver_daily_market_table)
        
        # Read dimension tables
        dim_date = self.spark.table(dim_date_table)
        dim_security = self.spark.table(dim_security_table)
        
        # Join with dimensions
        fact_df = silver_dm \
            .join(dim_date,
                  col("dm_date") == dim_date["date_value"],
                  "left") \
            .join(dim_security,
                  col("dm_s_symb") == dim_security["symbol"],
                  "left") \
            .select(
                # Surrogate keys
                dim_date["sk_date_id"].alias("sk_date_id"),
                dim_security["sk_security_id"].alias("sk_security_id"),
                
                # Fact measures
                col("dm_date").alias("market_date"),
                col("dm_s_symb").alias("symbol"),
                col("dm_close").alias("close_price"),
                col("dm_high").alias("high_price"),
                col("dm_low").alias("low_price"),
                col("dm_vol").alias("volume"),
                
                # Metadata
                col("batch_id"),
                current_timestamp().alias("etl_timestamp"),
            )
        
        return self._write_gold_table(fact_df, target_table, mode=fact_write_mode)


class GoldFactCashBalances(GoldLoaderBase):
    """Gold fact table: FactCashBalances (cash transaction aggregates)."""
    
    def load(self, silver_cash_transaction_table: str, target_table: str,
             dim_date_table: str, dim_account_table: str,
             fact_write_mode: str = "overwrite") -> DataFrame:
        """
        Create FactCashBalances by aggregating silver_cash_transaction.
        TPC-DI spec: append only (fact_write_mode=append for incremental).
        """
        logger.info(f"Loading gold.FactCashBalances from {silver_cash_transaction_table}")
        
        # Note: silver_cash_transaction may not exist yet
        # This is a placeholder for when cash transaction loader is implemented
        try:
            silver_ct = self.spark.table(silver_cash_transaction_table)
            
            dim_date = self.spark.table(dim_date_table)
            dim_account = self.spark.table(dim_account_table)
            
            # Aggregate cash by account and date (spec: FactCashBalances Cash = sum of CT_AMT per account/date)
            fact_df = silver_ct \
                .join(dim_date,
                      to_date(silver_ct["transaction_date"]) == dim_date["date_value"],
                      "left") \
                .join(dim_account,
                      silver_ct["account_id"] == dim_account["account_id"],
                      "left") \
                .groupBy(
                    dim_date["sk_date_id"],
                    dim_account["sk_account_id"],
                    dim_account["account_id"],
                ) \
                .agg(
                    spark_sum("ct_amt").alias("cash_balance"),
                    count("ct_ca_id").alias("transaction_count")
                ) \
                .select(
                    col("sk_date_id"),
                    col("sk_account_id"),
                    col("account_id"),
                    col("cash_balance"),
                    col("transaction_count"),
                    current_timestamp().alias("etl_timestamp"),
                )
            
            return self._write_gold_table(fact_df, target_table, mode=fact_write_mode)
        except Exception as e:
            logger.warning(f"Could not load FactCashBalances: {e}")
            return None


class GoldFactHoldings(GoldLoaderBase):
    """Gold fact table: FactHoldings (current holdings with dimension keys)."""
    
    def load(self, silver_holding_history_table: str, target_table: str,
             dim_date_table: str, dim_account_table: str,
             dim_security_table: str,
             fact_write_mode: str = "overwrite") -> DataFrame:
        """
        Create FactHoldings from silver_holding_history.
        TPC-DI spec: append only (fact_write_mode=append for incremental).
        """
        logger.info(f"Loading gold.FactHoldings from {silver_holding_history_table}")
        
        try:
            # Get all holding history from Silver (no SCD filtering)
            silver_hh = self.spark.table(silver_holding_history_table)
            
            dim_date = self.spark.table(dim_date_table)
            dim_account = self.spark.table(dim_account_table)
            dim_security = self.spark.table(dim_security_table)
            
            # Get current holdings (qualify columns to avoid ambiguity)
            fact_df = silver_hh \
                .join(dim_date,
                      to_date(silver_hh["holding_date"]) == dim_date["date_value"],
                      "left") \
                .join(dim_account,
                      silver_hh["account_id"] == dim_account["account_id"],
                      "left") \
                .join(dim_security,
                      silver_hh["symbol"] == dim_security["symbol"],
                      "left") \
                .select(
                    dim_date["sk_date_id"],
                    dim_account["sk_account_id"],
                    dim_security["sk_security_id"],
                    silver_hh["account_id"],
                    silver_hh["symbol"],
                    silver_hh["quantity"],
                    silver_hh["purchase_price"],
                    silver_hh["purchase_date"],
                    current_timestamp().alias("etl_timestamp"),
                )
            
            return self._write_gold_table(fact_df, target_table, mode=fact_write_mode)
        except Exception as e:
            logger.warning(f"Could not load FactHoldings: {e}")
            return None
