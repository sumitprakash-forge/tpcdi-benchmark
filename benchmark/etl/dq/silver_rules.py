"""
Silver-layer Data Quality rules for TPC-DI.

Runs mandatory TPC-DI validation rules and generic checks (completeness, uniqueness,
validity, consistency). Failures are logged to gold.dim_messages.

You can add more complex rules by:
- Adding checks in existing _run_*_rules methods and calling log().
- Adding new _run_*_rules methods and calling them from run_silver_dq().
- Passing custom_rules=[fn, ...] to run_silver_dq(); each fn(runner, prefix, batch_id, log, messages_table).
See docs/DATA_QUALITY.md for examples.
"""

import logging
import time
from datetime import datetime
from typing import Callable, List, Optional, Dict, Any, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, current_date, current_timestamp, length, lit, trim

from benchmark.etl.dq.dim_messages import ensure_dim_messages_exists, log_message
from benchmark.etl.table_timing import is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverDQRunner:
    """
    Runs DQ rules against Silver tables and logs to gold.dim_messages.
    """

    def __init__(self, platform):
        self.platform = platform
        self.spark = platform.get_spark()

    def run_silver_dq(
        self,
        batch_id: int,
        prefix: str,
        dim_messages_table: Optional[str] = None,
        custom_rules: Optional[List[Callable[..., None]]] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Run all Silver DQ rules for the given batch and prefix.
        Logs failures to gold_dim_messages (or dim_messages_table if provided).

        Returns:
            Tuple of (dq_table_timings, message_count): timings per validated table, and number of
            DQ messages written to gold_dim_messages (for table_timing row count).
        """
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting Silver DQ validation for batch {batch_id} at {start_datetime}")
        
        messages_table = dim_messages_table or f"{prefix}.gold_dim_messages"
        ensure_dim_messages_exists(self.spark, messages_table, self.platform)

        dq_table_timings: List[Dict[str, Any]] = []
        message_count: List[int] = [0]  # mutable so log() can increment

        def log(component: str, message: str, severity: str = "Alert", source: str = ""):
            log_message(
                self.spark, self.platform, messages_table,
                batch_id=batch_id, component_name=component,
                message_text=message, severity=severity, source_table=source,
            )
            message_count[0] += 1

        def _timed(table_name: str, fn):
            t0 = time.time()
            try:
                fn()
            finally:
                dq_table_timings.append({"table": table_name, "duration_seconds": time.time() - t0})

        # --- DimCustomer (silver_customers) ---
        try:
            _timed("silver_customers", lambda: self._run_customer_rules(prefix, batch_id, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_customers failed: {e}")
            log("Silver_Customer_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_customers")

        # --- DimAccount (silver_accounts) ---
        try:
            _timed("silver_accounts", lambda: self._run_account_rules(prefix, batch_id, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_accounts failed: {e}")
            log("Silver_Account_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_accounts")

        # --- FactTrades / silver_trades ---
        try:
            _timed("silver_trades", lambda: self._run_trade_rules(prefix, batch_id, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_trades failed: {e}")
            log("Silver_Trade_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_trades")

        # --- DimDate (silver_date) ---
        _timed("silver_date", lambda: self._run_date_rules(prefix, log, messages_table))

        # --- Securities (silver_securities) ---
        try:
            _timed("silver_securities", lambda: self._run_security_rules(prefix, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_securities failed: {e}")
            log("Silver_Security_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_securities")

        # --- Daily Market (silver_daily_market) ---
        try:
            _timed("silver_daily_market", lambda: self._run_daily_market_rules(prefix, batch_id, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_daily_market failed: {e}")
            log("Silver_DailyMarket_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_daily_market")

        # --- Cash Transaction (silver_cash_transaction) ---
        try:
            _timed("silver_cash_transaction", lambda: self._run_cash_transaction_rules(prefix, batch_id, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_cash_transaction failed: {e}")
            log("Silver_CashTransaction_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_cash_transaction")

        # --- Reference: StatusType, TradeType, Industry ---
        try:
            _timed("silver_status_type", lambda: self._run_status_type_rules(prefix, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_status_type failed: {e}")
            log("Silver_StatusType_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_status_type")
        _timed("silver_trade_type", lambda: self._run_trade_type_rules(prefix, log, messages_table))
        try:
            _timed("silver_industry", lambda: self._run_industry_rules(prefix, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_industry failed: {e}")
            log("Silver_Industry_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_industry")

        # --- silver_companies, silver_financials (Batch 1) ---
        try:
            _timed("silver_companies", lambda: self._run_companies_rules(prefix, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_companies failed: {e}")
            log("Silver_Companies_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_companies")
        try:
            _timed("silver_financials", lambda: self._run_financials_rules(prefix, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_financials failed: {e}")
            log("Silver_Financials_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_financials")

        # --- silver_prospect, silver_watch_history, silver_holding_history ---
        try:
            _timed("silver_prospect", lambda: self._run_prospect_rules(prefix, batch_id, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_prospect failed: {e}")
            log("Silver_Prospect_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_prospect")
        try:
            _timed("silver_watch_history", lambda: self._run_watch_history_rules(prefix, batch_id, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_watch_history failed: {e}")
            log("Silver_WatchHistory_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_watch_history")
        try:
            _timed("silver_holding_history", lambda: self._run_holding_history_rules(prefix, batch_id, log, messages_table))
        except Exception as e:
            logger.warning(f"Silver DQ silver_holding_history failed: {e}")
            log("Silver_HoldingHistory_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_holding_history")

        # --- Custom rules ---
        if custom_rules:
            for fn in custom_rules:
                t0 = time.time()
                try:
                    fn(self, prefix, batch_id, log, messages_table)
                except Exception as e:
                    logger.warning(f"Silver DQ custom rule {fn.__name__} failed: {e}")
                    log("Silver_Custom_Validation", f"DQ run failed: {e}", "Alert", "")
                dq_table_timings.append({"table": f"custom_{fn.__name__}", "duration_seconds": time.time() - t0})

        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed Silver DQ validation for batch {batch_id} at {end_datetime}")
            logger.info(f"[TIMING] Silver DQ - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s")
        
        logger.info(f"Silver DQ completed for batch_id={batch_id}, prefix={prefix} in {duration:.2f}s ({message_count[0]} messages)")
        return dq_table_timings, message_count[0]

    def _run_customer_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_customers"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping customer DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)

        null_key = df.filter(col("customer_id").isNull() | col("tax_id").isNull())
        n = null_key.count()
        if n > 0:
            log("Silver_Customer_Validation", f"customer_id/tax_id NULL: {n} row(s)", "Reject", source)

        bad_tier = df.filter(col("tier").isNotNull() & ~(col("tier").isin([1, 2, 3])))
        n = bad_tier.count()
        if n > 0:
            log("Silver_Customer_Validation", f"tier not in (1,2,3): {n} row(s)", "Alert", source)

        today = current_date()
        future_dob = df.filter(col("dob").isNotNull() & (col("dob") > today))
        n = future_dob.count()
        if n > 0:
            log("Silver_Customer_Validation", f"dob in future: {n} row(s)", "Alert", source)

        dup = df.groupBy("customer_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
        n = dup.count()
        if n > 0:
            log("Silver_Customer_Validation", f"duplicate customer_id within batch: {n} key(s)", "Alert", source)

        if "end_date" in df.columns and "effective_date" in df.columns:
            cond = (col("end_date").isNotNull() & col("effective_date").isNotNull()
                    & (col("end_date") < col("effective_date")))
            bad_dates = df.filter(cond)
            n = bad_dates.count()
            if n > 0:
                log("Silver_Customer_Validation", f"end_date < effective_date: {n} row(s)", "Alert", source)

        # Gender valid (M/F/U or empty)
        if "gender" in df.columns:
            bad_gender = df.filter(
                col("gender").isNotNull() & (trim(col("gender").cast("string")) != "")
                & ~(trim(col("gender").cast("string")).isin("M", "F", "U"))
            )
            n = bad_gender.count()
            if n > 0:
                log("Silver_Customer_Validation", f"gender not in (M,F,U): {n} row(s)", "Alert", source)

        # Status non-empty and in valid set (ACTV, INAC, etc.)
        if "status" in df.columns:
            null_status = df.filter(col("status").isNull() | (trim(col("status").cast("string")) == ""))
            n = null_status.count()
            if n > 0:
                log("Silver_Customer_Validation", f"status NULL or empty: {n} row(s)", "Alert", source)
            valid_status = ["ACTV", "INAC", "ACTIVE", "NEW", "UPDCUST", "INACT"]
            st = trim(col("status").cast("string"))
            bad_status = df.filter(col("status").isNotNull() & (st != "") & ~(st.isin(valid_status)))
            if bad_status.count() > 0:
                log("Silver_Customer_Validation", "status not in (ACTV,INAC,NEW,UPDCUST,INACT)", "Alert", source)

        # First/last name non-empty when present
        if "first_name" in df.columns:
            empty_first = df.filter(col("first_name").isNull() | (trim(col("first_name").cast("string")) == ""))
            if empty_first.count() > 0:
                log("Silver_Customer_Validation", "first_name NULL or empty", "Alert", source)
        if "last_name" in df.columns:
            empty_last = df.filter(col("last_name").isNull() | (trim(col("last_name").cast("string")) == ""))
            if empty_last.count() > 0:
                log("Silver_Customer_Validation", "last_name NULL or empty", "Alert", source)

        # tax_id non-empty when present (beyond null check)
        if "tax_id" in df.columns:
            empty_tax = df.filter(col("customer_id").isNotNull() & (trim(col("tax_id").cast("string")) == ""))
            if empty_tax.count() > 0:
                log("Silver_Customer_Validation", "tax_id empty for non-null customer", "Alert", source)
        # Duplicate tax_id within batch
        if "tax_id" in df.columns:
            dup_tax = df.groupBy(trim(col("tax_id").cast("string"))).agg(count("*").alias("cnt")).filter(col("cnt") > 1)
            if dup_tax.count() > 0:
                log("Silver_Customer_Validation", "duplicate tax_id within batch", "Alert", source)
        # dob in reasonable range (e.g. not before 1900)
        if "dob" in df.columns:
            old_dob = df.filter(col("dob").isNotNull() & (col("dob") < lit("1900-01-01").cast("date")))
            if old_dob.count() > 0:
                log("Silver_Customer_Validation", "dob before 1900-01-01", "Alert", source)
        # effective_date not in future
        if "effective_date" in df.columns:
            future_eff = df.filter(col("effective_date").isNotNull() & (col("effective_date") > current_timestamp()))
            if future_eff.count() > 0:
                log("Silver_Customer_Validation", "effective_date in future", "Alert", source)
        # postal_code length when present (e.g. 1–20 chars)
        if "postal_code" in df.columns:
            pc = trim(col("postal_code").cast("string"))
            bad_pc = df.filter((pc != "") & (length(pc) > 20))
            if bad_pc.count() > 0:
                log("Silver_Customer_Validation", "postal_code length > 20", "Alert", source)
        # email format: when non-empty, contain @
        for email_col in ("email1", "email2"):
            if email_col in df.columns:
                em = trim(col(email_col).cast("string"))
                bad_email = df.filter((em != "") & ~(em.contains("@")))
                if bad_email.count() > 0:
                    log("Silver_Customer_Validation", f"{email_col} missing @ when non-empty", "Alert", source)

    def _run_account_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_accounts"
        try:
            acc = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping account DQ")
            return
        if "batch_id" in acc.columns:
            acc = acc.filter(col("batch_id") == batch_id)
        try:
            cust = self.spark.table(f"{prefix}.silver_customers")
            cust_ids = cust.select("customer_id").distinct()
            missing = acc.join(cust_ids, acc["customer_id"] == cust_ids["customer_id"], "left_anti")
            n = missing.count()
            if n > 0:
                log("Silver_Account_Validation", f"customer_id not in silver_customers: {n} row(s)", "Alert", source)
        except Exception as e:
            log("Silver_Account_Validation", f"RI check failed (silver_customers): {e}", "Alert", source)
        null_cust = acc.filter(col("customer_id").isNull())
        if null_cust.count() > 0:
            log("Silver_Account_Validation", "customer_id NULL in silver_accounts", "Reject", source)
        # account_id must be non-null
        if "account_id" in acc.columns:
            null_acc = acc.filter(col("account_id").isNull())
            if null_acc.count() > 0:
                log("Silver_Account_Validation", "account_id NULL in silver_accounts", "Reject", source)
        if "end_date" in acc.columns and "effective_date" in acc.columns:
            cond = col("end_date").isNotNull() & col("effective_date").isNotNull()
            cond = cond & (col("end_date") < col("effective_date"))
            bad = acc.filter(cond)
            if bad.count() > 0:
                log("Silver_Account_Validation", "end_date < effective_date in silver_accounts", "Alert", source)
        # account name non-empty when present
        name_col = "account_name" if "account_name" in acc.columns else "ca_name"
        if name_col in acc.columns:
            empty_name = acc.filter(col(name_col).isNull() | (trim(col(name_col).cast("string")) == ""))
            if empty_name.count() > 0:
                log("Silver_Account_Validation", f"{name_col} NULL or empty", "Alert", source)
        # duplicate account_id within batch
        if "account_id" in acc.columns:
            dup_acc = acc.groupBy("account_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
            if dup_acc.count() > 0:
                log("Silver_Account_Validation", "duplicate account_id within batch", "Alert", source)

    def _run_trade_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_trades"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping trade DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)

        bad_logic = df.filter((col("bid_price").isNull()) | (col("bid_price") <= 0))
        n = bad_logic.count()
        if n > 0:
            log("Silver_Trade_Validation", f"bid_price NULL or <= 0: {n} row(s)", "Alert", source)
        bad_qty = df.filter((col("quantity").isNull()) | (col("quantity") <= 0))
        n = bad_qty.count()
        if n > 0:
            log("Silver_Trade_Validation", f"quantity NULL or <= 0: {n} row(s)", "Alert", source)

        dup = df.groupBy("trade_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
        n = dup.count()
        if n > 0:
            log("Silver_Trade_Validation", f"duplicate trade_id within batch: {n} key(s)", "Alert", source)

        # account_id required for fact join
        if "account_id" in df.columns:
            null_acc = df.filter(col("account_id").isNull())
            n = null_acc.count()
            if n > 0:
                log("Silver_Trade_Validation", f"account_id NULL: {n} row(s)", "Reject", source)

        # trade_price positive when present
        if "trade_price" in df.columns:
            bad_price = df.filter(col("trade_price").isNotNull() & (col("trade_price") <= 0))
            n = bad_price.count()
            if n > 0:
                log("Silver_Trade_Validation", f"trade_price <= 0: {n} row(s)", "Alert", source)

        # commission and tax non-negative
        if "commission" in df.columns:
            bad_comm = df.filter(col("commission").isNotNull() & (col("commission") < 0))
            if bad_comm.count() > 0:
                log("Silver_Trade_Validation", "commission < 0", "Alert", source)
        if "tax" in df.columns:
            bad_tax = df.filter(col("tax").isNotNull() & (col("tax") < 0))
            if bad_tax.count() > 0:
                log("Silver_Trade_Validation", "tax < 0", "Alert", source)
        # trade_dts required for fact join
        if "trade_dts" in df.columns:
            null_dts = df.filter(col("trade_dts").isNull())
            n = null_dts.count()
            if n > 0:
                log("Silver_Trade_Validation", f"trade_dts NULL: {n} row(s)", "Alert", source)
        # cash_amount non-negative when present
        if "cash_amount" in df.columns:
            bad_cash = df.filter(col("cash_amount").isNotNull() & (col("cash_amount") < 0))
            if bad_cash.count() > 0:
                log("Silver_Trade_Validation", "cash_amount < 0", "Alert", source)

        # --- Complex DQ: record_type, symbol, RI, temporal, consistency ---
        # record_type valid (I, U, D)
        if "record_type" in df.columns:
            rt = trim(col("record_type").cast("string"))
            bad_rt = df.filter(col("record_type").isNotNull() & (rt != "") & ~(rt.isin("I", "U", "D")))
            if bad_rt.count() > 0:
                log("Silver_Trade_Validation", "record_type not in (I,U,D)", "Alert", source)
        # symbol non-empty when present
        if "symbol" in df.columns:
            empty_symb = df.filter(col("symbol").isNull() | (trim(col("symbol").cast("string")) == ""))
            if empty_symb.count() > 0:
                log("Silver_Trade_Validation", "symbol NULL or empty", "Alert", source)
        # trade_dts not in future
        if "trade_dts" in df.columns:
            future_dts = df.filter(col("trade_dts").isNotNull() & (col("trade_dts") > current_timestamp()))
            if future_dts.count() > 0:
                log("Silver_Trade_Validation", "trade_dts in future", "Alert", source)
        # charge non-negative when present
        if "charge" in df.columns:
            bad_charge = df.filter(col("charge").isNotNull() & (col("charge") < 0))
            if bad_charge.count() > 0:
                log("Silver_Trade_Validation", "charge < 0", "Alert", source)
        # quantity in reasonable range (e.g. not > 1e9)
        if "quantity" in df.columns:
            unreason_qty = df.filter(col("quantity").isNotNull() & (col("quantity") > 1e9))
            if unreason_qty.count() > 0:
                log("Silver_Trade_Validation", "quantity exceeds 1e9", "Alert", source)
        # effective_date/end_date consistency
        if "end_date" in df.columns and "effective_date" in df.columns:
            bad_dates = df.filter(
                col("end_date").isNotNull() & col("effective_date").isNotNull()
                & (col("end_date") < col("effective_date"))
            )
            if bad_dates.count() > 0:
                log("Silver_Trade_Validation", "end_date < effective_date", "Alert", source)
        # RI: account_id in silver_accounts
        if "account_id" in df.columns:
            try:
                accounts = self.spark.table(f"{prefix}.silver_accounts")
                acc_ids = accounts.select(col("account_id").alias("_acc_id")).distinct()
                trades_with_acc = df.filter(col("account_id").isNotNull())
                missing_acc = trades_with_acc.join(acc_ids, trades_with_acc["account_id"] == acc_ids["_acc_id"], "left_anti")
                n = missing_acc.count()
                if n > 0:
                    log("Silver_Trade_Validation", f"account_id not in silver_accounts: {n} row(s)", "Alert", source)
            except Exception as e:
                log("Silver_Trade_Validation", f"RI check (silver_accounts) failed: {e}", "Alert", source)
        # RI: symbol in silver_securities
        if "symbol" in df.columns:
            try:
                sec = self.spark.table(f"{prefix}.silver_securities")
                symbs = sec.select(col("symbol").alias("_sym")).distinct()
                trades_with_symb = df.filter(trim(col("symbol").cast("string")) != "")
                missing_symb = trades_with_symb.join(
                    symbs,
                    trim(trades_with_symb["symbol"].cast("string")) == trim(symbs["_sym"].cast("string")),
                    "left_anti",
                )
                n = missing_symb.count()
                if n > 0:
                    log("Silver_Trade_Validation", f"symbol not in silver_securities: {n} row(s)", "Alert", source)
            except Exception as e:
                log("Silver_Trade_Validation", f"RI check (silver_securities) failed: {e}", "Alert", source)

    def _run_date_rules(self, prefix: str, log, messages_table: str) -> None:
        source = f"{prefix}.silver_date"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping date DQ")
            return

        from pyspark.sql.functions import length
        str_id = col("sk_date_id").cast("string")
        invalid = df.filter(
            col("sk_date_id").isNull()
            | (length(str_id) != 8)
            | ~str_id.rlike("^[0-9]{8}$")
        )
        n = invalid.count()
        if n > 0:
            log("Silver_Date_Validation", f"sk_date_id not valid YYYYMMDD format: {n} row(s)", "Alert", source)
        # sk_date_id in reasonable range 19000101-21001231 when valid format
        try:
            sk_int = col("sk_date_id").cast("int")
            bad_range = df.filter(
                sk_int.isNotNull() & ((sk_int < 19000101) | (sk_int > 21001231))
            )
            if bad_range.count() > 0:
                log("Silver_Date_Validation", "sk_date_id outside 19000101-21001231", "Alert", source)
        except Exception:
            pass

    def _run_security_rules(self, prefix: str, log, messages_table: str) -> None:
        source = f"{prefix}.silver_securities"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping security DQ")
            return
        # symbol non-empty (business key)
        bad_symbol = df.filter(col("symbol").isNull() | (trim(col("symbol")) == ""))
        n = bad_symbol.count()
        if n > 0:
            log("Silver_Security_Validation", f"symbol NULL or empty: {n} row(s)", "Alert", source)
        # duplicate symbol (uniqueness)
        dup = df.groupBy("symbol").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
        n = dup.count()
        if n > 0:
            log("Silver_Security_Validation", f"duplicate symbol: {n} key(s)", "Alert", source)
        # name non-empty when present
        if "name" in df.columns:
            empty_name = df.filter(col("name").isNull() | (trim(col("name").cast("string")) == ""))
            if empty_name.count() > 0:
                log("Silver_Security_Validation", "name NULL or empty", "Alert", source)

    def _run_daily_market_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_daily_market"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping daily_market DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)
        # dm_date required
        null_date = df.filter(col("dm_date").isNull())
        n = null_date.count()
        if n > 0:
            log("Silver_DailyMarket_Validation", f"dm_date NULL: {n} row(s)", "Alert", source)
        # price/volume non-negative
        for col_name in ("dm_close", "dm_high", "dm_low"):
            if col_name in df.columns:
                bad = df.filter(col(col_name).isNotNull() & (col(col_name) < 0))
                n = bad.count()
                if n > 0:
                    log("Silver_DailyMarket_Validation", f"{col_name} < 0: {n} row(s)", "Alert", source)
        if "dm_vol" in df.columns:
            bad_vol = df.filter(col("dm_vol").isNotNull() & (col("dm_vol") < 0))
            if bad_vol.count() > 0:
                log("Silver_DailyMarket_Validation", "dm_vol < 0", "Alert", source)
        # dm_high >= dm_low when both present
        if "dm_high" in df.columns and "dm_low" in df.columns:
            bad_hl = df.filter(
                col("dm_high").isNotNull() & col("dm_low").isNotNull()
                & (col("dm_high") < col("dm_low"))
            )
            n = bad_hl.count()
            if n > 0:
                log("Silver_DailyMarket_Validation", f"dm_high < dm_low: {n} row(s)", "Alert", source)
        # dm_close between dm_low and dm_high when all present
        if "dm_close" in df.columns and "dm_high" in df.columns and "dm_low" in df.columns:
            bad_close = df.filter(
                col("dm_close").isNotNull() & col("dm_high").isNotNull() & col("dm_low").isNotNull()
                & ((col("dm_close") < col("dm_low")) | (col("dm_close") > col("dm_high")))
            )
            n = bad_close.count()
            if n > 0:
                log("Silver_DailyMarket_Validation", f"dm_close outside [dm_low,dm_high]: {n} row(s)", "Alert", source)

    def _run_cash_transaction_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_cash_transaction"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping cash_transaction DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)
        # account_id / ct_ca_id required
        acc_col = "account_id" if "account_id" in df.columns else "ct_ca_id"
        if acc_col in df.columns:
            null_acc = df.filter(col(acc_col).isNull())
            n = null_acc.count()
            if n > 0:
                log("Silver_CashTransaction_Validation", f"{acc_col} NULL: {n} row(s)", "Reject", source)
        # transaction_date / ct_dts required when present
        ts_col = "transaction_date" if "transaction_date" in df.columns else "ct_dts"
        if ts_col in df.columns:
            null_ts = df.filter(col(ts_col).isNull())
            n = null_ts.count()
            if n > 0:
                log("Silver_CashTransaction_Validation", f"{ts_col} NULL: {n} row(s)", "Alert", source)
        # amount non-negative when present (ct_amt or amount)
        amt_col = "amount" if "amount" in df.columns else "ct_amt"
        if amt_col in df.columns:
            bad_amt = df.filter(col(amt_col).isNotNull() & (col(amt_col) < 0))
            if bad_amt.count() > 0:
                log("Silver_CashTransaction_Validation", f"{amt_col} < 0", "Alert", source)

    def _run_status_type_rules(self, prefix: str, log, messages_table: str) -> None:
        source = f"{prefix}.silver_status_type"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping status_type DQ")
            return
        null_key = df.filter(col("st_id").isNull() | (trim(col("st_id").cast("string")) == ""))
        n = null_key.count()
        if n > 0:
            log("Silver_StatusType_Validation", f"st_id NULL or empty: {n} row(s)", "Alert", source)
        null_name = df.filter(col("st_name").isNull() | (trim(col("st_name").cast("string")) == ""))
        n = null_name.count()
        if n > 0:
            log("Silver_StatusType_Validation", f"st_name NULL or empty: {n} row(s)", "Alert", source)

    def _run_trade_type_rules(self, prefix: str, log, messages_table: str) -> None:
        source = f"{prefix}.silver_trade_type"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping trade_type DQ")
            return
        null_key = df.filter(col("tt_id").isNull() | (trim(col("tt_id").cast("string")) == ""))
        n = null_key.count()
        if n > 0:
            log("Silver_TradeType_Validation", f"tt_id NULL or empty: {n} row(s)", "Alert", source)
        null_name = df.filter(col("tt_name").isNull() | (trim(col("tt_name").cast("string")) == ""))
        n = null_name.count()
        if n > 0:
            log("Silver_TradeType_Validation", f"tt_name NULL or empty: {n} row(s)", "Alert", source)

    def _run_industry_rules(self, prefix: str, log, messages_table: str) -> None:
        source = f"{prefix}.silver_industry"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping industry DQ")
            return
        if "in_id" in df.columns:
            null_key = df.filter(col("in_id").isNull() | (trim(col("in_id").cast("string")) == ""))
            n = null_key.count()
            if n > 0:
                log("Silver_Industry_Validation", f"in_id NULL or empty: {n} row(s)", "Alert", source)
        if "in_name" in df.columns:
            null_name = df.filter(col("in_name").isNull() | (trim(col("in_name").cast("string")) == ""))
            n = null_name.count()
            if n > 0:
                log("Silver_Industry_Validation", f"in_name NULL or empty: {n} row(s)", "Alert", source)

    def _run_companies_rules(self, prefix: str, log, messages_table: str) -> None:
        source = f"{prefix}.silver_companies"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping companies DQ")
            return
        if "company_name" in df.columns:
            empty = df.filter(col("company_name").isNull() | (trim(col("company_name").cast("string")) == ""))
            if empty.count() > 0:
                log("Silver_Companies_Validation", "company_name NULL or empty", "Alert", source)
        if "cik" in df.columns:
            empty_cik = df.filter(col("cik").isNull() | (trim(col("cik").cast("string")) == ""))
            if empty_cik.count() > 0:
                log("Silver_Companies_Validation", "cik NULL or empty", "Alert", source)
            dup_cik = df.groupBy("cik").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
            if dup_cik.count() > 0:
                log("Silver_Companies_Validation", "duplicate cik", "Alert", source)
        if "founding_date" in df.columns:
            fd_str = trim(col("founding_date").cast("string"))
            bad_date = df.filter(
                col("founding_date").isNotNull() & (fd_str != "")
                & ~(fd_str.rlike("^[0-9]{8}$"))
            )
            if bad_date.count() > 0:
                log("Silver_Companies_Validation", "founding_date not YYYYMMDD format", "Alert", source)

    def _run_financials_rules(self, prefix: str, log, messages_table: str) -> None:
        source = f"{prefix}.silver_financials"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping financials DQ")
            return
        if "year" in df.columns:
            bad_year = df.filter(col("year").isNotNull() & ((col("year") < 1900) | (col("year") > 2100)))
            if bad_year.count() > 0:
                log("Silver_Financials_Validation", "year outside 1900-2100", "Alert", source)
        if "quarter" in df.columns:
            bad_qtr = df.filter(col("quarter").isNotNull() & ~(col("quarter").isin([1, 2, 3, 4])))
            if bad_qtr.count() > 0:
                log("Silver_Financials_Validation", "quarter not in (1,2,3,4)", "Alert", source)
        for col_name in ("revenue", "earnings", "assets", "liabilities"):
            if col_name in df.columns:
                neg = df.filter(col(col_name).isNotNull() & (col(col_name) < 0))
                if neg.count() > 0:
                    log("Silver_Financials_Validation", f"{col_name} < 0", "Alert", source)
        if "co_name_or_cik" in df.columns:
            empty = df.filter(col("co_name_or_cik").isNull() | (trim(col("co_name_or_cik").cast("string")) == ""))
            if empty.count() > 0:
                log("Silver_Financials_Validation", "co_name_or_cik NULL or empty", "Alert", source)

    def _run_prospect_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_prospect"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping prospect DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)
        if "agency_id" in df.columns:
            null_agency = df.filter(col("agency_id").isNull() | (trim(col("agency_id").cast("string")) == ""))
            if null_agency.count() > 0:
                log("Silver_Prospect_Validation", "agency_id NULL or empty", "Alert", source)
        if "last_name" in df.columns and "first_name" in df.columns:
            both_empty = df.filter(
                (trim(col("last_name").cast("string")) == "") & (trim(col("first_name").cast("string")) == "")
            )
            if both_empty.count() > 0:
                log("Silver_Prospect_Validation", "last_name and first_name both empty", "Alert", source)
        if "gender" in df.columns:
            g = trim(col("gender").cast("string"))
            bad_gender = df.filter(col("gender").isNotNull() & (g != "") & ~(g.isin(["M", "F", "U"])))
            if bad_gender.count() > 0:
                log("Silver_Prospect_Validation", "gender not in (M,F,U)", "Alert", source)
        if "income" in df.columns:
            neg_income = df.filter(col("income").isNotNull() & (col("income") < 0))
            if neg_income.count() > 0:
                log("Silver_Prospect_Validation", "income < 0", "Alert", source)
        if "age" in df.columns:
            bad_age = df.filter(col("age").isNotNull() & ((col("age") < 0) | (col("age") > 120)))
            if bad_age.count() > 0:
                log("Silver_Prospect_Validation", "age outside 0-120", "Alert", source)

    def _run_watch_history_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_watch_history"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping watch_history DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)
        if "w_c_id" in df.columns:
            null_cid = df.filter(col("w_c_id").isNull())
            if null_cid.count() > 0:
                log("Silver_WatchHistory_Validation", "w_c_id NULL", "Alert", source)
        if "w_s_symb" in df.columns:
            empty_symb = df.filter(col("w_s_symb").isNull() | (trim(col("w_s_symb").cast("string")) == ""))
            if empty_symb.count() > 0:
                log("Silver_WatchHistory_Validation", "w_s_symb NULL or empty", "Alert", source)
        if "w_action" in df.columns:
            valid_actions = ["ACTV", "CNCL", "INAC"]
            w_act = trim(col("w_action").cast("string"))
            bad_action = df.filter(
                col("w_action").isNotNull() & (w_act != "") & ~(w_act.isin(valid_actions))
            )
            if bad_action.count() > 0:
                log("Silver_WatchHistory_Validation", "w_action not in (ACTV,CNCL,INAC)", "Alert", source)

    def _run_holding_history_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
        source = f"{prefix}.silver_holding_history"
        try:
            df = self.spark.table(source)
        except Exception:
            logger.debug(f"Table {source} not found, skipping holding_history DQ")
            return
        if "batch_id" in df.columns:
            df = df.filter(col("batch_id") == batch_id)
        # --- Keys and non-null ---
        if "hh_h_t_id" in df.columns:
            null_ht = df.filter(col("hh_h_t_id").isNull())
            if null_ht.count() > 0:
                log("Silver_HoldingHistory_Validation", "hh_h_t_id NULL", "Alert", source)
        if "hh_t_id" in df.columns:
            null_t = df.filter(col("hh_t_id").isNull())
            if null_t.count() > 0:
                log("Silver_HoldingHistory_Validation", "hh_t_id NULL", "Alert", source)
        # --- Quantities non-negative ---
        if "hh_before_qty" in df.columns:
            neg_before = df.filter(col("hh_before_qty").isNotNull() & (col("hh_before_qty") < 0))
            if neg_before.count() > 0:
                log("Silver_HoldingHistory_Validation", "hh_before_qty < 0", "Alert", source)
        if "hh_after_qty" in df.columns:
            neg_after = df.filter(col("hh_after_qty").isNotNull() & (col("hh_after_qty") < 0))
            if neg_after.count() > 0:
                log("Silver_HoldingHistory_Validation", "hh_after_qty < 0", "Alert", source)
        # --- record_type valid (I, U, D) ---
        if "record_type" in df.columns:
            valid_rec = ["I", "U", "D"]
            rt = trim(col("record_type").cast("string"))
            bad_rt = df.filter(col("record_type").isNotNull() & (rt != "") & ~(rt.isin(valid_rec)))
            if bad_rt.count() > 0:
                log("Silver_HoldingHistory_Validation", "record_type not in (I,U,D)", "Alert", source)
        # --- quantity = hh_after_qty when both present ---
        if "quantity" in df.columns and "hh_after_qty" in df.columns:
            mismatch = df.filter(
                col("quantity").isNotNull() & col("hh_after_qty").isNotNull()
                & (col("quantity") != col("hh_after_qty"))
            )
            if mismatch.count() > 0:
                log("Silver_HoldingHistory_Validation", "quantity != hh_after_qty", "Alert", source)
        # --- purchase_price non-negative when present ---
        if "purchase_price" in df.columns:
            neg_price = df.filter(col("purchase_price").isNotNull() & (col("purchase_price") < 0))
            if neg_price.count() > 0:
                log("Silver_HoldingHistory_Validation", "purchase_price < 0", "Alert", source)
        # --- account_id required for fact join (from silver_trades) ---
        if "account_id" in df.columns:
            null_acc = df.filter(col("hh_t_id").isNotNull() & col("account_id").isNull())
            n = null_acc.count()
            if n > 0:
                log("Silver_HoldingHistory_Validation", f"account_id NULL (trade not in silver_trades): {n} row(s)", "Alert", source)
        # --- symbol non-empty when present ---
        if "symbol" in df.columns:
            empty_symb = df.filter(col("hh_t_id").isNotNull() & (col("symbol").isNull() | (trim(col("symbol").cast("string")) == "")))
            if empty_symb.count() > 0:
                log("Silver_HoldingHistory_Validation", "symbol NULL or empty for linked trade", "Alert", source)
        # --- Duplicate hh_h_t_id within batch (batch 1: expect unique hh_h_t_id; incremental: may have I/U/D per key) ---
        if "hh_h_t_id" in df.columns and batch_id == 1:
            dup_key = df.groupBy("hh_h_t_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
            if dup_key.count() > 0:
                log("Silver_HoldingHistory_Validation", "duplicate hh_h_t_id in batch 1", "Alert", source)
        # --- holding_date / effective_date not in future when present ---
        if "holding_date" in df.columns or "effective_date" in df.columns:
            ts_col = col("effective_date") if "effective_date" in df.columns else col("holding_date")
            future = df.filter(ts_col.isNotNull() & (ts_col > current_timestamp()))
            if future.count() > 0:
                log("Silver_HoldingHistory_Validation", "holding_date/effective_date in future", "Alert", source)
        # --- Quantity in reasonable range (e.g. not > 1e12) ---
        if "hh_after_qty" in df.columns:
            unreason = df.filter(col("hh_after_qty").isNotNull() & (col("hh_after_qty") > 1e12))
            if unreason.count() > 0:
                log("Silver_HoldingHistory_Validation", "hh_after_qty exceeds 1e12", "Alert", source)
        # --- Referential integrity: hh_t_id should exist in silver_trades ---
        try:
            trades = self.spark.table(f"{prefix}.silver_trades").filter(col("batch_id") == batch_id)
            trade_ids = trades.select("trade_id").distinct()
            missing = df.join(trade_ids, df["hh_t_id"] == trade_ids["trade_id"], "left_anti")
            n = missing.filter(col("hh_t_id").isNotNull()).count()
            if n > 0:
                log("Silver_HoldingHistory_Validation", f"hh_t_id not in silver_trades: {n} row(s)", "Alert", source)
        except Exception as e:
            log("Silver_HoldingHistory_Validation", f"RI check (silver_trades) failed: {e}", "Alert", source)
        # --- RI: account_id in silver_accounts (when present) ---
        if "account_id" in df.columns:
            try:
                accounts = self.spark.table(f"{prefix}.silver_accounts")
                acc_ids = accounts.select(col("account_id").alias("_acc_id")).distinct()
                hh_with_acc = df.filter(col("account_id").isNotNull())
                missing_acc = hh_with_acc.join(acc_ids, hh_with_acc["account_id"] == acc_ids["_acc_id"], "left_anti")
                n = missing_acc.count()
                if n > 0:
                    log("Silver_HoldingHistory_Validation", f"account_id not in silver_accounts: {n} row(s)", "Alert", source)
            except Exception as e:
                log("Silver_HoldingHistory_Validation", f"RI check (silver_accounts) failed: {e}", "Alert", source)
        # --- RI: symbol in silver_securities (when present) ---
        if "symbol" in df.columns:
            try:
                sec = self.spark.table(f"{prefix}.silver_securities")
                symbs = sec.select(col("symbol").alias("_sym")).distinct()
                hh_with_symb = df.filter(trim(col("symbol").cast("string")) != "")
                missing_symb = hh_with_symb.join(symbs, trim(hh_with_symb["symbol"].cast("string")) == trim(symbs["_sym"].cast("string")), "left_anti")
                n = missing_symb.count()
                if n > 0:
                    log("Silver_HoldingHistory_Validation", f"symbol not in silver_securities: {n} row(s)", "Alert", source)
            except Exception as e:
                log("Silver_HoldingHistory_Validation", f"RI check (silver_securities) failed: {e}", "Alert", source)
