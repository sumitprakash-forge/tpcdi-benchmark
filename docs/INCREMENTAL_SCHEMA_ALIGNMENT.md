# Incremental load – schema alignment with v2 Databricks

Incremental logic in the tpcdi flow is aligned to **v2 Databricks** (`v2/databricks/sql/gold/incremental/` and silver/bronze incremental).

## Schema alignment (batch gold dimensions) — implemented

**v2** batch gold tables **gold_dim_customer** and **gold_dim_account** include SCD Type 2 columns. Tpcdi batch flow now matches:

| Table | v2 batch columns | tpcdi batch (implemented) |
|-------|-------------------|---------------------------|
| **gold_dim_customer** | `is_current`, `start_date`, `end_date`, `batch_id` | ✅ Same: `is_current`, `start_date` (from effective_date/load_timestamp), `end_date` (9999-12-31), `batch_id` (1 for batch) |
| **gold_dim_account**  | `sk_customer_id`, `is_current`, `start_date`, `end_date`, `batch_id` | ✅ Same: join to gold_dim_customer for `sk_customer_id`, plus `is_current`, `start_date`, `end_date`, `batch_id` |

**Why it matters**

- **Incremental gold dim_customer/dim_account (v2):** MERGE closes current rows (`is_current=false`, `end_date=effective_date`), then INSERTs new versions from silver with `is_current`, `start_date`, `end_date`, `batch_id`. So the gold table must have these columns.
- **Fact tables (e.g. fact_trade):** v2 joins to dim_customer/dim_account with **point-in-time** conditions:  
  `trade_dts >= dim.start_date AND trade_dts < dim.end_date`.  
  So dimensions must have `start_date` and `end_date`.

**Conclusion:** To run v2-style incremental in tpcdi (MERGE close + INSERT for dim_customer/dim_account, and correct fact joins), batch gold **must** produce the same schema as v2 (including `is_current`, `start_date`, `end_date`, `batch_id` for both; and `sk_customer_id` for dim_account).

## Status

Schema and incremental logic are **implemented** (user confirmed).

- **gold_dim_customer (batch):** outputs `is_current`, `start_date`, `end_date`, `batch_id`.
- **gold_dim_account (batch):** outputs `sk_customer_id` (from join to gold_dim_customer), `is_current`, `start_date`, `end_date`, `batch_id`.

Incremental load:

1. **Bronze:** Already correct (batch 2+ loads Customer.txt, Account.txt, Trade.txt, etc., append mode).
2. **Silver:** Already correct (SCD Type 2 MERGE + INSERT for customers/accounts/trades from bronze_customer/bronze_account/bronze_trade).
3. **Gold dim_customer:** MERGE close current row by customer_id, then INSERT from silver_customers where batch_id = current batch.
4. **Gold dim_account:** MERGE close current row by account_id, then INSERT from silver_accounts where batch_id = current batch (with join to gold_dim_customer for sk_customer_id and point-in-time).
5. **Gold facts:** When appending (incremental), filter silver by `batch_id = current_batch` and (where applicable) `is_current = true`, `record_type IN ('I','U')`; join to dimensions using start_date/end_date when present.
