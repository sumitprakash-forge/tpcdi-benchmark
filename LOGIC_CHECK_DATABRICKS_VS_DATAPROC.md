# V1 Logic Check: Databricks vs Dataproc (per table)

**Conclusion:** For every table, **the same transform/load logic runs on both platforms**. There are **no platform-specific branches** inside ETL loaders. Differences are limited to (1) platform adapter implementation (read/write paths, default table format), (2) runner orchestration (database/catalog creation), and (3) optional config (e.g. CustomerMgmt XML format) that can be set the same for both.

---

## Summary table

| Layer  | Table                     | Same logic? | Notes |
|--------|---------------------------|-------------|--------|
| Bronze | bronze_date               | ✅ Yes      | Single code path; platform.read_raw_file + write_table only. |
| Bronze | bronze_time               | ✅ Yes      | Same. |
| Bronze | bronze_status_type        | ✅ Yes      | Same. |
| Bronze | bronze_tax_rate           | ✅ Yes      | Same. |
| Bronze | bronze_trade_type         | ✅ Yes      | Same. |
| Bronze | bronze_industry          | ✅ Yes      | Same. |
| Bronze | bronze_hr                 | ✅ Yes      | Same. |
| Bronze | bronze_customer_mgmt      | ✅ Yes*     | *Same code path when same config (customer_mgmt_xml_format). No `isinstance(platform)` in loader; config is passed from runner and can be identical. |
| Bronze | bronze_trade              | ✅ Yes      | Same. |
| Bronze | bronze_daily_market       | ✅ Yes      | Same. |
| Bronze | bronze_prospect           | ✅ Yes      | Same. |
| Bronze | bronze_cash_transaction   | ✅ Yes      | Same. |
| Bronze | bronze_holding_history   | ✅ Yes      | Same. |
| Bronze | bronze_watch_history      | ✅ Yes      | Same. |
| Bronze | bronze_finwire            | ✅ Yes      | Same. |
| Silver | silver_date              | ✅ Yes      | Single code path. |
| Silver | silver_time              | ✅ Yes      | Same. |
| Silver | silver_status_type       | ✅ Yes      | Same. |
| Silver | silver_trade_type        | ✅ Yes      | Same. |
| Silver | silver_industry          | ✅ Yes      | Same. |
| Silver | silver_tax_rate          | ✅ Yes      | Same. |
| Silver | silver_companies         | ✅ Yes      | Same (try_to_date for dates). |
| Silver | silver_securities        | ✅ Yes      | Same. |
| Silver | silver_financials        | ✅ Yes      | Same. |
| Silver | silver_customers         | ✅ Yes      | Same. |
| Silver | silver_accounts          | ✅ Yes      | Same. |
| Silver | silver_trades             | ✅ Yes      | Same. |
| Silver | silver_daily_market       | ✅ Yes      | Same. |
| Silver | silver_prospect           | ✅ Yes      | Same. |
| Silver | silver_cash_transaction  | ✅ Yes      | Same. |
| Silver | silver_watch_history     | ✅ Yes      | Same. |
| Silver | silver_holding_history  | ✅ Yes      | Same. |
| Silver | silver_dq_validation     | ✅ Yes      | DQ runner uses platform.get_spark() only; no platform branch in rules. |
| Gold   | gold_dim_date             | ✅ Yes      | Single code path. |
| Gold   | gold_dim_time             | ✅ Yes      | Same. |
| Gold   | gold_dim_customer         | ✅ Yes      | Uses hasattr(platform, "merge_upsert"); both adapters have it. Same logic. |
| Gold   | gold_dim_account          | ✅ Yes      | Same. |
| Gold   | gold_dim_broker           | ✅ Yes      | Reads bronze_hr; same filter (broker job code string or "1"). |
| Gold   | gold_dim_company          | ✅ Yes      | Same. |
| Gold   | gold_dim_security         | ✅ Yes      | Same. |
| Gold   | gold_dim_trade_type       | ✅ Yes      | Same. |
| Gold   | gold_dim_status_type     | ✅ Yes      | Same. |
| Gold   | gold_dim_industry        | ✅ Yes      | Same. |
| Gold   | gold_financials           | ✅ Yes      | Same (try/except on both; not platform-specific). |
| Gold   | gold_prospect             | ✅ Yes      | Same. |
| Gold   | gold_fact_trade           | ✅ Yes      | Same. |
| Gold   | gold_fact_market_history  | ✅ Yes      | Same. |
| Gold   | gold_dim_messages         | ✅ Yes      | Created/ensured by DQ; same. |
| Gold   | gold_fact_cash_balances   | ✅ Yes      | Same. |
| Gold   | gold_fact_holdings       | ✅ Yes      | Same. |
| Gold   | gold_fact_watches        | ✅ Yes      | Same. |

---

## Where code differs (orchestration / adapter only)

1. **Runner**
   - **create_platform_adapter:** Returns `DatabricksPlatform` or `DataprocPlatform`; same interface.
   - **create_database:** Databricks: `platform.create_database("", catalog=..., schema=...)` (Unity Catalog). Dataproc: `platform.create_database(spark_db)` (Hive DB). Table ETL logic is unchanged; only DB/schema creation differs.

2. **Platform adapters**
   - **read_raw_file / read_batch_files:** Same signature; Databricks resolves DBFS/Volumes/GCS, Dataproc resolves GCS. Same behavior from ETL’s perspective.
   - **write_table:** Same signature. Databricks uses Delta by default; Dataproc uses `config.table_format` (default parquet). If Dataproc is run with `--table_format delta`, write behavior aligns with Databricks.
   - **merge_upsert / merge_scd2:** Both implement the same MERGE SQL. Dataproc falls back to overwrite when format ≠ delta. Same **logic** when both use Delta.

3. **Config (optional)**
   - **customer_mgmt_xml_format:** Default `None` → `"xml"` in bronze. Can be set per run; no platform-specific default in code.

---

## Checks performed

- **Grep:** No `isinstance(platform, DatabricksPlatform)` or `isinstance(platform, DataprocPlatform)` in any ETL loader (bronze/*.py, silver/*.py, gold/*.py). Only TYPE_CHECKING imports reference those types.
- **Gold dimensions:** Uses `hasattr(self.platform, "merge_upsert")`; both adapters implement `merge_upsert` → same path.
- **Bronze customer_mgmt:** Uses config `use_udtf` and `xml_fmt` only; no platform branch. Same config ⇒ same logic.
- **Silver:** No platform conditionals; all loaders use `self.platform` for read/write only.
- **Gold try/except:** gold_financials, gold_prospect, gold_fact_cash_balances, gold_fact_holdings, gold_fact_watches use try/except on both platforms; behavior is the same.

---

## Recommendation

- To keep **identical logic** on Databricks and Dataproc: use the same config (e.g. `customer_mgmt_xml_format` unset or same value) and, on Dataproc, use `table_format=delta` if you want merge_upsert/merge_scd2 behavior to match Databricks.
