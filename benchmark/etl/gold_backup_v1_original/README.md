# Backup: Original Gold Logic

This folder is a **backup of the original gold layer logic** (tpcdi) before replacing it with v2-style logic.

## Contents

- `base.py` – GoldLoaderBase and _write_gold_table
- `dimensions.py` – DimCustomer, DimAccount, DimCompany, DimSecurity, DimDate, DimTradeType, DimStatusType, DimIndustry (with placeholder rows, dedupe, MERGE for incremental)
- `facts.py` – FactTrade (left joins, late_arriving_flag), FactMarketHistory, FactCashBalances, FactHoldings (left joins; no silver_trades join)
- `financials.py` – GoldFinancials (MERGE upsert on co_name_or_cik, year, quarter)
- `__init__.py` – GoldETL orchestrator and run_gold_load

## How to revert to this logic

If you later want to restore the original gold behavior:

1. Copy these files **over** the current `gold/` module:
   ```bash
   cd tpcdi/benchmark/etl
   cp -f gold_backup_v1_original/base.py       gold/
   cp -f gold_backup_v1_original/dimensions.py gold/
   cp -f gold_backup_v1_original/facts.py      gold/
   cp -f gold_backup_v1_original/financials.py gold/
   cp -f gold_backup_v1_original/__init__.py   gold/
   ```

2. Or from repo root:
   ```bash
   cp tpcdi/benchmark/etl/gold_backup_v1_original/*.py tpcdi/benchmark/etl/gold/
   ```
   (Only `.py` so README is not copied into `gold/`.)

3. Run your tpcdi pipeline again; gold will use the original logic (placeholders, left joins, no dim_time/dim_broker/fact_watches, etc.).

## Do not import from this backup

Code should import from `benchmark.etl.gold`, not `benchmark.etl.gold_backup_v1_original`. This folder is for restore only.
