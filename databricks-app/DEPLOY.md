# TPC-DI Benchmark App - Deployment Guide

## Architecture

```
databricks-app/
├── app.yaml              # Databricks Apps config (entry point command)
├── app.py                # FastAPI server serving API + React SPA
├── requirements.txt      # Python dependencies (installed by Databricks Apps)
├── server/
│   ├── config.py         # Dual-mode auth (local dev vs Databricks Apps)
│   └── routes.py         # All API endpoints
└── frontend/
    ├── package.json      # React/Vite project
    ├── src/              # React source (TypeScript + TailwindCSS)
    └── dist/             # Built frontend (committed, served by FastAPI)
```

## Prerequisites

- **Databricks CLI** v0.229.0+ installed (`databricks --version`)
- **Node.js** 18+ and npm (`node --version`)
- **Python** 3.11+ (for local development)
- A Databricks workspace with:
  - Unity Catalog enabled
  - Permission to create apps, jobs, and access catalogs
  - A service principal (auto-created by Databricks Apps)

## Step 1: Authenticate with Databricks CLI

```bash
# Login to your workspace
databricks auth login <WORKSPACE_URL> --profile=<PROFILE_NAME>

# Verify authentication
databricks auth profiles | grep <PROFILE_NAME>
```

Example:
```bash
databricks auth login https://8259564945738773.3.gcp.databricks.com --profile=fe-vm-gcp-tpcdi-classic
```

## Step 2: Build the Frontend

```bash
cd databricks-app/frontend
npm install
npm run build
```

This produces the `frontend/dist/` directory which FastAPI serves as a static SPA.

## Step 3: Create the Databricks App

```bash
databricks apps create <APP_NAME> \
  --description "TPC-DI Benchmark Runner" \
  --profile=<PROFILE_NAME>
```

Example:
```bash
databricks apps create tpcdi-benchmark \
  --description "TPC-DI Benchmark Runner" \
  --profile=fe-vm-gcp-tpcdi-classic
```

## Step 4: Upload Code to Workspace

Upload all app files to a workspace directory:

```bash
cd databricks-app

# Upload Python files
databricks workspace import <WORKSPACE_PATH>/app.yaml --file app.yaml --format AUTO --overwrite --profile=<PROFILE_NAME>
databricks workspace import <WORKSPACE_PATH>/app.py --file app.py --format AUTO --overwrite --profile=<PROFILE_NAME>
databricks workspace import <WORKSPACE_PATH>/requirements.txt --file requirements.txt --format AUTO --overwrite --profile=<PROFILE_NAME>

# Upload server directory
databricks workspace import-dir server <WORKSPACE_PATH>/server --overwrite --profile=<PROFILE_NAME>

# Upload built frontend
databricks workspace import-dir frontend/dist <WORKSPACE_PATH>/frontend/dist --overwrite --profile=<PROFILE_NAME>
```

Replace `<WORKSPACE_PATH>` with your target path, e.g.:
```
/Workspace/Users/<SERVICE_PRINCIPAL_ID>/tpcdi-benchmark
```

## Step 5: Deploy the App

```bash
databricks apps deploy <APP_NAME> \
  --source-code-path <WORKSPACE_PATH> \
  --profile=<PROFILE_NAME>
```

Example:
```bash
databricks apps deploy tpcdi-benchmark \
  --source-code-path /Workspace/Users/2c054b5f-823d-48f2-a387-6bb8eb852e04/tpcdi-benchmark \
  --profile=fe-vm-gcp-tpcdi-classic
```

Wait for the deployment to show `"state": "SUCCEEDED"`.

## Step 6: Configure App Permissions

The app runs as a service principal (SP). The SP needs permissions to:

1. **Read/create Unity Catalog resources** (catalogs, external locations, volumes)
2. **Create and run jobs**
3. **Access storage credentials** (for external locations)

### Grant SP Permissions

Find the SP ID from the app details:
```bash
databricks apps get <APP_NAME> --profile=<PROFILE_NAME>
```

Grant catalog access:
```sql
-- In a Databricks SQL warehouse or notebook
GRANT USE CATALOG ON CATALOG <catalog_name> TO `<sp-application-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog_name>.* TO `<sp-application-id>`;
GRANT ALL PRIVILEGES ON CATALOG <catalog_name> TO `<sp-application-id>`;
```

## Step 7: Verify Deployment

Get the app URL:
```bash
databricks apps get <APP_NAME> --profile=<PROFILE_NAME>
```

Open the app URL in your browser. The app has three tabs:
- **Data Generation** - Generate TPC-DI raw data
- **Databricks** - Configure and run benchmarks (3 sequential steps)
- **Results** - View benchmark results

Check app logs if needed:
```bash
databricks apps logs <APP_NAME> --tail-lines 100 --profile=<PROFILE_NAME>
```

Or visit `<APP_URL>/logz` in the browser.

## Updating the App

After making code changes:

```bash
# 1. Rebuild frontend (if frontend changed)
cd frontend && npm run build && cd ..

# 2. Upload changed files
databricks workspace import-dir frontend/dist <WORKSPACE_PATH>/frontend/dist --overwrite --profile=<PROFILE_NAME>
databricks workspace import-dir server <WORKSPACE_PATH>/server --overwrite --profile=<PROFILE_NAME>

# 3. Redeploy
databricks apps deploy <APP_NAME> \
  --source-code-path <WORKSPACE_PATH> \
  --profile=<PROFILE_NAME>
```

## Local Development

Run the app locally for faster iteration:

```bash
# Terminal 1: Start backend
cd databricks-app
export DATABRICKS_PROFILE=<PROFILE_NAME>
pip install -r requirements.txt
python app.py

# Terminal 2: Start frontend dev server (hot reload)
cd databricks-app/frontend
npm run dev
```

The frontend dev server (port 5173) proxies API calls to the backend (port 8000). Configure the proxy in `frontend/vite.config.ts` if not already set.

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `No command to run` on deploy | Ensure `app.yaml` is uploaded to the workspace path |
| `App Not Available` | Check app.py listens on port 8000 (`PORT` env var) |
| Prerequisites check fails | Verify SP has catalog/storage permissions |
| Frontend shows blank page | Rebuild frontend (`npm run build`) and re-upload `dist/` |
| `INTERNAL_ERROR` on job runs | Check job logs in Databricks UI for notebook errors |
| Stale frontend after deploy | Browser cache - hard refresh (Ctrl+Shift+R) |
