"""Run history persistence using Databricks workspace files API."""
import json
import traceback
from datetime import datetime, timezone
from typing import Optional
from fastapi import Request

from server.config import get_workspace_client, get_user_email

RUN_HISTORY_DIR = "/Workspace/Users/{user_email}/tpcdi-benchmark/runs"

def _get_runs_path(user_email: str = "") -> str:
    if user_email:
        return RUN_HISTORY_DIR.format(user_email=user_email)
    return "/Workspace/Users/sumit.prakash@databricks.com/tpcdi-benchmark/runs"

def save_run(request: Request, run_data: dict) -> dict:
    try:
        w = get_workspace_client(request)
        user_email = get_user_email(request)
        runs_path = _get_runs_path(user_email)
        run_id = run_data.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        platform = run_data.get("platform", "unknown")
        mode = run_data.get("mode", "unknown")
        filename = f"{platform}_{mode}_{run_id}.json"
        file_path = f"{runs_path}/{filename}"
        run_data["saved_at"] = datetime.now(timezone.utc).isoformat()
        run_data["user_email"] = user_email
        content = json.dumps(run_data, indent=2, default=str)
        import io
        w.workspace.upload(file_path, io.BytesIO(content.encode("utf-8")), overwrite=True, format=None)
        return {"status": "saved", "path": file_path, "filename": filename}
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

def list_runs(request: Request, limit: int = 50) -> list:
    try:
        w = get_workspace_client(request)
        user_email = get_user_email(request)
        runs_path = _get_runs_path(user_email)
        try:
            items = list(w.workspace.list(runs_path))
        except Exception:
            return []
        runs = []
        for item in items:
            if not item.path or not item.path.endswith(".json"):
                continue
            try:
                resp = w.workspace.download(item.path)
                content = resp.read().decode("utf-8")
                run_data = json.loads(content)
                run_data["_filename"] = item.path.split("/")[-1]
                runs.append(run_data)
            except Exception:
                continue
        runs.sort(key=lambda r: r.get("saved_at", ""), reverse=True)
        return runs[:limit]
    except Exception as e:
        traceback.print_exc()
        return []

def delete_run(request: Request, filename: str) -> dict:
    try:
        w = get_workspace_client(request)
        user_email = get_user_email(request)
        runs_path = _get_runs_path(user_email)
        file_path = f"{runs_path}/{filename}"
        w.workspace.delete(file_path)
        return {"status": "deleted", "filename": filename}
    except Exception as e:
        return {"status": "error", "message": str(e)}
