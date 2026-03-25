"""Run history persistence using Databricks workspace files API."""
import json
import base64
import traceback
from datetime import datetime, timezone
from fastapi import Request

from server.config import get_workspace_client, get_user_email

RUN_HISTORY_DIR = "/Workspace/Users/{user_email}/tpcdi-benchmark/runs"

def _get_runs_path(w, user_email: str = "") -> str:
    """Resolve runs path using SP identity (same pattern as _resolve_notebook_path)."""
    try:
        me = w.current_user.me()
        identity = me.user_name or str(me.id)
    except Exception:
        identity = user_email or "sumit.prakash@databricks.com"
    return f"/Workspace/Users/{identity}/tpcdi-benchmark/runs"

def save_run(request: Request, run_data: dict, w=None) -> dict:
    try:
        if w is None:
            w = get_workspace_client(request)
        user_email = get_user_email(request)
        runs_path = _get_runs_path(w, user_email)
        run_id = run_data.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        platform = run_data.get("platform", "unknown")
        mode = run_data.get("mode", "unknown")
        filename = f"{platform}_{mode}_{run_id}.json"
        file_path = f"{runs_path}/{filename}"
        run_data["saved_at"] = datetime.now(timezone.utc).isoformat()
        run_data["user_email"] = user_email
        content = json.dumps(run_data, indent=2, default=str)
        # Ensure parent directory exists
        try:
            w.workspace.mkdirs(runs_path)
        except Exception:
            pass
        # Use workspace import REST API with base64 content
        content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        w.api_client.do(
            "POST",
            "/api/2.0/workspace/import",
            body={
                "path": file_path,
                "content": content_b64,
                "format": "AUTO",
                "overwrite": True,
            },
        )
        return {"status": "saved", "path": file_path, "filename": filename}
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

def list_runs(request: Request, limit: int = 50, w=None) -> list:
    try:
        if w is None:
            w = get_workspace_client(request)
        user_email = get_user_email(request)
        runs_path = _get_runs_path(w, user_email)
        try:
            items = list(w.workspace.list(runs_path))
        except Exception:
            return []
        runs = []
        for item in items:
            if not item.path or not item.path.endswith(".json"):
                continue
            try:
                # Use workspace export REST API to read raw content
                resp = w.api_client.do(
                    "GET",
                    "/api/2.0/workspace/export",
                    query={"path": item.path, "format": "AUTO"},
                )
                file_content = base64.b64decode(resp.get("content", "")).decode("utf-8")
                run_data = json.loads(file_content)
                run_data["_filename"] = item.path.split("/")[-1]
                runs.append(run_data)
            except Exception:
                continue
        runs.sort(key=lambda r: r.get("saved_at", ""), reverse=True)
        return runs[:limit]
    except Exception as e:
        traceback.print_exc()
        return []

def patch_run(request: Request, filename: str, updates: dict, w=None) -> dict:
    """Merge updates into an existing run history file."""
    try:
        if w is None:
            w = get_workspace_client(request)
        user_email = get_user_email(request)
        runs_path = _get_runs_path(w, user_email)
        file_path = f"{runs_path}/{filename}"
        # Read existing
        resp = w.api_client.do("GET", "/api/2.0/workspace/export", query={"path": file_path, "format": "AUTO"})
        existing = json.loads(base64.b64decode(resp.get("content", "")).decode("utf-8"))
        # Deep merge steps if provided
        if "steps" in updates and isinstance(updates["steps"], list) and isinstance(existing.get("steps"), list):
            for i, step_update in enumerate(updates["steps"]):
                if step_update and i < len(existing["steps"]):
                    existing["steps"][i].update(step_update)
            updates = {k: v for k, v in updates.items() if k != "steps"}
        existing.update(updates)
        existing["updated_at"] = datetime.now(timezone.utc).isoformat()
        content = json.dumps(existing, indent=2, default=str)
        content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        w.api_client.do("POST", "/api/2.0/workspace/import", body={
            "path": file_path, "content": content_b64, "format": "AUTO", "overwrite": True,
        })
        return {"status": "updated", "path": file_path}
    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


def delete_run(request: Request, filename: str, w=None) -> dict:
    try:
        if w is None:
            w = get_workspace_client(request)
        user_email = get_user_email(request)
        runs_path = _get_runs_path(w, user_email)
        file_path = f"{runs_path}/{filename}"
        w.workspace.delete(file_path)
        return {"status": "deleted", "filename": filename}
    except Exception as e:
        return {"status": "error", "message": str(e)}
