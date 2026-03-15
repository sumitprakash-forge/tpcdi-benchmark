"""Dual-mode auth configuration for Databricks Apps and local development.

In Databricks Apps (GCP): Runs as SP. User identity from x-forwarded-email header.
Locally: Uses Databricks CLI profile.
"""
import os
from fastapi import Request
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))


def get_workspace_client(request: Request = None) -> WorkspaceClient:
    """Get WorkspaceClient - uses SP credentials in Databricks Apps (GCP doesn't forward user tokens)."""
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE")
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def get_workspace_client_with_token(token: str) -> WorkspaceClient:
    """Get WorkspaceClient authenticated with a specific PAT token.

    Must explicitly set credentials_provider to prevent SDK from
    auto-detecting SP OAuth env vars (DATABRICKS_CLIENT_ID/SECRET).
    """
    from databricks.sdk.config import Config
    host = get_workspace_host()
    if not host:
        if IS_DATABRICKS_APP:
            host = f"https://{os.environ.get('DATABRICKS_HOST', '')}"
        else:
            w = get_workspace_client()
            host = str(w.config.host)
    config = Config(
        host=host,
        token=token,
        # Explicitly clear OAuth fields so SDK doesn't see multiple auth methods
        client_id=None,
        client_secret=None,
        auth_type="pat",
    )
    return WorkspaceClient(config=config)


def get_user_email(request: Request = None) -> str:
    """Get the logged-in user's email from Databricks Apps proxy headers."""
    if request:
        return (
            request.headers.get("x-forwarded-email", "")
            or request.headers.get("x-forwarded-preferred-username", "")
            or request.headers.get("x-forwarded-user", "")
        )
    return ""


def get_workspace_host() -> str:
    """Get workspace host URL with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    w = get_workspace_client()
    return w.config.host
