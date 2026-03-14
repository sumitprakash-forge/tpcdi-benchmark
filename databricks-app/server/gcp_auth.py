"""Google Cloud authentication for Dataproc operations.

Supports multiple auth strategies:
1. Application Default Credentials (ADC) - works locally with `gcloud auth application-default login`
2. User-provided access token (pasted from `gcloud auth print-access-token`)
3. User-provided refresh token (from ~/.config/gcloud/application_default_credentials.json)
   Auto-refreshes using the standard Google Cloud SDK OAuth client credentials.
"""
import json
import os
from typing import Optional

import google.auth
import google.auth.transport.requests
from google.oauth2 import credentials as oauth2_credentials

# Google Cloud SDK's public OAuth2 client credentials (embedded in gcloud source, publicly known)
GCLOUD_CLIENT_ID = "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com"
GCLOUD_CLIENT_SECRET = "d-FL95Q19q7MQmFpd7hHD0Ty"
DATAPROC_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

# In-memory token store
_cached_credentials: Optional[oauth2_credentials.Credentials] = None
_auth_method: str = "none"  # "adc", "access_token", "refresh_token"
_user_email: Optional[str] = None


def get_auth_status() -> dict:
    """Return current Google auth status."""
    global _cached_credentials, _auth_method, _user_email

    # Try ADC first if we have no cached creds
    if _cached_credentials is None:
        _try_adc()

    if _cached_credentials is None:
        return {"status": "unauthenticated", "method": "none", "message": "Not authenticated. Provide a Google Cloud token."}

    # Check if token is valid/refreshable
    try:
        if _cached_credentials.expired or not _cached_credentials.token:
            request = google.auth.transport.requests.Request()
            _cached_credentials.refresh(request)
    except Exception as e:
        return {
            "status": "expired",
            "method": _auth_method,
            "message": f"Token expired and could not refresh: {e}",
            "account": _user_email,
        }

    return {
        "status": "authenticated",
        "method": _auth_method,
        "account": _user_email or "unknown",
        "message": f"Authenticated via {_auth_method}",
    }


def get_credentials() -> Optional[oauth2_credentials.Credentials]:
    """Get valid Google credentials, refreshing if needed.

    Proactively refreshes tokens that will expire within 5 minutes to avoid
    mid-request 401 errors from gRPC clients that cache the token at creation.
    """
    global _cached_credentials

    if _cached_credentials is None:
        _try_adc()

    if _cached_credentials is None:
        return None

    try:
        import datetime
        _REFRESH_BUFFER = datetime.timedelta(minutes=5)
        needs_refresh = (
            not _cached_credentials.token
            or _cached_credentials.expired
            or (
                _cached_credentials.expiry is not None
                and _cached_credentials.expiry - datetime.datetime.utcnow() < _REFRESH_BUFFER
            )
        )
        if needs_refresh:
            request = google.auth.transport.requests.Request()
            _cached_credentials.refresh(request)
    except Exception:
        return None

    return _cached_credentials


def set_access_token(token: str) -> dict:
    """Set credentials from a user-provided access token."""
    global _cached_credentials, _auth_method, _user_email

    creds = oauth2_credentials.Credentials(token=token)

    # Validate the token by making a tokeninfo request
    try:
        import urllib.request
        req = urllib.request.Request(
            f"https://oauth2.googleapis.com/tokeninfo?access_token={token}"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            info = json.loads(resp.read())
            _user_email = info.get("email", "unknown")
    except Exception as e:
        return {"status": "error", "message": f"Invalid access token: {e}"}

    _cached_credentials = creds
    _auth_method = "access_token"
    return {"status": "authenticated", "method": "access_token", "account": _user_email}


def set_refresh_token(refresh_token: str, client_id: str = "", client_secret: str = "") -> dict:
    """Set credentials from a user-provided refresh token. Uses gcloud's OAuth client by default."""
    global _cached_credentials, _auth_method, _user_email

    cid = client_id or GCLOUD_CLIENT_ID
    csecret = client_secret or GCLOUD_CLIENT_SECRET

    creds = oauth2_credentials.Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=cid,
        client_secret=csecret,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=DATAPROC_SCOPES,
    )

    # Try refreshing to validate
    try:
        request = google.auth.transport.requests.Request()
        creds.refresh(request)
    except Exception as e:
        return {"status": "error", "message": f"Failed to refresh token: {e}"}

    # Get user email from the refreshed token
    try:
        import urllib.request
        req = urllib.request.Request(
            f"https://oauth2.googleapis.com/tokeninfo?access_token={creds.token}"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            info = json.loads(resp.read())
            _user_email = info.get("email", "unknown")
    except Exception:
        _user_email = "unknown"

    _cached_credentials = creds
    _auth_method = "refresh_token"
    return {"status": "authenticated", "method": "refresh_token", "account": _user_email}


def set_adc_json(adc_json: str) -> dict:
    """Set credentials from the full ADC JSON content (from ~/.config/gcloud/application_default_credentials.json)."""
    try:
        data = json.loads(adc_json)
    except json.JSONDecodeError:
        return {"status": "error", "message": "Invalid JSON"}

    if data.get("type") == "authorized_user":
        return set_refresh_token(
            refresh_token=data.get("refresh_token", ""),
            client_id=data.get("client_id", ""),
            client_secret=data.get("client_secret", ""),
        )
    elif data.get("type") == "service_account":
        return {"status": "error", "message": "Service account JSON not supported. Use refresh token or access token."}
    else:
        return {"status": "error", "message": f"Unsupported credential type: {data.get('type')}"}


def clear_credentials():
    """Clear cached credentials."""
    global _cached_credentials, _auth_method, _user_email
    _cached_credentials = None
    _auth_method = "none"
    _user_email = None


def _try_adc():
    """Try to load Application Default Credentials."""
    global _cached_credentials, _auth_method, _user_email
    try:
        creds, project = google.auth.default(scopes=DATAPROC_SCOPES)
        request = google.auth.transport.requests.Request()
        creds.refresh(request)
        _cached_credentials = creds
        _auth_method = "adc"

        # Try to get email
        try:
            if hasattr(creds, 'service_account_email'):
                _user_email = creds.service_account_email
            elif hasattr(creds, '_account'):
                _user_email = creds._account
            else:
                import urllib.request
                req = urllib.request.Request(
                    f"https://oauth2.googleapis.com/tokeninfo?access_token={creds.token}"
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    info = json.loads(resp.read())
                    _user_email = info.get("email", "unknown")
        except Exception:
            _user_email = "adc-user"
    except Exception:
        pass  # ADC not available
