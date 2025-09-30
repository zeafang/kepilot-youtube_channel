import os, json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

def _creds_from_env():
    # Refresh token is just a string (not JSON)
    refresh_token = os.environ.get("YT_TOKEN_JSON")
    if not refresh_token:
        raise RuntimeError("Missing YT_TOKEN_JSON (refresh token)")

    # Client secret is full JSON
    client_cfg_raw = os.environ.get("YT_CLIENT_SECRET_JSON")
    if not client_cfg_raw:
        raise RuntimeError("Missing YT_CLIENT_SECRET_JSON (client JSON)")

    client_cfg = json.loads(client_cfg_raw)
    block = client_cfg.get("installed") or client_cfg.get("web") or {}
    client_id = block.get("client_id")
    client_secret = block.get("client_secret")

    if not client_id or not client_secret:
        raise RuntimeError("client_id/client_secret missing from YT_CLIENT_SECRET_JSON")

    return Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES,
    )

def get_yta_service():
    creds = _creds_from_env()
    return build("youtubeAnalytics", "v2", credentials=creds, cache_discovery=False)

def get_yt_data_api_service():
    creds = _creds_from_env()
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

