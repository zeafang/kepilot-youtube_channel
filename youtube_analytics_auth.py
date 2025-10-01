import os, json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]
TOKEN_URI = "https://oauth2.googleapis.com/token"

def _creds_from_env():
    # Accept either secret name for the refresh token
    refresh_token = os.environ.get("YT_TOKEN_JSON") or os.environ.get("GOOGLE_REFRESH_TOKEN")
    client_cfg_raw = os.environ.get("YT_CLIENT_SECRET_JSON")

    missing = []
    if not refresh_token:
        missing.append("YT_TOKEN_JSON/GOOGLE_REFRESH_TOKEN")
    if not client_cfg_raw:
        missing.append("YT_CLIENT_SECRET_JSON")
    if missing:
        present = {k: ("SET" if os.environ.get(k) else "MISSING")
                   for k in ["YT_TOKEN_JSON", "GOOGLE_REFRESH_TOKEN", "YT_CLIENT_SECRET_JSON"]}
        raise RuntimeError(f"Missing required envs: {', '.join(missing)} | Presence: {present}")

    # Parse client secret JSON (from Google Cloud)
    client_cfg = json.loads(client_cfg_raw)
    block = client_cfg.get("installed") or client_cfg.get("web") or {}
    client_id = block.get("client_id")
    client_secret = block.get("client_secret")
    if not client_id or not client_secret:
        raise RuntimeError("client_id/client_secret missing in YT_CLIENT_SECRET_JSON")

    return Credentials(
        token=None,  # access token will be fetched via refresh
        refresh_token=refresh_token,
        token_uri=TOKEN_URI,
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

