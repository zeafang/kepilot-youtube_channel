# youtube_analytics_auth.py
import os, json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

def _creds_from_env():
    token_json = os.environ.get("YT_TOKEN_JSON")
    if token_json:
        info = json.loads(token_json)
        info.setdefault("token_uri", "https://oauth2.googleapis.com/token")
        info.setdefault("scopes", SCOPES)

        # Ensure client_id/secret exist. If not, fill from client secret JSON.
        if not info.get("client_id") or not info.get("client_secret"):
            client_cfg_raw = os.environ.get("YT_CLIENT_SECRET_JSON")
            if not client_cfg_raw:
                raise RuntimeError(
                    "YT_TOKEN_JSON provided but missing client_id/client_secret, and "
                    "YT_CLIENT_SECRET_JSON is not set."
                )
            cfg = json.loads(client_cfg_raw)
            block = cfg.get("installed") or cfg.get("web") or {}
            info["client_id"] = info.get("client_id") or block.get("client_id")
            info["client_secret"] = info.get("client_secret") or block.get("client_secret")

        return Credentials.from_authorized_user_info(info, scopes=SCOPES)

    # Fallback path: use client secret JSON + refresh token secret
    client_cfg_raw = os.environ.get("YT_CLIENT_SECRET_JSON")
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")
    if not client_cfg_raw or not refresh_token:
        raise RuntimeError(
            "Missing required env vars. Set either YT_TOKEN_JSON, or both "
            "YT_CLIENT_SECRET_JSON and GOOGLE_REFRESH_TOKEN."
        )

    cfg = json.loads(client_cfg_raw)
    block = cfg.get("installed") or cfg.get("web") or {}
    client_id = block["client_id"]
    client_secret = block["client_secret"]

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

