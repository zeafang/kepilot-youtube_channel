# youtube_analytics_auth.py
import os, json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

def _creds_from_env():
    # Preferred: use the full "authorized user" JSON if provided
    yta = os.environ.get("YT_TOKEN_JSON", "").strip()
    if yta:
        info = json.loads(yta)  # must be a single valid JSON object
        # Ensure scopes are present (just in case)
        info.setdefault("scopes", SCOPES)
        return Credentials.from_authorized_user_info(info, scopes=SCOPES)

    # Fallback: build from client JSON + refresh token (if you keep a separate refresh token secret)
    client_cfg = json.loads(os.environ["YT_CLIENT_SECRET_JSON"])
    refresh_token = os.environ["GOOGLE_REFRESH_TOKEN"]  # only needed for fallback path
    if "installed" in client_cfg:
        client_id = client_cfg["installed"]["client_id"]
        client_secret = client_cfg["installed"]["client_secret"]
    else:  # sometimes it's "web"
        client_id = client_cfg["web"]["client_id"]
        client_secret = client_cfg["web"]["client_secret"]

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
