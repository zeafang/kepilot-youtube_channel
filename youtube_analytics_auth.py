# youtube_analytics_auth.py
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

def _build_creds_from_env():
    return Credentials(
        token=None,  # will be fetched via refresh using the refresh_token
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=SCOPES,
    )

def get_yta_service():
    creds = _build_creds_from_env()
    return build("youtubeAnalytics", "v2", credentials=creds, cache_discovery=False)

def get_yt_data_api_service():
    creds = _build_creds_from_env()
    return build("youtube", "v3", credentials=creds, cache_discovery=False)
