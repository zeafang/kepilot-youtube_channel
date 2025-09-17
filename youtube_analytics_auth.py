# youtube_analytics_auth.py
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",   # <= add this
]


def get_yta_service(client_secret="client_secret_1049838121814-va0b088vak8n1a3jk5m8novntfltivjh.apps.googleusercontent.com.json", token_path="token.json"):
    token_path = Path(token_path)
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret, SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json())
    return build("youtubeAnalytics", "v2", credentials=creds)

if __name__ == "__main__":
    yta = get_yta_service()
    print("✅ YouTube Analytics service ready!")
