#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, List

import pandas as pd

from youtube_analytics_auth import get_yta_service  # Analytics API (v2)

# Optional: YouTube Data API (v3) for channel/video dates.
# If your token doesn't include youtube.readonly, these helpers will fall back gracefully.
try:
    from googleapiclient.discovery import build as build_service
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    HAVE_DATA_API = True
except Exception:
    HAVE_DATA_API = False

OUTPUT_DIR = Path("yta_outputs")
MAX_RESULTS = 200

# ----------------- Metrics -----------------
METRICS_BASIC = ",".join([
    "views",
    "estimatedMinutesWatched",
    "averageViewDuration",
    "averageViewPercentage",
    "subscribersGained",
    "subscribersLost",
    "likes",
    "dislikes",
    "comments",
    "shares",
])

REPORTS: Dict[str, Tuple[str, Optional[str], Optional[str], Optional[int], str, str]] = {
    "day": (
        METRICS_BASIC, "day", None, None, "day", "yta_daily_trend"
    ),
    "country": (
        METRICS_BASIC, "-views", None, MAX_RESULTS, "country", "country"
    ),
    "traffic_sources": (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage",
        "-views", None, None, "insightTrafficSourceType", "yta_traffic_sources"
    ),
    "top_videos": (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost,likes,comments,shares",
        "-views", None, MAX_RESULTS, "video", "yta_top_videos"
    ),
}

# ----------------- YouTube Data API helpers (optional) -----------------
def build_youtube_data_client() -> Optional[any]:
    """
    Build YouTube Data API v3 client using the same token.json as Analytics.
    Requires the scope: https://www.googleapis.com/auth/youtube.readonly
    """
    if not HAVE_DATA_API:
        return None
    token = Path("token.json")
    if not token.exists():
        return None
    try:
        creds = Credentials.from_authorized_user_file(token, scopes=[
            "https://www.googleapis.com/auth/yt-analytics.readonly",
            "https://www.googleapis.com/auth/youtube.readonly",
        ])
        if not creds.valid and creds.refresh_token:
            creds.refresh(Request())
            token.write_text(creds.to_json())
        return build_service("youtube", "v3", credentials=creds)
    except Exception:
        return None

def get_channel_created_date(ytd) -> Optional[date]:
    """Return channel snippet.publishedAt as date, or None."""
    try:
        resp = ytd.channels().list(part="snippet", mine=True, maxResults=1).execute()
        items = resp.get("items", [])
        if not items:
            return None
        published = items[0]["snippet"]["publishedAt"]  # e.g., "2012-05-01T12:34:56Z"
        return datetime.fromisoformat(published.replace("Z", "+00:00")).date()
    except Exception:
        return None

def get_first_video_published_date(ytd) -> Optional[date]:
    """Return the earliest uploaded video's publishedAt date (may paginate a bit)."""
    try:
        # First, get uploads playlist id
        ch = ytd.channels().list(part="contentDetails", mine=True, maxResults=1).execute()
        items = ch.get("items", [])
        if not items:
            return None
        uploads_pl = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

        earliest: Optional[date] = None
        page_token = None
        seen = 0
        while True:
            pl = ytd.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_pl,
                maxResults=50,
                pageToken=page_token
            ).execute()
            for it in pl.get("items", []):
                published = it["contentDetails"]["videoPublishedAt"]  # "YYYY-MM-DDThh:mm:ssZ"
                d = datetime.fromisoformat(published.replace("Z", "+00:00")).date()
                earliest = d if earliest is None or d < earliest else earliest
                seen += 1
                if seen >= 500:  # guard
                    break
            if seen >= 500:
                break
            page_token = pl.get("nextPageToken")
            if not page_token:
                break
        return earliest
    except Exception:
        return None

def get_videos_with_publish_dates(ytd) -> Dict[str, date]:
    """
    Map videoId -> publishedAt(date) for your uploads (limited to ~1000 for speed).
    """
    out: Dict[str, date] = {}
    try:
        ch = ytd.channels().list(part="contentDetails", mine=True, maxResults=1).execute()
        items = ch.get("items", [])
        if not items:
            return out
        uploads_pl = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

        page_token = None
        seen = 0
        while True:
            pl = ytd.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_pl,
                maxResults=50,
                pageToken=page_token
            ).execute()
            for it in pl.get("items", []):
                vid = it["contentDetails"]["videoId"]
                published = it["contentDetails"]["videoPublishedAt"]
                d = datetime.fromisoformat(published.replace("Z", "+00:00")).date()
                out[vid] = d
                seen += 1
                if seen >= 1000:
                    break
            if seen >= 1000:
                break
            page_token = pl.get("nextPageToken")
            if not page_token:
                break
    except Exception:
        pass
    return out

# ----------------- Analytics helpers -----------------
def latest_analytics_date(yta: any) -> Optional[date]:
    """
    Ask Analytics for the most recent 'day' row it knows about and return that date.
    We query a generous window and read the last row.
    """
    try:
        today = date.today()
        start = (today - timedelta(days=90)).isoformat()
        end = today.isoformat()
        resp = yta.reports().query(
            ids="channel==MINE",
            startDate=start,
            endDate=end,
            metrics="views",
            dimensions="day",
            sort="day",
            maxResults=200
        ).execute()
        rows = resp.get("rows", [])
        if not rows:
            return None
        last_day_str = rows[-1][0]
        return datetime.fromisoformat(last_day_str).date()
    except Exception:
        return None

# ----------------- Core run functions -----------------
def run_report_dataframe(yta, start: date, end: date, metrics: str, dimensions: str,
                         sort: Optional[str] = None, filters: Optional[str] = None,
                         max_results: Optional[int] = None) -> pd.DataFrame:
    start_s, end_s = start.isoformat(), end.isoformat()
    all_rows: List[List] = []
    columns: List[str] = []
    start_index = 1
    page_size = max_results or MAX_RESULTS

    while True:
        req = dict(
            ids="channel==MINE",
            startDate=start_s,
            endDate=end_s,
            metrics=metrics,
            dimensions=dimensions,
            maxResults=page_size,
            startIndex=start_index,
        )
        if sort:
            req["sort"] = sort
        if filters:
            req["filters"] = filters
        resp = yta.reports().query(**req).execute()
        rows = resp.get("rows", []) or []
        if not columns:
            columns = [h["name"] for h in resp.get("columnHeaders", [])]
        all_rows.extend(rows)
        if len(rows) < page_size or max_results is None:
            break
        start_index += page_size

    df = pd.DataFrame(all_rows, columns=columns)
    return df

def save_with_refresh(df: pd.DataFrame, path: Path, refresh_end: date):
    # Add refresh column even if df is empty; keep schema explicit
    df = df.copy()
    df["refresh_datetime"] = refresh_end.isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved {path} ({len(df)} rows)")

# ----------------- Main -----------------
def main():
    parser = argparse.ArgumentParser(description="YouTube Analytics with smart dates and refresh timestamp")
    # CLI --start/--end are kept as overrides if you want to force a window
    parser.add_argument("--start", help="YYYY-MM-DD (override)")
    parser.add_argument("--end", help="YYYY-MM-DD (override)")
    parser.add_argument("--reports", nargs="*", help="Subset: day country traffic_sources top_videos")
    args = parser.parse_args()

    yta = get_yta_service()

    # Compute the latest available Analytics date (right edge)
    latest = latest_analytics_date(yta)
    if latest is None:
        # Fallback: today-3 (very conservative)
        latest = date.today() - timedelta(days=3)
    print(f"Latest analytics date: {latest}")

    # Optional: YouTube Data API client (for channel/video dates)
    ytd = build_youtube_data_client()

    # Determine channel-start for country/traffic_sources
    channel_start: Optional[date] = None
    if ytd:
        channel_start = get_channel_created_date(ytd)
        if not channel_start:
            channel_start = get_first_video_published_date(ytd)

    # Fallback chain for country/traffic_sources start
    fallback_start = date(2025, 8, 28)
    default_start = channel_start or fallback_start

    # CLI overrides take precedence (for all reports)
    if args.start and args.end:
        start_override = datetime.fromisoformat(args.start).date()
        end_override = datetime.fromisoformat(args.end).date()
    else:
        start_override = None
        end_override = None

    keys = list(REPORTS.keys()) if not args.reports else args.reports
    print(f"Reports to run: {', '.join(keys)}")

    for key in keys:
        metrics, sort, filters, maxres, dims, prefix = REPORTS[key]

        if key == "top_videos":
            # Strategy: if we have Data API, run one query per video with its own start=publishedAt
            out_path = OUTPUT_DIR / f"{prefix}_{(end_override or latest).isoformat()}_to_{(end_override or latest).isoformat()}.csv"
            # We'll accumulate rows manually so each row reflects that video's lifetime window.
            rows_acc: List[List] = []
            cols_acc: Optional[List[str]] = None

            # Try to get (videoId -> publishedAt)
            vid_map = get_videos_with_publish_dates(ytd) if ytd else {}
            if not vid_map:
                # Fallback: single shot (previous behavior) using a broad start
                start_for_all = start_override or (latest - timedelta(days=30))
                end_for_all = end_override or latest
                df = run_report_dataframe(yta, start_for_all, end_for_all, metrics, dims, sort, filters, maxres)
                save_with_refresh(df, out_path, end_for_all)
                continue

            # For each known video, fetch its lifetime stats up to latest
            for vid, vstart in vid_map.items():
                start_for_vid = max(vstart, date(2006, 1, 1))  # YouTube launch guard
                end_for_vid = end_override or latest
                df = run_report_dataframe(
                    yta, start_for_vid, end_for_vid,
                    metrics=metrics, dimensions="video",
                    sort=None, filters=f"video=={vid}", max_results=1
                )
                if df.empty:
                    # still include a row with video id and zeros?
                    continue
                # Add the per-video start date and videoPublishedAt for transparency
                df["videoPublishedAt"] = vstart.isoformat()
                if cols_acc is None:
                    cols_acc = list(df.columns) + ["refresh_datetime"]
                # append with refresh col later
                rows_acc.extend(df.values.tolist())

            # Build final dataframe
            final_df = pd.DataFrame(rows_acc, columns=cols_acc[:-1] if cols_acc else [])
            save_with_refresh(final_df, out_path, (end_override or latest))

        else:
            # country / traffic_sources / day
            if key == "day":
                # Reasonable default: last 30 days ending at latest
                start_for_report = start_override or (latest - timedelta(days=30))
            else:
                # country and traffic_sources use channel lifetime (or fallback)
                start_for_report = start_override or default_start

            end_for_report = end_override or latest

            # Run and save
            df = run_report_dataframe(yta, start_for_report, end_for_report, metrics, dims, sort, filters, maxres)

            # Optional: make day series continuous by filling missing days
            if key == "day" and not df.empty and "day" in df.columns:
                df["day"] = pd.to_datetime(df["day"])
                full = pd.DataFrame({"day": pd.date_range(start_for_report, end_for_report, freq="D")})
                df = full.merge(df, on="day", how="left").fillna(0)
                df["day"] = df["day"].dt.date.astype(str)

            out_path = OUTPUT_DIR / f"{prefix}_{start_for_report.isoformat()}_to_{end_for_report.isoformat()}.csv"
            save_with_refresh(df, out_path, end_for_report)

    print("✅ Done.")

if __name__ == "__main__":
    main()

