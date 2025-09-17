#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, List

import pandas as pd

from youtube_analytics_auth import get_yta_service  # Analytics API v2 (your existing helper)

# =========================
# Config
# =========================
OUTPUT_DIR = Path("yta_outputs")
MAX_RESULTS = 200
PACIFIC_TZ = "America/Los_Angeles"

# Metrics
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

# key -> (metrics, sort, filters, maxResults, dimensions, filename_prefix)
REPORTS: Dict[str, Tuple[str, Optional[str], Optional[str], Optional[int], str, str]] = {
    "day": (
        METRICS_BASIC, "day", None, None, "day", "yta_daily_trend"
    ),
    "country": (
        METRICS_BASIC, "-views", None, MAX_RESULTS, "country", "yta_country"
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

# =========================
# Small helpers
# =========================
def now_pacific_iso() -> str:
    """Current timestamp in America/Los_Angeles (ISO, seconds)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(PACIFIC_TZ)).isoformat(timespec="seconds")
    except Exception:
        return datetime.now().isoformat(timespec="seconds")


def save_csv(df: pd.DataFrame, prefix: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / f"{prefix}.csv"
    df.to_csv(path, index=False)
    print(f"Saved {path} ({len(df)} rows)")
    return path


# =========================
# YouTube Data API (v3)
# =========================
def build_youtube_data_client():
    """
    Build a Data API v3 client using token.json that has BOTH scopes:
    - https://www.googleapis.com/auth/yt-analytics.readonly
    - https://www.googleapis.com/auth/youtube.readonly
    Returns None if the token/scopes aren’t available; code will fall back.
    """
    try:
        from googleapiclient.discovery import build as build_service
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        tok = Path("token.json")
        if not tok.exists():
            return None
        creds = Credentials.from_authorized_user_file(tok, scopes=[
            "https://www.googleapis.com/auth/yt-analytics.readonly",
            "https://www.googleapis.com/auth/youtube.readonly",
        ])
        if not creds.valid and creds.refresh_token:
            creds.refresh(Request())
            tok.write_text(creds.to_json())
        return build_service("youtube", "v3", credentials=creds)
    except Exception:
        return None


def get_channel_created_date(ytd) -> Optional[date]:
    """Channel snippet.publishedAt (date) or None."""
    if not ytd:
        return None
    try:
        resp = ytd.channels().list(part="snippet", mine=True, maxResults=1).execute()
        items = resp.get("items", [])
        if not items:
            return None
        published = items[0]["snippet"]["publishedAt"]  # "YYYY-MM-DDThh:mm:ssZ"
        return datetime.fromisoformat(published.replace("Z", "+00:00")).date()
    except Exception:
        return None


def get_first_video_published_date(ytd) -> Optional[date]:
    """Earliest upload date (scans uploads playlist; capped to ~1000 items)."""
    if not ytd:
        return None
    try:
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
                part="contentDetails", playlistId=uploads_pl, maxResults=50, pageToken=page_token
            ).execute()
            for it in pl.get("items", []):
                published = it["contentDetails"]["videoPublishedAt"]
                d = datetime.fromisoformat(published.replace("Z", "+00:00")).date()
                earliest = d if earliest is None or d < earliest else earliest
                seen += 1
                if seen >= 1000:
                    break
            if seen >= 1000:
                break
            page_token = pl.get("nextPageToken")
            if not page_token:
                break
        return earliest
    except Exception:
        return None


def get_video_publish_map(ytd, video_ids: List[str]) -> Dict[str, date]:
    """Return {videoId: publishedAt(date)} for the given IDs (batched)."""
    out: Dict[str, date] = {}
    if not ytd or not video_ids:
        return out
    try:
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i + 50]
            resp = ytd.videos().list(part="snippet", id=",".join(batch), maxResults=len(batch)).execute()
            for item in resp.get("items", []):
                vid = item["id"]
                published = item["snippet"]["publishedAt"]
                out[vid] = datetime.fromisoformat(published.replace("Z", "+00:00")).date()
    except Exception:
        pass
    return out


# =========================
# Analytics helpers (v2)
# =========================
def latest_analytics_date(yta) -> Optional[date]:
    """Most recent 'day' the Analytics API has published for this channel."""
    try:
        today = date.today()
        resp = yta.reports().query(
            ids="channel==MINE",
            startDate=(today - timedelta(days=90)).isoformat(),
            endDate=today.isoformat(),
            metrics="views",
            dimensions="day",
            sort="day",
            maxResults=200,
        ).execute()
        rows = resp.get("rows", []) or []
        if not rows:
            return None
        return datetime.fromisoformat(rows[-1][0]).date()  # 'YYYY-MM-DD'
    except Exception:
        return None


def run_report(
    yta,
    start: str,
    end: str,
    *,
    metrics: str,
    dimensions: str,
    sort: Optional[str] = None,
    filters: Optional[str] = None,
    max_results: Optional[int] = None,
) -> pd.DataFrame:
    """Generic Analytics call with pagination."""
    all_rows: List[List] = []
    columns: List[str] = []
    start_index = 1
    page_size = max_results or MAX_RESULTS

    while True:
        req = dict(
            ids="channel==MINE",
            startDate=start,
            endDate=end,
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

        # stop when page short or we've hit explicit cap
        reached_cap = max_results is not None and (start_index - 1) + len(rows) >= max_results
        if len(rows) < page_size or reached_cap:
            break
        start_index += page_size

    return pd.DataFrame(all_rows, columns=columns)


# =========================
# Main
# =========================
def main():
    parser = argparse.ArgumentParser(description="YouTube Analytics: day, country, traffic sources, top videos (with Data API dates)")
    parser.add_argument("--start", help="YYYY-MM-DD (optional override for all reports)")
    parser.add_argument("--end", help="YYYY-MM-DD (optional override for all reports)")
    parser.add_argument("--reports", nargs="*", help="Subset: day country traffic_sources top_videos")
    args = parser.parse_args()

    # Clients
    yta = get_yta_service()
    ytd = build_youtube_data_client()

    # Last recorded Analytics day (also used as to_date_time)
    last_rec_date = latest_analytics_date(yta) or (date.today() - timedelta(days=3))
    last_rec_iso = last_rec_date.isoformat()
    print(f"Latest Analytics day: {last_rec_iso}")

    # Lifetime start for country/traffic_sources
    channel_start = get_channel_created_date(ytd) or get_first_video_published_date(ytd) or date(2025, 8, 28)
    channel_start_iso = channel_start.isoformat()
    print(f"Lifetime start (channel/traffic): {channel_start_iso}")

    # CLI overrides (apply to all reports if provided)
    start_override = datetime.fromisoformat(args.start).date().isoformat() if args.start and args.end else None
    end_override = datetime.fromisoformat(args.end).date().isoformat() if args.start and args.end else None

    keys = list(REPORTS.keys()) if not args.reports else args.reports
    print(f"Reports: {', '.join(keys)}")

    for key in keys:
        metrics, sort, filters, maxres, dims, prefix = REPORTS[key]

        try:
            if key == "top_videos":
                # Broad list of candidate videos first (fast)
                broad_start = start_override or channel_start_iso
                seed_df = run_report(
                    yta,
                    start=broad_start,
                    end=end_override or last_rec_iso,
                    metrics=metrics,
                    dimensions="video",
                    sort="-views",
                    max_results=MAX_RESULTS,
                )

                if seed_df.empty or "video" not in seed_df.columns:
                    # Nothing to refine; still emit with date columns + refresh
                    out = seed_df.copy()
                    out["from_date_time"] = broad_start
                    out["to_date_time"] = last_rec_iso
                    out["refresh_date"] = now_pacific_iso()
                    save_csv(out, prefix)
                    continue

                vids = seed_df["video"].astype(str).tolist()
                pub_map = get_video_publish_map(ytd, vids) if ytd else {}

                # Refine: for each video, start at its publish date if known (else fallback)
                rows: List[pd.DataFrame] = []
                for vid in vids:
                    vstart_date = pub_map.get(vid, date(2025, 8, 28))
                    vstart_iso = vstart_date.isoformat()
                    dfv = run_report(
                        yta,
                        start=vstart_iso,
                        end=end_override or last_rec_iso,
                        metrics=metrics,
                        dimensions="video",
                        filters=f"video=={vid}",
                        max_results=1,
                    )
                    if dfv.empty:
                        continue
                    dfv = dfv.copy()
                    dfv["from_date_time"] = vstart_iso
                    dfv["to_date_time"] = last_rec_iso          # <-- API last recorded date
                    dfv["refresh_date"] = now_pacific_iso()      # <-- actual run time
                    rows.append(dfv)

                final = pd.concat(rows, ignore_index=True) if rows else seed_df
                if final is seed_df:
                    final = final.copy()
                    final["from_date_time"] = broad_start
                    final["to_date_time"] = last_rec_iso
                    final["refresh_date"] = now_pacific_iso()

                save_csv(final, prefix)

            elif key in {"country", "traffic_sources"}:
                start_iso = start_override or channel_start_iso
                df = run_report(
                    yta,
                    start=start_iso,
                    end=end_override or last_rec_iso,
                    metrics=metrics,
                    dimensions=dims,
                    sort=sort,
                    max_results=maxres,
                )
                df = df.copy()
                df["from_date_time"] = start_iso
                df["to_date_time"] = last_rec_iso              # <-- API last recorded date
                df["refresh_date"] = now_pacific_iso()         # <-- actual run time
                save_csv(df, prefix)

            else:  # day
                start_iso = start_override or (last_rec_date - timedelta(days=30)).isoformat()
                df = run_report(
                    yta,
                    start=start_iso,
                    end=end_override or last_rec_iso,
                    metrics=metrics,
                    dimensions=dims,
                    sort=sort,
                )
                df = df.copy()
                df["refresh_date"] = now_pacific_iso()
                save_csv(df, prefix)

        except Exception as e:
            print(f"[error] {key}: {e}")
            # Emit schema-aligned empty CSV so pipelines don't break
            empty_cols = {
                "country": ["country", "from_date_time", "to_date_time", "refresh_date"],
                "traffic_sources": ["insightTrafficSourceType", "from_date_time", "to_date_time", "refresh_date"],
                "top_videos": ["video", "from_date_time", "to_date_time", "refresh_date"],
                "day": ["day", "refresh_date"],
            }.get(key, [])
            df_empty = pd.DataFrame(columns=empty_cols)
            save_csv(df_empty, prefix)

    print("✅ Done.")


if __name__ == "__main__":
    main()

