#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, List
import shutil
import pandas as pd

from youtube_analytics_auth import get_yta_service

# ----- Config -----
OUTPUT_DIR = Path("yta_outputs")
MAX_RESULTS = 200
PACIFIC_TZ = "America/Los_Angeles"

# Common metrics
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
    # 1) Day (timeseries)
    "day": (
        METRICS_BASIC, "day", None, None, "day", "yta_daily_trend"
    ),
    # 2) Country
    "country": (
        METRICS_BASIC, "-views", None, MAX_RESULTS, "country", "yta_country"
    ),
    # 3) Overall traffic sources
    "traffic_sources": (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage",
        "-views", None, None, "insightTrafficSourceType", "yta_traffic_sources"
    ),
    # 4) Top videos
    "top_videos": (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost,likes,comments,shares",
        "-views", None, MAX_RESULTS, "video", "yta_top_videos"
    ),
}

# ----- Helpers -----
def now_pacific_iso() -> str:
    """Current timestamp in America/Los_Angeles (ISO, seconds)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(PACIFIC_TZ)).isoformat(timespec="seconds")
    except Exception:
        # Fallback to naive local time if zoneinfo is unavailable
        return datetime.now().isoformat(timespec="seconds")

def today_pacific_date() -> str:
    """Today's date in America/Los_Angeles (YYYY-MM-DD)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(PACIFIC_TZ)).date().isoformat()
    except Exception:
        return date.today().isoformat()

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Light typing for convenience."""
    if "day" in df.columns:
        df["day"] = pd.to_datetime(df["day"], errors="coerce")
    for c in df.columns:
        if c not in {"day", "country", "video", "insightTrafficSourceType", "from_date_time", "to_date_time", "updated_at"}:
            df[c] = pd.to_numeric(df[c], errors="ignore")
    return df

def backup_then_path(canonical: Path, fname_prefix: str, outdir: Path) -> Optional[Path]:
    """If canonical exists, rename to *_uptodate_to_YYYY-MM-DD.csv (Pacific). Return backup path or None."""
    if canonical.exists():
        backup = outdir / f"{fname_prefix}_uptodate_to_{today_pacific_date()}.csv"
        try:
            shutil.move(str(canonical), str(backup))
            return backup
        except Exception:
            return None
    return None

def append_or_create_table(df_new: pd.DataFrame, outdir: Path, fname_prefix: str, key_cols: List[str]) -> Path:
    """
    Append df_new into canonical table CSV (prefix.csv). If a canonical exists,
    rename it to *_uptodate_to_YYYY-MM-DD.csv (Pacific) as a backup first,
    then merge+dedupe by key and write the canonical.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    canonical = outdir / f"{fname_prefix}.csv"

    # Light typing
    df_new = coerce_types(df_new.copy())

    if canonical.exists():
        backup = backup_then_path(canonical, fname_prefix, outdir)
        try:
            df_old = pd.read_csv(backup if (backup and backup.exists()) else canonical)
        except Exception:
            df_old = pd.DataFrame()
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        if key_cols:
            df_all.drop_duplicates(subset=key_cols, keep="last", inplace=True)
    else:
        df_all = df_new

    # Sort for readability
    if "day" in df_all.columns:
        df_all = df_all.sort_values("day")
    elif "views" in df_all.columns:
        df_all = df_all.sort_values("views", ascending=False)

    df_all.to_csv(canonical, index=False)
    print(f"Upserted {len(df_new)} rows into {canonical} (now {len(df_all)} total)")
    return canonical

def overwrite_daily_table(df_new: pd.DataFrame, outdir: Path, fname_prefix: str) -> Path:
    """
    For the daily trend: add updated_at (Pacific), de-dupe by day, then
    replace the canonical file. Keep a dated backup first.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    canonical = outdir / f"{fname_prefix}.csv"

    df_new = coerce_types(df_new.copy())
    if "day" not in df_new.columns:
        # Safety: if API changes
        df_new["day"] = pd.NaT
    df_new["updated_at"] = now_pacific_iso()

    # If a previous file exists, read & merge to avoid accidental duplicates, but final behavior is "replace".
    existing = None
    if canonical.exists():
        backup_then_path(canonical, fname_prefix, outdir)  # rotate backup
        try:
            existing = pd.read_csv(outdir / f"{fname_prefix}_uptodate_to_{today_pacific_date()}.csv")
        except Exception:
            existing = None

    if isinstance(existing, pd.DataFrame) and not existing.empty and "day" in existing.columns:
        existing = coerce_types(existing)
        merged = pd.concat([existing, df_new], ignore_index=True)
        merged.drop_duplicates(subset=["day"], keep="last", inplace=True)
        merged = merged.sort_values("day")
        merged.to_csv(canonical, index=False)
        final = merged
    else:
        # Just write the fresh df
        df_new.drop_duplicates(subset=["day"], keep="last", inplace=True)
        df_new = df_new.sort_values("day")
        df_new.to_csv(canonical, index=False)
        final = df_new

    print(f"Rewrote daily trend to {canonical} with {len(final)} rows.")
    return canonical

# ----- API runner -----
def run_report(
    yta,
    start: str,
    end: str,
    *,
    key: str,
    metrics: str,
    sort: Optional[str],
    filters: Optional[str],
    max_results: Optional[int],
    dimensions: str,
    fname_prefix: str,
) -> pd.DataFrame:
    all_rows: List[List] = []
    columns: List[str] = []
    start_index = 1
    page_size = max_results or MAX_RESULTS  # pagination fix

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

        reached_cap = max_results is not None and (start_index - 1) + len(rows) >= max_results
        if len(rows) < page_size or reached_cap:
            break
        start_index += page_size

    return pd.DataFrame(all_rows, columns=columns)

def daterange_defaults(args) -> tuple[str, str]:
    if args.start and args.end:
        return args.start, args.end
    end = date.today()
    start = end - timedelta(days=30)
    return start.isoformat(), end.isoformat()

# ----- Main -----
def main():
    parser = argparse.ArgumentParser(description="YouTube Analytics: day, country, traffic sources, top videos")
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    parser.add_argument("--reports", nargs="*", help="Subset: day country traffic_sources top_videos")
    args = parser.parse_args()

    start, end = daterange_defaults(args)
    yta = get_yta_service()

    keys = list(REPORTS.keys()) if not args.reports else args.reports
    print(f"Date range: {start} → {end}")
    print(f"Reports: {', '.join(keys)}")
    print("-" * 60)

    for k in keys:
        if k not in REPORTS:
            print(f"[skip] Unknown report: {k}")
            continue

        metrics, sort, filters, maxres, dims, prefix = REPORTS[k]

        try:
            df = run_report(
                yta, start, end,
                key=k, metrics=metrics, sort=sort, filters=filters,
                max_results=maxres, dimensions=dims, fname_prefix=prefix
            )

            # ---- Aggregated reports ----
            if k in {"country", "top_videos", "traffic_sources"}:
                df = df.copy()

                # Earliest record time from the API if a 'day' column exists; else fall back to requested start.
                earliest = None
                if "day" in df.columns:
                    parsed_day = pd.to_datetime(df["day"], errors="coerce")
                    if parsed_day.notna().any():
                        earliest = parsed_day.min()
                if earliest is None:
                    earliest = pd.to_datetime(start, errors="coerce")

                # Use Pacific "now" for to_date_time
                df["from_date_time"] = pd.Timestamp(earliest).isoformat()
                df["to_date_time"] = now_pacific_iso()

                # Natural keys per report
                if k == "country":
                    key_cols = ["country", "from_date_time", "to_date_time"]
                elif k == "top_videos":
                    key_cols = ["video", "from_date_time", "to_date_time"]
                else:  # traffic_sources
                    key_cols = ["insightTrafficSourceType", "from_date_time", "to_date_time"]

                append_or_create_table(df, OUTPUT_DIR, prefix, key_cols)

            # ---- Daily trend ----
            else:  # k == "day"
                overwrite_daily_table(df, OUTPUT_DIR, prefix)

        except Exception as e:
            # Keep pipelines stable with an empty schema-aligned write
            empty_cols = {
                "country": ["country", "from_date_time", "to_date_time"],
                "top_videos": ["video", "from_date_time", "to_date_time"],
                "traffic_sources": ["insightTrafficSourceType", "from_date_time", "to_date_time"],
                "day": ["day", "updated_at"],
            }.get(k, [])
            df_empty = pd.DataFrame(columns=empty_cols)

            if k == "day":
                overwrite_daily_table(df_empty, OUTPUT_DIR, prefix)
            else:
                append_or_create_table(df_e

