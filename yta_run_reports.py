#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, List
import shutil
import pandas as pd

from youtube_analytics_auth import get_yta_service

OUTPUT_DIR = Path("yta_outputs")
MAX_RESULTS = 200

# Common metrics that work broadly
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
    # 4) Top videos (replaces video_traffic_sources)
    "top_videos": (
        "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained,subscribersLost,likes,comments,shares",
        "-views", None, MAX_RESULTS, "video", "yta_top_videos"
    ),
}

def append_or_create_table(df_new: pd.DataFrame, outdir: Path, fname_prefix: str, key_cols: List[str]) -> Path:
    """
    Append df_new into canonical table CSV (prefix.csv). If an old canonical file exists,
    rename it to *_uptodate_to_YYYY-MM-DD.csv as a backup first.
    Then write the combined, de-duplicated table to the canonical filename.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    canonical = outdir / f"{fname_prefix}.csv"

    # Light typing: parse date if present; coerce numerics elsewhere
    if "day" in df_new.columns:
        df_new["day"] = pd.to_datetime(df_new["day"], errors="coerce")
    for c in df_new.columns:
        if c not in {"day", "country", "video", "insightTrafficSourceType", "from_date_time", "to_date_time"}:
            df_new[c] = pd.to_numeric(df_new[c], errors="ignore")

    if canonical.exists():
        # Rename current canonical to an "uptodate_to" backup (using today as the label)
        today = date.today().isoformat()
        backup = outdir / f"{fname_prefix}_uptodate_to_{today}.csv"
        try:
            shutil.move(str(canonical), str(backup))
        except Exception:
            # Fallback: keep canonical, still proceed to read it
            backup = None

        try:
            df_old = pd.read_csv(backup if backup and backup.exists() else canonical)
        except Exception:
            df_old = pd.DataFrame()

        # Concatenate & de-duplicate by key
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=key_cols, keep="last", inplace=True)

    else:
        df_all = df_new.copy()

    # Sort for readability
    if "day" in df_all.columns:
        df_all = df_all.sort_values("day")
    elif "views" in df_all.columns:
        df_all = df_all.sort_values("views", ascending=False)

    df_all.to_csv(canonical, index=False)
    print(f"Upserted {len(df_new)} rows into {canonical} (now {len(df_all)} total)")
    return canonical

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

        reached_cap = max_results is not None and (start_index - 1) + len(rows) >= max_results
        if len(rows) < page_size or reached_cap:
            break
        start_index += page_size

    df = pd.DataFrame(all_rows, columns=columns)
    return df

def daterange_defaults(args) -> tuple[str, str]:
    if args.start and args.end:
        return args.start, args.end
    end = date.today()
    start = end - timedelta(days=30)
    return start.isoformat(), end.isoformat()

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

            # For aggregation reports, add from/to date-time columns; for 'day' just upsert by day.
            if k in {"country", "top_videos", "traffic_sources"}:
                df = df.copy()
                df["from_date_time"] = f"{start}T00:00:00"
                df["to_date_time"]   = f"{end}T23:59:59"

                if k == "country":
                    key_cols = ["country", "from_date_time", "to_date_time"]
                elif k == "top_videos":
                    key_cols = ["video", "from_date_time", "to_date_time"]
                else:  # traffic_sources
                    key_cols = ["insightTrafficSourceType", "from_date_time", "to_date_time"]

                append_or_create_table(df, OUTPUT_DIR, prefix, key_cols)

            else:
                # 'day' report: upsert by unique day
                key_cols = ["day"]
                append_or_create_table(df, OUTPUT_DIR, prefix, key_cols)

        except Exception as e:
            # Create/append an empty frame to keep pipelines stable
            empty_cols = {
                "country": ["country", "from_date_time", "to_date_time"],
                "top_videos": ["video", "from_date_time", "to_date_time"],
                "traffic_sources": ["insightTrafficSourceType", "from_date_time", "to_date_time"],
                "day": ["day"]
            }.get(k, [])
            df_empty = pd.DataFrame(columns=empty_cols)
            append_or_create_table(df_empty, OUTPUT_DIR, prefix, empty_cols or None)
            print(f"[error] {k}: {e}")

    print("✅ Done.")

if __name__ == "__main__":
    main()
