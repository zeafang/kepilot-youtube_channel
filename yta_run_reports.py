#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, List
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
        METRICS_BASIC, "-views", None, MAX_RESULTS, "country", "country"
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
    outdir: Path = OUTPUT_DIR,
) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    fname = outdir / f"{fname_prefix}_{start}_to_{end}.csv"

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

        if len(rows) < page_size or max_results is None:
            break
        start_index += page_size

    df = pd.DataFrame(all_rows, columns=columns)
    df.to_csv(fname, index=False)
    print(f"Saved {fname} ({len(df)} rows)")
    return fname

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
            run_report(
                yta, start, end,
                key=k, metrics=metrics, sort=sort, filters=filters,
                max_results=maxres, dimensions=dims, fname_prefix=prefix
            )
        except Exception as e:
            # write empty CSV so downstream jobs don't break
            empty = OUTPUT_DIR / f"{prefix}_{start}_to_{end}.csv"
            if not empty.exists():
                pd.DataFrame([]).to_csv(empty, index=False)
            print(f"[error] {k}: {e}")

    print("✅ Done.")

if __name__ == "__main__":
    main()
