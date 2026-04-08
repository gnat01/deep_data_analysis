"""Small CLI helpers for early project workflows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from discovery import google_queries_for_entries, google_query_variants
from fetch import fetch_and_write_thread, fetchable_entries
from source_index import default_source_index_path, entry_to_dict, load_source_index, verified_entries


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="yc-hiring-posts")
    subparsers = parser.add_subparsers(dest="command", required=True)

    index_parser = subparsers.add_parser("show-source-index", help="Print source-index rows as JSON.")
    index_parser.add_argument("--path", type=Path, default=default_source_index_path())
    index_parser.add_argument(
        "--verified-only",
        action="store_true",
        help="Only show verified or already fetched rows.",
    )

    query_parser = subparsers.add_parser(
        "show-discovery-queries",
        help="Print the preferred Google search queries for a target month.",
    )
    query_parser.add_argument("thread_month", help="Month in YYYY-MM format.")

    all_queries_parser = subparsers.add_parser(
        "show-all-discovery-queries",
        help="Print Google search queries for all months in the source index.",
    )
    all_queries_parser.add_argument("--path", type=Path, default=default_source_index_path())

    fetch_parser = subparsers.add_parser(
        "fetch-thread-raw",
        help="Fetch and write raw thread artifacts for one verified month.",
    )
    fetch_parser.add_argument("thread_month", help="Month in YYYY-MM format.")
    fetch_parser.add_argument("--path", type=Path, default=default_source_index_path())
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "show-source-index":
        entries = load_source_index(args.path)
        if args.verified_only:
            entries = verified_entries(entries)
        print(json.dumps([entry_to_dict(entry) for entry in entries], indent=2))
        return 0

    if args.command == "show-discovery-queries":
        queries = google_query_variants(args.thread_month)
        print(json.dumps([query.__dict__ for query in queries], indent=2))
        return 0

    if args.command == "show-all-discovery-queries":
        entries = load_source_index(args.path)
        queries = google_queries_for_entries(entries)
        print(json.dumps([query.__dict__ for query in queries], indent=2))
        return 0

    if args.command == "fetch-thread-raw":
        entries = load_source_index(args.path)
        month_matches = [entry for entry in fetchable_entries(entries) if entry.thread_month == args.thread_month]
        if not month_matches:
            parser.error(f"No verified source-index row found for month: {args.thread_month}")
        html_path, metadata_path, manifest_path = fetch_and_write_thread(month_matches[0])
        print(
            json.dumps(
                {
                    "thread_month": args.thread_month,
                    "html_path": str(html_path),
                    "metadata_path": str(metadata_path),
                    "manifest_path": str(manifest_path),
                },
                indent=2,
            )
        )
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
