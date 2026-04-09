"""Small CLI helpers for early project workflows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from analytics import materialize_core_analytics
from companies import normalize_and_write_companies
from discovery import google_queries_for_entries, google_query_variants
from fetch import fetch_and_write_thread, fetchable_entries
from materialize import materialize_v1_core_tables
from normalize import normalize_and_write_thread_posts
from parse import parse_and_write_thread_posts
from postgres_kb import (
    company_activity_timeline_postgres,
    company_role_presence_postgres,
    DEFAULT_DB_SCHEMA,
    initialize_postgres_kb,
    inspect_postgres_kb,
    load_postgres_kb,
    search_posts_postgres,
    search_roles_postgres,
    summarize_search_result,
)
from question_catalog import build_question_catalog, possible_questions_path, write_question_catalog
from roles import extract_and_write_roles
from source_index import default_source_index_path, entry_to_dict, load_source_index, verified_entries
from validate import validation_report_to_dict, validate_many_thread_months, validate_thread_month


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

    parse_parser = subparsers.add_parser(
        "parse-thread-posts",
        help="Parse stored raw thread HTML into top-level posts JSONL.",
    )
    parse_parser.add_argument("thread_month", help="Month in YYYY-MM format.")

    normalize_parser = subparsers.add_parser(
        "normalize-thread-posts",
        help="Normalize parsed raw posts into interim post records.",
    )
    normalize_parser.add_argument("thread_month", help="Month in YYYY-MM format.")

    roles_parser = subparsers.add_parser(
        "extract-thread-roles",
        help="Extract role-level records from normalized posts.",
    )
    roles_parser.add_argument("thread_month", help="Month in YYYY-MM format.")

    companies_parser = subparsers.add_parser(
        "normalize-thread-companies",
        help="Resolve conservative company ids and write the company dimension.",
    )
    companies_parser.add_argument("thread_month", help="Month in YYYY-MM format.")

    materialize_parser = subparsers.add_parser(
        "materialize-v1-core-tables",
        help="Materialize consolidated V1 core tables into the processed layer.",
    )

    analytics_parser = subparsers.add_parser(
        "materialize-core-analytics",
        help="Materialize recurring analytical outputs from the processed core tables.",
    )

    init_db_parser = subparsers.add_parser(
        "init-postgres-kb",
        help="Initialize the PostgreSQL knowledge-base schema for Step 18.",
    )
    init_db_parser.add_argument("--database-url", default=None)
    init_db_parser.add_argument("--schema", default=DEFAULT_DB_SCHEMA)

    load_db_parser = subparsers.add_parser(
        "load-postgres-kb",
        help="Load processed V1 core tables into PostgreSQL.",
    )
    load_db_parser.add_argument("--database-url", default=None)
    load_db_parser.add_argument("--schema", default=DEFAULT_DB_SCHEMA)
    load_db_parser.add_argument("--batch-size", type=int, default=500)

    inspect_db_parser = subparsers.add_parser(
        "inspect-postgres-kb",
        help="Inspect row counts in the PostgreSQL knowledge base.",
    )
    inspect_db_parser.add_argument("--database-url", default=None)
    inspect_db_parser.add_argument("--schema", default=DEFAULT_DB_SCHEMA)

    search_posts_parser = subparsers.add_parser(
        "search-postgres-posts",
        help="Run structured and text retrieval over hiring posts in PostgreSQL.",
    )
    search_posts_parser.add_argument("--database-url", default=None)
    search_posts_parser.add_argument("--schema", default=DEFAULT_DB_SCHEMA)
    search_posts_parser.add_argument("--query", default=None)
    search_posts_parser.add_argument("--company", dest="company_name", default=None)
    search_posts_parser.add_argument("--role-family", default=None)
    search_posts_parser.add_argument("--remote-status", default=None)
    search_posts_parser.add_argument("--month-from", default=None)
    search_posts_parser.add_argument("--month-to", default=None)
    search_posts_parser.add_argument("--limit", type=int, default=20)
    search_posts_parser.add_argument("--summary-only", action="store_true")

    search_roles_parser = subparsers.add_parser(
        "search-postgres-roles",
        help="Run structured and text retrieval over role rows in PostgreSQL.",
    )
    search_roles_parser.add_argument("--database-url", default=None)
    search_roles_parser.add_argument("--schema", default=DEFAULT_DB_SCHEMA)
    search_roles_parser.add_argument("--query", default=None)
    search_roles_parser.add_argument("--company", dest="company_name", default=None)
    search_roles_parser.add_argument("--role-family", default=None)
    search_roles_parser.add_argument("--remote-status", default=None)
    search_roles_parser.add_argument("--month-from", default=None)
    search_roles_parser.add_argument("--month-to", default=None)
    search_roles_parser.add_argument("--limit", type=int, default=20)
    search_roles_parser.add_argument("--summary-only", action="store_true")

    company_timeline_parser = subparsers.add_parser(
        "company-activity-postgres",
        help="Show month-by-month activity for one company from PostgreSQL.",
    )
    company_timeline_parser.add_argument("--database-url", default=None)
    company_timeline_parser.add_argument("--schema", default=DEFAULT_DB_SCHEMA)
    company_timeline_parser.add_argument("--company", dest="company_name", required=True)
    company_timeline_parser.add_argument("--month-from", default=None)
    company_timeline_parser.add_argument("--month-to", default=None)
    company_timeline_parser.add_argument("--limit-evidence", type=int, default=10)

    company_presence_parser = subparsers.add_parser(
        "company-role-presence-postgres",
        help="Check whether a company hired for a role query/family in a month range.",
    )
    company_presence_parser.add_argument("--database-url", default=None)
    company_presence_parser.add_argument("--schema", default=DEFAULT_DB_SCHEMA)
    company_presence_parser.add_argument("--company", dest="company_name", required=True)
    company_presence_parser.add_argument("--query", default=None)
    company_presence_parser.add_argument("--role-family", default=None)
    company_presence_parser.add_argument("--month-from", default=None)
    company_presence_parser.add_argument("--month-to", default=None)
    company_presence_parser.add_argument("--limit-evidence", type=int, default=10)

    validate_parser = subparsers.add_parser(
        "validate-thread-raw",
        help="Validate raw artifacts and parsed posts for one month.",
    )
    validate_parser.add_argument("thread_month", help="Month in YYYY-MM format.")

    validate_many_parser = subparsers.add_parser(
        "validate-many-thread-raw",
        help="Validate raw artifacts and parsed posts for multiple months.",
    )
    validate_many_parser.add_argument("thread_months", nargs="+", help="Months in YYYY-MM format.")

    question_catalog_parser = subparsers.add_parser(
        "annotate-possible-questions",
        help="Classify and annotate the KB question bank into helper families.",
    )
    question_catalog_parser.add_argument("--path", type=Path, default=possible_questions_path())
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

    if args.command == "parse-thread-posts":
        output_path = parse_and_write_thread_posts(args.thread_month)
        print(
            json.dumps(
                {
                    "thread_month": args.thread_month,
                    "posts_jsonl_path": str(output_path),
                },
                indent=2,
            )
        )
        return 0

    if args.command == "normalize-thread-posts":
        output_path = normalize_and_write_thread_posts(args.thread_month)
        print(
            json.dumps(
                {
                    "thread_month": args.thread_month,
                    "normalized_posts_jsonl_path": str(output_path),
                },
                indent=2,
            )
        )
        return 0

    if args.command == "extract-thread-roles":
        output_path = extract_and_write_roles(args.thread_month)
        print(
            json.dumps(
                {
                    "thread_month": args.thread_month,
                    "roles_jsonl_path": str(output_path),
                },
                indent=2,
            )
        )
        return 0

    if args.command == "normalize-thread-companies":
        companies_path, posts_path, roles_path = normalize_and_write_companies(args.thread_month)
        print(
            json.dumps(
                {
                    "thread_month": args.thread_month,
                    "companies_jsonl_path": str(companies_path),
                    "posts_normalized_jsonl_path": str(posts_path),
                    "roles_jsonl_path": str(roles_path) if roles_path is not None else None,
                },
                indent=2,
            )
        )
        return 0

    if args.command == "materialize-v1-core-tables":
        outputs = materialize_v1_core_tables()
        print(json.dumps({key: str(value) for key, value in outputs.items()}, indent=2))
        return 0

    if args.command == "materialize-core-analytics":
        outputs = materialize_core_analytics()
        print(json.dumps({key: str(value) for key, value in outputs.items()}, indent=2))
        return 0

    if args.command == "init-postgres-kb":
        result = initialize_postgres_kb(database_url=args.database_url, schema=args.schema)
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "load-postgres-kb":
        result = load_postgres_kb(database_url=args.database_url, schema=args.schema, batch_size=args.batch_size)
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "inspect-postgres-kb":
        result = inspect_postgres_kb(database_url=args.database_url, schema=args.schema)
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "search-postgres-posts":
        result = search_posts_postgres(
            database_url=args.database_url,
            schema=args.schema,
            query=args.query,
            company_name=args.company_name,
            role_family=args.role_family,
            remote_status=args.remote_status,
            month_from=args.month_from,
            month_to=args.month_to,
            limit=args.limit,
        )
        if args.summary_only:
            result = summarize_search_result(result)
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.command == "search-postgres-roles":
        result = search_roles_postgres(
            database_url=args.database_url,
            schema=args.schema,
            query=args.query,
            company_name=args.company_name,
            role_family=args.role_family,
            remote_status=args.remote_status,
            month_from=args.month_from,
            month_to=args.month_to,
            limit=args.limit,
        )
        if args.summary_only:
            result = summarize_search_result(result)
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.command == "company-activity-postgres":
        result = company_activity_timeline_postgres(
            database_url=args.database_url,
            schema=args.schema,
            company_name=args.company_name,
            month_from=args.month_from,
            month_to=args.month_to,
            limit_evidence=args.limit_evidence,
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.command == "company-role-presence-postgres":
        result = company_role_presence_postgres(
            database_url=args.database_url,
            schema=args.schema,
            company_name=args.company_name,
            query=args.query,
            role_family=args.role_family,
            month_from=args.month_from,
            month_to=args.month_to,
            limit_evidence=args.limit_evidence,
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.command == "validate-thread-raw":
        report = validate_thread_month(args.thread_month)
        print(json.dumps(validation_report_to_dict(report), indent=2))
        return 0 if report.checks_passed else 1

    if args.command == "validate-many-thread-raw":
        reports = validate_many_thread_months(args.thread_months)
        print(json.dumps([validation_report_to_dict(report) for report in reports], indent=2))
        return 0 if all(report.checks_passed for report in reports) else 1

    if args.command == "annotate-possible-questions":
        annotations = build_question_catalog(args.path)
        outputs = write_question_catalog(annotations)
        print(
            json.dumps(
                {
                    "question_count": len(annotations),
                    "markdown_path": str(outputs["markdown_path"]),
                    "json_path": str(outputs["json_path"]),
                },
                indent=2,
            )
        )
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
