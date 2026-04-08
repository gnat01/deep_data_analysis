"""Materialize consolidated V1 core tables into the processed layer."""

from __future__ import annotations

import json
from pathlib import Path

from source_index import default_source_index_path, load_source_index
from storage import ensure_processed_dir, interim_data_dir, posts_jsonl_path, raw_data_dir, thread_metadata_path

CORE_TABLE_FILENAMES = {
    "threads": "threads.jsonl",
    "raw_posts": "raw_posts.jsonl",
    "posts": "posts.jsonl",
    "roles": "roles.jsonl",
    "companies": "companies.jsonl",
}


def materialize_v1_core_tables() -> dict[str, Path]:
    """Materialize the currently available V1 core tables into processed outputs."""

    months = available_materialization_months()
    processed_dir = ensure_processed_dir()
    tables_dir = processed_dir / "v1_core_tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    threads = load_threads(months)
    raw_posts = load_many_jsonl([raw_data_dir() / month / "posts.jsonl" for month in months])
    posts = load_many_jsonl([interim_data_dir() / month / "posts_normalized.jsonl" for month in months])
    roles = load_many_jsonl([interim_data_dir() / month / "roles.jsonl" for month in months])
    companies = load_many_jsonl([interim_data_dir() / month / "companies.jsonl" for month in months])

    outputs = {
        "threads": write_jsonl(tables_dir / CORE_TABLE_FILENAMES["threads"], threads),
        "raw_posts": write_jsonl(tables_dir / CORE_TABLE_FILENAMES["raw_posts"], raw_posts),
        "posts": write_jsonl(tables_dir / CORE_TABLE_FILENAMES["posts"], posts),
        "roles": write_jsonl(tables_dir / CORE_TABLE_FILENAMES["roles"], roles),
        "companies": write_jsonl(tables_dir / CORE_TABLE_FILENAMES["companies"], companies),
    }
    outputs["manifest"] = write_manifest(processed_dir / "v1_core_tables_manifest.json", months, outputs)
    return outputs


def available_materialization_months() -> list[str]:
    """Return months that currently have the full set of V1 core-table ingredients."""

    source_months = [entry.thread_month for entry in load_source_index(default_source_index_path())]
    months: list[str] = []
    for month in source_months:
        required = [
            thread_metadata_path(month),
            raw_data_dir() / month / "posts.jsonl",
            interim_data_dir() / month / "posts_normalized.jsonl",
            interim_data_dir() / month / "roles.jsonl",
            interim_data_dir() / month / "companies.jsonl",
        ]
        if all(path.exists() for path in required):
            months.append(month)
    return months


def load_threads(months: list[str]) -> list[dict[str, object]]:
    """Load thread-level records from per-month raw metadata."""

    threads = []
    for month in months:
        record = json.loads(thread_metadata_path(month).read_text(encoding="utf-8"))
        record["thread_month"] = month
        threads.append(record)
    return threads


def load_many_jsonl(paths: list[Path]) -> list[dict[str, object]]:
    """Load and concatenate JSONL files in order."""

    rows: list[dict[str, object]] = []
    for path in paths:
        rows.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())
    return rows


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> Path:
    """Write rows to JSON Lines with deterministic key ordering."""

    lines = [json.dumps(row, sort_keys=True) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return path


def write_manifest(path: Path, months: list[str], outputs: dict[str, Path]) -> Path:
    """Write a manifest describing the materialized V1 table set."""

    payload = {
        "materialization_version": "v1",
        "included_months": months,
        "table_paths": {name: str(table_path) for name, table_path in outputs.items()},
        "row_counts": {
            name: count_jsonl_rows(table_path)
            for name, table_path in outputs.items()
            if table_path.suffix == ".jsonl"
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def count_jsonl_rows(path: Path) -> int:
    """Count JSONL rows in a file."""

    return len(path.read_text(encoding="utf-8").splitlines())
