"""Filesystem helpers for raw ingestion artifacts."""

from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    """Return the yc_hiring_posts project root."""

    return Path(__file__).resolve().parents[2]


def data_dir() -> Path:
    return project_root() / "data"


def raw_data_dir() -> Path:
    return data_dir() / "raw"


def month_raw_dir(thread_month: str) -> Path:
    """Return the raw-data folder for one thread month."""

    return raw_data_dir() / thread_month


def ensure_month_raw_dir(thread_month: str) -> Path:
    """Create and return the raw-data folder for one thread month."""

    target = month_raw_dir(thread_month)
    target.mkdir(parents=True, exist_ok=True)
    return target


def thread_metadata_path(thread_month: str) -> Path:
    """Return the canonical path for thread-level raw metadata."""

    return month_raw_dir(thread_month) / "thread.json"


def posts_jsonl_path(thread_month: str) -> Path:
    """Return the canonical path for raw top-level posts."""

    return month_raw_dir(thread_month) / "posts.jsonl"
