"""Filesystem helpers for raw ingestion artifacts."""

from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    """Return the yc_hiring_posts project root."""

    return Path(__file__).resolve().parents[1]


def data_dir() -> Path:
    return project_root() / "data"


def raw_data_dir() -> Path:
    return data_dir() / "raw"


def interim_data_dir() -> Path:
    return data_dir() / "interim"


def processed_data_dir() -> Path:
    return data_dir() / "processed"


def month_raw_dir(thread_month: str) -> Path:
    """Return the raw-data folder for one thread month."""

    return raw_data_dir() / thread_month


def ensure_month_raw_dir(thread_month: str) -> Path:
    """Create and return the raw-data folder for one thread month."""

    target = month_raw_dir(thread_month)
    target.mkdir(parents=True, exist_ok=True)
    return target


def month_interim_dir(thread_month: str) -> Path:
    """Return the interim-data folder for one thread month."""

    return interim_data_dir() / thread_month


def ensure_month_interim_dir(thread_month: str) -> Path:
    """Create and return the interim-data folder for one thread month."""

    target = month_interim_dir(thread_month)
    target.mkdir(parents=True, exist_ok=True)
    return target


def ensure_processed_dir() -> Path:
    """Create and return the processed-data folder."""

    target = processed_data_dir()
    target.mkdir(parents=True, exist_ok=True)
    return target


def thread_metadata_path(thread_month: str) -> Path:
    """Return the canonical path for thread-level raw metadata."""

    return month_raw_dir(thread_month) / "thread.json"


def thread_html_path(thread_month: str) -> Path:
    """Return the canonical path for raw thread HTML."""

    return month_raw_dir(thread_month) / "thread.html"


def fetch_manifest_path(thread_month: str) -> Path:
    """Return the canonical path for per-month raw fetch metadata."""

    return month_raw_dir(thread_month) / "fetch_manifest.json"


def posts_jsonl_path(thread_month: str) -> Path:
    """Return the canonical path for raw top-level posts."""

    return month_raw_dir(thread_month) / "posts.jsonl"


def normalized_posts_jsonl_path(thread_month: str) -> Path:
    """Return the canonical path for normalized top-level posts."""

    return month_interim_dir(thread_month) / "posts_normalized.jsonl"


def roles_jsonl_path(thread_month: str) -> Path:
    """Return the canonical path for extracted role rows."""

    return month_interim_dir(thread_month) / "roles.jsonl"
