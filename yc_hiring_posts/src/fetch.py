"""Raw thread fetching for verified source-index entries."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

from models import SourceIndexEntry, ThreadRecord
from source_index import verified_entries
from storage import ensure_month_raw_dir, fetch_manifest_path, thread_html_path, thread_metadata_path

DEFAULT_USER_AGENT = "yc-hiring-posts/0.1 (+https://news.ycombinator.com/)"


class FetchError(RuntimeError):
    """Raised when raw fetching cannot proceed."""


def fetch_thread_html(entry: SourceIndexEntry, *, timeout_seconds: float = 30.0) -> str:
    """Fetch raw HTML for one verified thread entry."""

    if entry.status not in {"verified", "fetched"}:
        raise FetchError(f"Cannot fetch unverified source-index row: {entry.thread_month}")
    if not entry.source_url:
        raise FetchError(f"Missing source_url for {entry.thread_month}")

    request = Request(
        entry.source_url,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8", errors="replace")


def build_thread_record(entry: SourceIndexEntry, html: str, *, collected_at: datetime | None = None) -> ThreadRecord:
    """Build the thread-level raw metadata record."""

    if not entry.thread_id:
        raise FetchError(f"Missing thread_id for {entry.thread_month}")
    if not entry.thread_title:
        raise FetchError(f"Missing thread_title for {entry.thread_month}")
    if not entry.source_url:
        raise FetchError(f"Missing source_url for {entry.thread_month}")

    timestamp = collected_at or datetime.now(UTC)
    return ThreadRecord(
        thread_id=entry.thread_id,
        thread_month=entry.thread_month,
        thread_date=entry.thread_date,
        thread_title=entry.thread_title,
        source_url=entry.source_url,
        source_system=entry.source_system,
        collection_timestamp_utc=timestamp,
        raw_payload_hash=sha256_text(html),
    )


def write_raw_thread_artifacts(
    entry: SourceIndexEntry,
    html: str,
    *,
    collected_at: datetime | None = None,
) -> tuple[Path, Path, Path]:
    """Write raw thread HTML and thread metadata to canonical paths."""

    ensure_month_raw_dir(entry.thread_month)
    html_path = thread_html_path(entry.thread_month)
    metadata_path = thread_metadata_path(entry.thread_month)
    manifest_path = fetch_manifest_path(entry.thread_month)

    html_path.write_text(html, encoding="utf-8")
    thread_record = build_thread_record(entry, html, collected_at=collected_at)
    metadata_path.write_text(dumps_json(thread_record_to_dict(thread_record)), encoding="utf-8")
    manifest_path.write_text(
        dumps_json(
            build_fetch_manifest(
                entry,
                thread_record=thread_record,
                html_path=html_path,
                metadata_path=metadata_path,
            )
        ),
        encoding="utf-8",
    )
    return html_path, metadata_path, manifest_path


def fetch_and_write_thread(
    entry: SourceIndexEntry,
    *,
    timeout_seconds: float = 30.0,
    collected_at: datetime | None = None,
) -> tuple[Path, Path, Path]:
    """Fetch one verified thread and write its raw artifacts."""

    html = fetch_thread_html(entry, timeout_seconds=timeout_seconds)
    return write_raw_thread_artifacts(entry, html, collected_at=collected_at)


def fetchable_entries(entries: list[SourceIndexEntry]) -> list[SourceIndexEntry]:
    """Return verified entries that are eligible for raw fetching."""

    return verified_entries(entries)


def thread_record_to_dict(record: ThreadRecord) -> dict[str, str]:
    """Serialize thread metadata to a JSON-friendly dict."""

    raw = asdict(record)
    return {
        key: value.isoformat() if hasattr(value, "isoformat") else value
        for key, value in raw.items()
    }


def build_fetch_manifest(
    entry: SourceIndexEntry,
    *,
    thread_record: ThreadRecord,
    html_path: Path,
    metadata_path: Path,
) -> dict[str, object]:
    """Build a manifest describing one raw thread fetch."""

    return {
        "thread_month": entry.thread_month,
        "source_url": entry.source_url,
        "thread_id": entry.thread_id,
        "status": entry.status,
        "fetched_at_utc": thread_record.collection_timestamp_utc.isoformat(),
        "artifacts": {
            "thread_html": str(html_path),
            "thread_metadata": str(metadata_path),
        },
        "raw_payload_hash": thread_record.raw_payload_hash,
    }


def dumps_json(value: object) -> str:
    """Return deterministic JSON text for raw metadata artifacts."""

    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def sha256_text(value: str) -> str:
    """Return the SHA-256 hash of a UTF-8 string."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()
