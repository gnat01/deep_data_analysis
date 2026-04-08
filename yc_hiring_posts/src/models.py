"""Typed models for YC hiring-post ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal


SourceIndexStatus = Literal["planned", "verified", "fetched", "blocked"]


@dataclass(frozen=True)
class SourceIndexEntry:
    """Canonical monthly thread record used to drive ingestion."""

    thread_month: str
    thread_date: date | None
    thread_title: str | None
    thread_id: str | None
    source_url: str | None
    source_system: str
    status: SourceIndexStatus
    notes: str | None = None


@dataclass(frozen=True)
class ThreadRecord:
    """One YC monthly hiring thread."""

    thread_id: str
    thread_month: str
    thread_date: date | None
    thread_title: str
    source_url: str
    source_system: str
    collection_timestamp_utc: datetime
    raw_payload_hash: str | None = None


@dataclass(frozen=True)
class RawPostRecord:
    """One top-level hiring post captured from a thread."""

    raw_post_id: str
    thread_id: str
    source_comment_id: str
    author_handle: str | None
    posted_at_utc: datetime | None
    edited_at_utc: datetime | None
    raw_text: str
    source_url: str
    collection_timestamp_utc: datetime
    is_deleted: bool = False
    is_dead: bool = False
    raw_html: str | None = None
    raw_payload_json: str | None = None
    raw_payload_hash: str | None = None
