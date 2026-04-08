"""Typed models for YC hiring-post ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal


SourceIndexStatus = Literal["planned", "verified", "fetched", "blocked"]
RemoteStatus = Literal["remote", "hybrid", "onsite", "unspecified", "unknown"]
CompensationTextAccuracy = Literal["high", "low"]


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
    """One top-level post captured from a thread."""

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
    misc: dict[str, object] | None = None
    raw_payload_hash: str | None = None


@dataclass(frozen=True)
class NormalizedPostRecord:
    """One normalized post record derived from a raw top-level post."""

    post_id: str
    raw_post_id: str
    thread_id: str
    company_id: str | None
    company_name_observed: str | None
    company_name_normalized: str | None
    is_hiring_post: bool
    location_text: str | None
    remote_status: RemoteStatus
    employment_type: str | None
    visa_sponsorship_text: str | None
    compensation_text: str | None
    compensation_text_accuracy: CompensationTextAccuracy | None
    funding: str | None
    post_text_clean: str
    misc: dict[str, object] | None
    parser_version: str
    parse_confidence: float
    created_at_utc: datetime


@dataclass(frozen=True)
class RoleRecord:
    """One distinct role extracted from a normalized hiring post."""

    role_id: str
    post_id: str
    company_id: str | None
    role_title_observed: str
    role_title_normalized: str | None
    role_family: str | None
    role_subfamily: str | None
    seniority: str | None
    headcount_text: str | None
    skills_text: str | None
    responsibilities_text: str | None
    requirements_text: str | None
    role_location_text: str | None
    role_remote_status: RemoteStatus
    role_compensation_id: str | None
    misc: dict[str, object] | None
