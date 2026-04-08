"""Raw schema versioning and field contracts."""

from __future__ import annotations

RAW_SCHEMA_VERSION = "v1"

THREAD_RECORD_FIELDS = (
    "thread_id",
    "thread_month",
    "thread_date",
    "thread_title",
    "source_url",
    "source_system",
    "collection_timestamp_utc",
    "raw_payload_hash",
)

RAW_POST_FIELDS = (
    "raw_post_id",
    "thread_id",
    "source_comment_id",
    "author_handle",
    "posted_at_utc",
    "edited_at_utc",
    "raw_text",
    "source_url",
    "collection_timestamp_utc",
    "is_deleted",
    "is_dead",
    "raw_html",
    "raw_payload_json",
    "misc",
    "raw_payload_hash",
)

RAW_POST_REQUIRED_NONEMPTY_FIELDS = (
    "raw_post_id",
    "thread_id",
    "source_comment_id",
    "source_url",
    "collection_timestamp_utc",
    "raw_html",
    "misc",
    "raw_payload_hash",
)
