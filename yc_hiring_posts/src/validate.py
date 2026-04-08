"""Validation for raw ingestion artifacts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from fetch import sha256_text
from parse import extract_comment_row_fragments, extract_indent
from raw_schema import RAW_POST_FIELDS, RAW_POST_REQUIRED_NONEMPTY_FIELDS, RAW_SCHEMA_VERSION
from storage import fetch_manifest_path, posts_jsonl_path, thread_html_path, thread_metadata_path

LOW_POST_COUNT_WARNING_THRESHOLD = 100


@dataclass(frozen=True)
class ValidationReport:
    """Validation output for one thread month."""

    thread_month: str
    checks_passed: bool
    top_level_post_count: int
    expected_top_level_post_count: int
    hard_failures: list[str]
    soft_warnings: list[str]
    files_checked: list[str]


def validate_thread_month(thread_month: str) -> ValidationReport:
    """Validate raw artifacts and parsed posts for one month."""

    hard_failures: list[str] = []
    soft_warnings: list[str] = []
    files_checked: list[str] = []

    html_path = thread_html_path(thread_month)
    metadata_path = thread_metadata_path(thread_month)
    posts_path = posts_jsonl_path(thread_month)
    manifest_path = fetch_manifest_path(thread_month)

    for path in [html_path, metadata_path, posts_path]:
        files_checked.append(str(path))
        if not path.exists():
            hard_failures.append(f"Missing required raw artifact: {path}")

    files_checked.append(str(manifest_path))
    if not manifest_path.exists():
        soft_warnings.append(f"Optional fetch manifest missing: {manifest_path}")

    if hard_failures:
        return ValidationReport(
            thread_month=thread_month,
            checks_passed=False,
            top_level_post_count=0,
            expected_top_level_post_count=0,
            hard_failures=hard_failures,
            soft_warnings=soft_warnings,
            files_checked=files_checked,
        )

    html = html_path.read_text(encoding="utf-8")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    lines = posts_path.read_text(encoding="utf-8").splitlines()

    expected_top_level_post_count = count_top_level_rows_in_html(html)
    parsed_rows = [json.loads(line) for line in lines]
    top_level_post_count = len(parsed_rows)

    if metadata.get("raw_payload_hash") != sha256_text(html):
        hard_failures.append("thread.json raw_payload_hash does not match thread.html")
    if metadata.get("raw_schema_version") != RAW_SCHEMA_VERSION:
        hard_failures.append(
            f"thread.json raw_schema_version mismatch: expected {RAW_SCHEMA_VERSION}, got {metadata.get('raw_schema_version')}"
        )

    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("raw_schema_version") != RAW_SCHEMA_VERSION:
            hard_failures.append(
                f"fetch_manifest.json raw_schema_version mismatch: expected {RAW_SCHEMA_VERSION}, got {manifest.get('raw_schema_version')}"
            )

    seen_comment_ids: set[str] = set()
    missing_timestamp_count = 0
    for index, row in enumerate(parsed_rows, start=1):
        _validate_required_row_fields(row, index=index, hard_failures=hard_failures)

        source_comment_id = row.get("source_comment_id")
        if source_comment_id in seen_comment_ids:
            hard_failures.append(f"Duplicate source_comment_id in posts.jsonl: {source_comment_id}")
        else:
            seen_comment_ids.add(source_comment_id)

        if row.get("posted_at_utc") in {None, ""}:
            missing_timestamp_count += 1

    if top_level_post_count != expected_top_level_post_count:
        hard_failures.append(
            f"Top-level post count mismatch: posts.jsonl={top_level_post_count}, html={expected_top_level_post_count}"
        )

    if top_level_post_count < LOW_POST_COUNT_WARNING_THRESHOLD:
        soft_warnings.append(
            f"Possible anomaly, use with discretion: top-level post count is {top_level_post_count}, below {LOW_POST_COUNT_WARNING_THRESHOLD}"
        )

    if top_level_post_count == 0:
        hard_failures.append("posts.jsonl contains zero parsed top-level posts")

    if missing_timestamp_count > 0:
        soft_warnings.append(
            f"Possible anomaly, use with discretion: {missing_timestamp_count} parsed posts are missing posted_at_utc"
        )

    return ValidationReport(
        thread_month=thread_month,
        checks_passed=not hard_failures,
        top_level_post_count=top_level_post_count,
        expected_top_level_post_count=expected_top_level_post_count,
        hard_failures=hard_failures,
        soft_warnings=soft_warnings,
        files_checked=files_checked,
    )


def validate_many_thread_months(thread_months: list[str]) -> list[ValidationReport]:
    """Validate many months in order."""

    return [validate_thread_month(thread_month) for thread_month in thread_months]


def validation_report_to_dict(report: ValidationReport) -> dict[str, object]:
    """Serialize a validation report for JSON output."""

    return asdict(report)


def count_top_level_rows_in_html(html: str) -> int:
    """Count indent-0 comment rows in stored HN thread HTML."""

    return sum(1 for _, fragment in extract_comment_row_fragments(html) if extract_indent(fragment) == 0)


def _validate_required_row_fields(row: dict[str, object], *, index: int, hard_failures: list[str]) -> None:
    schema_presence_fields = [*RAW_POST_FIELDS, "raw_schema_version"]
    for field in schema_presence_fields:
        if field not in row:
            hard_failures.append(f"Missing schema field '{field}' in posts.jsonl row {index}")

    for field in RAW_POST_REQUIRED_NONEMPTY_FIELDS:
        value = row.get(field)
        if value is None or value == "":
            hard_failures.append(f"Missing required field '{field}' in posts.jsonl row {index}")

    if row.get("raw_schema_version") != RAW_SCHEMA_VERSION:
        hard_failures.append(
            f"raw_schema_version mismatch in posts.jsonl row {index}: expected {RAW_SCHEMA_VERSION}, got {row.get('raw_schema_version')}"
        )

    raw_text = row.get("raw_text")
    is_deleted = bool(row.get("is_deleted"))
    if (raw_text is None or raw_text == "") and not is_deleted:
        hard_failures.append(f"Missing required field 'raw_text' in non-deleted posts.jsonl row {index}")
