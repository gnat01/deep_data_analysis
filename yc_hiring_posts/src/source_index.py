"""Load and validate the monthly source index."""

from __future__ import annotations

import csv
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Iterable

from models import SourceIndexEntry

ALLOWED_STATUSES = {"planned", "verified", "fetched", "blocked"}
REQUIRED_COLUMNS = {
    "thread_month",
    "thread_date",
    "thread_title",
    "thread_id",
    "source_url",
    "source_system",
    "status",
    "notes",
}


class SourceIndexError(ValueError):
    """Raised when the source index is malformed."""


def default_source_index_path() -> Path:
    """Return the default CSV path relative to the project root."""

    return Path(__file__).resolve().parents[1] / "data" / "source_index.csv"


def load_source_index(path: str | Path | None = None) -> list[SourceIndexEntry]:
    """Load, validate, and return all source index rows."""

    csv_path = Path(path) if path is not None else default_source_index_path()
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        _validate_columns(reader.fieldnames or [])
        rows = [_parse_row(row, line_number=index + 2) for index, row in enumerate(reader)]

    _validate_rows(rows)
    return rows


def verified_entries(entries: Iterable[SourceIndexEntry]) -> list[SourceIndexEntry]:
    """Return rows that are ready for ingestion."""

    return [entry for entry in entries if entry.status in {"verified", "fetched"}]


def entry_to_dict(entry: SourceIndexEntry) -> dict[str, str]:
    """Serialize an entry into a string-only dict for simple reporting."""

    row = asdict(entry)
    return {
        key: "" if value is None else value.isoformat() if hasattr(value, "isoformat") else str(value)
        for key, value in row.items()
    }


def _validate_columns(fieldnames: list[str]) -> None:
    missing = REQUIRED_COLUMNS.difference(fieldnames)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise SourceIndexError(f"Missing required source index columns: {missing_text}")


def _parse_row(row: dict[str, str], *, line_number: int) -> SourceIndexEntry:
    status = row["status"].strip()
    if status not in ALLOWED_STATUSES:
        raise SourceIndexError(f"Invalid status '{status}' on line {line_number}")

    thread_month = row["thread_month"].strip()
    if not _is_valid_thread_month(thread_month):
        raise SourceIndexError(f"Invalid thread_month '{thread_month}' on line {line_number}")

    thread_date = _parse_optional_date(row["thread_date"].strip(), line_number=line_number)
    thread_title = _clean_optional(row["thread_title"])
    thread_id = _clean_optional(row["thread_id"])
    source_url = _clean_optional(row["source_url"])
    source_system = row["source_system"].strip()
    notes = _clean_optional(row["notes"])

    if status in {"verified", "fetched"}:
        if not thread_id:
            raise SourceIndexError(f"thread_id is required for status '{status}' on line {line_number}")
        if not source_url:
            raise SourceIndexError(f"source_url is required for status '{status}' on line {line_number}")

    return SourceIndexEntry(
        thread_month=thread_month,
        thread_date=thread_date,
        thread_title=thread_title,
        thread_id=thread_id,
        source_url=source_url,
        source_system=source_system,
        status=status,
        notes=notes,
    )


def _validate_rows(rows: list[SourceIndexEntry]) -> None:
    seen_months: set[str] = set()
    for entry in rows:
        if entry.thread_month in seen_months:
            raise SourceIndexError(f"Duplicate thread_month found: {entry.thread_month}")
        seen_months.add(entry.thread_month)


def _parse_optional_date(value: str, *, line_number: int) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SourceIndexError(f"Invalid thread_date '{value}' on line {line_number}") from exc


def _is_valid_thread_month(value: str) -> bool:
    if len(value) != 7 or value[4] != "-":
        return False
    year_text, month_text = value.split("-", maxsplit=1)
    if not (year_text.isdigit() and month_text.isdigit()):
        return False
    month_number = int(month_text)
    return 1 <= month_number <= 12


def _clean_optional(value: str) -> str | None:
    cleaned = value.strip()
    return cleaned or None
