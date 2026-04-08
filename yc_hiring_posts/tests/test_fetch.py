import json
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fetch import build_thread_record, fetchable_entries, sha256_text, write_raw_thread_artifacts
from models import SourceIndexEntry


def make_entry() -> SourceIndexEntry:
    return SourceIndexEntry(
        thread_month="2025-03",
        thread_date=None,
        thread_title="Ask HN: Who is hiring? (March 2025)",
        thread_id="43243024",
        source_url="https://news.ycombinator.com/item?id=43243024",
        source_system="hacker_news",
        status="verified",
        notes="test fixture",
    )


def test_build_thread_record_hashes_html() -> None:
    entry = make_entry()
    html = "<html><body>hello</body></html>"
    collected_at = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)

    record = build_thread_record(entry, html, collected_at=collected_at)

    assert record.thread_id == "43243024"
    assert record.collection_timestamp_utc == collected_at
    assert record.raw_payload_hash == sha256_text(html)


def test_write_raw_thread_artifacts_writes_expected_files(monkeypatch, tmp_path: Path) -> None:
    import storage

    entry = make_entry()
    html = "<html><body>hello</body></html>"
    collected_at = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)

    monkeypatch.setattr(storage, "project_root", lambda: tmp_path)

    html_path, metadata_path, manifest_path = write_raw_thread_artifacts(entry, html, collected_at=collected_at)

    assert html_path.read_text(encoding="utf-8") == html
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["thread_id"] == "43243024"
    assert metadata["thread_month"] == "2025-03"
    assert metadata["collection_timestamp_utc"] == "2026-04-08T12:00:00+00:00"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["thread_month"] == "2025-03"
    assert manifest["artifacts"]["thread_html"].endswith("/data/raw/2025-03/thread.html")
    assert manifest["artifacts"]["thread_metadata"].endswith("/data/raw/2025-03/thread.json")


def test_fetchable_entries_filters_verified_and_fetched() -> None:
    entries = [
        make_entry(),
        SourceIndexEntry(
            thread_month="2025-04",
            thread_date=None,
            thread_title="Ask HN: Who is hiring? (April 2025)",
            thread_id="43547611",
            source_url="https://news.ycombinator.com/item?id=43547611",
            source_system="hacker_news",
            status="fetched",
            notes="already fetched",
        ),
        SourceIndexEntry(
            thread_month="2025-05",
            thread_date=None,
            thread_title=None,
            thread_id=None,
            source_url=None,
            source_system="hacker_news",
            status="planned",
            notes="not ready",
        ),
    ]

    assert [entry.thread_month for entry in fetchable_entries(entries)] == ["2025-03", "2025-04"]
