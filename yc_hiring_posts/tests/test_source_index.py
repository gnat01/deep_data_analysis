import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from discovery import google_queries_for_entries, google_query_variants
from source_index import load_source_index, verified_entries


def test_load_source_index_returns_seeded_months() -> None:
    source_index_path = PROJECT_ROOT / "data" / "source_index.csv"
    entries = load_source_index(source_index_path)

    assert entries
    assert entries[0].thread_month == "2023-01"
    assert entries[-1].thread_month == "2026-04"


def test_verified_entries_present_for_verified_source_index() -> None:
    source_index_path = PROJECT_ROOT / "data" / "source_index.csv"
    entries = load_source_index(source_index_path)

    verified = verified_entries(entries)

    assert len(verified) == len(entries)
    assert verified[0].thread_month == "2023-01"
    assert verified[-1].thread_month == "2026-04"


def test_google_query_variants_match_expected_forms() -> None:
    queries = google_query_variants("2024-01")

    assert [query.search_query for query in queries] == [
        "news.ycombinator hiring for January 2024",
        "Hacker News hiring for January 2024",
    ]


def test_google_queries_for_entries_returns_two_per_month() -> None:
    source_index_path = PROJECT_ROOT / "data" / "source_index.csv"
    entries = load_source_index(source_index_path)

    queries = google_queries_for_entries(entries[:2])

    assert len(queries) == 4
    assert [query.thread_month for query in queries] == [
        "2023-01",
        "2023-01",
        "2023-02",
        "2023-02",
    ]
