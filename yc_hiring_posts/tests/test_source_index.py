import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from yc_hiring_posts.discovery import google_query_variants
from yc_hiring_posts.source_index import load_source_index, verified_entries


def test_load_source_index_returns_seeded_months() -> None:
    source_index_path = PROJECT_ROOT / "data" / "source_index.csv"
    entries = load_source_index(source_index_path)

    assert entries
    assert entries[0].thread_month == "2024-01"
    assert entries[-1].thread_month == "2026-04"


def test_verified_entries_empty_for_seed_file() -> None:
    source_index_path = PROJECT_ROOT / "data" / "source_index.csv"
    entries = load_source_index(source_index_path)

    assert verified_entries(entries) == []


def test_google_query_variants_match_expected_forms() -> None:
    queries = google_query_variants("2024-01")

    assert [query.search_query for query in queries] == [
        "news.ycombinator hiring for January 2024",
        "Hacker News hiring for January 2024",
    ]
