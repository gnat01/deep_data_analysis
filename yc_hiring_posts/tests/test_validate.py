import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from validate import count_top_level_rows_in_html, validate_thread_month


def test_count_top_level_rows_in_html_matches_parsed_month() -> None:
    html = (PROJECT_ROOT / "data" / "raw" / "2025-03" / "thread.html").read_text(encoding="utf-8")

    assert count_top_level_rows_in_html(html) == 357


def test_validate_thread_month_passes_for_parsed_month() -> None:
    report = validate_thread_month("2025-03")

    assert report.checks_passed is True
    assert report.hard_failures == []
    assert report.top_level_post_count == 357
    assert report.expected_top_level_post_count == 357
