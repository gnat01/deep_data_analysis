import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from normalize import (
    NORMALIZED_POST_PARSER_VERSION,
    normalize_thread_month_to_posts,
    normalized_post_to_dict,
    write_normalized_posts_jsonl,
)


def test_normalize_thread_month_to_posts_returns_records() -> None:
    normalized_posts = normalize_thread_month_to_posts("2025-03")

    assert normalized_posts
    assert len(normalized_posts) == 357
    assert all(post.thread_id == "43243024" for post in normalized_posts)


def test_normalized_post_extracts_company_location_and_remote_status() -> None:
    normalized_posts = normalize_thread_month_to_posts("2025-03")
    first = normalized_posts[0]

    assert first.company_name_observed == "Stellar Science"
    assert first.remote_status == "hybrid"
    assert first.location_text == "Hybrid (USA) Albuquerque NM, Washington DC (Tysons VA), Dayton OH"
    assert first.employment_type == "mixed"
    assert first.visa_sponsorship_text == "U.S. citizenship required"
    assert first.compensation_text is None
    assert first.compensation_text_accuracy is None
    assert first.funding is None
    assert first.is_hiring_post is True
    assert first.misc is not None
    assert "classification_signals" in first.misc


def test_write_normalized_posts_jsonl_writes_json_lines(tmp_path: Path, monkeypatch) -> None:
    import storage

    normalized_posts = normalize_thread_month_to_posts("2025-03")[:2]
    monkeypatch.setattr(storage, "project_root", lambda: tmp_path)
    output_path = write_normalized_posts_jsonl("2025-03", normalized_posts)

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    parsed = json.loads(lines[0])
    assert parsed["thread_id"] == "43243024"
    assert parsed["parser_version"] == NORMALIZED_POST_PARSER_VERSION
    assert parsed["company_name_observed"] == "Stellar Science"
    assert parsed["misc"]["header_segment_count"] >= 1


def test_normalized_post_to_dict_serializes_timestamps() -> None:
    first = normalize_thread_month_to_posts("2025-03")[0]
    payload = normalized_post_to_dict(first)

    assert isinstance(payload["created_at_utc"], str)


def test_large_compensation_like_spans_are_flagged_low_accuracy() -> None:
    target = next(
        post
        for post in normalize_thread_month_to_posts("2024-05")
        if post.company_name_observed == "PostHog"
    )

    assert target.compensation_text is None
    assert target.compensation_text_accuracy is None
    assert target.funding == "$10M"
    assert target.misc is not None
    assert target.misc["compensation_text_accuracy_reason"] is None
    assert target.misc["funding_context_detected"] is True


def test_filled_update_post_is_not_hiring() -> None:
    target = next(
        post
        for post in normalize_thread_month_to_posts("2025-03")
        if post.company_name_observed
        == "*Update*: Thanks everyone for applying, this job has been filled! I'll leave the initial job description below for transparency."
    )

    assert target.is_hiring_post is False
    assert target.misc is not None
    assert "closure_or_filled_notice" in target.misc["classification_signals"]


def test_closed_post_is_not_hiring() -> None:
    target = next(
        post
        for post in normalize_thread_month_to_posts("2025-08")
        if post.company_name_observed == "* CLOSED *"
    )

    assert target.is_hiring_post is False
    assert target.misc is not None
    assert "closure_or_filled_notice" in target.misc["classification_signals"]
