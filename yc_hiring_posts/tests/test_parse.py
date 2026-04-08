import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from parse import parse_thread_month_to_raw_posts, raw_post_to_dict, write_raw_posts_jsonl


def test_parse_thread_month_to_raw_posts_returns_top_level_posts_only() -> None:
    raw_posts = parse_thread_month_to_raw_posts("2025-03")

    assert raw_posts
    ids = {post.source_comment_id for post in raw_posts}
    assert "43247390" in ids
    assert "43313585" not in ids
    assert all(post.misc and post.misc["indent"] == 0 for post in raw_posts)


def test_parsed_raw_posts_preserve_misc_and_html() -> None:
    raw_posts = parse_thread_month_to_raw_posts("2025-03")
    first = raw_posts[0]

    assert first.raw_html
    assert first.misc is not None
    assert "commtext_html" in first.misc
    assert "links" in first.misc


def test_write_raw_posts_jsonl_writes_json_lines(tmp_path: Path, monkeypatch) -> None:
    import storage

    raw_posts = parse_thread_month_to_raw_posts("2025-03")[:2]
    monkeypatch.setattr(storage, "project_root", lambda: tmp_path)
    output_path = write_raw_posts_jsonl("2025-03", raw_posts)

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    parsed = json.loads(lines[0])
    assert parsed["thread_id"] == "43243024"
    assert parsed["misc"]["indent"] == 0
