"""Parse raw Hacker News thread HTML into top-level raw post records."""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import UTC, datetime
from html import unescape
from pathlib import Path

from fetch import dumps_json, sha256_text
from models import RawPostRecord
from storage import ensure_month_raw_dir, posts_jsonl_path, thread_html_path, thread_metadata_path

COMMENT_ROW_START_RE = re.compile(r'<tr class="athing comtr" id="(?P<comment_id>\d+)">')
INDENT_RE = re.compile(r'<td class="ind" indent="(?P<indent>\d+)">')
HNUSER_RE = re.compile(r'<a href="user\?id=[^"]+" class="hnuser">(?P<user>.*?)</a>')
AGE_TITLE_RE = re.compile(r'<span class="age" title="(?P<title>[^"]+)">')
AGE_LABEL_RE = re.compile(r'<span class="age" title="[^"]+"><a href="item\?id=\d+">(?P<label>.*?)</a></span>')
COMM_TEXT_RE = re.compile(r'<div class="commtext[^"]*">(?P<html>.*?)</div><div class="reply">', re.DOTALL)
LINK_RE = re.compile(r'<a href="(?P<href>[^"]+)"[^>]*>(?P<label>.*?)</a>', re.DOTALL)
NAV_LINK_RE = re.compile(r'<span class="navs">(?P<navs>.*?)</span>', re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"[ \t]+")
BLANKLINE_RE = re.compile(r"\n{3,}")


def parse_thread_month_to_raw_posts(thread_month: str) -> list[RawPostRecord]:
    """Parse the stored raw thread HTML for one month into top-level raw posts."""

    html = thread_html_path(thread_month).read_text(encoding="utf-8")
    thread_metadata = json.loads(thread_metadata_path(thread_month).read_text(encoding="utf-8"))
    collected_at = datetime.fromisoformat(thread_metadata["collection_timestamp_utc"])
    thread_id = thread_metadata["thread_id"]
    return parse_thread_html(
        html,
        thread_id=thread_id,
        collected_at=collected_at,
    )


def parse_thread_html(html: str, *, thread_id: str, collected_at: datetime) -> list[RawPostRecord]:
    """Parse one HN thread HTML document into top-level raw post records."""

    comment_rows = extract_comment_row_fragments(html)
    raw_posts: list[RawPostRecord] = []
    for position, (comment_id, fragment) in enumerate(comment_rows):
        indent = extract_indent(fragment)
        if indent != 0:
            continue
        raw_posts.append(
            build_raw_post_record(
                fragment,
                thread_id=thread_id,
                source_comment_id=comment_id,
                collected_at=collected_at,
                position_in_thread=position,
                indent=indent,
            )
        )
    return raw_posts


def extract_comment_row_fragments(html: str) -> list[tuple[str, str]]:
    """Return each HN comment row fragment keyed by source comment id."""

    matches = list(COMMENT_ROW_START_RE.finditer(html))
    fragments: list[tuple[str, str]] = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(html)
        fragments.append((match.group("comment_id"), html[start:end]))
    return fragments


def extract_indent(fragment: str) -> int:
    """Return the Hacker News indent level for one comment row fragment."""

    match = INDENT_RE.search(fragment)
    if not match:
        return -1
    return int(match.group("indent"))


def build_raw_post_record(
    fragment: str,
    *,
    thread_id: str,
    source_comment_id: str,
    collected_at: datetime,
    position_in_thread: int,
    indent: int,
) -> RawPostRecord:
    """Build one raw top-level post record from a comment-row fragment."""

    author_handle = extract_author(fragment)
    posted_at = extract_posted_at(fragment)
    commtext_html = extract_commtext_html(fragment)
    raw_text = html_fragment_to_text(commtext_html)
    misc = {
        "indent": indent,
        "position_in_thread": position_in_thread,
        "age_label": extract_age_label(fragment),
        "age_title_raw": extract_age_title(fragment),
        "links": extract_links(commtext_html),
        "nav_labels": extract_nav_labels(fragment),
        "commtext_html": commtext_html,
        "text_extraction_method": "stdlib_regex_html_cleanup_v1",
    }
    return RawPostRecord(
        raw_post_id=f"{thread_id}:{source_comment_id}",
        thread_id=thread_id,
        source_comment_id=source_comment_id,
        author_handle=author_handle,
        posted_at_utc=posted_at,
        edited_at_utc=None,
        raw_text=raw_text,
        source_url=f"https://news.ycombinator.com/item?id={source_comment_id}",
        collection_timestamp_utc=collected_at,
        is_deleted="coll" in fragment and commtext_html == "",
        is_dead='class="athing comtr dead"' in fragment,
        raw_html=fragment,
        raw_payload_json=None,
        misc=misc,
        raw_payload_hash=sha256_text(fragment),
    )


def write_raw_posts_jsonl(thread_month: str, raw_posts: list[RawPostRecord]) -> Path:
    """Write parsed top-level raw posts for one month to JSON Lines."""

    ensure_month_raw_dir(thread_month)
    output_path = posts_jsonl_path(thread_month)
    lines = [json.dumps(raw_post_to_dict(post), sort_keys=True) for post in raw_posts]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return output_path


def parse_and_write_thread_posts(thread_month: str) -> Path:
    """Parse one stored raw thread and write top-level posts JSONL."""

    raw_posts = parse_thread_month_to_raw_posts(thread_month)
    return write_raw_posts_jsonl(thread_month, raw_posts)


def raw_post_to_dict(raw_post: RawPostRecord) -> dict[str, object]:
    """Serialize a raw-post record into a JSON-friendly dict."""

    data = asdict(raw_post)
    for key, value in list(data.items()):
        if hasattr(value, "isoformat"):
            data[key] = value.isoformat()
    return data


def extract_author(fragment: str) -> str | None:
    match = HNUSER_RE.search(fragment)
    return unescape(match.group("user")) if match else None


def extract_age_title(fragment: str) -> str | None:
    match = AGE_TITLE_RE.search(fragment)
    return match.group("title") if match else None


def extract_age_label(fragment: str) -> str | None:
    match = AGE_LABEL_RE.search(fragment)
    return unescape(match.group("label")) if match else None


def extract_posted_at(fragment: str) -> datetime | None:
    age_title = extract_age_title(fragment)
    if not age_title:
        return None
    iso_text = age_title.split(" ", maxsplit=1)[0]
    return datetime.fromisoformat(iso_text).replace(tzinfo=UTC)


def extract_commtext_html(fragment: str) -> str:
    match = COMM_TEXT_RE.search(fragment)
    if not match:
        return ""
    return match.group("html").strip()


def extract_links(fragment_html: str) -> list[dict[str, str]]:
    return [
        {
            "href": unescape(match.group("href")),
            "label": clean_text(unescape(TAG_RE.sub("", match.group("label")))),
        }
        for match in LINK_RE.finditer(fragment_html)
    ]


def extract_nav_labels(fragment: str) -> list[str]:
    match = NAV_LINK_RE.search(fragment)
    if not match:
        return []
    return [clean_text(unescape(TAG_RE.sub("", value))) for value in match.group("navs").split("|") if clean_text(unescape(TAG_RE.sub("", value)))]


def html_fragment_to_text(fragment_html: str) -> str:
    """Convert a comment HTML fragment into readable plain text."""

    value = fragment_html
    replacements = {
        "<p>": "\n\n",
        "</p>": "",
        "<pre>": "\n\n",
        "</pre>": "\n\n",
        "<code>": "",
        "</code>": "",
        "<br>": "\n",
        "<br/>": "\n",
        "<br />": "\n",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    value = TAG_RE.sub("", value)
    value = unescape(value)
    return clean_text(value)


def clean_text(value: str) -> str:
    value = value.replace("\r", "")
    value = "\n".join(WHITESPACE_RE.sub(" ", line).strip() for line in value.splitlines())
    value = BLANKLINE_RE.sub("\n\n", value)
    return value.strip()
