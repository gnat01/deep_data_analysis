"""Normalize raw top-level posts into analysis-ready post records."""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from models import CompensationTextAccuracy, NormalizedPostRecord, RemoteStatus
from parse import clean_text
from storage import ensure_month_interim_dir, normalized_posts_jsonl_path, posts_jsonl_path

NORMALIZED_POST_PARSER_VERSION = "v1"

ROLE_KEYWORDS = (
    "engineer",
    "engineering",
    "developer",
    "scientist",
    "designer",
    "product",
    "manager",
    "cto",
    "founder",
    "frontend",
    "front-end",
    "backend",
    "back-end",
    "fullstack",
    "full-stack",
    "data",
    "ml",
    "ai",
    "devops",
    "sre",
    "qa",
    "security",
    "intern",
    "staff",
    "senior",
    "principal",
    "lead",
)
LOCATION_HINTS = (
    "remote",
    "hybrid",
    "onsite",
    "on-site",
    "in person",
    "usa",
    "us ",
    "u.s.",
    "uk",
    "europe",
    "eu",
    "canada",
    "india",
    "nyc",
    "sf",
    "bay area",
    "london",
    "berlin",
)
EMPLOYMENT_PATTERNS = (
    (re.compile(r"\bfull[\s-]?time\b", re.IGNORECASE), "full-time"),
    (re.compile(r"\bpart[\s-]?time\b", re.IGNORECASE), "part-time"),
    (re.compile(r"\bcontract(or)?\b", re.IGNORECASE), "contract"),
    (re.compile(r"\btemp(orary)?\b", re.IGNORECASE), "temporary"),
    (re.compile(r"\b(intern(ship)?s?|co-?ops?)\b", re.IGNORECASE), "internship"),
)
REMOTE_PATTERNS: tuple[tuple[re.Pattern[str], RemoteStatus], ...] = (
    (re.compile(r"\bhybrid\b", re.IGNORECASE), "hybrid"),
    (re.compile(r"\bremote\b", re.IGNORECASE), "remote"),
    (re.compile(r"\bonsite\b|\bon-site\b|\bin person\b", re.IGNORECASE), "onsite"),
)
COMPENSATION_RE = re.compile(
    r"(\$[\d,]+(?:\.\d+)?(?:\s?[kKmM])?(?:\s*-\s*\$?[\d,]+(?:\.\d+)?(?:\s?[kKmM])?)?(?:\s*/\s*(?:year|yr|hour|hr|month|mo))?)",
    re.IGNORECASE,
)
VISA_RE = re.compile(
    r"([^.]*\b(?:visa|sponsor(?:ship)?|citizenship|work authorization|authorized to work)\b[^.]*)",
    re.IGNORECASE,
)
HIRING_SIGNAL_PATTERNS = (
    re.compile(r"\bwe('?| a)re hiring\b", re.IGNORECASE),
    re.compile(r"\bhiring\b", re.IGNORECASE),
    re.compile(r"\bapply\b", re.IGNORECASE),
    re.compile(r"\bcareers?\b", re.IGNORECASE),
    re.compile(r"\bjob\b", re.IGNORECASE),
    re.compile(r"\bpositions?\b", re.IGNORECASE),
)
FUNDING_CONTEXT_RE = re.compile(
    r"\b(raised|funding|funded|venture capital|capital|seed|series [a-z]|arr|revenue)\b",
    re.IGNORECASE,
)


def normalize_thread_month_to_posts(thread_month: str) -> list[NormalizedPostRecord]:
    """Normalize one parsed thread month into post-level records."""

    raw_rows = load_raw_post_dicts(thread_month)
    return [normalize_raw_post_dict(row) for row in raw_rows]


def load_raw_post_dicts(thread_month: str) -> list[dict[str, object]]:
    """Load stored raw posts JSONL for one month."""

    lines = posts_jsonl_path(thread_month).read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def normalize_raw_post_dict(raw_post: dict[str, object]) -> NormalizedPostRecord:
    """Normalize one raw-post row into a post-level record."""

    raw_text = clean_text(str(raw_post.get("raw_text") or ""))
    headline, header_segments = extract_headline_and_segments(raw_text)
    company_name = extract_company_name(header_segments)
    location_segments = extract_location_segments(header_segments)
    compensation_matches = extract_compensation_matches(raw_text)
    funding = extract_funding(raw_text)
    compensation_text = select_compensation_text(compensation_matches, funding)
    compensation_text_accuracy = assess_compensation_text_accuracy(compensation_text)
    visa_text = extract_visa_text(raw_text, header_segments)
    employment_type, employment_type_signals = extract_employment_type(raw_text)
    remote_status, remote_signals = extract_remote_status(raw_text, header_segments)
    is_hiring_post, classification_signals = classify_hiring_post(
        raw_post=raw_post,
        raw_text=raw_text,
        headline=headline,
        company_name=company_name,
        employment_type=employment_type,
        remote_status=remote_status,
    )
    parse_confidence = score_parse_confidence(
        is_hiring_post=is_hiring_post,
        company_name=company_name,
        classification_signal_count=len(classification_signals),
    )

    misc = {
        "headline": headline,
        "header_segments": header_segments,
        "header_segment_count": len(header_segments),
        "location_segments": location_segments,
        "remote_signals": remote_signals,
        "employment_type_signals": employment_type_signals,
        "compensation_matches": compensation_matches,
        "compensation_text_accuracy_reason": compensation_accuracy_reason(compensation_text, compensation_text_accuracy),
        "funding_context_detected": funding is not None,
        "visa_signal_text": visa_text,
        "classification_signals": classification_signals,
        "normalized_from_raw_schema_version": raw_post.get("raw_schema_version"),
    }

    created_at = datetime.now(UTC)
    collection_timestamp = raw_post.get("collection_timestamp_utc")
    if isinstance(collection_timestamp, str) and collection_timestamp:
        created_at = datetime.fromisoformat(collection_timestamp)

    return NormalizedPostRecord(
        post_id=str(raw_post["raw_post_id"]),
        raw_post_id=str(raw_post["raw_post_id"]),
        thread_id=str(raw_post["thread_id"]),
        company_id=None,
        company_name_observed=company_name,
        is_hiring_post=is_hiring_post,
        location_text=" | ".join(location_segments) if location_segments else None,
        remote_status=remote_status,
        employment_type=employment_type,
        visa_sponsorship_text=visa_text,
        compensation_text=compensation_text,
        compensation_text_accuracy=compensation_text_accuracy,
        funding=funding,
        post_text_clean=raw_text,
        misc=misc,
        parser_version=NORMALIZED_POST_PARSER_VERSION,
        parse_confidence=parse_confidence,
        created_at_utc=created_at,
    )


def write_normalized_posts_jsonl(thread_month: str, normalized_posts: list[NormalizedPostRecord]) -> Path:
    """Write normalized post rows for one month to JSON Lines."""

    ensure_month_interim_dir(thread_month)
    output_path = normalized_posts_jsonl_path(thread_month)
    lines = [json.dumps(normalized_post_to_dict(post), sort_keys=True) for post in normalized_posts]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return output_path


def normalize_and_write_thread_posts(thread_month: str) -> Path:
    """Normalize one parsed month into the interim post layer."""

    normalized_posts = normalize_thread_month_to_posts(thread_month)
    return write_normalized_posts_jsonl(thread_month, normalized_posts)


def normalized_post_to_dict(post: NormalizedPostRecord) -> dict[str, object]:
    """Serialize a normalized-post record into a JSON-friendly dict."""

    data = asdict(post)
    for key, value in list(data.items()):
        if hasattr(value, "isoformat"):
            data[key] = value.isoformat()
    return data


def extract_headline_and_segments(raw_text: str) -> tuple[str, list[str]]:
    """Extract the first line and split it into conservative header segments."""

    if not raw_text:
        return "", []
    headline = raw_text.splitlines()[0].strip()
    segments = [clean_text(segment) for segment in headline.split("|") if clean_text(segment)]
    return headline, segments


def extract_company_name(header_segments: list[str]) -> str | None:
    """Infer the observed company name from the first headline segment."""

    if not header_segments:
        return None
    company = clean_text(header_segments[0]).strip("-")
    return company or None


def extract_location_segments(header_segments: list[str]) -> list[str]:
    """Return plausible location-bearing headline segments."""

    location_segments: list[str] = []
    for segment in header_segments[1:]:
        lowered = segment.lower()
        if is_url_like(segment):
            continue
        if any(keyword in lowered for keyword in ROLE_KEYWORDS):
            continue
        if is_compensation_like(segment):
            continue
        if any(pattern.search(segment) for pattern, _ in EMPLOYMENT_PATTERNS):
            continue
        if VISA_RE.search(segment):
            continue
        if any(hint in lowered for hint in LOCATION_HINTS) or "," in segment or "(" in segment:
            location_segments.append(segment)
    return location_segments


def extract_employment_type(raw_text: str) -> tuple[str | None, list[str]]:
    """Return a conservative employment type plus raw matched labels."""

    matches = [label for pattern, label in EMPLOYMENT_PATTERNS if pattern.search(raw_text)]
    deduped = list(dict.fromkeys(matches))
    if not deduped:
        return None, []
    if len(deduped) == 1:
        return deduped[0], deduped
    return "mixed", deduped


def extract_remote_status(raw_text: str, header_segments: list[str]) -> tuple[RemoteStatus, list[str]]:
    """Return normalized remote status and the matched raw signals."""

    search_text = "\n".join([raw_text, *header_segments])
    matched_labels: list[str] = []
    statuses: list[RemoteStatus] = []
    for pattern, status in REMOTE_PATTERNS:
        if pattern.search(search_text):
            statuses.append(status)
            matched_labels.append(status)
    if "hybrid" in statuses:
        return "hybrid", matched_labels
    if "remote" in statuses and "onsite" in statuses:
        return "hybrid", matched_labels
    if "remote" in statuses:
        return "remote", matched_labels
    if "onsite" in statuses:
        return "onsite", matched_labels
    return "unspecified", matched_labels


def extract_compensation_matches(raw_text: str) -> list[str]:
    """Extract raw compensation-like spans conservatively."""

    return list(dict.fromkeys(match.group(1).strip() for match in COMPENSATION_RE.finditer(raw_text)))


def assess_compensation_text_accuracy(compensation_text: str | None) -> CompensationTextAccuracy | None:
    """Return a simple accuracy flag for extracted compensation-like text."""

    if compensation_text is None:
        return None
    amount = parse_compensation_amount(compensation_text)
    if amount is None:
        return "low"
    if amount > 1_500_000:
        return "low"
    return "high"


def extract_funding(raw_text: str) -> str | None:
    """Extract a funding amount when compensation-like text appears in funding context."""

    for line in raw_text.splitlines():
        line_clean = clean_text(line)
        if not line_clean:
            continue
        if not FUNDING_CONTEXT_RE.search(line_clean):
            continue
        matches = extract_compensation_matches(line_clean)
        if matches:
            return matches[0]
    return None


def select_compensation_text(compensation_matches: list[str], funding: str | None) -> str | None:
    """Choose compensation text only when it is not already explained as funding."""

    if not compensation_matches:
        return None
    for match in compensation_matches:
        if funding is not None and match == funding:
            continue
        return match
    return None


def compensation_accuracy_reason(
    compensation_text: str | None,
    accuracy: CompensationTextAccuracy | None,
) -> str | None:
    """Provide a short explanation for the current compensation accuracy flag."""

    if compensation_text is None or accuracy is None:
        return None
    if accuracy == "low":
        amount = parse_compensation_amount(compensation_text)
        if amount is None:
            return "unparsed_compensation_like_span"
        if amount > 1_500_000:
            return "extracted_amount_above_1_5m_threshold"
        return "conservative_low_accuracy_flag"
    return "amount_within_reasonable_job_compensation_range"


def parse_compensation_amount(compensation_text: str) -> float | None:
    """Parse the leading amount from a compensation-like span into a numeric value."""

    match = re.search(r"\$([\d,]+(?:\.\d+)?)(?:\s?([kKmM]))?", compensation_text)
    if not match:
        return None
    amount = float(match.group(1).replace(",", ""))
    suffix = (match.group(2) or "").lower()
    if suffix == "k":
        amount *= 1_000
    elif suffix == "m":
        amount *= 1_000_000
    return amount


def extract_visa_text(raw_text: str, header_segments: list[str]) -> str | None:
    """Extract the first visa or work-authorization statement when present."""

    for segment in header_segments:
        if VISA_RE.search(segment):
            return clean_text(segment)
    for line in raw_text.splitlines():
        if VISA_RE.search(line):
            return clean_text(line)
    match = VISA_RE.search(raw_text)
    return clean_text(match.group(1)) if match else None


def classify_hiring_post(
    *,
    raw_post: dict[str, object],
    raw_text: str,
    headline: str,
    company_name: str | None,
    employment_type: str | None,
    remote_status: RemoteStatus,
) -> tuple[bool, list[str]]:
    """Classify a normalized post as hiring or not with reviewable signals."""

    signals: list[str] = []
    if bool(raw_post.get("is_deleted")):
        signals.append("deleted_post")
        return False, signals
    if not raw_text:
        signals.append("empty_text")
        return False, signals
    if company_name:
        signals.append("headline_company_segment")
    if len([segment for segment in headline.split("|") if clean_text(segment)]) >= 3:
        signals.append("multi_segment_header")
    if employment_type:
        signals.append("employment_type_detected")
    if remote_status != "unspecified":
        signals.append("remote_status_detected")
    if any(keyword in raw_text.lower() for keyword in ROLE_KEYWORDS):
        signals.append("role_keyword_detected")
    if any(pattern.search(raw_text) for pattern in HIRING_SIGNAL_PATTERNS):
        signals.append("hiring_language_detected")
    if "company:" in raw_text.lower():
        signals.append("company_section_detected")
    if "apply" in raw_text.lower() or "@" in raw_text or "careers" in raw_text.lower():
        signals.append("application_path_detected")
    return len(signals) >= 2, signals


def score_parse_confidence(*, is_hiring_post: bool, company_name: str | None, classification_signal_count: int) -> float:
    """Assign a conservative confidence score to the normalization output."""

    score = 0.3
    if company_name:
        score += 0.2
    score += min(classification_signal_count, 4) * 0.12
    if not is_hiring_post:
        score -= 0.2
    return max(0.05, min(round(score, 2), 0.95))


def is_url_like(value: str) -> bool:
    """Return true when a segment looks like a URL."""

    lowered = value.lower()
    return lowered.startswith("http://") or lowered.startswith("https://") or "." in lowered and "/" in lowered


def is_compensation_like(value: str) -> bool:
    """Return true when a segment looks like compensation text."""

    return bool(COMPENSATION_RE.search(value))
