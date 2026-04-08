"""Extract role-level records from normalized hiring posts."""

from __future__ import annotations

import json
import re
from dataclasses import asdict
from pathlib import Path

from models import RemoteStatus, RoleRecord
from normalize import (
    EMPLOYMENT_PATTERNS,
    LOCATION_HINTS,
    REMOTE_PATTERNS,
    is_compensation_like,
    is_url_like,
)
from parse import clean_text
from storage import ensure_month_interim_dir, normalized_posts_jsonl_path, roles_jsonl_path

ROLE_EXTRACTOR_VERSION = "v1"

ROLE_KEYWORDS = (
    "engineer",
    "engineering",
    "design",
    "designer",
    "manager",
    "marketing",
    "researcher",
    "research",
    "scientist",
    "recruiter",
    "account executive",
    "customer success",
    "analytics",
    "analyst",
    "sales",
    "qa",
    "security",
    "product",
)
ROLE_SPLIT_RE = re.compile(r"\s*(?:,|/| and )\s*", re.IGNORECASE)
BULLET_ROLE_RE = re.compile(r"^(?:[-*]\s+)?(?P<title>[^:]+?)(?:\s*[-:–—]\s+.+)?$")
HIRING_SECTION_RE = re.compile(r"we(?:'| a)?re hiring(?: for| across| in)?", re.IGNORECASE)
SENIORITY_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bvp\b|vice president", re.IGNORECASE), "vp"),
    (re.compile(r"\bhead\b", re.IGNORECASE), "head"),
    (re.compile(r"\bprincipal\b", re.IGNORECASE), "principal"),
    (re.compile(r"\bstaff\b", re.IGNORECASE), "staff"),
    (re.compile(r"\bsenior\b|sr\.?\b", re.IGNORECASE), "senior"),
    (re.compile(r"\bmid[-\s]?level\b", re.IGNORECASE), "mid"),
    (re.compile(r"\bfounding\b", re.IGNORECASE), "founding"),
    (re.compile(r"\blead\b", re.IGNORECASE), "lead"),
    (re.compile(r"\bintern\b", re.IGNORECASE), "intern"),
)
ROLE_FAMILY_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"machine learning|mlops|applied ai|ai engineer|research engineer|researcher", re.IGNORECASE), "ml_ai"),
    (re.compile(r"data engineer|analytics engineer|data scientist|business intelligence|analytics", re.IGNORECASE), "data"),
    (re.compile(r"product designer|design engineer|designer|head of design", re.IGNORECASE), "design"),
    (re.compile(r"product manager|product management|head of product", re.IGNORECASE), "product"),
    (re.compile(r"account executive|sales", re.IGNORECASE), "sales"),
    (re.compile(r"customer success|technical account manager|technical customer success manager", re.IGNORECASE), "customer_success"),
    (re.compile(r"marketing", re.IGNORECASE), "marketing"),
    (re.compile(r"recruiter", re.IGNORECASE), "recruiting"),
    (re.compile(r"security", re.IGNORECASE), "security"),
    (re.compile(r"manager", re.IGNORECASE), "management"),
    (re.compile(r"engineer|engineering", re.IGNORECASE), "engineering"),
)


def extract_roles_for_thread_month(thread_month: str) -> list[RoleRecord]:
    """Extract role records for one normalized thread month."""

    normalized_posts = load_normalized_posts(thread_month)
    roles: list[RoleRecord] = []
    for post in normalized_posts:
        if not post.get("is_hiring_post"):
            continue
        roles.extend(extract_roles_from_post(post))
    return roles


def load_normalized_posts(thread_month: str) -> list[dict[str, object]]:
    """Load normalized post rows for one month."""

    lines = normalized_posts_jsonl_path(thread_month).read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def extract_roles_from_post(post: dict[str, object]) -> list[RoleRecord]:
    """Extract conservative role records from one normalized hiring post."""

    header_segments = list((((post.get("misc") or {}).get("header_segments")) or []))
    role_candidates = collect_header_role_candidates(header_segments)
    role_candidates.extend(collect_body_role_candidates(str(post.get("post_text_clean") or "")))
    deduped_candidates = dedupe_role_candidates(role_candidates)
    role_records: list[RoleRecord] = []
    for index, candidate in enumerate(deduped_candidates, start=1):
        role_records.append(build_role_record(post, candidate, index=index))
    return role_records


def collect_header_role_candidates(header_segments: list[str]) -> list[dict[str, str]]:
    """Extract role candidates from normalized header segments."""

    candidates: list[dict[str, str]] = []
    for segment in header_segments[1:]:
        if not is_role_like_segment(segment):
            continue
        for title in split_role_titles(segment):
            if is_role_title(title):
                candidates.append({"title": title, "source": "header", "source_fragment": segment})
    return candidates


def collect_body_role_candidates(post_text_clean: str) -> list[dict[str, str]]:
    """Extract role candidates from structured body lines."""

    lines = [clean_text(line) for line in post_text_clean.splitlines()]
    candidates: list[dict[str, str]] = []
    in_hiring_section = False
    for line in lines[1:]:
        if not line:
            continue
        if HIRING_SECTION_RE.search(line):
            in_hiring_section = True
            inline_titles = split_inline_hiring_titles(line)
            for title in inline_titles:
                if is_role_title(title):
                    candidates.append({"title": title, "source": "body_inline", "source_fragment": line})
            continue
        if in_hiring_section and looks_like_section_break(line):
            in_hiring_section = False
        if in_hiring_section:
            title = extract_role_title_from_line(line)
            if title and is_role_title(title):
                candidates.append({"title": title, "source": "body_section", "source_fragment": line})
                continue
        title = extract_role_title_from_line(line)
        if title and is_role_title(title) and is_likely_listed_role_line(line):
            candidates.append({"title": title, "source": "body_list", "source_fragment": line})
    return candidates


def build_role_record(post: dict[str, object], candidate: dict[str, str], *, index: int) -> RoleRecord:
    """Build one role record from a role candidate and its source post."""

    observed = clean_text(candidate["title"])
    return RoleRecord(
        role_id=f"{post['post_id']}#{index}",
        post_id=str(post["post_id"]),
        company_id=post.get("company_id"),
        role_title_observed=observed,
        role_title_normalized=normalize_role_title(observed),
        role_family=detect_role_family(observed),
        role_subfamily=None,
        seniority=detect_seniority(observed),
        headcount_text=detect_headcount_text(candidate["source_fragment"]),
        skills_text=None,
        responsibilities_text=None,
        requirements_text=None,
        role_location_text=extract_role_location_text(candidate["source_fragment"], post),
        role_remote_status=extract_role_remote_status(candidate["source_fragment"], post),
        role_compensation_id=None,
        misc={
            "extraction_source": candidate["source"],
            "source_fragment": candidate["source_fragment"],
            "role_extractor_version": ROLE_EXTRACTOR_VERSION,
        },
    )


def write_roles_jsonl(thread_month: str, roles: list[RoleRecord]) -> Path:
    """Write extracted roles for one month to JSON Lines."""

    ensure_month_interim_dir(thread_month)
    output_path = roles_jsonl_path(thread_month)
    lines = [json.dumps(role_to_dict(role), sort_keys=True) for role in roles]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return output_path


def extract_and_write_roles(thread_month: str) -> Path:
    """Extract roles for one month and write them to the interim layer."""

    roles = extract_roles_for_thread_month(thread_month)
    return write_roles_jsonl(thread_month, roles)


def role_to_dict(role: RoleRecord) -> dict[str, object]:
    """Serialize a role record into a JSON-friendly dict."""

    return asdict(role)


def is_role_like_segment(segment: str) -> bool:
    """Return true when a header segment plausibly contains role titles."""

    lowered = segment.lower()
    if is_url_like(segment):
        return False
    if any(pattern.search(segment) for pattern, _ in EMPLOYMENT_PATTERNS):
        return False
    if is_compensation_like(segment):
        return False
    if any(hint in lowered for hint in LOCATION_HINTS):
        return False
    return any(keyword in lowered for keyword in ROLE_KEYWORDS) or "multiple roles" in lowered


def split_role_titles(segment: str) -> list[str]:
    """Split a role-bearing text fragment into role titles."""

    value = clean_text(segment)
    value = re.sub(r"\betc\.?$", "", value, flags=re.IGNORECASE).strip(", ")
    if value.lower() == "multiple roles":
        return []
    if "+" in value and "engineer" in value.lower():
        parts = [clean_text(part) for part in value.split("+") if clean_text(part)]
        if len(parts) == 2 and "engineer" in parts[-1].lower():
            suffix = parts[-1]
            tail = " ".join(suffix.split()[1:])
            return [f"{parts[0]} {tail}".strip(), suffix]
    normalized = value.replace("&", ",")
    titles = [clean_text(part) for part in ROLE_SPLIT_RE.split(normalized) if clean_text(part)]
    expanded: list[str] = []
    for title in titles:
        if re.search(r"\bengineers?\b", title, re.IGNORECASE):
            singular = re.sub(r"\bengineers\b", "Engineer", title, flags=re.IGNORECASE)
            singular = re.sub(r"\bengineer\b", "Engineer", singular, flags=re.IGNORECASE)
            expanded.append(singular)
        elif re.search(r"\bdesigners\b", title, re.IGNORECASE):
            expanded.append(re.sub(r"\bdesigners\b", "Designer", title, flags=re.IGNORECASE))
        elif re.search(r"\bresearchers\b", title, re.IGNORECASE):
            expanded.append(re.sub(r"\bresearchers\b", "Researcher", title, flags=re.IGNORECASE))
        else:
            expanded.append(title)
    return expanded


def split_inline_hiring_titles(line: str) -> list[str]:
    """Split titles from inline 'we are hiring' phrasing."""

    lowered = line.lower()
    marker_positions = [
        lowered.find("we're hiring:"),
        lowered.find("we are hiring:"),
        lowered.find("we need:"),
        lowered.find("hiring:"),
    ]
    marker_positions = [value for value in marker_positions if value >= 0]
    if not marker_positions:
        return []
    start = min(marker_positions)
    fragment = line[start:].split(":", maxsplit=1)[-1]
    return [title for title in split_role_titles(fragment) if is_role_title(title)]


def extract_role_title_from_line(line: str) -> str | None:
    """Extract the role title portion from a structured body line."""

    match = BULLET_ROLE_RE.match(line)
    if not match:
        return None
    title = clean_text(match.group("title"))
    title = re.sub(r"\(.*?\)$", "", title).strip()
    title = re.sub(r"\bOpen to both.*$", "", title, flags=re.IGNORECASE).strip()
    if title.lower().startswith("apply"):
        return None
    return title or None


def is_likely_listed_role_line(line: str) -> bool:
    """Return true when a line looks like a structured role listing."""

    return line.startswith(("-", "*")) or " - http" in line.lower() or " – " in line or ": http" in line.lower()


def is_role_title(title: str) -> bool:
    """Return true when a candidate title looks like a role title."""

    lowered = title.lower()
    if not title or is_url_like(title):
        return False
    if len(title) > 120:
        return False
    return any(keyword in lowered for keyword in ROLE_KEYWORDS)


def dedupe_role_candidates(candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    """Deduplicate role candidates while preserving order."""

    seen: set[str] = set()
    result: list[dict[str, str]] = []
    for candidate in candidates:
        key = normalize_role_title(candidate["title"]) or clean_text(candidate["title"]).lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(candidate)
    return result


def normalize_role_title(title: str) -> str | None:
    """Normalize a role title into a stable first-pass canonical form."""

    value = clean_text(title)
    if not value:
        return None
    value = re.sub(r"\bEng\b", "Engineer", value, flags=re.IGNORECASE)
    value = re.sub(r"\bFullstack\b", "Full Stack", value, flags=re.IGNORECASE)
    value = re.sub(r"\bFull-Stack\b", "Full Stack", value, flags=re.IGNORECASE)
    value = re.sub(r"\bBackend\b", "Backend", value, flags=re.IGNORECASE)
    value = re.sub(r"\bFront-?end\b", "Frontend", value, flags=re.IGNORECASE)
    value = re.sub(r"\bMLOps\b", "MLOps", value, flags=re.IGNORECASE)
    return value


def detect_seniority(title: str) -> str | None:
    """Infer a simple seniority label from a role title."""

    for pattern, label in SENIORITY_PATTERNS:
        if pattern.search(title):
            return label
    return None


def detect_role_family(title: str) -> str | None:
    """Infer a simple role family from a title."""

    for pattern, family in ROLE_FAMILY_RULES:
        if pattern.search(title):
            return family
    return None


def detect_headcount_text(source_fragment: str) -> str | None:
    """Extract lightweight headcount hints from the source fragment."""

    lowered = source_fragment.lower()
    if "multiple roles" in lowered:
        return "multiple roles"
    if "various levels" in lowered:
        return "various levels"
    return None


def extract_role_location_text(source_fragment: str, post: dict[str, object]) -> str | None:
    """Return role-level location text, inheriting from the normalized post by default."""

    lowered = source_fragment.lower()
    if any(hint in lowered for hint in LOCATION_HINTS):
        return clean_text(source_fragment)
    return post.get("location_text")


def extract_role_remote_status(source_fragment: str, post: dict[str, object]) -> RemoteStatus:
    """Return role-level remote status, inheriting from the post when needed."""

    for pattern, status in REMOTE_PATTERNS:
        if pattern.search(source_fragment):
            return status
    value = post.get("remote_status")
    return value if value in {"remote", "hybrid", "onsite", "unspecified", "unknown"} else "unspecified"


def looks_like_section_break(line: str) -> bool:
    """Return true when a line likely ends a hiring-section block."""

    lowered = line.lower()
    if lowered.startswith(("tech stack", "apply", "how to apply", "for more information", "benefits", "email", "all applications")):
        return True
    return False
