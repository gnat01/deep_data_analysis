"""Conservative company normalization for normalized posts and roles."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path

from models import CompanyRecord
from parse import clean_text
from storage import (
    ensure_month_interim_dir,
    month_interim_dir,
    normalized_posts_jsonl_path,
    roles_jsonl_path,
)

YC_BATCH_RE = re.compile(r"\(yc\s+[a-z]\d{2}\)", re.IGNORECASE)
URL_RE = re.compile(r"https?://\S+|\bwww\.\S+", re.IGNORECASE)
LEADING_SYMBOLS_RE = re.compile(r"^[#*`~\s]+")
TRAILING_SYMBOLS_RE = re.compile(r"[\s|,.;:!]+$")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
WEAK_NAME_MARKERS = (
    "closed",
    "filled",
    "update",
    "aggregated for easier reading",
    "thanks everyone",
    "absolute control freaks",
)


def normalize_companies_for_thread_month(thread_month: str) -> tuple[list[CompanyRecord], list[dict[str, object]], list[dict[str, object]]]:
    """Build companies and backfill company ids for one normalized month."""

    posts = load_jsonl(normalized_posts_jsonl_path(thread_month))
    roles = load_jsonl(roles_jsonl_path(thread_month)) if roles_jsonl_path(thread_month).exists() else []

    company_groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for post in posts:
        match_key = derive_company_match_key(post.get("company_name_observed"))
        if not match_key:
            continue
        company_groups[match_key].append(post)

    companies = [
        build_company_record(match_key, grouped_posts, thread_month=thread_month)
        for match_key, grouped_posts in sorted(company_groups.items())
    ]
    company_id_by_key = {company.company_match_key: company.company_id for company in companies}

    enriched_posts = []
    for post in posts:
        updated = dict(post)
        match_key = derive_company_match_key(post.get("company_name_observed"))
        updated["company_id"] = company_id_by_key.get(match_key)
        misc = dict(updated.get("misc") or {})
        misc["company_match_key"] = match_key
        updated["misc"] = misc
        enriched_posts.append(updated)

    role_company_by_post = {post["post_id"]: post.get("company_id") for post in enriched_posts}
    enriched_roles = []
    for role in roles:
        updated = dict(role)
        updated["company_id"] = role_company_by_post.get(role["post_id"])
        enriched_roles.append(updated)

    return companies, enriched_posts, enriched_roles


def derive_company_match_key(company_name_observed: object) -> str | None:
    """Return a conservative company matching key from an observed name."""

    if not isinstance(company_name_observed, str):
        return None
    value = clean_text(company_name_observed)
    if not value:
        return None
    lowered = value.lower()
    if any(marker in lowered for marker in WEAK_NAME_MARKERS):
        return None
    value = URL_RE.sub("", value)
    value = YC_BATCH_RE.sub("", value)
    value = LEADING_SYMBOLS_RE.sub("", value)
    value = TRAILING_SYMBOLS_RE.sub("", value)
    value = re.sub(r"\([^)]*https?://[^)]*\)", "", value, flags=re.IGNORECASE)
    value = clean_text(value)
    if not value or len(value) > 80:
        return None
    compact = NON_ALNUM_RE.sub("", value.lower())
    if len(compact) < 2:
        return None
    return compact


def build_company_record(match_key: str, posts: list[dict[str, object]], *, thread_month: str) -> CompanyRecord:
    """Build one company record from grouped post rows."""

    variants = sorted({clean_text(str(post["company_name_observed"])) for post in posts if post.get("company_name_observed")})
    preferred = pick_preferred_company_name(variants)
    websites = [extract_company_website(post) for post in posts]
    websites = [website for website in websites if website]
    yc_batches = sorted({batch for post in posts if (batch := extract_yc_batch(str(post.get("company_name_observed") or "")))})
    return CompanyRecord(
        company_id=company_id_from_match_key(match_key),
        company_name_observed_preferred=preferred,
        company_match_key=match_key,
        company_name_variants=variants,
        company_website=websites[0] if websites else None,
        yc_batch=yc_batches[0] if yc_batches else None,
        entity_resolution_notes="conservative_match_key_v1",
        first_seen_thread_month=thread_month,
        last_seen_thread_month=thread_month,
        misc={
            "matched_post_count": len(posts),
            "variant_count": len(variants),
        },
    )


def company_id_from_match_key(match_key: str) -> str:
    """Return a stable company id from a conservative match key."""

    digest = hashlib.sha1(match_key.encode("utf-8")).hexdigest()[:12]
    return f"company_{digest}"


def pick_preferred_company_name(variants: list[str]) -> str:
    """Choose a display-preferred observed variant conservatively."""

    def score(value: str) -> tuple[int, int, str]:
        return (
            1 if URL_RE.search(value) else 0,
            len(value),
            value.lower(),
        )

    return sorted(variants, key=score)[0]


def extract_company_website(post: dict[str, object]) -> str | None:
    """Extract a likely company website from post misc links or header segments."""

    misc = post.get("misc") or {}
    links = misc.get("links") or []
    for link in links:
        if not isinstance(link, dict):
            continue
        href = link.get("href")
        if isinstance(href, str) and href.startswith("http"):
            return href.rstrip(").,;:")
    for segment in misc.get("header_segments") or []:
        if not isinstance(segment, str):
            continue
        match = URL_RE.search(segment)
        if match:
            href = match.group(0)
            href = href.rstrip(").,;:")
            return href if href.startswith("http") else f"https://{href}"
    return None


def extract_yc_batch(company_name_observed: str) -> str | None:
    """Extract YC batch notation from an observed company name."""

    match = YC_BATCH_RE.search(company_name_observed)
    if not match:
        return None
    return match.group(0).strip("()")


def load_jsonl(path: Path) -> list[dict[str, object]]:
    """Load JSON lines from a path."""

    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def write_companies_jsonl(thread_month: str, companies: list[CompanyRecord]) -> Path:
    """Write company dimension rows for one month."""

    ensure_month_interim_dir(thread_month)
    output_path = month_interim_dir(thread_month) / "companies.jsonl"
    lines = [json.dumps(company_to_dict(company), sort_keys=True) for company in companies]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return output_path


def write_updated_posts_jsonl(thread_month: str, posts: list[dict[str, object]]) -> Path:
    """Rewrite normalized posts with backfilled company ids."""

    output_path = normalized_posts_jsonl_path(thread_month)
    lines = [json.dumps(post, sort_keys=True) for post in posts]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return output_path


def write_updated_roles_jsonl(thread_month: str, roles: list[dict[str, object]]) -> Path | None:
    """Rewrite role rows with backfilled company ids when roles exist."""

    output_path = roles_jsonl_path(thread_month)
    if not output_path.exists() and not roles:
        return None
    lines = [json.dumps(role, sort_keys=True) for role in roles]
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return output_path


def normalize_and_write_companies(thread_month: str) -> tuple[Path, Path, Path | None]:
    """Run company normalization for one month and persist all outputs."""

    companies, posts, roles = normalize_companies_for_thread_month(thread_month)
    companies_path = write_companies_jsonl(thread_month, companies)
    posts_path = write_updated_posts_jsonl(thread_month, posts)
    roles_path = write_updated_roles_jsonl(thread_month, roles)
    return companies_path, posts_path, roles_path


def company_to_dict(company: CompanyRecord) -> dict[str, object]:
    """Serialize a company record to a JSON-friendly dict."""

    return asdict(company)
