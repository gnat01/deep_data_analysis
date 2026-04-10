"""Conservative natural-language layer over the PostgreSQL KB router."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import re
from pathlib import Path
from typing import Any

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from analytics import AI_CONCEPT_PATTERNS
from kb_router import answer_catalog_question_postgres, load_question_catalog
from question_catalog import question_catalog_json_path
from storage import processed_data_dir


MONTH_NAME_TO_NUM = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

ROLE_FAMILY_KEYWORDS = {
    "ml_ai": [r"\bml\b", r"\bai\b", r"\bmachine learning\b", r"\bllm\b", r"\bgenai\b"],
    "data": [r"\bdata engineer\b", r"\bdata science\b", r"\bdata scientist\b", r"\banalytics\b"],
    "design": [r"\bdesign\b", r"\bdesigner\b", r"\bproduct designer\b"],
    "engineering": [r"\bbackend\b", r"\bfrontend\b", r"\bfull[- ]?stack\b", r"\bplatform engineer\b", r"\bsoftware engineer\b", r"\bengineering\b"],
    "product": [r"\bproduct manager\b", r"\bproduct\b"],
    "sales": [r"\bsales\b", r"\baccount executive\b"],
    "marketing": [r"\bmarketing\b", r"\bgrowth\b"],
}


@dataclass(frozen=True)
class ParsedQuestion:
    question_id: int | None
    confidence: float
    candidates: list[dict[str, Any]]
    company_name: str | None
    query: str | None
    role_family: str | None
    role_family_a: str | None
    role_family_b: str | None
    concept_name: str | None
    month_from: str | None
    month_to: str | None
    year: int | None
    mode: str | None
    requires_clarification: bool
    clarification_reason: str | None


def _catalog_rows() -> list[dict[str, Any]]:
    return json.loads(question_catalog_json_path().read_text(encoding="utf-8"))


def load_company_names(limit: int = 12000) -> list[str]:
    path = processed_data_dir() / "v1_core_tables" / "companies.jsonl"
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        name = str(row.get("company_name_observed_preferred") or "").strip()
        if name:
            rows.append(name)
    rows = sorted(set(rows), key=lambda value: (-len(value), value.lower()))
    return rows[:limit]


def extract_company_name(question: str, company_names: list[str] | None = None) -> str | None:
    lowered = question.lower()
    names = company_names or load_company_names()
    for company_name in names:
        candidate = company_name.lower()
        if len(candidate) < 3:
            continue
        pattern = r"(?<!\w)" + re.escape(candidate) + r"(?!\w)"
        if re.search(pattern, lowered):
            return company_name
    return None


def extract_role_families(question: str) -> tuple[str | None, str | None, str | None]:
    lowered = question.lower()
    matched = []
    for role_family, patterns in ROLE_FAMILY_KEYWORDS.items():
        if any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in patterns):
            matched.append(role_family)
    matched = list(dict.fromkeys(matched))
    if len(matched) >= 2:
        return matched[0], matched[0], matched[1]
    if len(matched) == 1:
        return matched[0], None, None
    return None, None, None


def extract_concept_name(question: str) -> str | None:
    lowered = question.lower()
    for concept_name, patterns in AI_CONCEPT_PATTERNS:
        if any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in patterns):
            return concept_name
    return None


def extract_year(question: str) -> int | None:
    years = [int(match) for match in re.findall(r"\b(20\d{2})\b", question)]
    return years[0] if len(years) == 1 else None


def _month_str(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def extract_month_range(question: str) -> tuple[str | None, str | None]:
    matches = re.findall(r"\b(20\d{2})-(0[1-9]|1[0-2])\b", question)
    if matches:
        months = [_month_str(int(year), int(month)) for year, month in matches]
        if len(months) >= 2:
            return months[0], months[-1]
        return months[0], months[0]

    month_name_matches = re.findall(
        r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(20\d{2})\b",
        question,
        flags=re.IGNORECASE,
    )
    if month_name_matches:
        months = [_month_str(int(year), MONTH_NAME_TO_NUM[name.lower()]) for name, year in month_name_matches]
        if len(months) >= 2:
            return months[0], months[-1]
        return months[0], months[0]

    years = [int(match) for match in re.findall(r"\b(20\d{2})\b", question)]
    if "between" in question.lower() or "from" in question.lower():
        if len(years) >= 2:
            today = date.today()
            end_month = today.month if years[-1] == today.year else 12
            return _month_str(years[0], 1), _month_str(years[-1], end_month)
    if len(years) == 1:
        year = years[0]
        if "ytd" in question.lower() or "year to date" in question.lower():
            today = date.today()
            return _month_str(year, 1), _month_str(year, today.month)
    return None, None


def extract_query_text(question: str, company_name: str | None = None) -> str | None:
    text = question.strip()
    if company_name:
        text = re.sub(re.escape(company_name), "", text, flags=re.IGNORECASE)
    patterns = [
        r"hiring for ([^?]+)",
        r"requirements for ([^?]+)",
        r"roles? ([^?]+)",
        r"mentioned ([^?]+)",
        r"for ([^?]+) in 20\d{2}",
        r"for ([^?]+) between",
        r"for ([^?]+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = match.group(1).strip(" .?")
            if candidate:
                return candidate
    return None


def _build_similarity_index() -> tuple[TfidfVectorizer, Any, list[dict[str, Any]]]:
    rows = _catalog_rows()
    vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2))
    matrix = vectorizer.fit_transform([row["question_text"] for row in rows])
    return vectorizer, matrix, rows


def infer_catalog_candidates(question: str, top_k: int = 5) -> list[dict[str, Any]]:
    vectorizer, matrix, rows = _build_similarity_index()
    query_vector = vectorizer.transform([question])
    similarities = cosine_similarity(query_vector, matrix)[0]
    ranked = sorted(
        zip(rows, similarities, strict=False),
        key=lambda item: float(item[1]),
        reverse=True,
    )[:top_k]
    return [
        {
            "question_id": int(row["question_id"]),
            "question_text": row["question_text"],
            "question_family": row["question_family"],
            "similarity": round(float(score), 4),
        }
        for row, score in ranked
    ]


def parse_nl_question(question: str) -> ParsedQuestion:
    company_names = load_company_names()
    company_name = extract_company_name(question, company_names)
    role_family, role_family_a, role_family_b = extract_role_families(question)
    concept_name = extract_concept_name(question)
    month_from, month_to = extract_month_range(question)
    year = extract_year(question)
    query = extract_query_text(question, company_name=company_name)
    candidates = infer_catalog_candidates(question)
    top = candidates[0] if candidates else None
    question_id = None if top is None else int(top["question_id"])
    confidence = 0.0 if top is None else float(top["similarity"])

    clarification_reason = None
    if question_id in {1, 2, 12, 23, 24, 28, 29, 37, 38, 40, 42, 44, 47, 49, 50} and not company_name and question_id not in {23, 24}:
        clarification_reason = "Need a company name to answer this question reliably."
    elif question_id in {23, 24, 25, 26, 28, 43} and not (query or role_family or role_family_a):
        clarification_reason = "Need a role or concept target to answer this question reliably."
    elif confidence < 0.18:
        clarification_reason = "Question match confidence is too low to answer safely."

    return ParsedQuestion(
        question_id=question_id,
        confidence=confidence,
        candidates=candidates,
        company_name=company_name,
        query=query,
        role_family=role_family,
        role_family_a=role_family_a,
        role_family_b=role_family_b,
        concept_name=concept_name,
        month_from=month_from,
        month_to=month_to,
        year=year,
        mode=None,
        requires_clarification=clarification_reason is not None,
        clarification_reason=clarification_reason,
    )


def summarize_answer(result: dict[str, Any]) -> str:
    helper = result["routed_helper"]
    answer = result["answer"]
    rows = answer.get("rows", [])
    if helper == "companies-every-month-postgres":
        count = int(answer.get("row_count", 0))
        names = [row["company_name"] for row in rows[:5]]
        return f"{count} companies posted in every month of the year. Examples: {', '.join(names)}."
    if helper == "remote-first-companies-postgres":
        count = int(answer.get("row_count", 0))
        names = [row["company_name"] for row in rows[:5]]
        return f"{count} companies were remote-first in the selected year. Examples: {', '.join(names)}."
    if helper == "company-remote-change-postgres":
        top = rows[:5]
        return "Companies with changing remote patterns include " + ", ".join(
            f"{row['company_name']} ({', '.join(row['remote_statuses'])})" for row in top
        ) + "."
    if helper == "month-summary-postgres":
        if rows:
            top = sorted(rows, key=lambda row: int(row.get("company_count", 0)), reverse=True)[:3]
            return "Months with the highest distinct company counts include " + "; ".join(
                f"{row['thread_month']} ({row['company_count']} companies)" for row in top
            ) + "."
    if helper == "role-family-timeline-postgres":
        if rows:
            top = sorted(rows, key=lambda row: int(row.get("role_count", 0)), reverse=True)[:3]
            role_family = top[0].get("role_family")
            return f"Top months for {role_family} include " + "; ".join(
                f"{row['thread_month']} ({row['role_count']} roles)" for row in top
            ) + "."
    if helper == "companies-for-role-postgres":
        companies = answer.get("companies", [])
        if companies:
            top = companies[:5]
            return "Companies matched include " + ", ".join(
                f"{row['company_name']} ({row['matched_role_count']} roles)" for row in top
            ) + "."
    if helper == "role-requirement-change-summary-postgres":
        query = str(answer.get("filters", {}).get("query") or "the selected role")
        early_months = answer.get("early_months", [])
        late_months = answer.get("late_months", [])
        early_label = f"{early_months[0]} to {early_months[-1]}" if early_months else "the earlier window"
        late_label = f"{late_months[0]} to {late_months[-1]}" if late_months else "the later window"
        points = answer.get("summary_points", [])
        emerging_terms: list[str] = []
        fading_terms: list[str] = []
        concept_gains: list[str] = []
        for point in points:
            match = re.match(r"Later windows emphasize terms like (.+)\.$", str(point))
            if match:
                emerging_terms = [part.strip() for part in match.group(1).split(",") if part.strip()]
                continue
            match = re.match(r"Earlier windows leaned more on (.+)\.$", str(point))
            if match:
                fading_terms = [part.strip() for part in match.group(1).split(",") if part.strip()]
                continue
            match = re.match(r"AI-related emphasis increased for concepts such as (.+)\.$", str(point))
            if match:
                concept_gains = [part.strip() for part in match.group(1).split(",") if part.strip()]
        clauses: list[str] = [f"For {query}, posts shifted between {early_label} and {late_label}."]
        if emerging_terms:
            clauses.append(f"Later posts emphasized {', '.join(emerging_terms[:3])}.")
        if fading_terms:
            clauses.append(f"Earlier posts leaned more on {', '.join(fading_terms[:3])}.")
        if concept_gains:
            clauses.append(f"AI emphasis rose around {', '.join(concept_gains[:3])}.")
        if len(clauses) == 1:
            clauses.append("Requirement language stayed broadly stable across the selected window.")
        summary = " ".join(clauses)
        words = summary.split()
        return " ".join(words[:50]) + ("..." if len(words) > 50 else "")
    if helper == "ai-concept-timeline-postgres":
        if rows:
            concept_name = rows[0].get("concept_name")
            top = sorted(rows, key=lambda row: int(row.get("mentioning_post_count", 0)), reverse=True)[:4]
            return f"{concept_name} appeared most strongly in " + "; ".join(
                f"{row['thread_month']} ({row['mentioning_post_count']} posts)" for row in top
            ) + "."
    if helper == "global-remote-share-postgres":
        return " ".join(
            f"{row['year']}: {row['global_remote_share_pct']}% of remote roles looked global-remote."
            for row in rows[:4]
        )
    if helper == "compensation-history-postgres":
        if rows:
            top = rows[:4]
            return "Compensation evidence includes " + "; ".join(
                f"{row['company_name']} {row['thread_month']} ({row['compensation_text']})" for row in top
            ) + "."
    if helper == "company-theme-history-postgres":
        if rows and "building_theme" in rows[0]:
            top = rows[:3]
            return "Top theme rows returned for the selected window: " + "; ".join(
                f"{row['thread_month']} {row['building_theme']} ({row['hiring_post_count']} posts)" for row in top
            )
        if rows:
            top = rows[:3]
            return "Largest theme shifts: " + "; ".join(
                f"{row['company_name']} emerged into {', '.join(row['emerging_themes']) or 'no clear emerging themes'}"
                for row in top
            )
    if helper == "company-change-summary-postgres":
        if rows:
            top = rows[:3]
            return "Most changed companies in the selected window: " + "; ".join(
                f"{row['company_name']} (score {row['changed_score']})" for row in top
            )
    if helper == "evidence-lookup-postgres":
        if rows:
            top = rows[:3]
            return "Matched evidence includes " + "; ".join(
                f"{row.get('thread_month', '?')} {row.get('company_name', '[unknown]')}" for row in top
            ) + "."
    if helper == "post-shape-summary-postgres":
        if rows:
            return "Year-by-year post lengths: " + "; ".join(
                f"{row['year']} median {int(float(row['median_post_length_chars']))} chars"
                for row in rows[:4]
            ) + "."
    if helper == "company-post-length-consistency-postgres":
        if rows:
            top = rows[:5]
            return "Most length-consistent companies include " + ", ".join(
                f"{row['company_name']} (stddev {row['stddev_post_length_chars']})" for row in top
            ) + "."
    if helper == "company-activity-postgres":
        months = answer.get("months", [])
        company_name = answer.get("company_name")
        return f"{company_name} posted in {len(months)} months across the selected window."
    if helper == "company-role-presence-postgres":
        if answer.get("match_found"):
            months = ", ".join(answer.get("matched_months", [])[:6])
            return f"Yes. Matched months include {months}."
        return "No matched evidence was found in the selected window."
    return f"Routed to {helper} and returned {answer.get('row_count', 'some')} rows."


def answer_nl_question_postgres(
    question: str,
    *,
    database_url: str | None = None,
    schema: str = "yc_hiring",
    limit: int = 10,
    limit_evidence: int = 5,
) -> dict[str, Any]:
    parsed = parse_nl_question(question)
    if parsed.question_id == 43 and (parsed.query is None or "change from" in parsed.query.lower()):
        parsed = ParsedQuestion(
            question_id=parsed.question_id,
            confidence=parsed.confidence,
            candidates=parsed.candidates,
            company_name=parsed.company_name,
            query="AI Engineer",
            role_family=parsed.role_family,
            role_family_a=parsed.role_family_a,
            role_family_b=parsed.role_family_b,
            concept_name=parsed.concept_name,
            month_from=parsed.month_from,
            month_to=parsed.month_to,
            year=parsed.year,
            mode=parsed.mode,
            requires_clarification=parsed.requires_clarification,
            clarification_reason=parsed.clarification_reason,
        )
    if parsed.question_id is None or parsed.requires_clarification:
        return {
            "status": "clarification_needed",
            "question": question,
            "reason": parsed.clarification_reason,
            "parsed": parsed.__dict__,
        }

    routed = answer_catalog_question_postgres(
        question_id=parsed.question_id,
        database_url=database_url,
        schema=schema,
        company_name=parsed.company_name,
        query=parsed.query,
        role_family=parsed.role_family,
        role_family_a=parsed.role_family_a,
        role_family_b=parsed.role_family_b,
        concept_name=parsed.concept_name,
        month_from=parsed.month_from,
        month_to=parsed.month_to,
        year=parsed.year,
        mode=parsed.mode,
        limit=limit,
        limit_evidence=limit_evidence,
    )
    return {
        "status": "answered",
        "question": question,
        "parsed": parsed.__dict__,
        "routed": routed,
        "summary": summarize_answer(routed),
    }
