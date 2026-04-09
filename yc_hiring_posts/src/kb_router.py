"""Structured routing layer over PostgreSQL KB helpers."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from question_catalog import question_catalog_json_path
from postgres_kb import (
    ai_concept_timeline_postgres,
    companies_every_month_postgres,
    companies_for_role_postgres,
    companies_with_role_family_pair_postgres,
    company_activity_timeline_postgres,
    company_change_summary_postgres,
    company_post_length_consistency_postgres,
    company_role_presence_postgres,
    company_theme_history_postgres,
    compensation_history_postgres,
    evidence_lookup_postgres,
    global_remote_share_postgres,
    month_summary_postgres,
    post_shape_summary_postgres,
    remote_first_companies_postgres,
    remote_mix_postgres,
    role_family_timeline_postgres,
    role_requirement_change_summary_postgres,
    search_posts_postgres,
)


@dataclass(frozen=True)
class CatalogQuestion:
    question_id: int
    question_text: str
    question_family: str
    primary_helper: str
    secondary_helper: str | None


def load_question_catalog(path: Path | None = None) -> dict[int, CatalogQuestion]:
    """Load the question catalog keyed by question id."""

    target = path or question_catalog_json_path()
    rows = json.loads(target.read_text(encoding="utf-8"))
    return {
        int(row["question_id"]): CatalogQuestion(
            question_id=int(row["question_id"]),
            question_text=str(row["question_text"]),
            question_family=str(row["question_family"]),
            primary_helper=str(row["primary_helper"]),
            secondary_helper=None if row.get("secondary_helper") is None else str(row["secondary_helper"]),
        )
        for row in rows
    }


def answer_catalog_question_postgres(
    *,
    question_id: int,
    database_url: str | None = None,
    schema: str = "yc_hiring",
    company_name: str | None = None,
    query: str | None = None,
    role_family: str | None = None,
    role_family_a: str | None = None,
    role_family_b: str | None = None,
    concept_name: str | None = None,
    month_from: str | None = None,
    month_to: str | None = None,
    year: int | None = None,
    mode: str | None = None,
    limit: int = 10,
    limit_evidence: int = 5,
) -> dict[str, Any]:
    """Route one catalog question id to the best current KB helper/composition."""

    catalog = load_question_catalog()
    question = catalog.get(question_id)
    if question is None:
        raise ValueError(f"Unknown question_id: {question_id}")

    routed_helper: str
    answer: dict[str, Any]

    if question_id == 1:
        routed_helper = "company_activity_postgres"
        answer = company_activity_timeline_postgres(
            database_url=database_url, schema=schema, company_name=company_name or "DuckDuckGo", month_from=month_from or "2023-01", month_to=month_to or "2026-04", limit_evidence=limit_evidence
        )
    elif question_id == 2:
        routed_helper = "company_role_presence_postgres"
        answer = company_role_presence_postgres(
            database_url=database_url, schema=schema, company_name=company_name or "DuckDuckGo", query=query or "data science", month_from=month_from or "2024-12", month_to=month_to or "2026-01", limit_evidence=limit_evidence
        )
    elif question_id == 3:
        routed_helper = "companies-every-month-postgres"
        answer = companies_every_month_postgres(database_url=database_url, schema=schema, year=year or 2025)
    elif question_id in {4, 5}:
        routed_helper = "company-activity-postgres"
        answer = {
            "entity": "composed_company_activity_ranking",
            "schema": schema,
            "note": "Requires secondary ranking/composition logic over company activity; raw helper coverage exists but no dedicated ranking helper yet.",
            "suggested_helper": "company_activity_postgres",
        }
    elif question_id in {6, 7}:
        routed_helper = "month-summary-postgres"
        answer = month_summary_postgres(database_url=database_url, schema=schema, month_from=month_from, month_to=month_to)
    elif question_id == 8:
        routed_helper = "remote-mix-postgres"
        answer = remote_mix_postgres(database_url=database_url, schema=schema, company_name=company_name, month_from=month_from or "2023-01", month_to=month_to or "2026-04")
    elif question_id == 9:
        routed_helper = "remote-first-companies-postgres"
        answer = remote_first_companies_postgres(database_url=database_url, schema=schema, year=year or 2025, min_posts=2)
    elif question_id == 10:
        routed_helper = "company-remote-change-postgres"
        from postgres_kb import company_remote_change_postgres
        answer = company_remote_change_postgres(database_url=database_url, schema=schema, month_from=month_from or "2023-01", month_to=month_to or "2026-04", limit=limit)
    elif question_id == 11:
        routed_helper = "search-postgres-posts"
        answer = search_posts_postgres(database_url=database_url, schema=schema, query=query or '"visa sponsorship" OR sponsor', month_from=month_from, month_to=month_to, limit=limit)
    elif question_id == 12:
        routed_helper = "compensation-history-postgres"
        answer = compensation_history_postgres(database_url=database_url, schema=schema, company_name=company_name, query=query, month_from=month_from, month_to=month_to, limit=limit)
    elif question_id in {13, 18, 19}:
        routed_helper = "company-change-summary-postgres"
        answer = company_change_summary_postgres(database_url=database_url, schema=schema, company_name=company_name, month_from=month_from, month_to=month_to, mode=mode or "most_changed", limit=limit)
    elif question_id == 14:
        routed_helper = "company-change-summary-postgres"
        answer = company_change_summary_postgres(database_url=database_url, schema=schema, company_name=company_name, month_from=month_from, month_to=month_to, mode=mode or "least_changed", limit=limit)
    elif question_id == 20:
        routed_helper = "company-change-summary-postgres"
        answer = company_change_summary_postgres(database_url=database_url, schema=schema, company_name=company_name, month_from=month_from, month_to=month_to, mode=mode or "pivot_return", limit=limit)
    elif question_id in {21, 22, 27, 32}:
        routed_helper = "role-family-timeline-postgres"
        answer = role_family_timeline_postgres(database_url=database_url, schema=schema, role_family=role_family, month_from=month_from, month_to=month_to)
    elif question_id in {23, 24, 25, 28}:
        routed_helper = "companies-for-role-postgres"
        answer = companies_for_role_postgres(database_url=database_url, schema=schema, query=query, role_family=role_family, remote_status=None, month_from=month_from, month_to=month_to, limit_evidence=limit_evidence)
    elif question_id == 26:
        routed_helper = "companies-role-pair-postgres"
        answer = companies_with_role_family_pair_postgres(database_url=database_url, schema=schema, role_family_a=role_family_a or "engineering", role_family_b=role_family_b or "ml_ai", month_from=month_from, month_to=month_to, limit=limit)
    elif question_id in {29, 41}:
        routed_helper = "search-postgres-posts"
        answer = search_posts_postgres(database_url=database_url, schema=schema, query=query, company_name=company_name, month_from=month_from, month_to=month_to, limit=limit)
    elif question_id in {30, 33}:
        routed_helper = "ai-concept-timeline-postgres"
        answer = ai_concept_timeline_postgres(database_url=database_url, schema=schema, concept_name=concept_name, month_from=month_from, month_to=month_to, limit_evidence=limit_evidence)
    elif question_id == 31:
        routed_helper = "ai-concept-timeline-postgres"
        answer = ai_concept_timeline_postgres(database_url=database_url, schema=schema, concept_name=concept_name or "agents", month_from=month_from or "2023-01", month_to=month_to or "2026-04", limit_evidence=limit_evidence)
    elif question_id == 34:
        routed_helper = "company-change-summary-postgres"
        answer = company_change_summary_postgres(database_url=database_url, schema=schema, company_name=company_name, month_from=month_from, month_to=month_to, mode=mode or "most_changed", limit=limit)
    elif question_id == 35:
        routed_helper = "company-theme-history-postgres"
        answer = company_theme_history_postgres(database_url=database_url, schema=schema, company_name=company_name, month_from=month_from or "2023-01", month_to=month_to or "2026-04", mode=mode or "timeline", limit=limit)
    elif question_id == 36:
        routed_helper = "company-theme-history-postgres"
        answer = company_theme_history_postgres(database_url=database_url, schema=schema, company_name=company_name, month_from=month_from or "2023-01", month_to=month_to or "2026-04", mode=mode or "shift_summary", limit=limit)
    elif question_id in {37, 38, 42, 50}:
        routed_helper = "compensation-history-postgres"
        answer = compensation_history_postgres(database_url=database_url, schema=schema, company_name=company_name, query=query, month_from=month_from, month_to=month_to, limit=limit)
    elif question_id in {39, 40}:
        routed_helper = "evidence-lookup-postgres"
        answer = evidence_lookup_postgres(database_url=database_url, schema=schema, query=query or "privacy browser search", month_from=month_from, month_to=month_to, limit=limit)
    elif question_id == 43:
        routed_helper = "role-requirement-change-summary-postgres"
        answer = role_requirement_change_summary_postgres(database_url=database_url, schema=schema, query=query or "AI Engineer", month_from=month_from or "2024-01", month_to=month_to or "2026-04", limit_evidence=limit_evidence)
    elif question_id == 44:
        routed_helper = "company-role-presence-postgres"
        answer = company_role_presence_postgres(database_url=database_url, schema=schema, company_name=company_name, query=query or "ML OR AI", role_family=role_family or "ml_ai", month_from=month_from or "2023-01", month_to=month_to or "2026-04", limit_evidence=limit_evidence)
    elif question_id == 45:
        routed_helper = "unavailable_company_geography"
        answer = {
            "entity": "unavailable",
            "reason": "Reliable company-headquarters geography is not present in the current normalized schema.",
            "question_id": question_id,
        }
    elif question_id == 46:
        routed_helper = "global-remote-share-postgres"
        answer = global_remote_share_postgres(database_url=database_url, schema=schema, month_from=month_from, month_to=month_to)
    elif question_id == 47:
        routed_helper = "remote-mix-postgres"
        answer = remote_mix_postgres(database_url=database_url, schema=schema, company_name=company_name, month_from=month_from, month_to=month_to)
    elif question_id == 48:
        routed_helper = "post-shape-summary-postgres"
        answer = post_shape_summary_postgres(database_url=database_url, schema=schema, month_from=month_from, month_to=month_to)
    elif question_id == 49:
        routed_helper = "company-post-length-consistency-postgres"
        answer = company_post_length_consistency_postgres(database_url=database_url, schema=schema, month_from=month_from, month_to=month_to, min_posts=3, limit=limit)
    else:
        raise ValueError(f"Question {question_id} is not yet routed.")

    return {
        "question_id": question.question_id,
        "question_text": question.question_text,
        "question_family": question.question_family,
        "routed_helper": routed_helper,
        "answer": answer,
    }
