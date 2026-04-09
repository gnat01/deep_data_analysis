"""Recurring analytical outputs built from the processed V1 core tables."""

from __future__ import annotations

import csv
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

from storage import ensure_processed_dir, processed_data_dir


AI_CONCEPT_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("gpt_llm", (r"\bgpt-?4\b", r"\bgpt-?3(\.5)?\b", r"\bchatgpt\b", r"\bllms?\b", r"\blarge language model(s)?\b")),
    ("generative_ai", (r"generative ai", r"\bgenai\b", r"\bfoundation model(s)?\b")),
    ("transformers", (r"\btransformers?\b", r"\bbert\b", r"\blangchain\b")),
    ("embeddings_vector_db", (r"\bembeddings?\b", r"vector database", r"vector db", r"vector search")),
    ("agents", (r"\bagentic\b", r"\bagents?\b", r"\bmulti-agent\b", r"\bagent framework\b")),
    ("mcp", (r"\bmcp\b", r"model context protocol")),
    (
        "agent_tooling",
        (
            r"\btool use\b",
            r"\btooling\b",
            r"\bfunction calling\b",
            r"\btool calling\b",
            r"\btoolformer\b",
            r"\bagent orchestration\b",
            r"\borchestration layer\b",
            r"\bagent workflow(s)?\b",
            r"\bworkflow engine\b",
        ),
    ),
    ("rag", (r"\brag\b", r"retrieval augmented")),
    ("evals", (r"\bevals?\b", r"\bevaluation(s)?\b")),
    ("fine_tuning", (r"fine[- ]?tuning", r"fine[- ]?tun")),
    ("prompting", (r"\bprompt(s|ing)?\b",)),
    ("reasoning", (r"\breasoning\b",)),
    ("inference", (r"\binference\b", r"\bmodel serving\b")),
]

PRODUCT_THEME_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("ai_ml", (r"\bai\b", r"\bml\b", r"\bllms?\b", r"generative ai", r"\bmodel(s)?\b", r"\brag\b", r"\bagents?\b")),
    ("developer_tools", (r"developer tool", r"\bdevops\b", r"\bapi\b", r"\bsdk\b", r"\bplatform engineering\b", r"\bobservability\b", r"\bci/cd\b", r"\binfrastructure\b")),
    ("data_infra", (r"\bdata platform\b", r"\bwarehouse\b", r"\betl\b", r"\banalytics\b", r"\bdatabase\b", r"\bvector db\b", r"\bvector database\b")),
    ("security_identity", (r"\bsecurity\b", r"\bcybersecurity\b", r"\bidentity\b", r"\bauth\b", r"\bauthentication\b", r"\bcompliance\b", r"\bthreat\b")),
    ("fintech", (r"\bpayments?\b", r"\bfintech\b", r"\bbanking\b", r"\binsurance\b", r"\bpayroll\b", r"\btax\b", r"\baccounting\b", r"\bcrypto\b")),
    ("health_bio", (r"\bhealthcare\b", r"\bmedical\b", r"\bclinical\b", r"\bpatient\b", r"\bbiotech\b", r"\bpharma\b", r"\bgenomics\b")),
    ("climate_energy", (r"\bclimate\b", r"\benergy\b", r"\bsolar\b", r"\bgrid\b", r"\bbattery\b", r"\bcarbon\b")),
    ("robotics_hardware", (r"\brobotics\b", r"\bhardware\b", r"\bembedded\b", r"\bsensors?\b", r"\bdrone\b", r"\bsemiconductor\b", r"\bmanufacturing\b")),
    ("commerce_logistics", (r"\be-?commerce\b", r"\bmarketplace\b", r"\blogistics\b", r"\bsupply chain\b", r"\bfreight\b", r"\bretail\b")),
]

MONTH_ABBREVIATIONS = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def materialize_core_analytics() -> dict[str, Path]:
    """Write the core Step 16 analytical outputs."""

    processed_dir = ensure_processed_dir()
    analytics_dir = processed_dir / "analytics"
    analytics_dir.mkdir(parents=True, exist_ok=True)
    visuals_dir = analytics_dir / "visuals"
    visuals_dir.mkdir(parents=True, exist_ok=True)

    tables_dir = processed_dir / "v1_core_tables"
    posts = load_jsonl(tables_dir / "posts.jsonl")
    roles = load_jsonl(tables_dir / "roles.jsonl")
    companies = load_jsonl(tables_dir / "companies.jsonl")
    threads = load_jsonl(tables_dir / "threads.jsonl")

    company_name_by_id = {row["company_id"]: row["company_name_observed_preferred"] for row in companies}
    month_by_thread_id = {row["thread_id"]: row["thread_month"] for row in threads}

    company_posting_rows = company_posting_counts_by_month(posts, company_name_by_id, month_by_thread_id)
    company_summary_rows = company_summary_by_month(posts, month_by_thread_id)
    remote_rows = remote_status_trends_by_month(posts, month_by_thread_id)
    remote_share_rows = remote_status_share_by_month(remote_rows)
    role_family_rows = role_family_trends_by_month(roles, posts, company_name_by_id, month_by_thread_id)
    distinct_role_rows = distinct_roles_by_month(roles, posts, month_by_thread_id)
    ai_concept_rows = ai_concepts_by_month(posts, roles, month_by_thread_id)
    ai_concept_role_family_rows = ai_concepts_by_role_family(roles, month_by_thread_id)
    product_theme_rows = company_building_themes_by_month(posts, company_name_by_id, month_by_thread_id)
    recurring_rows = recurring_company_hiring_patterns(posts, company_name_by_id, month_by_thread_id)
    company_semantic_rows = company_semantic_spread(posts, company_name_by_id, month_by_thread_id)
    company_role_rows = company_role_semantic_spread(roles, posts, company_name_by_id, month_by_thread_id)
    post_vs_role_rows = company_post_vs_role_spread(company_semantic_rows, company_role_rows)
    windowed_post_vs_role_rows = company_post_vs_role_spread_windowed(posts, roles, company_name_by_id, month_by_thread_id)
    company_drift_rows, company_drift_monthly_rows = company_embedding_drift(posts, company_name_by_id, month_by_thread_id)
    changed_company_rows = changed_companies_ranked(post_vs_role_rows, company_drift_rows)

    outputs = {
        "company_posting_counts_by_month": write_csv(
            analytics_dir / "company_posting_counts_by_month.csv",
            company_posting_rows,
        ),
        "company_summary_by_month": write_csv(
            analytics_dir / "company_summary_by_month.csv",
            company_summary_rows,
        ),
        "remote_status_trends_by_month": write_csv(
            analytics_dir / "remote_status_trends_by_month.csv",
            remote_rows,
        ),
        "remote_status_share_by_month": write_csv(
            analytics_dir / "remote_status_share_by_month.csv",
            remote_share_rows,
        ),
        "role_family_trends_by_month": write_csv(
            analytics_dir / "role_family_trends_by_month.csv",
            role_family_rows,
        ),
        "distinct_roles_by_month": write_csv(
            analytics_dir / "distinct_roles_by_month.csv",
            distinct_role_rows,
        ),
        "ai_concepts_by_month": write_csv(
            analytics_dir / "ai_concepts_by_month.csv",
            ai_concept_rows,
        ),
        "ai_concepts_by_role_family": write_csv(
            analytics_dir / "ai_concepts_by_role_family.csv",
            ai_concept_role_family_rows,
        ),
        "company_building_themes_by_month": write_csv(
            analytics_dir / "company_building_themes_by_month.csv",
            product_theme_rows,
        ),
        "recurring_company_hiring_patterns": write_csv(
            analytics_dir / "recurring_company_hiring_patterns.csv",
            recurring_rows,
        ),
        "company_semantic_spread": write_csv(
            analytics_dir / "company_semantic_spread.csv",
            company_semantic_rows,
        ),
        "company_role_semantic_spread": write_csv(
            analytics_dir / "company_role_semantic_spread.csv",
            company_role_rows,
        ),
        "company_post_vs_role_spread": write_csv(
            analytics_dir / "company_post_vs_role_spread.csv",
            post_vs_role_rows,
        ),
        "company_post_vs_role_spread_6m": write_csv(
            analytics_dir / "company_post_vs_role_spread_6m.csv",
            windowed_post_vs_role_rows,
        ),
        "company_embedding_drift": write_csv(
            analytics_dir / "company_embedding_drift.csv",
            company_drift_rows,
        ),
        "company_embedding_drift_monthly": write_csv(
            analytics_dir / "company_embedding_drift_monthly.csv",
            company_drift_monthly_rows,
        ),
        "changed_companies_ranked": write_csv(
            analytics_dir / "changed_companies_ranked.csv",
            changed_company_rows,
        ),
    }
    visual_outputs = write_analytics_visuals(
        visuals_dir=visuals_dir,
        posts=posts,
        company_name_by_id=company_name_by_id,
        month_by_thread_id=month_by_thread_id,
        company_posting_rows=company_posting_rows,
        company_summary_rows=company_summary_rows,
        remote_rows=remote_rows,
        remote_share_rows=remote_share_rows,
        role_family_rows=role_family_rows,
        distinct_role_rows=distinct_role_rows,
        ai_concept_rows=ai_concept_rows,
        ai_concept_role_family_rows=ai_concept_role_family_rows,
        product_theme_rows=product_theme_rows,
        recurring_rows=recurring_rows,
        company_semantic_rows=company_semantic_rows,
        company_role_rows=company_role_rows,
        post_vs_role_rows=post_vs_role_rows,
        windowed_post_vs_role_rows=windowed_post_vs_role_rows,
        company_drift_rows=company_drift_rows,
        company_drift_monthly_rows=company_drift_monthly_rows,
        changed_company_rows=changed_company_rows,
    )
    outputs.update(visual_outputs)
    outputs["manifest"] = write_manifest(analytics_dir / "analytics_manifest.json", outputs)
    return outputs


def company_posting_counts_by_month(
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
) -> list[dict[str, object]]:
    """Aggregate hiring-post counts by company and month."""

    counts: dict[tuple[str, str | None], int] = defaultdict(int)
    observed_names: dict[tuple[str, str | None], Counter[str]] = defaultdict(Counter)
    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        thread_id = str(post["thread_id"])
        month = month_by_thread_id[thread_id]
        company_id = post.get("company_id")
        counts[(month, company_id)] += 1
        name = post.get("company_name_observed")
        if isinstance(name, str) and name:
            observed_names[(month, company_id)][name] += 1
    rows = []
    for (month, company_id), hiring_post_count in sorted(
        counts.items(),
        key=lambda item: (item[0][0], item[0][1] or ""),
    ):
        preferred_name = company_name_by_id.get(company_id) if company_id else None
        fallback_name = observed_names[(month, company_id)].most_common(1)[0][0] if observed_names[(month, company_id)] else None
        rows.append(
            {
                "thread_month": month,
                "company_id": company_id,
                "company_name": preferred_name or fallback_name,
                "hiring_post_count": hiring_post_count,
            }
        )
    return rows


def remote_status_trends_by_month(posts: list[dict[str, object]], month_by_thread_id: dict[str, str]) -> list[dict[str, object]]:
    """Aggregate hiring-post counts by remote status and month."""

    counts: dict[tuple[str, str], int] = defaultdict(int)
    total_counts: Counter[str] = Counter()
    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        month = month_by_thread_id[str(post["thread_id"])]
        status = str(post.get("remote_status") or "unspecified")
        counts[(month, status)] += 1
        total_counts[month] += 1
    rows = []
    for (month, status), count in sorted(counts.items()):
        rows.append(
            {
                "thread_month": month,
                "remote_status": status,
                "hiring_post_count": count,
                "month_total_hiring_posts": total_counts[month],
            }
        )
    return rows


def remote_status_share_by_month(remote_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Convert remote-status counts to monthly percentages."""

    rows = []
    for row in remote_rows:
        month_total = int(row["month_total_hiring_posts"])
        count = int(row["hiring_post_count"])
        share = 0.0 if month_total == 0 else round((count / month_total) * 100.0, 2)
        rows.append(
            {
                "thread_month": row["thread_month"],
                "remote_status": row["remote_status"],
                "hiring_post_count": count,
                "month_total_hiring_posts": month_total,
                "share_pct": share,
            }
        )
    return rows


def company_summary_by_month(posts: list[dict[str, object]], month_by_thread_id: dict[str, str]) -> list[dict[str, object]]:
    """Summarize company participation by month."""

    company_ids_by_month: dict[str, set[str]] = defaultdict(set)
    observed_names_by_month: dict[str, set[str]] = defaultdict(set)
    total_hiring_posts: Counter[str] = Counter()

    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        month = month_by_thread_id[str(post["thread_id"])]
        total_hiring_posts[month] += 1
        company_id = post.get("company_id")
        if company_id is not None:
            company_ids_by_month[month].add(str(company_id))
        observed_name = post.get("company_name_observed")
        if isinstance(observed_name, str) and observed_name.strip():
            observed_names_by_month[month].add(observed_name.strip())

    rows = []
    for month in sorted(total_hiring_posts):
        rows.append(
            {
                "thread_month": month,
                "company_count": len(company_ids_by_month[month]),
                "observed_company_name_count": len(observed_names_by_month[month]),
                "total_hiring_posts": total_hiring_posts[month],
            }
        )
    return rows


def role_family_trends_by_month(
    roles: list[dict[str, object]],
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
) -> list[dict[str, object]]:
    """Aggregate role-family counts by month."""

    thread_id_by_post_id = {row["post_id"]: row["thread_id"] for row in posts}
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for role in roles:
        role_family = str(role.get("role_family") or "unknown")
        post_id = str(role["post_id"])
        thread_id = thread_id_by_post_id[post_id]
        month = month_by_thread_id[str(thread_id)]
        counts[(month, role_family)] += 1
    rows = []
    for (month, role_family), role_count in sorted(counts.items()):
        rows.append(
            {
                "thread_month": month,
                "role_family": role_family,
                "role_count": role_count,
            }
        )
    return rows


def distinct_roles_by_month(
    roles: list[dict[str, object]],
    posts: list[dict[str, object]],
    month_by_thread_id: dict[str, str],
) -> list[dict[str, object]]:
    """Summarize distinct role titles by month."""

    thread_id_by_post_id = {str(row["post_id"]): str(row["thread_id"]) for row in posts if row.get("is_hiring_post")}
    normalized_roles_by_month: dict[str, set[str]] = defaultdict(set)
    observed_roles_by_month: dict[str, set[str]] = defaultdict(set)
    total_role_rows: Counter[str] = Counter()

    for role in roles:
        post_id = str(role["post_id"])
        thread_id = thread_id_by_post_id.get(post_id)
        if thread_id is None:
            continue
        month = month_by_thread_id[thread_id]
        total_role_rows[month] += 1

        normalized_title = role.get("role_title_normalized")
        if isinstance(normalized_title, str) and normalized_title.strip():
            normalized_roles_by_month[month].add(normalized_title.strip())

        observed_title = role.get("role_title_observed")
        if isinstance(observed_title, str) and observed_title.strip():
            observed_roles_by_month[month].add(observed_title.strip())

    rows = []
    for month in sorted(total_role_rows):
        rows.append(
            {
                "thread_month": month,
                "distinct_role_count": len(normalized_roles_by_month[month]),
                "distinct_observed_role_count": len(observed_roles_by_month[month]),
                "total_role_rows": total_role_rows[month],
            }
        )
    return rows


def ai_concepts_by_month(
    posts: list[dict[str, object]],
    roles: list[dict[str, object]],
    month_by_thread_id: dict[str, str],
) -> list[dict[str, object]]:
    """Track AI capability concepts over time from post and role text."""

    role_texts_by_post_id: dict[str, list[str]] = defaultdict(list)
    for role in roles:
        post_id = str(role["post_id"])
        fragments = [
            role.get("role_title_observed"),
            role.get("role_title_normalized"),
            role.get("skills_text"),
            role.get("requirements_text"),
            role.get("responsibilities_text"),
        ]
        role_texts_by_post_id[post_id].extend(fragment for fragment in fragments if isinstance(fragment, str) and fragment.strip())

    month_totals: Counter[str] = Counter()
    concept_counts: dict[tuple[str, str], int] = defaultdict(int)

    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        post_id = str(post["post_id"])
        thread_id = str(post["thread_id"])
        month = month_by_thread_id[thread_id]
        month_totals[month] += 1
        combined_text = "\n".join(
            [
                str(post.get("post_text_clean") or ""),
                *role_texts_by_post_id.get(post_id, []),
            ]
        ).lower()
        for concept_name, patterns in AI_CONCEPT_PATTERNS:
            if any(re.search(pattern, combined_text, flags=re.IGNORECASE) for pattern in patterns):
                concept_counts[(month, concept_name)] += 1

    rows = []
    for (month, concept_name), mentioning_post_count in sorted(concept_counts.items()):
        total = month_totals[month]
        rows.append(
            {
                "thread_month": month,
                "concept_name": concept_name,
                "mentioning_post_count": mentioning_post_count,
                "month_total_hiring_posts": total,
                "mention_share_pct": round((mentioning_post_count / total) * 100.0, 2) if total else 0.0,
            }
        )
    return rows


def ai_concepts_by_role_family(
    roles: list[dict[str, object]],
    month_by_thread_id: dict[str, str],
) -> list[dict[str, object]]:
    """Track AI concepts within extracted role-family text."""

    counts: dict[tuple[str, str], int] = defaultdict(int)
    role_family_totals: Counter[str] = Counter()
    for role in roles:
        role_family = str(role.get("role_family") or "unknown")
        role_family_totals[role_family] += 1
        text = "\n".join(
            [
                str(role.get("role_title_observed") or ""),
                str(role.get("role_title_normalized") or ""),
                str(role.get("skills_text") or ""),
                str(role.get("requirements_text") or ""),
                str(role.get("responsibilities_text") or ""),
            ]
        ).lower()
        for concept_name, patterns in AI_CONCEPT_PATTERNS:
            if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
                counts[(role_family, concept_name)] += 1
    rows = []
    for (role_family, concept_name), role_count in sorted(counts.items()):
        total = role_family_totals[role_family]
        rows.append(
            {
                "role_family": role_family,
                "concept_name": concept_name,
                "role_count": role_count,
                "role_family_total_roles": total,
                "role_share_pct": round((role_count / total) * 100.0, 2) if total else 0.0,
            }
        )
    return rows


def company_building_themes_by_month(
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
) -> list[dict[str, object]]:
    """Track broad product themes inferred from company hiring-post text."""

    company_ids_by_theme_month: dict[tuple[str, str], set[str]] = defaultdict(set)
    observed_names_by_theme_month: dict[tuple[str, str], set[str]] = defaultdict(set)
    hiring_posts_by_theme_month: Counter[tuple[str, str]] = Counter()

    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        month = month_by_thread_id[str(post["thread_id"])]
        text = str(post.get("post_text_clean") or "").lower()
        company_id = post.get("company_id")
        observed_name = post.get("company_name_observed")
        for theme_name, patterns in PRODUCT_THEME_PATTERNS:
            if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns):
                key = (month, theme_name)
                hiring_posts_by_theme_month[key] += 1
                if company_id is not None:
                    company_ids_by_theme_month[key].add(str(company_id))
                if isinstance(observed_name, str) and observed_name.strip():
                    observed_names_by_theme_month[key].add(observed_name.strip())

    rows = []
    for (month, theme_name), hiring_post_count in sorted(hiring_posts_by_theme_month.items()):
        rows.append(
            {
                "thread_month": month,
                "building_theme": theme_name,
                "company_count": len(company_ids_by_theme_month[(month, theme_name)]),
                "observed_company_count": len(observed_names_by_theme_month[(month, theme_name)]),
                "hiring_post_count": hiring_post_count,
            }
        )
    return rows


def recurring_company_hiring_patterns(
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
) -> list[dict[str, object]]:
    """Summarize recurring company activity across months."""

    active_months_by_company: dict[str, set[str]] = defaultdict(set)
    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        company_id = post.get("company_id")
        if company_id is None:
            continue
        month = month_by_thread_id[str(post["thread_id"])]
        active_months_by_company[str(company_id)].add(month)
    rows = []
    for company_id, months in sorted(active_months_by_company.items()):
        sorted_months = sorted(months)
        rows.append(
            {
                "company_id": company_id,
                "company_name": company_name_by_id.get(company_id),
                "active_month_count": len(sorted_months),
                "first_seen_thread_month": sorted_months[0],
                "last_seen_thread_month": sorted_months[-1],
                "active_months": ",".join(sorted_months),
            }
        )
    rows.sort(key=lambda row: (-int(row["active_month_count"]), str(row["company_name"] or "")))
    return rows


def company_semantic_spread(
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
    top_n: int = 100,
) -> list[dict[str, object]]:
    """Rank companies by how semantically varied their hiring posts are."""

    grouped_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
    company_name_lookup: dict[str, str] = {}
    company_id_lookup: dict[str, str | None] = {}
    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        company_id = post.get("company_id")
        observed_name = str(post.get("company_name_observed") or "").strip()
        company_name = company_name_by_id.get(company_id) if company_id else None
        if not company_name:
            company_name = observed_name
        if not company_name:
            continue
        company_key = str(company_id) if company_id else f"observed:{company_name.lower()}"
        grouped_rows[company_key].append(
            {
                "post_id": str(post["post_id"]),
                "thread_id": str(post["thread_id"]),
                "thread_month": month_by_thread_id[str(post["thread_id"])],
                "text": str(post.get("post_text_clean") or "").strip(),
            }
        )
        company_name_lookup[company_key] = company_name
        company_id_lookup[company_key] = str(company_id) if company_id else None

    ranked_keys = sorted(
        grouped_rows,
        key=lambda key: (-len(grouped_rows[key]), company_name_lookup.get(key, key).lower()),
    )[:top_n]

    rows: list[dict[str, object]] = []
    for company_key in ranked_keys:
        company_posts = grouped_rows[company_key]
        texts = [row["text"] for row in company_posts if row["text"]]
        metrics = semantic_angle_metrics(texts)
        months = sorted({row["thread_month"] for row in company_posts})
        rows.append(
            {
                "company_key": company_key,
                "company_id": company_id_lookup.get(company_key),
                "company_name": company_name_lookup.get(company_key),
                "post_count": len(company_posts),
                "pair_count": metrics["pair_count"],
                "exact_reuse_share": metrics["exact_reuse_share"],
                "mean_pairwise_cosine": metrics["mean_pairwise_cosine"],
                "mean_pairwise_angle_deg": metrics["mean_pairwise_angle_deg"],
                "median_pairwise_angle_deg": metrics["median_pairwise_angle_deg"],
                "p90_pairwise_angle_deg": metrics["p90_pairwise_angle_deg"],
                "max_pairwise_angle_deg": metrics["max_pairwise_angle_deg"],
                "active_month_count": len(months),
                "first_month": months[0] if months else None,
                "last_month": months[-1] if months else None,
            }
        )
    return rows


def company_role_semantic_spread(
    roles: list[dict[str, object]],
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
    top_n: int = 100,
) -> list[dict[str, object]]:
    """Rank companies by semantic spread across extracted role text."""

    post_by_id = {str(post["post_id"]): post for post in posts if post.get("is_hiring_post")}
    grouped_rows: dict[str, list[str]] = defaultdict(list)
    company_name_lookup: dict[str, str] = {}
    company_id_lookup: dict[str, str | None] = {}
    month_sets: dict[str, set[str]] = defaultdict(set)

    for role in roles:
        post = post_by_id.get(str(role.get("post_id")))
        if post is None:
            continue
        company_id = role.get("company_id") or post.get("company_id")
        observed_name = str(post.get("company_name_observed") or "").strip()
        company_name = company_name_by_id.get(company_id) if company_id else None
        if not company_name:
            company_name = observed_name
        if not company_name:
            continue
        company_key = str(company_id) if company_id else f"observed:{company_name.lower()}"
        text = role_text_from_row(role)
        if not text.strip():
            continue
        grouped_rows[company_key].append(text)
        company_name_lookup[company_key] = company_name
        company_id_lookup[company_key] = str(company_id) if company_id else None
        month_sets[company_key].add(month_by_thread_id[str(post["thread_id"])])

    ranked_keys = sorted(
        grouped_rows,
        key=lambda key: (-len(grouped_rows[key]), company_name_lookup.get(key, key).lower()),
    )[:top_n]

    rows: list[dict[str, object]] = []
    for company_key in ranked_keys:
        texts = grouped_rows[company_key]
        metrics = semantic_angle_metrics(texts)
        months = sorted(month_sets.get(company_key, set()))
        rows.append(
            {
                "company_key": company_key,
                "company_id": company_id_lookup.get(company_key),
                "company_name": company_name_lookup.get(company_key),
                "role_count": len(texts),
                "role_pair_count": metrics["pair_count"],
                "role_exact_reuse_share": metrics["exact_reuse_share"],
                "role_mean_pairwise_cosine": metrics["mean_pairwise_cosine"],
                "role_mean_angle_deg": metrics["mean_pairwise_angle_deg"],
                "role_median_angle_deg": metrics["median_pairwise_angle_deg"],
                "role_p90_angle_deg": metrics["p90_pairwise_angle_deg"],
                "role_max_angle_deg": metrics["max_pairwise_angle_deg"],
                "active_month_count": len(months),
            }
        )
    return rows


def company_post_vs_role_spread(
    company_semantic_rows: list[dict[str, object]],
    company_role_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Join company post spread and role spread into one comparison table."""

    role_by_key = {str(row["company_key"]): row for row in company_role_rows}
    rows: list[dict[str, object]] = []
    for post_row in company_semantic_rows:
        company_key = str(post_row["company_key"])
        role_row = role_by_key.get(company_key)
        if role_row is None:
            continue
        post_mean = float(post_row["mean_pairwise_angle_deg"])
        role_mean = float(role_row["role_mean_angle_deg"])
        rows.append(
            {
                "company_key": company_key,
                "company_id": post_row.get("company_id"),
                "company_name": post_row.get("company_name"),
                "post_count": post_row.get("post_count"),
                "role_count": role_row.get("role_count"),
                "post_mean_angle_deg": post_mean,
                "role_mean_angle_deg": role_mean,
                "post_median_angle_deg": post_row.get("median_pairwise_angle_deg"),
                "role_median_angle_deg": role_row.get("role_median_angle_deg"),
                "post_p90_angle_deg": post_row.get("p90_pairwise_angle_deg"),
                "role_p90_angle_deg": role_row.get("role_p90_angle_deg"),
                "spread_ratio": round(post_mean / role_mean, 3) if role_mean else None,
                "post_exact_reuse_share": post_row.get("exact_reuse_share"),
                "role_exact_reuse_share": role_row.get("role_exact_reuse_share"),
            }
        )
    return sorted(rows, key=lambda row: (-float(row["post_mean_angle_deg"]), -(float(row["role_mean_angle_deg"]))))


def windowed_month_ranges(months: list[str], window_size_months: int = 6) -> list[dict[str, object]]:
    """Return non-overlapping chronological month windows."""

    ordered_months = sorted(set(months))
    windows: list[dict[str, object]] = []
    for index in range(0, len(ordered_months), window_size_months):
        chunk = ordered_months[index : index + window_size_months]
        if not chunk:
            continue
        windows.append(
            {
                "window_index": (index // window_size_months) + 1,
                "window_start_month": chunk[0],
                "window_end_month": chunk[-1],
                "window_label": f"{chunk[0]} to {chunk[-1]}",
                "months": chunk,
            }
        )
    return windows


def company_post_vs_role_spread_windowed(
    posts: list[dict[str, object]],
    roles: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
    window_size_months: int = 6,
    top_n: int | None = None,
) -> list[dict[str, object]]:
    """Compute company post-vs-role spread over non-overlapping time windows."""

    months = [month_by_thread_id[str(post["thread_id"])] for post in posts if post.get("is_hiring_post")]
    windows = windowed_month_ranges(months, window_size_months=window_size_months)
    rows: list[dict[str, object]] = []
    for window in windows:
        month_set = set(window["months"])
        window_posts = [post for post in posts if post.get("is_hiring_post") and month_by_thread_id[str(post["thread_id"])] in month_set]
        if not window_posts:
            continue
        allowed_post_ids = {str(post["post_id"]) for post in window_posts}
        window_roles = [role for role in roles if str(role["post_id"]) in allowed_post_ids]
        semantic_rows = company_semantic_spread(
            window_posts,
            company_name_by_id,
            month_by_thread_id,
            top_n=top_n or 100000,
        )
        role_rows = company_role_semantic_spread(
            window_roles,
            window_posts,
            company_name_by_id,
            month_by_thread_id,
            top_n=top_n or 100000,
        )
        comparison_rows = company_post_vs_role_spread(semantic_rows, role_rows)
        for row in comparison_rows:
            rows.append(
                {
                    "window_index": window["window_index"],
                    "window_start_month": window["window_start_month"],
                    "window_end_month": window["window_end_month"],
                    "window_label": window["window_label"],
                    **row,
                }
            )
    return sorted(rows, key=lambda row: (int(row["window_index"]), str(row["company_name"] or "")))


def company_embedding_drift(
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
    top_n: int = 80,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Measure month-by-month embedding drift for recurring companies."""

    grouped_posts: dict[str, list[dict[str, str]]] = defaultdict(list)
    company_name_lookup: dict[str, str] = {}
    company_id_lookup: dict[str, str | None] = {}
    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        company_id = post.get("company_id")
        observed_name = str(post.get("company_name_observed") or "").strip()
        company_name = company_name_by_id.get(company_id) if company_id else None
        if not company_name:
            company_name = observed_name
        if not company_name:
            continue
        company_key = str(company_id) if company_id else f"observed:{company_name.lower()}"
        grouped_posts[company_key].append(
            {
                "post_id": str(post["post_id"]),
                "thread_month": month_by_thread_id[str(post["thread_id"])],
                "text": str(post.get("post_text_clean") or "").strip(),
            }
        )
        company_name_lookup[company_key] = company_name
        company_id_lookup[company_key] = str(company_id) if company_id else None

    ranked_keys = sorted(
        grouped_posts,
        key=lambda key: (-len(grouped_posts[key]), company_name_lookup.get(key, key).lower()),
    )[:top_n]

    summary_rows: list[dict[str, object]] = []
    monthly_rows: list[dict[str, object]] = []
    for company_key in ranked_keys:
        company_posts = sorted(grouped_posts[company_key], key=lambda row: (row["thread_month"], row["post_id"]))
        texts = [row["text"] for row in company_posts if row["text"]]
        if len(texts) < 2:
            continue
        embedding_payload = company_projection_payload(texts)
        centroid_rows = company_month_centroid_rows(company_posts, embedding_payload["embeddings"])
        if len(centroid_rows) < 2:
            continue
        for row in centroid_rows:
            monthly_rows.append(
                {
                    "company_key": company_key,
                    "company_id": company_id_lookup.get(company_key),
                    "company_name": company_name_lookup.get(company_key),
                    **row,
                }
            )
        summary_rows.append(
            {
                "company_key": company_key,
                "company_id": company_id_lookup.get(company_key),
                "company_name": company_name_lookup.get(company_key),
                "post_count": len(company_posts),
                "active_month_count": len(centroid_rows),
                "mean_angle_from_first_deg": round(mean([float(row["angle_from_first_deg"]) for row in centroid_rows]), 2),
                "mean_angle_from_previous_deg": round(
                    mean([float(row["angle_from_previous_deg"]) for row in centroid_rows if row["angle_from_previous_deg"] is not None]),
                    2,
                ),
                "max_angle_from_first_deg": round(max(float(row["angle_from_first_deg"]) for row in centroid_rows), 2),
                "final_angle_from_first_deg": round(float(centroid_rows[-1]["angle_from_first_deg"]), 2),
                "drift_score": round(
                    mean([float(row["angle_from_first_deg"]) for row in centroid_rows])
                    + mean([float(row["angle_from_previous_deg"]) for row in centroid_rows if row["angle_from_previous_deg"] is not None]),
                    2,
                ),
            }
        )
    return (
        sorted(summary_rows, key=lambda row: (-float(row["drift_score"]), -(int(row["post_count"])))),
        sorted(monthly_rows, key=lambda row: (row["company_name"], row["thread_month"])),
    )


def company_projection_payload(texts: list[str]) -> dict[str, object]:
    """Return normalized embeddings plus a 2D projection for one company's posts."""

    import numpy as np
    from sklearn.decomposition import TruncatedSVD
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.manifold import TSNE
    from sklearn.preprocessing import normalize

    vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), max_features=4000)
    matrix = vectorizer.fit_transform(texts)
    if matrix.shape[1] == 0:
        dense = np.zeros((len(texts), 2), dtype=float)
    else:
        n_components = max(2, min(20, matrix.shape[0] - 1, matrix.shape[1] - 1))
        if n_components >= 2:
            dense = TruncatedSVD(n_components=n_components, random_state=42).fit_transform(matrix)
        else:
            dense = matrix.toarray()
    dense = normalize(dense) if len(dense) else dense
    if len(texts) >= 3:
        perplexity = max(2, min(8, len(texts) - 1))
        projection = TSNE(
            n_components=2,
            random_state=42,
            init="random",
            learning_rate="auto",
            perplexity=perplexity,
        ).fit_transform(dense)
    elif len(texts) == 2:
        projection = np.array([[0.0, 0.0], [1.0, 0.0]])
    else:
        projection = np.zeros((len(texts), 2), dtype=float)
    return {"embeddings": dense, "projection": projection}


def company_month_centroid_rows(company_posts: list[dict[str, str]], embeddings) -> list[dict[str, object]]:
    """Summarize month centroids and drift angles for one company's post embeddings."""

    import numpy as np

    grouped: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(company_posts):
        grouped[row["thread_month"]].append(index)
    ordered_months = sorted(grouped)
    centroid_rows: list[dict[str, object]] = []
    first_centroid = None
    previous_centroid = None
    for month in ordered_months:
        indices = grouped[month]
        month_vectors = embeddings[indices]
        centroid = month_vectors.mean(axis=0)
        centroid = normalize_vector(centroid)
        within_angles = []
        for vector in month_vectors:
            within_angles.append(angle_between_vectors(normalize_vector(vector), centroid))
        angle_from_first = 0.0 if first_centroid is None else angle_between_vectors(centroid, first_centroid)
        angle_from_previous = None if previous_centroid is None else angle_between_vectors(centroid, previous_centroid)
        centroid_rows.append(
            {
                "thread_month": month,
                "month_post_count": len(indices),
                "within_month_mean_angle_deg": round(mean(within_angles), 2),
                "angle_from_first_deg": round(angle_from_first, 2),
                "angle_from_previous_deg": None if angle_from_previous is None else round(angle_from_previous, 2),
            }
        )
        if first_centroid is None:
            first_centroid = centroid
        previous_centroid = centroid
    return centroid_rows


def changed_companies_ranked(
    post_vs_role_rows: list[dict[str, object]],
    company_drift_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Rank companies by overall change using spread and drift metrics."""

    drift_by_key = {str(row["company_key"]): row for row in company_drift_rows}
    rows: list[dict[str, object]] = []
    for row in post_vs_role_rows:
        company_key = str(row["company_key"])
        drift = drift_by_key.get(company_key)
        if drift is None:
            continue
        changed_score = (
            float(row["post_mean_angle_deg"]) * 0.45
            + float(row["role_mean_angle_deg"]) * 0.25
            + float(drift["drift_score"]) * 0.30
        )
        rows.append(
            {
                "company_key": company_key,
                "company_id": row.get("company_id"),
                "company_name": row.get("company_name"),
                "post_count": row.get("post_count"),
                "role_count": row.get("role_count"),
                "post_mean_angle_deg": row.get("post_mean_angle_deg"),
                "role_mean_angle_deg": row.get("role_mean_angle_deg"),
                "drift_score": drift.get("drift_score"),
                "changed_score": round(changed_score, 2),
            }
        )
    return sorted(rows, key=lambda row: (-float(row["changed_score"]), -(int(row["post_count"] or 0))))


def semantic_angle_metrics(texts: list[str]) -> dict[str, float | int]:
    """Summarize the pairwise semantic spread of a company's post texts."""

    cleaned_texts = [text.strip() for text in texts if text and text.strip()]
    post_count = len(cleaned_texts)
    if post_count <= 1:
        return {
            "pair_count": 0,
            "exact_reuse_share": 0.0,
            "mean_pairwise_cosine": 1.0 if post_count == 1 else 0.0,
            "mean_pairwise_angle_deg": 0.0,
            "median_pairwise_angle_deg": 0.0,
            "p90_pairwise_angle_deg": 0.0,
            "max_pairwise_angle_deg": 0.0,
        }

    angles, cosines = pairwise_semantic_geometry(cleaned_texts)
    unique_text_count = len(set(cleaned_texts))
    exact_reuse_share = 1.0 - (unique_text_count / post_count)
    return {
        "pair_count": len(angles),
        "exact_reuse_share": round(exact_reuse_share, 4),
        "mean_pairwise_cosine": round(sum(cosines) / len(cosines), 4),
        "mean_pairwise_angle_deg": round(mean(angles), 2),
        "median_pairwise_angle_deg": round(percentile(angles, 50), 2),
        "p90_pairwise_angle_deg": round(percentile(angles, 90), 2),
        "max_pairwise_angle_deg": round(max(angles), 2),
    }


def pairwise_semantic_geometry(texts: list[str]) -> tuple[list[float], list[float]]:
    """Return pairwise angle and cosine values for TF-IDF text embeddings."""

    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), max_features=4000)
    try:
        matrix = vectorizer.fit_transform(texts)
    except ValueError:
        pair_count = (len(texts) * (len(texts) - 1)) // 2
        return [0.0] * pair_count, [1.0] * pair_count
    cosine_matrix = cosine_similarity(matrix)
    upper_i, upper_j = np.triu_indices_from(cosine_matrix, k=1)
    cosine_values = [float(np.clip(value, -1.0, 1.0)) for value in cosine_matrix[upper_i, upper_j]]
    angle_values = [float(np.degrees(np.arccos(value))) for value in cosine_values]
    return angle_values, cosine_values


def role_text_from_row(role: dict[str, object]) -> str:
    parts = [
        str(role.get("role_title_observed") or ""),
        str(role.get("role_title_normalized") or ""),
        str(role.get("role_family") or ""),
        str(role.get("seniority") or ""),
        str(role.get("skills_text") or ""),
        str(role.get("requirements_text") or ""),
        str(role.get("responsibilities_text") or ""),
    ]
    return "\n".join(part for part in parts if part.strip())


def normalize_vector(vector):
    import numpy as np

    norm = float(np.linalg.norm(vector))
    if norm == 0.0:
        return vector
    return vector / norm


def angle_between_vectors(left, right) -> float:
    import numpy as np

    cosine = float(np.clip(np.dot(left, right), -1.0, 1.0))
    return float(np.degrees(np.arccos(cosine)))


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def percentile(values: list[float], pct: int) -> float:
    import numpy as np

    if not values:
        return 0.0
    return float(np.percentile(values, pct))


def load_jsonl(path: Path) -> list[dict[str, object]]:
    """Load rows from a JSONL file."""

    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def write_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    """Write rows to CSV."""

    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(rows)
    return path


def write_manifest(path: Path, outputs: dict[str, Path]) -> Path:
    """Write an analytics manifest."""

    payload = {
        "analytics_version": "v1",
        "output_paths": {name: str(output_path) for name, output_path in outputs.items()},
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def write_analytics_visuals(
    *,
    visuals_dir: Path,
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
    company_posting_rows: list[dict[str, object]],
    company_summary_rows: list[dict[str, object]],
    remote_rows: list[dict[str, object]],
    remote_share_rows: list[dict[str, object]],
    role_family_rows: list[dict[str, object]],
    distinct_role_rows: list[dict[str, object]],
    ai_concept_rows: list[dict[str, object]],
    ai_concept_role_family_rows: list[dict[str, object]],
    product_theme_rows: list[dict[str, object]],
    recurring_rows: list[dict[str, object]],
    company_semantic_rows: list[dict[str, object]],
    company_role_rows: list[dict[str, object]],
    post_vs_role_rows: list[dict[str, object]],
    windowed_post_vs_role_rows: list[dict[str, object]],
    company_drift_rows: list[dict[str, object]],
    company_drift_monthly_rows: list[dict[str, object]],
    changed_company_rows: list[dict[str, object]],
) -> dict[str, Path]:
    """Write a polished visual for each core analytical output."""

    plt, pd, sns = plotting_modules(visuals_dir)
    apply_plot_style(plt, sns)

    outputs = {
        "company_posting_counts_visual": plot_company_posting_counts(
            plt, pd, sns, visuals_dir / "company_posting_counts_by_month.png", company_posting_rows
        ),
        "company_summary_visual": plot_company_summary(
            plt, pd, visuals_dir / "company_summary_by_month.png", company_summary_rows
        ),
        "remote_status_trends_visual": plot_remote_status_trends(
            plt, pd, visuals_dir / "remote_status_trends_by_month.png", remote_rows
        ),
        "remote_status_share_visual": plot_remote_status_share(
            plt, pd, visuals_dir / "remote_status_share_by_month.png", remote_share_rows
        ),
        "remote_status_share_timeseries_visual": plot_remote_status_share_timeseries(
            plt, pd, visuals_dir / "remote_status_share_timeseries.png", remote_share_rows
        ),
        "role_family_trends_visual": plot_role_family_trends(
            plt, pd, sns, visuals_dir / "role_family_trends_by_month.png", role_family_rows
        ),
        "role_family_timeseries_visual": plot_role_family_timeseries(
            plt, pd, visuals_dir / "role_family_trends_timeseries.png", role_family_rows
        ),
        "distinct_roles_visual": plot_distinct_roles(
            plt, pd, visuals_dir / "distinct_roles_by_month.png", distinct_role_rows
        ),
        "ai_concepts_visual": plot_ai_concepts(
            plt, pd, visuals_dir / "ai_concepts_by_month.png", ai_concept_rows
        ),
        "ai_concepts_share_visual": plot_ai_concept_share(
            plt, pd, visuals_dir / "ai_concepts_share_by_month.png", ai_concept_rows
        ),
        "ai_concepts_role_family_visual": plot_ai_concepts_by_role_family(
            plt, pd, sns, visuals_dir / "ai_concepts_by_role_family.png", ai_concept_role_family_rows
        ),
        "ai_concepts_role_family_share_visual": plot_ai_concepts_by_role_family_share(
            plt, pd, sns, visuals_dir / "ai_concepts_by_role_family_share.png", ai_concept_role_family_rows
        ),
        "company_building_themes_timeseries_visual": plot_company_building_theme_timeseries(
            plt, pd, visuals_dir / "company_building_themes_timeseries.png", product_theme_rows
        ),
        "recurring_company_hiring_patterns_visual": plot_recurring_company_patterns(
            plt, pd, visuals_dir / "recurring_company_hiring_patterns.png", recurring_rows
        ),
        "company_semantic_spread_visual": plot_company_semantic_spread(
            plt, pd, visuals_dir / "company_semantic_spread.png", company_semantic_rows
        ),
        "company_role_semantic_spread_visual": plot_company_role_semantic_spread(
            plt, pd, visuals_dir / "company_role_semantic_spread.png", company_role_rows
        ),
        "company_post_vs_role_spread_visual": plot_company_post_vs_role_spread(
            plt, pd, visuals_dir / "company_post_vs_role_spread.png", post_vs_role_rows
        ),
        "company_post_vs_role_spread_6m_visual": plot_company_post_vs_role_spread_windowed(
            plt, pd, visuals_dir / "company_post_vs_role_spread_6m.png", windowed_post_vs_role_rows
        ),
        "changed_companies_ranked_visual": plot_changed_companies_ranked(
            plt, pd, visuals_dir / "changed_companies_ranked.png", changed_company_rows
        ),
    }
    outputs.update(
        plot_company_variation_histograms(
            plt,
            pd,
            visuals_dir / "company_variation_histograms",
            posts,
            company_name_by_id,
            company_semantic_rows,
        )
    )
    outputs.update(
        plot_company_drift_projection_bundle(
            plt,
            pd,
            visuals_dir / "company_drift_projections",
            posts,
            company_name_by_id,
            month_by_thread_id,
            company_drift_rows,
            changed_company_rows,
        )
    )
    outputs.update(
        plot_company_building_themes_by_year(
            plt,
            pd,
            sns,
            visuals_dir,
            product_theme_rows,
        )
    )
    outputs["visual_index"] = write_visual_index(visuals_dir / "README.md", outputs)
    return outputs


def plotting_modules(visuals_dir: Path):
    """Load plotting modules with a writable Matplotlib config directory."""

    mpl_config_dir = visuals_dir / ".mplconfig"
    mpl_config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_config_dir))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    import seaborn as sns

    return plt, pd, sns


def pretty_month_label(thread_month: str) -> str:
    """Format YYYY-MM as a compact plotting label."""

    year_text, month_text = thread_month.split("-")
    month_number = int(month_text)
    month_name = MONTH_ABBREVIATIONS[month_number]
    if month_number == 1:
        return f"{month_name}\n{year_text}"
    return month_name


def pretty_month_labels(thread_months: list[str]) -> list[str]:
    return [pretty_month_label(thread_month) for thread_month in thread_months]


def apply_month_axis(ax, thread_months: list[str]) -> None:
    """Apply readable month labels and subtle year separators to an x-axis."""

    positions = list(range(len(thread_months)))
    ax.set_xticks(positions)
    ax.set_xticklabels(pretty_month_labels(thread_months))
    ax.tick_params(axis="x", labelrotation=90, labelsize=9, pad=6)
    for position, thread_month in enumerate(thread_months):
        if thread_month.endswith("-01"):
            ax.axvline(position, color="#d9ccb8", linewidth=1.0, alpha=0.45, zorder=0)


def rename_month_columns(frame, pd):
    """Return a copy with month columns relabeled for readability."""

    renamed = frame.copy()
    renamed.columns = pretty_month_labels(list(frame.columns))
    return renamed


def apply_plot_style(plt, sns) -> None:
    """Apply a consistent, high-contrast visual style."""

    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update(
        {
            "figure.facecolor": "#f5f1e8",
            "axes.facecolor": "#fffaf0",
            "axes.edgecolor": "#2b2825",
            "axes.labelcolor": "#2b2825",
            "axes.titleweight": "bold",
            "text.color": "#2b2825",
            "xtick.color": "#2b2825",
            "ytick.color": "#2b2825",
            "font.size": 12,
        }
    )


def plot_company_posting_counts(plt, pd, sns, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot a dot-timeline for the most recurrent companies."""

    frame = pd.DataFrame(rows)
    frame["company_name"] = frame["company_name"].fillna("[unresolved]")
    recurring = (
        frame.groupby("company_name")["hiring_post_count"]
        .agg(["sum", "count"])
        .reset_index()
        .rename(columns={"sum": "total_posts", "count": "active_months"})
        .sort_values(["active_months", "total_posts", "company_name"], ascending=[False, False, True])
        .head(18)
    )
    filtered = frame[frame["company_name"].isin(recurring["company_name"])].copy()
    company_order = recurring["company_name"].tolist()
    month_order = sorted(filtered["thread_month"].unique().tolist())
    month_positions = {month: index for index, month in enumerate(month_order)}
    filtered["company_name"] = pd.Categorical(filtered["company_name"], categories=company_order, ordered=True)
    filtered["thread_month_position"] = filtered["thread_month"].map(month_positions)

    fig, ax = plt.subplots(figsize=(14, 9))
    scatter = ax.scatter(
        filtered["thread_month_position"],
        filtered["company_name"],
        s=filtered["hiring_post_count"].astype(int) * 260,
        c=filtered["hiring_post_count"].astype(int),
        cmap="YlOrBr",
        alpha=0.88,
        edgecolors="#2b2825",
        linewidths=1.1,
    )
    for _, row in filtered.iterrows():
        ax.text(
            row["thread_month_position"],
            row["company_name"],
            str(int(row["hiring_post_count"])),
            ha="center",
            va="center",
            fontsize=10,
            fontweight="bold",
            color="#1e1b18",
        )
    ax.set_title("Top Recurring Companies By Month")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Company")
    apply_month_axis(ax, month_order)
    cbar = fig.colorbar(scatter, ax=ax, pad=0.02)
    cbar.set_label("Hiring posts")
    ax.grid(axis="x", linestyle="--", linewidth=0.7, alpha=0.35)
    ax.grid(axis="y", visible=False)
    fig.subplots_adjust(left=0.33, right=0.92, top=0.9, bottom=0.12)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_remote_status_trends(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot stacked monthly remote-status counts."""

    frame = pd.DataFrame(rows)
    order = ["remote", "hybrid", "onsite", "unspecified"]
    pivot = frame.pivot_table(
        index="thread_month",
        columns="remote_status",
        values="hiring_post_count",
        aggfunc="sum",
        fill_value=0,
    )
    for status in order:
        if status not in pivot.columns:
            pivot[status] = 0
    pivot = pivot[order]
    colors = {
        "remote": "#2a9d8f",
        "hybrid": "#e9c46a",
        "onsite": "#e76f51",
        "unspecified": "#8d99ae",
    }
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(13.2, 7.2), constrained_layout=True)
    bottom = None
    for status in order:
        values = pivot[status].tolist()
        ax.bar(positions, values, bottom=bottom, label=status.title(), color=colors[status], edgecolor="#2b2825")
        bottom = values if bottom is None else [left + right for left, right in zip(bottom, values)]
    ax.set_title("Remote Status Trends By Month")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Hiring posts")
    apply_month_axis(ax, months)
    ax.legend(frameon=True, ncols=4, loc="upper center", bbox_to_anchor=(0.5, 1.12))
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_remote_status_share(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot stacked monthly remote-status percentages."""

    frame = pd.DataFrame(rows)
    order = ["remote", "hybrid", "onsite", "unspecified"]
    pivot = frame.pivot_table(
        index="thread_month",
        columns="remote_status",
        values="share_pct",
        aggfunc="sum",
        fill_value=0.0,
    )
    for status in order:
        if status not in pivot.columns:
            pivot[status] = 0.0
    pivot = pivot[order]
    colors = {
        "remote": "#2a9d8f",
        "hybrid": "#e9c46a",
        "onsite": "#e76f51",
        "unspecified": "#8d99ae",
    }
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(13.2, 7.2), constrained_layout=True)
    bottom = None
    for status in order:
        values = pivot[status].tolist()
        ax.bar(
            positions,
            values,
            bottom=bottom,
            label=status.title(),
            color=colors[status],
            edgecolor="#2b2825",
        )
        bottom = values if bottom is None else [left + right for left, right in zip(bottom, values)]
    ax.set_title("Remote Status Share By Month")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Share of hiring posts (%)")
    apply_month_axis(ax, months)
    ax.set_ylim(0, 100)
    ax.legend(frameon=True, ncols=4, loc="upper center", bbox_to_anchor=(0.5, 1.12))
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_remote_status_share_timeseries(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot remote-status percentage trends as lines for longer horizons."""

    frame = pd.DataFrame(rows)
    pivot = frame.pivot_table(
        index="thread_month",
        columns="remote_status",
        values="share_pct",
        aggfunc="sum",
        fill_value=0.0,
    )
    order = [status for status in ["remote", "hybrid", "onsite", "unspecified"] if status in pivot.columns]
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    colors = {
        "remote": "#2a9d8f",
        "hybrid": "#e9c46a",
        "onsite": "#e76f51",
        "unspecified": "#8d99ae",
    }
    fig, ax = plt.subplots(figsize=(13.2, 6.8), constrained_layout=True)
    for status in order:
        ax.plot(
            positions,
            pivot[status],
            linewidth=3,
            marker="o",
            markersize=7,
            color=colors[status],
            label=status.title(),
        )
    ax.set_title("Remote Status Share Over Time")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Share of hiring posts (%)")
    apply_month_axis(ax, months)
    ax.set_ylim(0, 100)
    ax.legend(frameon=True, ncols=4, loc="upper center", bbox_to_anchor=(0.5, 1.12))
    ax.grid(alpha=0.25, linestyle="--")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_company_summary(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot the number of companies posting by month as a time series."""

    frame = pd.DataFrame(rows).sort_values("thread_month")
    months = frame["thread_month"].tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(13.2, 6.8), constrained_layout=True)
    ax.plot(
        positions,
        frame["company_count"],
        color="#183a37",
        linewidth=3,
        marker="o",
        markersize=8,
        label="Resolved company count",
    )
    ax.fill_between(positions, frame["company_count"], color="#2a9d8f", alpha=0.16)
    ax.plot(
        positions,
        frame["observed_company_name_count"],
        color="#d17b0f",
        linewidth=2.2,
        marker="D",
        markersize=6,
        linestyle="--",
        label="Observed company-name count",
    )
    for position, (_, row) in enumerate(frame.iterrows()):
        ax.text(position, row["company_count"] + 1.5, str(int(row["company_count"])), ha="center", fontsize=10)
    ax.set_title("Companies Posting By Month")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Count of companies")
    apply_month_axis(ax, months)
    ax.legend(frameon=True, loc="upper left")
    ax.grid(alpha=0.25, linestyle="--")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_role_family_trends(plt, pd, sns, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot a heatmap of role families by month."""

    frame = pd.DataFrame(rows)
    top_families = (
        frame.groupby("role_family", as_index=False)["role_count"]
        .sum()
        .sort_values(["role_count", "role_family"], ascending=[False, True])
        .head(12)
    )
    filtered = frame[frame["role_family"].isin(top_families["role_family"])]
    pivot = filtered.pivot_table(
        index="role_family",
        columns="thread_month",
        values="role_count",
        aggfunc="sum",
        fill_value=0,
    )
    pivot = pivot.loc[top_families["role_family"]]
    pivot = rename_month_columns(pivot, pd)
    fig, ax = plt.subplots(figsize=(11.5, 7.5), constrained_layout=True)
    heatmap = sns.heatmap(
        pivot,
        cmap="crest",
        linewidths=0.5,
        linecolor="#e6dfd0",
        cbar_kws={"label": "Role count"},
        annot=True,
        fmt="g",
        annot_kws={"fontsize": 10, "fontweight": "bold"},
        ax=ax,
    )
    value_threshold = float(pivot.to_numpy().max()) * 0.45 if not pivot.empty else 0.0
    for text, value in zip(heatmap.texts, pivot.to_numpy().flatten(), strict=False):
        text.set_color("#fffaf0" if float(value) >= value_threshold else "#17323b")
    ax.set_title("Role Family Trends By Month")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Role family")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_role_family_timeseries(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot the top role families as time series for longer month ranges."""

    frame = pd.DataFrame(rows)
    top_families = (
        frame.groupby("role_family", as_index=False)["role_count"]
        .sum()
        .sort_values(["role_count", "role_family"], ascending=[False, True])
        .head(8)
    )
    filtered = frame[frame["role_family"].isin(top_families["role_family"])]
    pivot = filtered.pivot_table(index="thread_month", columns="role_family", values="role_count", aggfunc="sum", fill_value=0)
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(14.2, 7.4), constrained_layout=True)
    palette = ["#183a37", "#2a9d8f", "#e9c46a", "#e76f51", "#457b9d", "#7f5539", "#8d99ae", "#6d597a"]
    for color, family in zip(palette, pivot.columns, strict=False):
        ax.plot(positions, pivot[family], linewidth=2.6, marker="o", markersize=6, label=str(family), color=color)
    ax.set_title("Top Role Families Over Time")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Role count")
    apply_month_axis(ax, months)
    ax.legend(frameon=True, ncols=2, loc="upper left")
    ax.grid(alpha=0.25, linestyle="--")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_distinct_roles(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot the number of distinct roles posted by month as a time series."""

    frame = pd.DataFrame(rows).sort_values("thread_month")
    months = frame["thread_month"].tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(13.2, 6.8), constrained_layout=True)
    ax.plot(
        positions,
        frame["distinct_role_count"],
        color="#264653",
        linewidth=3,
        marker="o",
        markersize=8,
        label="Distinct normalized roles",
    )
    ax.fill_between(positions, frame["distinct_role_count"], color="#7db4b5", alpha=0.18)
    ax.plot(
        positions,
        frame["distinct_observed_role_count"],
        color="#e76f51",
        linewidth=2.2,
        marker="s",
        markersize=6,
        linestyle="--",
        label="Distinct observed role strings",
    )
    for position, (_, row) in enumerate(frame.iterrows()):
        ax.text(
            position,
            row["distinct_role_count"] + 1.5,
            str(int(row["distinct_role_count"])),
            ha="center",
            fontsize=10,
        )
    ax.set_title("Distinct Roles Posted By Month")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Count of distinct roles")
    apply_month_axis(ax, months)
    ax.legend(frameon=True, loc="upper left")
    ax.grid(alpha=0.25, linestyle="--")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_ai_concepts(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot absolute AI concept mentions over time."""

    frame = pd.DataFrame(rows)
    if frame.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_title("AI Hiring Concepts Over Time")
        ax.text(0.5, 0.5, "No AI concept mentions yet", ha="center", va="center")
        ax.axis("off")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return output_path
    pivot = frame.pivot_table(index="thread_month", columns="concept_name", values="mentioning_post_count", aggfunc="sum", fill_value=0)
    concept_order = (
        frame.groupby("concept_name", as_index=False)["mentioning_post_count"]
        .sum()
        .sort_values(["mentioning_post_count", "concept_name"], ascending=[False, True])["concept_name"]
        .tolist()
    )
    pivot = pivot[concept_order]
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(14.2, 7.6), constrained_layout=True)
    palette = ["#183a37", "#2a9d8f", "#e9c46a", "#e76f51", "#457b9d", "#6d597a", "#7f5539", "#8d99ae", "#c08497"]
    for color, concept in zip(palette, pivot.columns, strict=False):
        ax.plot(positions, pivot[concept], linewidth=2.8, marker="o", markersize=6, label=str(concept).replace("_", " "))
    ax.set_title("AI Hiring Concepts Over Time")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Posts mentioning concept")
    apply_month_axis(ax, months)
    ax.legend(frameon=True, ncols=3, loc="upper left")
    ax.grid(alpha=0.25, linestyle="--")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_ai_concept_share(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot AI concept mention share over time."""

    frame = pd.DataFrame(rows)
    if frame.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_title("AI Hiring Concept Share Over Time")
        ax.text(0.5, 0.5, "No AI concept mentions yet", ha="center", va="center")
        ax.axis("off")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return output_path
    top_concepts = (
        frame.groupby("concept_name", as_index=False)["mention_share_pct"]
        .max()
        .sort_values(["mention_share_pct", "concept_name"], ascending=[False, True])
        .head(6)["concept_name"]
        .tolist()
    )
    filtered = frame[frame["concept_name"].isin(top_concepts)]
    pivot = filtered.pivot_table(index="thread_month", columns="concept_name", values="mention_share_pct", aggfunc="sum", fill_value=0)
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(14.2, 7.4), constrained_layout=True)
    palette = ["#264653", "#2a9d8f", "#e9c46a", "#e76f51", "#6d597a", "#457b9d"]
    for color, concept in zip(palette, pivot.columns, strict=False):
        ax.plot(positions, pivot[concept], linewidth=3, marker="o", markersize=7, label=str(concept).replace("_", " "))
    ax.set_title("AI Concept Share Of Hiring Posts")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Share of hiring posts (%)")
    apply_month_axis(ax, months)
    ax.legend(frameon=True, ncols=2, loc="upper left")
    ax.grid(alpha=0.25, linestyle="--")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_ai_concepts_by_role_family(plt, pd, sns, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot AI concept counts by role family."""

    frame = pd.DataFrame(rows)
    if frame.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_title("AI Concepts By Role Family")
        ax.text(0.5, 0.5, "No AI role-family concept signals yet", ha="center", va="center")
        ax.axis("off")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return output_path
    top_families = (
        frame.groupby("role_family", as_index=False)["role_count"]
        .sum()
        .sort_values(["role_count", "role_family"], ascending=[False, True])
        .head(8)["role_family"]
        .tolist()
    )
    top_concepts = (
        frame.groupby("concept_name", as_index=False)["role_count"]
        .sum()
        .sort_values(["role_count", "concept_name"], ascending=[False, True])
        .head(8)["concept_name"]
        .tolist()
    )
    filtered = frame[frame["role_family"].isin(top_families) & frame["concept_name"].isin(top_concepts)]
    pivot = filtered.pivot_table(index="role_family", columns="concept_name", values="role_count", aggfunc="sum", fill_value=0)
    fig, ax = plt.subplots(figsize=(14.5, 8.2), constrained_layout=True)
    heatmap = sns.heatmap(
        pivot,
        cmap="YlOrBr",
        linewidths=0.6,
        linecolor="#ece4d8",
        annot=True,
        fmt="g",
        annot_kws={"fontsize": 9, "fontweight": "bold"},
        cbar_kws={"label": "Role count"},
        ax=ax,
    )
    threshold = float(pivot.to_numpy().max()) * 0.45 if not pivot.empty else 0.0
    for text, value in zip(heatmap.texts, pivot.to_numpy().flatten(), strict=False):
        text.set_color("#fffaf0" if float(value) >= threshold else "#102a43")
    ax.set_title("AI Concepts By Role Family")
    ax.set_xlabel("AI concept")
    ax.set_ylabel("Role family")
    ax.tick_params(axis="x", labelrotation=0, labelsize=10)
    ax.tick_params(axis="y", labelsize=10)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_ai_concepts_by_role_family_share(plt, pd, sns, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot AI concept share within each role family."""

    frame = pd.DataFrame(rows)
    if frame.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_title("AI Concept Share By Role Family")
        ax.text(0.5, 0.5, "No AI role-family concept signals yet", ha="center", va="center")
        ax.axis("off")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return output_path
    top_families = (
        frame.groupby("role_family", as_index=False)["role_count"]
        .sum()
        .sort_values(["role_count", "role_family"], ascending=[False, True])
        .head(8)["role_family"]
        .tolist()
    )
    top_concepts = (
        frame.groupby("concept_name", as_index=False)["role_count"]
        .sum()
        .sort_values(["role_count", "concept_name"], ascending=[False, True])
        .head(8)["concept_name"]
        .tolist()
    )
    filtered = frame[frame["role_family"].isin(top_families) & frame["concept_name"].isin(top_concepts)]
    pivot = filtered.pivot_table(index="role_family", columns="concept_name", values="role_share_pct", aggfunc="sum", fill_value=0.0)
    fig, ax = plt.subplots(figsize=(14.5, 8.2), constrained_layout=True)
    heatmap = sns.heatmap(
        pivot,
        cmap="crest",
        linewidths=0.6,
        linecolor="#ece4d8",
        annot=True,
        fmt=".1f",
        annot_kws={"fontsize": 9, "fontweight": "bold"},
        cbar_kws={"label": "Share of role family (%)"},
        ax=ax,
    )
    threshold = float(pivot.to_numpy().max()) * 0.45 if not pivot.empty else 0.0
    for text, value in zip(heatmap.texts, pivot.to_numpy().flatten(), strict=False):
        text.set_color("#fffaf0" if float(value) >= threshold else "#102a43")
    ax.set_title("AI Concept Share By Role Family")
    ax.set_xlabel("AI concept")
    ax.set_ylabel("Role family")
    ax.tick_params(axis="x", labelrotation=0, labelsize=10)
    ax.tick_params(axis="y", labelsize=10)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_company_building_themes_by_year(plt, pd, sns, visuals_dir: Path, rows: list[dict[str, object]]) -> dict[str, Path]:
    """Plot one readable product-theme heatmap per year."""

    frame = pd.DataFrame(rows)
    outputs: dict[str, Path] = {}
    for year in ["2023", "2024", "2025", "2026"]:
        output_key = f"company_building_themes_{year}_visual"
        output_path = visuals_dir / f"company_building_themes_{year}.png"
        if frame.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.set_title(f"What Companies Are Building ({year})")
            ax.text(0.5, 0.5, "No product-theme signals yet", ha="center", va="center")
            ax.axis("off")
            fig.savefig(output_path, dpi=180, bbox_inches="tight")
            plt.close(fig)
            outputs[output_key] = output_path
            continue

        year_frame = frame[frame["thread_month"].astype(str).str.startswith(year)].copy()
        if year_frame.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.set_title(f"What Companies Are Building ({year})")
            ax.text(0.5, 0.5, "No product-theme signals for this year", ha="center", va="center")
            ax.axis("off")
            fig.savefig(output_path, dpi=180, bbox_inches="tight")
            plt.close(fig)
            outputs[output_key] = output_path
            continue

        top_themes = (
            year_frame.groupby("building_theme", as_index=False)["company_count"]
            .sum()
            .sort_values(["company_count", "building_theme"], ascending=[False, True])
            .head(8)
        )
        filtered = year_frame[year_frame["building_theme"].isin(top_themes["building_theme"])]
        pivot = filtered.pivot_table(index="building_theme", columns="thread_month", values="company_count", aggfunc="sum", fill_value=0)
        pivot = pivot.loc[top_themes["building_theme"]]
        pivot = rename_month_columns(pivot, pd)
        fig, ax = plt.subplots(figsize=(10.5, 6.6), constrained_layout=True)
        heatmap = sns.heatmap(
            pivot,
            cmap="YlGnBu",
            linewidths=0.7,
            linecolor="#ece4d8",
            cbar_kws={"label": "Companies"},
            annot=True,
            fmt="g",
            annot_kws={"fontsize": 10, "fontweight": "bold"},
            ax=ax,
        )
        threshold = float(pivot.to_numpy().max()) * 0.42 if not pivot.empty else 0.0
        for text, value in zip(heatmap.texts, pivot.to_numpy().flatten(), strict=False):
            text.set_color("#fffaf0" if float(value) >= threshold else "#102a43")
        ax.set_title(f"What Companies Are Building ({year})")
        ax.set_xlabel("Thread month")
        ax.set_ylabel("Product theme")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        outputs[output_key] = output_path
    return outputs


def plot_company_building_theme_timeseries(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot top product themes over time."""

    frame = pd.DataFrame(rows)
    if frame.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_title("Product Themes Over Time")
        ax.text(0.5, 0.5, "No product-theme signals yet", ha="center", va="center")
        ax.axis("off")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return output_path
    top_themes = (
        frame.groupby("building_theme", as_index=False)["company_count"]
        .sum()
        .sort_values(["company_count", "building_theme"], ascending=[False, True])
        .head(6)["building_theme"]
        .tolist()
    )
    filtered = frame[frame["building_theme"].isin(top_themes)]
    pivot = filtered.pivot_table(index="thread_month", columns="building_theme", values="company_count", aggfunc="sum", fill_value=0)
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(14.2, 7.4), constrained_layout=True)
    palette = ["#183a37", "#2a9d8f", "#e9c46a", "#e76f51", "#457b9d", "#6d597a"]
    for color, theme in zip(palette, pivot.columns, strict=False):
        ax.plot(positions, pivot[theme], linewidth=2.8, marker="o", markersize=6, label=str(theme).replace("_", " "), color=color)
    ax.set_title("Product Themes Over Time")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Companies")
    apply_month_axis(ax, months)
    ax.legend(frameon=True, ncols=2, loc="upper left")
    ax.grid(alpha=0.25, linestyle="--")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_recurring_company_patterns(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot companies with the widest month coverage."""

    frame = pd.DataFrame(rows)
    top = frame.sort_values(["active_month_count", "company_name"], ascending=[False, True]).head(20)
    fig, ax = plt.subplots(figsize=(12, 8), constrained_layout=True)
    ax.barh(top["company_name"], top["active_month_count"], color="#264653", edgecolor="#1b1a17")
    ax.invert_yaxis()
    ax.set_title("Recurring Company Hiring Patterns")
    ax.set_xlabel("Active thread months")
    ax.set_ylabel("Company")
    for index, value in enumerate(top["active_month_count"]):
        ax.text(value + 0.03, index, str(value), va="center", fontsize=10)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_company_semantic_spread(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot the companies with the widest semantic posting spread."""

    frame = pd.DataFrame(rows)
    top = (
        frame.sort_values(["mean_pairwise_angle_deg", "post_count", "company_name"], ascending=[False, False, True])
        .head(20)
        .copy()
    )
    fig, ax = plt.subplots(figsize=(13, 8.5), constrained_layout=True)
    ax.barh(
        top["company_name"],
        top["mean_pairwise_angle_deg"],
        color="#8c564b",
        edgecolor="#1b1a17",
    )
    ax.invert_yaxis()
    ax.set_title("Companies With The Widest Semantic Spread")
    ax.set_xlabel("Mean pairwise angle (degrees)")
    ax.set_ylabel("Company")
    for index, (_, row) in enumerate(top.iterrows()):
        ax.text(
            float(row["mean_pairwise_angle_deg"]) + 0.6,
            index,
            f"{float(row['mean_pairwise_angle_deg']):.1f}°",
            va="center",
            fontsize=10,
        )
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_company_role_semantic_spread(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot the companies with the widest role-level semantic spread."""

    frame = pd.DataFrame(rows)
    top = (
        frame.sort_values(["role_mean_angle_deg", "role_count", "company_name"], ascending=[False, False, True])
        .head(20)
        .copy()
    )
    fig, ax = plt.subplots(figsize=(13, 8.5), constrained_layout=True)
    ax.barh(top["company_name"], top["role_mean_angle_deg"], color="#5b8c5a", edgecolor="#1b1a17")
    ax.invert_yaxis()
    ax.set_title("Companies With The Widest Role Spread")
    ax.set_xlabel("Mean role angle (degrees)")
    ax.set_ylabel("Company")
    for index, (_, row) in enumerate(top.iterrows()):
        ax.text(float(row["role_mean_angle_deg"]) + 0.6, index, f"{float(row['role_mean_angle_deg']):.1f}°", va="center", fontsize=10)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_company_post_vs_role_spread(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot post spread against role spread for companies."""

    frame = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(11.8, 8.4), constrained_layout=True)
    ax.scatter(
        frame["role_mean_angle_deg"],
        frame["post_mean_angle_deg"],
        s=frame["post_count"].astype(float) * 6.0,
        color="#457b9d",
        alpha=0.82,
        edgecolors="#1b1a17",
        linewidths=0.6,
    )
    limit = max(frame["role_mean_angle_deg"].max(), frame["post_mean_angle_deg"].max()) + 2.0 if not frame.empty else 1.0
    ax.plot([0, limit], [0, limit], linestyle="--", linewidth=1.2, color="#8d99ae")
    ax.set_xlim(0, limit)
    ax.set_ylim(0, limit)
    ax.set_title("Post Spread vs Role Spread")
    ax.set_xlabel("Role mean angle (degrees)")
    ax.set_ylabel("Post mean angle (degrees)")
    ax.grid(alpha=0.22, linestyle="--")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_company_post_vs_role_spread_windowed(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot windowed post-vs-role spread across non-overlapping 6-month periods."""

    frame = pd.DataFrame(rows)
    if frame.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_title("Windowed Post Spread vs Role Spread")
        ax.text(0.5, 0.5, "No windowed company spread rows yet", ha="center", va="center")
        ax.axis("off")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return output_path
    ordered_windows = frame[["window_index", "window_label"]].drop_duplicates().sort_values("window_index")
    window_labels = ordered_windows["window_label"].tolist()
    ncols = 2
    nrows = (len(window_labels) + 1) // 2
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(14.5, max(5.6, nrows * 5.2)), constrained_layout=True)
    axes = axes.flatten() if hasattr(axes, "flatten") else [axes]
    for axis_index, window_label in enumerate(window_labels):
        ax = axes[axis_index]
        subset = frame[frame["window_label"] == window_label]
        ax.scatter(
            subset["role_mean_angle_deg"],
            subset["post_mean_angle_deg"],
            s=subset["post_count"].astype(float) * 5.0,
            color="#457b9d",
            alpha=0.8,
            edgecolors="#1b1a17",
            linewidths=0.5,
        )
        limit = max(subset["role_mean_angle_deg"].max(), subset["post_mean_angle_deg"].max()) + 2.0
        ax.plot([0, limit], [0, limit], linestyle="--", linewidth=1.0, color="#8d99ae")
        ax.set_xlim(0, limit)
        ax.set_ylim(0, limit)
        ax.set_title(window_label)
        ax.set_xlabel("Role mean angle")
        ax.set_ylabel("Post mean angle")
        ax.grid(alpha=0.2, linestyle="--")
    for axis_index in range(len(window_labels), len(axes)):
        axes[axis_index].axis("off")
    fig.suptitle("Windowed Post Spread vs Role Spread", fontsize=17, fontweight="bold")
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_changed_companies_ranked(plt, pd, output_path: Path, rows: list[dict[str, object]]) -> Path:
    """Plot the top changed companies by the combined changed score."""

    frame = pd.DataFrame(rows).head(20).copy()
    fig, ax = plt.subplots(figsize=(13, 8.5), constrained_layout=True)
    ax.barh(frame["company_name"], frame["changed_score"], color="#6d597a", edgecolor="#1b1a17")
    ax.invert_yaxis()
    ax.set_title("Changed Companies Ranking")
    ax.set_xlabel("Changed score")
    ax.set_ylabel("Company")
    for index, (_, row) in enumerate(frame.iterrows()):
        ax.text(float(row["changed_score"]) + 0.5, index, f"{float(row['changed_score']):.1f}", va="center", fontsize=10)
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return output_path


def plot_company_variation_histograms(
    plt,
    pd,
    output_dir: Path,
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    semantic_rows: list[dict[str, object]],
    top_n: int = 24,
) -> dict[str, Path]:
    """Write static pairwise-angle histograms for the most relevant companies."""

    output_dir.mkdir(parents=True, exist_ok=True)
    rows_by_key = {
        str(row["company_key"]): row
        for row in semantic_rows
        if row.get("company_key") and int(row.get("post_count") or 0) >= 2
    }
    top_rows = sorted(
        rows_by_key.values(),
        key=lambda row: (-int(row["post_count"]), row.get("company_name") or ""),
    )[:top_n]

    posts_by_company: dict[str, list[str]] = defaultdict(list)
    for post in posts:
        if not post.get("is_hiring_post"):
            continue
        company_id = post.get("company_id")
        observed_name = str(post.get("company_name_observed") or "").strip()
        company_name = company_name_by_id.get(company_id) if company_id else None
        if not company_name:
            company_name = observed_name
        if not company_name:
            continue
        company_key = str(company_id) if company_id else f"observed:{company_name.lower()}"
        posts_by_company[company_key].append(str(post.get("post_text_clean") or ""))

    outputs: dict[str, Path] = {}
    index_lines = ["# Company Variation Histograms", "", "Static pairwise-angle histograms for top companies.", ""]
    for row in top_rows:
        company_key = str(row["company_key"])
        company_name = str(row["company_name"])
        texts = posts_by_company.get(company_key, [])
        if len([text for text in texts if text.strip()]) < 2:
            continue
        angles, _ = pairwise_semantic_geometry(texts)
        if not angles:
            continue
        output_path = output_dir / f"{slugify(company_name)}.png"
        fig, ax = plt.subplots(figsize=(11.2, 5.8), constrained_layout=True)
        ax.hist(angles, bins=min(18, max(6, len(angles))), color="#7f5539", edgecolor="#1f1d1a", alpha=0.88)
        ax.set_title(f"{company_name}: Pairwise Semantic Angle Distribution")
        ax.set_xlabel("Angle between posts (degrees)")
        ax.set_ylabel("Pair count")
        ax.grid(alpha=0.22, linestyle="--")
        fig.savefig(output_path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        outputs[f"company_variation_hist_{slugify(company_name)}"] = output_path
        index_lines.append(f"- `{company_name}`: `{output_path.name}`")

    index_path = output_dir / "README.md"
    index_path.write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    outputs["company_variation_histograms_index"] = index_path
    return outputs


def plot_company_drift_projection_bundle(
    plt,
    pd,
    output_dir: Path,
    posts: list[dict[str, object]],
    company_name_by_id: dict[str, str],
    month_by_thread_id: dict[str, str],
    company_drift_rows: list[dict[str, object]],
    changed_company_rows: list[dict[str, object]],
    top_n: int = 12,
) -> dict[str, Path]:
    """Write local PNG and GIF projection artifacts for the top changed companies."""

    import imageio.v2 as imageio
    import numpy as np

    output_dir.mkdir(parents=True, exist_ok=True)
    top_keys = [str(row["company_key"]) for row in changed_company_rows[:top_n]]
    outputs: dict[str, Path] = {}
    index_lines = ["# Company Drift Projections", "", "Static PNGs and animated GIFs for top changed companies.", ""]

    for company_key in top_keys:
        company_posts = []
        for post in posts:
            if not post.get("is_hiring_post"):
                continue
            cid = post.get("company_id")
            observed_name = str(post.get("company_name_observed") or "").strip()
            cname = company_name_by_id.get(cid) if cid else None
            if not cname:
                cname = observed_name
            if not cname:
                continue
            key = str(cid) if cid else f"observed:{cname.lower()}"
            if key != company_key:
                continue
            thread_month = month_by_thread_id.get(str(post["thread_id"]))
            if not thread_month:
                continue
            company_posts.append(
                {
                    "thread_month": thread_month,
                    "text": str(post.get("post_text_clean") or ""),
                    "post_id": str(post["post_id"]),
                    "company_name": cname,
                }
            )
        if len(company_posts) < 3:
            continue
        company_posts = sorted(company_posts, key=lambda row: (row["thread_month"], row["post_id"]))
        texts = [row["text"] for row in company_posts]
        payload = company_projection_payload(texts)
        coords = payload["projection"]
        months = [row["thread_month"] for row in company_posts]
        company_name = company_posts[0]["company_name"]
        slug = slugify(company_name)
        png_path = output_dir / f"{slug}.png"
        gif_path = output_dir / f"{slug}.gif"

        fig, ax = plt.subplots(figsize=(7.2, 6.2), constrained_layout=True)
        unique_months = sorted(set(months))
        palette = plt.cm.get_cmap("viridis", len(unique_months))
        for idx, month in enumerate(unique_months):
            mask = [value == month for value in months]
            month_coords = coords[mask]
            ax.scatter(month_coords[:, 0], month_coords[:, 1], s=58, alpha=0.88, color=palette(idx), label=month, edgecolors="#1b1a17", linewidths=0.4)
        ax.set_title(f"{company_name}: Projection")
        ax.set_xlabel("Projection X")
        ax.set_ylabel("Projection Y")
        ax.legend(frameon=True, fontsize=8, ncols=2)
        ax.grid(alpha=0.2, linestyle="--")
        fig.savefig(png_path, dpi=180, bbox_inches="tight")
        plt.close(fig)

        monthly_frame = pd.DataFrame(company_month_centroid_rows(company_posts, payload["embeddings"]))
        if not monthly_frame.empty:
            drift_png_path = output_dir / f"{slug}_drift.png"
            drift_months = monthly_frame["thread_month"].tolist()
            drift_positions = list(range(len(drift_months)))
            fig, ax = plt.subplots(figsize=(8.2, 4.8), constrained_layout=True)
            ax.plot(
                drift_positions,
                monthly_frame["angle_from_first_deg"],
                marker="o",
                linewidth=2.8,
                color="#264653",
                label="From first month",
            )
            previous_values = monthly_frame["angle_from_previous_deg"].fillna(0.0).tolist()
            ax.plot(
                drift_positions,
                previous_values,
                marker="s",
                linewidth=2.2,
                linestyle="--",
                color="#e76f51",
                label="From previous month",
            )
            ax.set_title(f"{company_name}: Drift Over Time")
            ax.set_xlabel("Thread month")
            ax.set_ylabel("Angle (degrees)")
            apply_month_axis(ax, drift_months)
            ax.legend(frameon=True, loc="upper left")
            ax.grid(alpha=0.25, linestyle="--")
            fig.savefig(drift_png_path, dpi=180, bbox_inches="tight")
            plt.close(fig)
            outputs[f"company_drift_timeline_{slug}"] = drift_png_path

        frames = []
        for idx, month in enumerate(unique_months):
            fig, ax = plt.subplots(figsize=(7.2, 6.2), constrained_layout=True)
            for past_idx, past_month in enumerate(unique_months[: idx + 1]):
                mask = [value == past_month for value in months]
                month_coords = coords[mask]
                ax.scatter(
                    month_coords[:, 0],
                    month_coords[:, 1],
                    s=58,
                    alpha=0.88,
                    color=palette(past_idx),
                    label=past_month,
                    edgecolors="#1b1a17",
                    linewidths=0.4,
                )
            ax.set_title(f"{company_name}: Through {month}")
            ax.set_xlabel("Projection X")
            ax.set_ylabel("Projection Y")
            ax.legend(frameon=True, fontsize=8, ncols=2)
            ax.grid(alpha=0.2, linestyle="--")
            fig.canvas.draw()
            frame = np.frombuffer(fig.canvas.buffer_rgba(), dtype=np.uint8)
            frame = frame.reshape(fig.canvas.get_width_height()[::-1] + (4,))
            frames.append(frame)
            plt.close(fig)
        imageio.mimsave(gif_path, frames, duration=0.9)

        outputs[f"company_drift_projection_{slug}"] = png_path
        outputs[f"company_drift_projection_gif_{slug}"] = gif_path
        if monthly_frame.empty:
            index_lines.append(f"- `{company_name}`: `{png_path.name}`, `{gif_path.name}`")
        else:
            index_lines.append(f"- `{company_name}`: `{png_path.name}`, `{gif_path.name}`, `{slug}_drift.png`")

    index_path = output_dir / "README.md"
    index_path.write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    outputs["company_drift_projection_index"] = index_path
    return outputs


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "company"


def write_visual_index(path: Path, outputs: dict[str, Path]) -> Path:
    """Write a small visual index for quick browsing."""

    lines = [
        "# Analytics Visuals",
        "",
        "Generated from Step 16 recurring analytical outputs.",
        "",
    ]
    for name, output_path in outputs.items():
        lines.append(f"- `{name}`: `{output_path.name}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
