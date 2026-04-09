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
    }
    visual_outputs = write_analytics_visuals(
        visuals_dir=visuals_dir,
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
    }
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
