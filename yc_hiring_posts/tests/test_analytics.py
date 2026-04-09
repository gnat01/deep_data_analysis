import csv
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from analytics import materialize_core_analytics


def test_materialize_core_analytics_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    import storage

    monkeypatch.setattr(storage, "project_root", lambda: tmp_path)
    source_processed = PROJECT_ROOT / "data" / "processed"
    target_processed = tmp_path / "data" / "processed"
    (target_processed / "v1_core_tables").mkdir(parents=True, exist_ok=True)

    for name in ["threads.jsonl", "raw_posts.jsonl", "posts.jsonl", "roles.jsonl", "companies.jsonl"]:
        (target_processed / "v1_core_tables" / name).write_text(
            (source_processed / "v1_core_tables" / name).read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    outputs = materialize_core_analytics()

    assert outputs["company_posting_counts_by_month"].exists()
    assert outputs["company_summary_by_month"].exists()
    assert outputs["remote_status_trends_by_month"].exists()
    assert outputs["remote_status_share_by_month"].exists()
    assert outputs["role_family_trends_by_month"].exists()
    assert outputs["distinct_roles_by_month"].exists()
    assert outputs["ai_concepts_by_month"].exists()
    assert outputs["ai_concepts_by_role_family"].exists()
    assert outputs["company_building_themes_by_month"].exists()
    assert outputs["recurring_company_hiring_patterns"].exists()
    assert outputs["company_semantic_spread"].exists()
    assert outputs["company_posting_counts_visual"].exists()
    assert outputs["company_summary_visual"].exists()
    assert outputs["remote_status_trends_visual"].exists()
    assert outputs["remote_status_share_visual"].exists()
    assert outputs["remote_status_share_timeseries_visual"].exists()
    assert outputs["role_family_trends_visual"].exists()
    assert outputs["role_family_timeseries_visual"].exists()
    assert outputs["distinct_roles_visual"].exists()
    assert outputs["ai_concepts_visual"].exists()
    assert outputs["ai_concepts_share_visual"].exists()
    assert outputs["ai_concepts_role_family_visual"].exists()
    assert outputs["ai_concepts_role_family_share_visual"].exists()
    assert outputs["company_building_themes_2023_visual"].exists()
    assert outputs["company_building_themes_2024_visual"].exists()
    assert outputs["company_building_themes_2025_visual"].exists()
    assert outputs["company_building_themes_2026_visual"].exists()
    assert outputs["company_building_themes_timeseries_visual"].exists()
    assert outputs["recurring_company_hiring_patterns_visual"].exists()
    assert outputs["company_semantic_spread_visual"].exists()
    assert outputs["company_variation_histograms_index"].exists()
    assert outputs["visual_index"].exists()
    assert outputs["manifest"].exists()

    with outputs["company_posting_counts_by_month"].open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert {"thread_month", "company_id", "company_name", "hiring_post_count"} <= set(rows[0].keys())

    with outputs["remote_status_share_by_month"].open(encoding="utf-8", newline="") as handle:
        share_rows = list(csv.DictReader(handle))
    assert share_rows
    assert {"thread_month", "remote_status", "share_pct"} <= set(share_rows[0].keys())

    with outputs["company_summary_by_month"].open(encoding="utf-8", newline="") as handle:
        company_summary_rows = list(csv.DictReader(handle))
    assert company_summary_rows
    assert {"thread_month", "company_count", "observed_company_name_count"} <= set(company_summary_rows[0].keys())

    with outputs["distinct_roles_by_month"].open(encoding="utf-8", newline="") as handle:
        distinct_role_rows = list(csv.DictReader(handle))
    assert distinct_role_rows
    assert {"thread_month", "distinct_role_count", "distinct_observed_role_count"} <= set(distinct_role_rows[0].keys())

    with outputs["ai_concepts_by_month"].open(encoding="utf-8", newline="") as handle:
        ai_rows = list(csv.DictReader(handle))
    assert ai_rows
    assert {"thread_month", "concept_name", "mentioning_post_count", "mention_share_pct"} <= set(ai_rows[0].keys())

    with outputs["ai_concepts_by_role_family"].open(encoding="utf-8", newline="") as handle:
        ai_role_rows = list(csv.DictReader(handle))
    assert ai_role_rows
    assert {"role_family", "concept_name", "role_count", "role_share_pct"} <= set(ai_role_rows[0].keys())

    with outputs["company_building_themes_by_month"].open(encoding="utf-8", newline="") as handle:
        theme_rows = list(csv.DictReader(handle))
    assert theme_rows
    assert {"thread_month", "building_theme", "company_count", "hiring_post_count"} <= set(theme_rows[0].keys())

    with outputs["company_semantic_spread"].open(encoding="utf-8", newline="") as handle:
        semantic_rows = list(csv.DictReader(handle))
    assert semantic_rows
    assert {
        "company_name",
        "post_count",
        "mean_pairwise_angle_deg",
        "median_pairwise_angle_deg",
        "p90_pairwise_angle_deg",
        "exact_reuse_share",
    } <= set(semantic_rows[0].keys())


def test_recurring_company_hiring_patterns_contains_repeat_companies() -> None:
    outputs = materialize_core_analytics()
    with outputs["recurring_company_hiring_patterns"].open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    assert any(int(row["active_month_count"]) >= 2 for row in rows)


def test_ai_concepts_include_rag_or_agentic_language() -> None:
    outputs = materialize_core_analytics()
    with outputs["ai_concepts_by_month"].open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    concept_names = {row["concept_name"] for row in rows}
    assert {"rag", "agents"} & concept_names
