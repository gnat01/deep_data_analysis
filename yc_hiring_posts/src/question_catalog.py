"""Classify and annotate the knowledge-base question bank."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import json
import re

from storage import project_root


@dataclass(frozen=True)
class QuestionAnnotation:
    question_id: int
    question_text: str
    question_family: str
    difficulty: str
    primary_entity: str
    time_scope: str
    expected_output_shape: str
    primary_helper: str
    secondary_helper: str | None
    evidence_required: str
    evaluation_mode: str
    notes: str


QUESTION_LINE_RE = re.compile(r"^\s*(\d+)\.\s+(.*\S)\s*$")


def possible_questions_path() -> Path:
    return project_root() / "possible_qs.md"


def question_catalog_json_path() -> Path:
    return project_root() / "docs" / "question_catalog.json"


def question_catalog_md_path() -> Path:
    return project_root() / "docs" / "question_catalog.md"


def parse_possible_questions(path: Path | None = None) -> list[tuple[int, str]]:
    """Parse numbered questions from the markdown bank."""

    target_path = path or possible_questions_path()
    rows: list[tuple[int, str]] = []
    for line in target_path.read_text(encoding="utf-8").splitlines():
        match = QUESTION_LINE_RE.match(line)
        if match:
            rows.append((int(match.group(1)), match.group(2).strip()))
    return rows


def infer_time_scope(question: str) -> str:
    lowered = question.lower()
    if "year by year" in lowered or "every year" in lowered:
        return "yearly"
    if "2026 ytd" in lowered:
        return "cross_year_comparison"
    if "between" in lowered or "from " in lowered or "over time" in lowered or "across all its posts" in lowered:
        return "range"
    if "same month" in lowered or "single month" in lowered or "in 2026" in lowered or "in 2025" in lowered or "in 2024" in lowered or "in 2023" in lowered:
        return "slice"
    return "all_time"


def classify_question_family(question: str) -> str:
    lowered = question.lower()
    if "in which months did" in lowered or "which months did" in lowered:
        return "company_activity_timeline"
    if "how did the requirements" in lowered or "in 50 words or less" in lowered:
        return "requirement_change_summary"
    if "evidence rows" in lowered or "show the posts and roles" in lowered:
        return "evidence_retrieval"
    if "did " in lowered or "was there" in lowered or "any companies" in lowered:
        if "hire" in lowered or "hiring" in lowered or "role" in lowered:
            return "company_role_presence"
        return "existence_lookup"
    if "appeared for the first time" in lowered or "returned to hiring" in lowered or "posted in every month" in lowered:
        return "company_activity_ranking"
    if "highest number of distinct companies" in lowered or "highest number of distinct roles" in lowered:
        return "month_summary_ranking"
    if "remote" in lowered and ("share" in lowered or "distribution" in lowered or "prefer" in lowered or "shifted" in lowered):
        return "remote_mix_analysis"
    if "compensation" in lowered or "pay" in lowered:
        return "compensation_history"
    if "visa sponsorship" in lowered:
        return "attribute_presence"
    if "most common roles" in lowered or "platform engineers" in lowered or "data engineers" in lowered or "design roles" in lowered:
        return "role_company_lookup"
    if "role families" in lowered or "ai roles" in lowered:
        return "role_family_trend"
    if "mcp" in lowered or "agents" in lowered or "rag" in lowered or "llm" in lowered or "gpt" in lowered or "generative ai" in lowered or "ai engineer" in lowered:
        return "ai_concept_trend"
    if "product themes" in lowered or "what they were building" in lowered:
        return "company_theme_change"
    if "widest variation" in lowered or "least variation" in lowered or "drift score" in lowered or "changed their hiring narrative" in lowered or "pivot" in lowered or "change score" in lowered or "semantic spread" in lowered:
        return "company_change_analysis"
    if "headquartered" in lowered:
        return "company_geography"
    if "post lengths" in lowered or "longer posts" in lowered:
        return "post_shape_analysis"
    return "general_analytical_lookup"


def family_defaults(question_family: str) -> dict[str, str]:
    mapping: dict[str, dict[str, str]] = {
        "company_activity_timeline": {
            "difficulty": "medium",
            "primary_entity": "company",
            "expected_output_shape": "ordered_month_list_with_counts_and_evidence",
            "primary_helper": "company_activity_postgres",
            "secondary_helper": "search_postgres_posts",
            "evidence_required": "high",
            "evaluation_mode": "set_match_plus_evidence",
            "notes": "Answer should include ordered months and at least a few source-linked posts.",
        },
        "company_role_presence": {
            "difficulty": "medium",
            "primary_entity": "company_role",
            "expected_output_shape": "boolean_plus_matched_months_plus_evidence",
            "primary_helper": "company_role_presence_postgres",
            "secondary_helper": "search_postgres_roles",
            "evidence_required": "high",
            "evaluation_mode": "boolean_plus_month_set",
            "notes": "Best for yes/no questions about a company hiring for a role or concept in a date range.",
        },
        "company_activity_ranking": {
            "difficulty": "medium",
            "primary_entity": "company",
            "expected_output_shape": "ranked_table",
            "primary_helper": "company_activity_ranking_postgres",
            "secondary_helper": "month_summary_postgres",
            "evidence_required": "medium",
            "evaluation_mode": "ranked_top_k",
            "notes": "Ranking answers should expose the sorting metric clearly.",
        },
        "month_summary_ranking": {
            "difficulty": "medium",
            "primary_entity": "month",
            "expected_output_shape": "ranked_month_table",
            "primary_helper": "month_summary_postgres",
            "secondary_helper": None,
            "evidence_required": "medium",
            "evaluation_mode": "exact_or_ranked_top_k",
            "notes": "Good candidate for deterministic aggregate SQL.",
        },
        "remote_mix_analysis": {
            "difficulty": "medium",
            "primary_entity": "remote_status",
            "expected_output_shape": "timeseries_or_distribution_table",
            "primary_helper": "remote_mix_postgres",
            "secondary_helper": "company_remote_mix_postgres",
            "evidence_required": "medium",
            "evaluation_mode": "aggregate_value_check",
            "notes": "Answers should clarify denominator and whether counts are posts or roles.",
        },
        "compensation_history": {
            "difficulty": "hard",
            "primary_entity": "compensation",
            "expected_output_shape": "timeline_with_bands_and_evidence",
            "primary_helper": "compensation_history_postgres",
            "secondary_helper": "company_activity_postgres",
            "evidence_required": "high",
            "evaluation_mode": "timeline_with_manual_review",
            "notes": "Compensation is noisier than company/month facts and needs evidence rows.",
        },
        "attribute_presence": {
            "difficulty": "easy",
            "primary_entity": "attribute",
            "expected_output_shape": "company_list_with_evidence",
            "primary_helper": "attribute_presence_postgres",
            "secondary_helper": "search_postgres_posts",
            "evidence_required": "high",
            "evaluation_mode": "set_match_plus_evidence",
            "notes": "Good fit for visa and similar explicit mentions.",
        },
        "role_company_lookup": {
            "difficulty": "medium",
            "primary_entity": "role",
            "expected_output_shape": "company_list_or_company_month_table",
            "primary_helper": "companies_for_role_postgres",
            "secondary_helper": "search_postgres_roles",
            "evidence_required": "high",
            "evaluation_mode": "set_match_plus_evidence",
            "notes": "Should return companies, months, and matched role rows.",
        },
        "role_family_trend": {
            "difficulty": "medium",
            "primary_entity": "role_family",
            "expected_output_shape": "timeseries_or_ranked_trend_table",
            "primary_helper": "role_family_timeline_postgres",
            "secondary_helper": "month_summary_postgres",
            "evidence_required": "medium",
            "evaluation_mode": "aggregate_value_check",
            "notes": "Trend answers should be explicit about the comparison window.",
        },
        "ai_concept_trend": {
            "difficulty": "hard",
            "primary_entity": "ai_concept",
            "expected_output_shape": "timeseries_with_matched_examples",
            "primary_helper": "ai_concept_timeline_postgres",
            "secondary_helper": "search_postgres_posts",
            "evidence_required": "high",
            "evaluation_mode": "trend_plus_evidence",
            "notes": "Concept-trend answers must stay grounded in the defined dictionary.",
        },
        "requirement_change_summary": {
            "difficulty": "hard",
            "primary_entity": "role_requirements",
            "expected_output_shape": "crisp_summary_with_supporting_evidence",
            "primary_helper": "role_requirement_change_summary_postgres",
            "secondary_helper": "search_postgres_posts",
            "evidence_required": "high",
            "evaluation_mode": "summary_with_evidence_review",
            "notes": "These are concise synthesis questions. The answer should summarize changes cleanly, not reduce them to yes/no.",
        },
        "company_theme_change": {
            "difficulty": "hard",
            "primary_entity": "company_theme",
            "expected_output_shape": "timeline_or_ranked_change_table_with_evidence",
            "primary_helper": "company_theme_history_postgres",
            "secondary_helper": "company_change_summary_postgres",
            "evidence_required": "high",
            "evaluation_mode": "heuristic_review",
            "notes": "Theme-change questions are heuristic and should not be overclaimed.",
        },
        "company_change_analysis": {
            "difficulty": "hard",
            "primary_entity": "company_change",
            "expected_output_shape": "ranked_table_or_metric_summary_with_evidence",
            "primary_helper": "company_change_summary_postgres",
            "secondary_helper": "changed_companies_postgres",
            "evidence_required": "high",
            "evaluation_mode": "heuristic_review",
            "notes": "Use metrics plus evidence rows; avoid pretending these are crisp facts.",
        },
        "company_geography": {
            "difficulty": "hard",
            "primary_entity": "company_geography",
            "expected_output_shape": "distribution_table",
            "primary_helper": "company_geography_postgres",
            "secondary_helper": None,
            "evidence_required": "medium",
            "evaluation_mode": "aggregate_value_check",
            "notes": "Requires care because headquarters may not be present in the current normalized schema.",
        },
        "post_shape_analysis": {
            "difficulty": "medium",
            "primary_entity": "post",
            "expected_output_shape": "distribution_or_timeseries_table",
            "primary_helper": "post_shape_summary_postgres",
            "secondary_helper": "company_activity_postgres",
            "evidence_required": "medium",
            "evaluation_mode": "aggregate_value_check",
            "notes": "Length- and shape-based questions are straightforward once defined carefully.",
        },
        "evidence_retrieval": {
            "difficulty": "easy",
            "primary_entity": "evidence",
            "expected_output_shape": "evidence_row_list",
            "primary_helper": "evidence_lookup_postgres",
            "secondary_helper": "search_postgres_posts",
            "evidence_required": "high",
            "evaluation_mode": "evidence_row_match",
            "notes": "These should optimize for source-linked recall rather than summary elegance.",
        },
        "existence_lookup": {
            "difficulty": "easy",
            "primary_entity": "existence",
            "expected_output_shape": "boolean_plus_short_evidence",
            "primary_helper": "existence_check_postgres",
            "secondary_helper": "search_postgres_posts",
            "evidence_required": "high",
            "evaluation_mode": "boolean_plus_evidence",
            "notes": "Simple yes/no answers still need supporting rows.",
        },
        "general_analytical_lookup": {
            "difficulty": "hard",
            "primary_entity": "mixed",
            "expected_output_shape": "custom_analysis",
            "primary_helper": "composite_question_router",
            "secondary_helper": "search_postgres_posts",
            "evidence_required": "high",
            "evaluation_mode": "manual_review",
            "notes": "Likely needs composition of multiple helpers.",
        },
    }
    return mapping.get(
        question_family,
        mapping["general_analytical_lookup"],
    )


def annotate_question(question_id: int, question_text: str) -> QuestionAnnotation:
    family = classify_question_family(question_text)
    defaults = family_defaults(family)
    return QuestionAnnotation(
        question_id=question_id,
        question_text=question_text,
        question_family=family,
        difficulty=defaults["difficulty"],
        primary_entity=defaults["primary_entity"],
        time_scope=infer_time_scope(question_text),
        expected_output_shape=defaults["expected_output_shape"],
        primary_helper=defaults["primary_helper"],
        secondary_helper=defaults["secondary_helper"],
        evidence_required=defaults["evidence_required"],
        evaluation_mode=defaults["evaluation_mode"],
        notes=defaults["notes"],
    )


def build_question_catalog(path: Path | None = None) -> list[QuestionAnnotation]:
    return [annotate_question(question_id, question_text) for question_id, question_text in parse_possible_questions(path)]


def render_question_catalog_markdown(annotations: list[QuestionAnnotation]) -> str:
    lines = [
        "# Question Catalog",
        "",
        "This catalog annotates the question bank into reusable KB-helper families.",
        "",
        "## Fields",
        "",
        "- `question_family`: the reusable analytical pattern",
        "- `expected_output_shape`: the answer contract the helper should satisfy",
        "- `evaluation_mode`: how correctness should be judged",
        "- `primary_helper`: the main KB helper Task 3 should eventually implement",
        "",
        "## Annotated Questions",
        "",
    ]
    for annotation in annotations:
        lines.extend(
            [
                f"### Q{annotation.question_id}",
                "",
                f"- question: {annotation.question_text}",
                f"- family: `{annotation.question_family}`",
                f"- difficulty: `{annotation.difficulty}`",
                f"- primary entity: `{annotation.primary_entity}`",
                f"- time scope: `{annotation.time_scope}`",
                f"- expected output: `{annotation.expected_output_shape}`",
                f"- primary helper: `{annotation.primary_helper}`",
                f"- secondary helper: `{annotation.secondary_helper}`" if annotation.secondary_helper else "- secondary helper: `none`",
                f"- evidence required: `{annotation.evidence_required}`",
                f"- evaluation mode: `{annotation.evaluation_mode}`",
                f"- notes: {annotation.notes}",
                "",
            ]
        )
    families: dict[str, int] = {}
    for annotation in annotations:
        families[annotation.question_family] = families.get(annotation.question_family, 0) + 1
    lines.extend(["## Family Coverage", ""])
    for family, count in sorted(families.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- `{family}`: {count}")
    lines.append("")
    return "\n".join(lines)


def write_question_catalog(
    annotations: list[QuestionAnnotation] | None = None,
    *,
    md_path: Path | None = None,
    json_path: Path | None = None,
) -> dict[str, Path]:
    catalog = annotations or build_question_catalog()
    resolved_md_path = md_path or question_catalog_md_path()
    resolved_json_path = json_path or question_catalog_json_path()
    resolved_md_path.write_text(render_question_catalog_markdown(catalog), encoding="utf-8")
    resolved_json_path.write_text(json.dumps([asdict(item) for item in catalog], indent=2), encoding="utf-8")
    return {"markdown_path": resolved_md_path, "json_path": resolved_json_path}
