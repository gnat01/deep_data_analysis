import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from question_catalog import (  # noqa: E402
    build_question_catalog,
    parse_possible_questions,
    render_question_catalog_markdown,
    write_question_catalog,
)


def test_parse_possible_questions_reads_numbered_rows(tmp_path: Path) -> None:
    source = tmp_path / "possible_qs.md"
    source.write_text("# Qs\n\n1. First question?\n\n2. Second question?\n", encoding="utf-8")
    rows = parse_possible_questions(source)
    assert rows == [(1, "First question?"), (2, "Second question?")]


def test_build_question_catalog_classifies_known_families(tmp_path: Path) -> None:
    source = tmp_path / "possible_qs.md"
    source.write_text(
        "\n".join(
            [
                "1. In which months did DuckDuckGo post on HN hiring threads between January 2023 and April 2026?",
                "2. Was there any month between December 2024 and January 2026 when DuckDuckGo was hiring for data science?",
                "3. Which companies explicitly mentioned MCP, tool use, agents, evals, or RAG in 2026?",
            ]
        ),
        encoding="utf-8",
    )
    catalog = build_question_catalog(source)
    assert catalog[0].question_family == "company_activity_timeline"
    assert catalog[1].question_family == "company_role_presence"
    assert catalog[2].question_family == "ai_concept_trend"


def test_requirement_change_summary_does_not_fall_into_existence_lookup(tmp_path: Path) -> None:
    source = tmp_path / "possible_qs.md"
    source.write_text(
        "1. In 50 words or less, how did the requirements for an AI engineer change from 2024 - 2026?\n",
        encoding="utf-8",
    )
    catalog = build_question_catalog(source)
    assert catalog[0].question_family == "requirement_change_summary"
    assert catalog[0].expected_output_shape == "crisp_summary_with_supporting_evidence"


def test_write_question_catalog_emits_markdown_and_json(tmp_path: Path) -> None:
    source = tmp_path / "possible_qs.md"
    source.write_text("1. Which month had the highest number of distinct companies posting?\n", encoding="utf-8")
    catalog = build_question_catalog(source)
    outputs = write_question_catalog(
        catalog,
        md_path=tmp_path / "question_catalog.md",
        json_path=tmp_path / "question_catalog.json",
    )
    markdown_text = outputs["markdown_path"].read_text(encoding="utf-8")
    json_rows = json.loads(outputs["json_path"].read_text(encoding="utf-8"))
    assert "Question Catalog" in markdown_text
    assert "month_summary_ranking" in markdown_text
    assert json_rows[0]["question_family"] == "month_summary_ranking"
    assert render_question_catalog_markdown(catalog).startswith("# Question Catalog")
