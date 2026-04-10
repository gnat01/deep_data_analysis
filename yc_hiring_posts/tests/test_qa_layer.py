import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qa_layer import answer_nl_question_postgres, parse_nl_question


def test_parse_nl_question_extracts_company_and_range(monkeypatch) -> None:
    monkeypatch.setattr("qa_layer.load_company_names", lambda limit=12000: ["DuckDuckGo", "OpenAI"])
    parsed = parse_nl_question("In which months did DuckDuckGo post on HN hiring threads between January 2023 and April 2026?")
    assert parsed.company_name == "DuckDuckGo"
    assert parsed.month_from == "2023-01"
    assert parsed.month_to == "2026-04"
    assert parsed.question_id == 1


def test_answer_nl_question_requires_clarification_when_company_missing(monkeypatch) -> None:
    monkeypatch.setattr("qa_layer.load_company_names", lambda limit=12000: ["DuckDuckGo", "OpenAI"])
    result = answer_nl_question_postgres("In which months did it post on HN hiring threads?")
    assert result["status"] == "clarification_needed"


def test_answer_nl_question_routes_and_summarizes(monkeypatch) -> None:
    monkeypatch.setattr("qa_layer.load_company_names", lambda limit=12000: ["DuckDuckGo", "OpenAI"])

    def fake_router(**kwargs):
        return {
            "question_id": 3,
            "question_text": "Which companies posted hiring ads in every month of 2025?",
            "question_family": "general_analytical_lookup",
            "routed_helper": "companies-every-month-postgres",
            "answer": {
                "entity": "companies_every_month",
                "row_count": 2,
                "rows": [{"company_name": "DuckDuckGo"}, {"company_name": "Coder"}],
            },
        }

    monkeypatch.setattr("qa_layer.answer_catalog_question_postgres", fake_router)
    result = answer_nl_question_postgres("Which companies posted hiring ads in every month of 2025?")
    assert result["status"] == "answered"
    assert result["routed"]["routed_helper"] == "companies-every-month-postgres"
    assert "DuckDuckGo" in result["summary"]
    assert result["parsed"]["company_name"] is None


def test_ai_engineer_summary_uses_clean_default_query(monkeypatch) -> None:
    monkeypatch.setattr("qa_layer.load_company_names", lambda limit=12000: ["DuckDuckGo", "OpenAI"])

    captured = {}

    def fake_router(**kwargs):
        captured.update(kwargs)
        return {
            "question_id": 43,
            "question_text": "In 50 words or less, how did the requirements for an AI engineer change from 2024 - 2026?",
            "question_family": "requirement_change_summary",
            "routed_helper": "role-requirement-change-summary-postgres",
            "answer": {
                "entity": "role_requirement_change_summary",
                "filters": {"query": "AI Engineer"},
                "early_months": ["2024-01", "2024-02"],
                "late_months": ["2026-03", "2026-04"],
                "summary_points": [
                    "Later windows emphasize terms like agents, rag, prompt.",
                    "Earlier windows leaned more on machine, learning, python.",
                    "AI-related emphasis increased for concepts such as agents, gpt_llm, agent_tooling.",
                ],
            },
        }

    monkeypatch.setattr("qa_layer.answer_catalog_question_postgres", fake_router)
    result = answer_nl_question_postgres("In 50 words or less, how did the requirements for an AI engineer change from 2024 - 2026?")
    assert result["status"] == "answered"
    assert captured["query"] == "AI Engineer"
    assert "For AI Engineer, posts shifted between 2024-01 to 2024-02 and 2026-03 to 2026-04." in result["summary"]
    assert "Later posts emphasized agents, rag, prompt." in result["summary"]
    assert "Earlier posts leaned more on machine, learning, python." in result["summary"]


def test_remote_first_question_returns_company_names(monkeypatch) -> None:
    monkeypatch.setattr("qa_layer.load_company_names", lambda limit=12000: ["DuckDuckGo", "OpenAI"])

    def fake_router(**kwargs):
        return {
            "question_id": 9,
            "question_text": "Which companies were remote-first in 2025?",
            "question_family": "general_analytical_lookup",
            "routed_helper": "remote-first-companies-postgres",
            "answer": {
                "entity": "remote_first_companies",
                "row_count": 3,
                "rows": [
                    {"company_name": "DuckDuckGo"},
                    {"company_name": "Coder"},
                    {"company_name": "PlantingSpace"},
                ],
            },
        }

    monkeypatch.setattr("qa_layer.answer_catalog_question_postgres", fake_router)
    result = answer_nl_question_postgres("Which companies were remote-first in 2025?")
    assert result["status"] == "answered"
    assert "DuckDuckGo" in result["summary"]
    assert "Coder" in result["summary"]
