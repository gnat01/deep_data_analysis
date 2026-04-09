import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from kb_router import answer_catalog_question_postgres


def test_router_routes_q3_to_every_month_helper(monkeypatch) -> None:
    monkeypatch.setattr(
        "kb_router.companies_every_month_postgres",
        lambda **kwargs: {"entity": "companies_every_month", "rows": [{"company_name": "DuckDuckGo"}]},
    )
    result = answer_catalog_question_postgres(question_id=3)
    assert result["routed_helper"] == "companies-every-month-postgres"
    assert result["answer"]["entity"] == "companies_every_month"


def test_router_routes_q43_to_requirement_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        "kb_router.role_requirement_change_summary_postgres",
        lambda **kwargs: {"entity": "role_requirement_change_summary", "summary_points": ["Later windows emphasize agents."]},
    )
    result = answer_catalog_question_postgres(question_id=43)
    assert result["routed_helper"] == "role-requirement-change-summary-postgres"
    assert result["answer"]["entity"] == "role_requirement_change_summary"


def test_router_marks_q45_unavailable() -> None:
    result = answer_catalog_question_postgres(question_id=45)
    assert result["routed_helper"] == "unavailable_company_geography"
    assert result["answer"]["entity"] == "unavailable"
