import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from companies import (
    derive_company_match_key,
    normalize_companies_for_thread_month,
    normalize_and_write_companies,
)


def test_derive_company_match_key_is_conservative() -> None:
    assert derive_company_match_key("Autotab (YC S23)") == "autotab"
    assert derive_company_match_key("Aha! (https://www.aha.io)") == "aha"
    assert derive_company_match_key("* CLOSED *") is None


def test_normalize_companies_for_thread_month_builds_company_dimension() -> None:
    companies, posts, roles = normalize_companies_for_thread_month("2025-03")

    assert companies
    angel = next(company for company in companies if company.company_name_observed_preferred == "AngelList")
    assert angel.company_match_key == "angellist"
    assert angel.company_id
    angel_post = next(post for post in posts if post["company_name_observed"] == "AngelList")
    assert angel_post["company_id"] == angel.company_id
    angel_roles = [role for role in roles if role["post_id"] == angel_post["post_id"]]
    assert angel_roles
    assert all(role["company_id"] == angel.company_id for role in angel_roles)


def test_normalize_and_write_companies_persists_outputs(tmp_path: Path, monkeypatch) -> None:
    import storage

    monkeypatch.setattr(storage, "project_root", lambda: tmp_path)
    (tmp_path / "data" / "interim" / "2025-03").mkdir(parents=True, exist_ok=True)
    source_dir = PROJECT_ROOT / "data" / "interim" / "2025-03"
    for name in ["posts_normalized.jsonl", "roles.jsonl"]:
        (tmp_path / "data" / "interim" / "2025-03" / name).write_text(
            (source_dir / name).read_text(encoding="utf-8"),
            encoding="utf-8",
        )

    companies_path, posts_path, roles_path = normalize_and_write_companies("2025-03")

    assert companies_path.exists()
    assert posts_path.exists()
    assert roles_path is not None and roles_path.exists()
    first_company = json.loads(companies_path.read_text(encoding="utf-8").splitlines()[0])
    assert first_company["company_id"]
