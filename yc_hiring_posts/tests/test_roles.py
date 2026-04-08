import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from roles import ROLE_EXTRACTOR_VERSION, extract_roles_for_thread_month, role_to_dict, write_roles_jsonl


def test_extract_roles_for_thread_month_returns_records() -> None:
    roles = extract_roles_for_thread_month("2025-03")

    assert roles
    assert any(role.post_id == "43243024:43247686" for role in roles)


def test_header_multi_role_post_splits_into_distinct_roles() -> None:
    roles = [role for role in extract_roles_for_thread_month("2025-03") if role.post_id == "43243024:43247942"]
    observed = {role.role_title_observed for role in roles}

    assert "Senior Software Engineer" in observed
    assert "Engineering Lead" in observed
    assert "Product Designer" in observed
    assert "Senior Data Engineer" in observed


def test_header_plus_splits_seniority_variants() -> None:
    roles = [role for role in extract_roles_for_thread_month("2025-03") if role.post_id == "43243024:43247686"]
    observed = {role.role_title_observed for role in roles}

    assert observed == {"Senior Software Engineer", "Staff Software Engineer"}


def test_body_role_section_is_used_when_header_has_no_role_segment() -> None:
    roles = [role for role in extract_roles_for_thread_month("2025-03") if role.post_id == "43243024:43247721"]
    observed = {role.role_title_observed for role in roles}

    assert "Head of Marketing" in observed
    assert "Head of Design" in observed
    assert "Senior/Staff Product Designer" in observed
    assert "Stealth Prototype Hardware Engineer" in observed


def test_body_bullet_roles_with_typographic_dash_are_extracted() -> None:
    roles = [role for role in extract_roles_for_thread_month("2025-08") if role.post_id == "44757794:44760610"]
    observed = {role.role_title_observed for role in roles}

    assert "Senior AI Engineer" in observed
    assert "Senior Full-Stack Engineer" in observed
    assert "Staff Backend Engineer" in observed
    assert "Founding Account Executive" in observed


def test_write_roles_jsonl_writes_json_lines(tmp_path: Path, monkeypatch) -> None:
    import storage

    roles = extract_roles_for_thread_month("2025-03")[:3]
    monkeypatch.setattr(storage, "project_root", lambda: tmp_path)
    output_path = write_roles_jsonl("2025-03", roles)

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    parsed = json.loads(lines[0])
    assert parsed["post_id"]
    assert parsed["misc"]["role_extractor_version"] == ROLE_EXTRACTOR_VERSION


def test_role_to_dict_returns_json_friendly_payload() -> None:
    first = extract_roles_for_thread_month("2025-03")[0]
    payload = role_to_dict(first)

    assert payload["role_id"]
