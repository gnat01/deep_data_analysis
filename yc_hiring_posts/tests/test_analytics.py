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
    assert outputs["remote_status_trends_by_month"].exists()
    assert outputs["remote_status_share_by_month"].exists()
    assert outputs["role_family_trends_by_month"].exists()
    assert outputs["recurring_company_hiring_patterns"].exists()
    assert outputs["company_posting_counts_visual"].exists()
    assert outputs["remote_status_trends_visual"].exists()
    assert outputs["remote_status_share_visual"].exists()
    assert outputs["role_family_trends_visual"].exists()
    assert outputs["recurring_company_hiring_patterns_visual"].exists()
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


def test_recurring_company_hiring_patterns_contains_repeat_companies() -> None:
    outputs = materialize_core_analytics()
    with outputs["recurring_company_hiring_patterns"].open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    assert any(int(row["active_month_count"]) >= 2 for row in rows)
