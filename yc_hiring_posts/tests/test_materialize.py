import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from materialize import available_materialization_months, materialize_v1_core_tables


def test_available_materialization_months_returns_fully_processed_months() -> None:
    months = available_materialization_months()

    assert months[0] == "2023-01"
    assert months[-1] == "2026-04"
    assert len(months) == 40


def test_materialize_v1_core_tables_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    import storage

    monkeypatch.setattr(storage, "project_root", lambda: tmp_path)

    for month in ["2024-05", "2024-11", "2025-03", "2025-08", "2026-01"]:
        for relative in [
            Path("data/raw") / month / "thread.json",
            Path("data/raw") / month / "posts.jsonl",
            Path("data/interim") / month / "posts_normalized.jsonl",
            Path("data/interim") / month / "roles.jsonl",
            Path("data/interim") / month / "companies.jsonl",
        ]:
            target = tmp_path / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            source = PROJECT_ROOT / relative
            target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

    source_index_target = tmp_path / "data" / "source_index.csv"
    source_index_target.parent.mkdir(parents=True, exist_ok=True)
    source_index_target.write_text((PROJECT_ROOT / "data" / "source_index.csv").read_text(encoding="utf-8"), encoding="utf-8")

    outputs = materialize_v1_core_tables()

    assert outputs["threads"].exists()
    assert outputs["raw_posts"].exists()
    assert outputs["posts"].exists()
    assert outputs["roles"].exists()
    assert outputs["companies"].exists()
    assert outputs["manifest"].exists()

    manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    assert manifest["included_months"] == ["2024-05", "2024-11", "2025-03", "2025-08", "2026-01"]
    assert manifest["row_counts"]["threads"] == 5
