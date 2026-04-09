import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from postgres_kb import (
    DEFAULT_DB_SCHEMA,
    TABLE_SPECS,
    company_activity_timeline_postgres,
    company_filters_sql,
    company_role_presence_postgres,
    insert_sql_for_spec,
    month_filters_sql,
    postgres_schema_sql,
    summarize_search_result,
    row_values_for_spec,
)


def test_postgres_schema_template_expands_schema_name() -> None:
    sql = postgres_schema_sql(schema="test_schema")
    assert "__SCHEMA__" not in sql
    assert "CREATE SCHEMA IF NOT EXISTS test_schema;" in sql
    assert "CREATE TABLE IF NOT EXISTS test_schema.posts" in sql


def test_insert_sql_for_posts_uses_jsonb_casts() -> None:
    spec = next(spec for spec in TABLE_SPECS if spec.table_name == "posts")
    sql = insert_sql_for_spec(spec, schema=DEFAULT_DB_SCHEMA)
    assert f"INSERT INTO {DEFAULT_DB_SCHEMA}.posts" in sql
    assert "ON CONFLICT (post_id) DO UPDATE SET" in sql
    assert "%s::jsonb" in sql


def test_row_values_for_spec_serializes_json_payloads() -> None:
    spec = next(spec for spec in TABLE_SPECS if spec.table_name == "companies")
    row = {
        "company_id": "company_123",
        "company_name_observed_preferred": "Example Co",
        "company_match_key": "exampleco",
        "company_name_variants": ["Example Co", "ExampleCo"],
        "company_website": "https://example.com",
        "yc_batch": None,
        "entity_resolution_notes": "test",
        "first_seen_thread_month": "2025-01",
        "last_seen_thread_month": "2025-03",
        "misc": {"variant_count": 2},
    }
    values = row_values_for_spec(row, spec)
    assert len(values) == len(spec.columns)
    variants_value = values[spec.columns.index("company_name_variants")]
    misc_value = values[spec.columns.index("misc")]
    payload_value = values[spec.columns.index("source_payload_json")]
    assert json.loads(variants_value) == ["Example Co", "ExampleCo"]
    assert json.loads(misc_value) == {"variant_count": 2}
    assert json.loads(payload_value)["company_id"] == "company_123"


def test_month_and_company_filter_helpers_build_expected_clauses() -> None:
    params: list[object] = []
    month_clauses = month_filters_sql("t.thread_month", "2024-01", "2024-06", params)
    company_clauses = company_filters_sql(
        "Datadog",
        "p.company_id",
        "coalesce(c.company_name_observed_preferred, p.company_name_observed)",
        params,
    )
    assert month_clauses == ["t.thread_month >= %s", "t.thread_month <= %s"]
    assert len(company_clauses) == 1
    assert "ILIKE %s" in company_clauses[0]
    assert params == ["2024-01", "2024-06", "Datadog", "Datadog", "%Datadog%"]


def test_summarize_search_result_truncates_rows() -> None:
    result = {
        "entity": "posts",
        "schema": "yc_hiring",
        "filters": {"query": "data science"},
        "row_count": 7,
        "rows": [{"post_id": f"post_{index}"} for index in range(7)],
    }
    summary = summarize_search_result(result)
    assert summary["entity"] == "posts"
    assert summary["row_count"] == 7
    assert len(summary["sample_rows"]) == 5
    assert summary["sample_rows"][0]["post_id"] == "post_0"


def test_company_helper_functions_return_expected_shapes(monkeypatch) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.description = []
            self._rows = []

        def execute(self, sql, params):  # noqa: ANN001
            if "GROUP BY t.thread_month" in sql:
                self.description = [
                    ("thread_month",),
                    ("post_count",),
                    ("distinct_post_count",),
                    ("role_count",),
                    ("role_family_count",),
                    ("role_families",),
                ]
                self._rows = [("2025-01", 2, 2, 3, 2, ["data", "engineering"])]
            elif "COUNT(*) AS matched_role_count" in sql:
                self.description = [
                    ("matched_role_count",),
                    ("matched_month_count",),
                    ("matched_months",),
                ]
                self._rows = [(2, 1, ["2025-01"])]
            elif "FROM yc_hiring.posts p" in sql:
                self.description = [
                    ("thread_month",),
                    ("company_name",),
                    ("remote_status",),
                    ("compensation_text",),
                    ("post_text_clean",),
                    ("source_url",),
                ]
                self._rows = [("2025-01", "DuckDuckGo", "remote", None, "Example post", "https://example.com/post")]
            else:
                self.description = [
                    ("thread_month",),
                    ("company_name",),
                    ("role_title_observed",),
                    ("role_title_normalized",),
                    ("role_family",),
                    ("role_remote_status",),
                    ("post_text_clean",),
                    ("source_url",),
                ]
                self._rows = [("2025-01", "DuckDuckGo", "Data Scientist", "Data Scientist", "data", "remote", "Example post", "https://example.com/post")]

        def fetchall(self):
            return list(self._rows)

        def fetchone(self):
            return self._rows[0]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    monkeypatch.setattr("postgres_kb.connect_postgres", lambda database_url=None: FakeConnection())

    timeline = company_activity_timeline_postgres(company_name="DuckDuckGo")
    assert timeline["active_month_count"] == 1
    assert timeline["months"][0]["thread_month"] == "2025-01"
    assert timeline["evidence_rows"][0]["company_name"] == "DuckDuckGo"

    presence = company_role_presence_postgres(company_name="DuckDuckGo", query="data science")
    assert presence["match_found"] is True
    assert presence["matched_role_count"] == 2
    assert presence["matched_months"] == ["2025-01"]
