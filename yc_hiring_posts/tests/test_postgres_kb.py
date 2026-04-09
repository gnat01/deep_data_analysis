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
    company_filters_sql,
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
