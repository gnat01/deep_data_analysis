"""PostgreSQL-backed knowledge-base initialization, loading, and retrieval."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from storage import processed_data_dir, project_root


DEFAULT_DB_SCHEMA = "yc_hiring"


@dataclass(frozen=True)
class TableSpec:
    """Metadata for loading one processed JSONL table into PostgreSQL."""

    table_name: str
    source_filename: str
    conflict_target: tuple[str, ...]
    columns: tuple[str, ...]
    jsonb_columns: tuple[str, ...] = ()


TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        table_name="threads",
        source_filename="threads.jsonl",
        conflict_target=("thread_id",),
        columns=(
            "thread_id",
            "thread_month",
            "thread_date",
            "thread_title",
            "source_url",
            "source_system",
            "collection_timestamp_utc",
            "raw_payload_hash",
            "raw_schema_version",
            "source_payload_json",
        ),
        jsonb_columns=("source_payload_json",),
    ),
    TableSpec(
        table_name="raw_posts",
        source_filename="raw_posts.jsonl",
        conflict_target=("raw_post_id",),
        columns=(
            "raw_post_id",
            "thread_id",
            "source_comment_id",
            "author_handle",
            "posted_at_utc",
            "edited_at_utc",
            "raw_text",
            "source_url",
            "collection_timestamp_utc",
            "is_deleted",
            "is_dead",
            "raw_html",
            "raw_payload_json",
            "misc",
            "raw_payload_hash",
            "raw_schema_version",
            "source_payload_json",
        ),
        jsonb_columns=("raw_payload_json", "misc", "source_payload_json"),
    ),
    TableSpec(
        table_name="companies",
        source_filename="companies.jsonl",
        conflict_target=("company_id",),
        columns=(
            "company_id",
            "company_name_observed_preferred",
            "company_match_key",
            "company_name_variants",
            "company_website",
            "yc_batch",
            "entity_resolution_notes",
            "first_seen_thread_month",
            "last_seen_thread_month",
            "misc",
            "source_payload_json",
        ),
        jsonb_columns=("company_name_variants", "misc", "source_payload_json"),
    ),
    TableSpec(
        table_name="posts",
        source_filename="posts.jsonl",
        conflict_target=("post_id",),
        columns=(
            "post_id",
            "raw_post_id",
            "thread_id",
            "company_id",
            "company_name_observed",
            "is_hiring_post",
            "location_text",
            "remote_status",
            "employment_type",
            "visa_sponsorship_text",
            "compensation_text",
            "compensation_text_accuracy",
            "funding",
            "post_text_clean",
            "misc",
            "parser_version",
            "parse_confidence",
            "created_at_utc",
            "source_payload_json",
        ),
        jsonb_columns=("misc", "source_payload_json"),
    ),
    TableSpec(
        table_name="roles",
        source_filename="roles.jsonl",
        conflict_target=("role_id",),
        columns=(
            "role_id",
            "post_id",
            "company_id",
            "role_title_observed",
            "role_title_normalized",
            "role_family",
            "role_subfamily",
            "seniority",
            "headcount_text",
            "skills_text",
            "responsibilities_text",
            "requirements_text",
            "role_location_text",
            "role_remote_status",
            "role_compensation_id",
            "misc",
            "source_payload_json",
        ),
        jsonb_columns=("misc", "source_payload_json"),
    ),
)


def default_database_url() -> str | None:
    """Return the preferred database URL from environment variables."""

    return os.getenv("YC_HIRING_POSTS_DB_URL") or os.getenv("DATABASE_URL")


def postgres_schema_template_path() -> Path:
    """Return the PostgreSQL schema template path."""

    return project_root() / "sql" / "postgres_schema.sql"


def postgres_schema_sql(schema: str = DEFAULT_DB_SCHEMA) -> str:
    """Load and parameterize the PostgreSQL schema template."""

    template = postgres_schema_template_path().read_text(encoding="utf-8")
    return template.replace("__SCHEMA__", schema)


def connect_postgres(database_url: str | None = None):
    """Create a psycopg connection to PostgreSQL."""

    resolved_database_url = database_url or default_database_url()
    if not resolved_database_url:
        raise ValueError("Provide a PostgreSQL URL via --database-url or YC_HIRING_POSTS_DB_URL.")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency error path
        raise RuntimeError("psycopg is required for Step 18. Install project requirements first.") from exc
    return psycopg.connect(resolved_database_url)


def initialize_postgres_kb(database_url: str | None = None, schema: str = DEFAULT_DB_SCHEMA) -> dict[str, object]:
    """Initialize the PostgreSQL knowledge-base schema."""

    sql = postgres_schema_sql(schema=schema)
    with connect_postgres(database_url) as connection:
        connection.execute(sql)
        connection.commit()
    return {"database_url_supplied": bool(database_url or default_database_url()), "schema": schema}


def processed_table_path(source_filename: str) -> Path:
    """Return the canonical processed-table JSONL path."""

    return processed_data_dir() / "v1_core_tables" / source_filename


def load_jsonl_rows(path: Path) -> list[dict[str, object]]:
    """Load JSONL rows from disk."""

    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def serialize_record_value(value: object) -> object:
    """Convert Python values to PostgreSQL-friendly insert values."""

    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return value


def row_values_for_spec(row: dict[str, object], spec: TableSpec) -> list[object]:
    """Return ordered values for one loader table spec."""

    record = dict(row)
    record["source_payload_json"] = json.dumps(row, sort_keys=True)
    return [serialize_record_value(record.get(column)) for column in spec.columns]


def insert_sql_for_spec(spec: TableSpec, schema: str = DEFAULT_DB_SCHEMA) -> str:
    """Return the upsert SQL for one table spec."""

    placeholder_parts = []
    for column in spec.columns:
        if column in spec.jsonb_columns:
            placeholder_parts.append("%s::jsonb")
        else:
            placeholder_parts.append("%s")
    update_columns = [column for column in spec.columns if column not in spec.conflict_target]
    update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    return (
        f"INSERT INTO {schema}.{spec.table_name} ({', '.join(spec.columns)}) "
        f"VALUES ({', '.join(placeholder_parts)}) "
        f"ON CONFLICT ({', '.join(spec.conflict_target)}) DO UPDATE SET {update_sql}"
    )


def load_postgres_kb(
    database_url: str | None = None,
    schema: str = DEFAULT_DB_SCHEMA,
    batch_size: int = 500,
) -> dict[str, object]:
    """Load processed core tables into PostgreSQL."""

    summary: dict[str, object] = {"schema": schema, "tables": {}}
    with connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            for spec in TABLE_SPECS:
                rows = load_jsonl_rows(processed_table_path(spec.source_filename))
                sql = insert_sql_for_spec(spec, schema=schema)
                value_rows = [row_values_for_spec(row, spec) for row in rows]
                for start in range(0, len(value_rows), batch_size):
                    cursor.executemany(sql, value_rows[start : start + batch_size])
                summary["tables"][spec.table_name] = {
                    "source_filename": spec.source_filename,
                    "row_count": len(rows),
                }
        connection.commit()
    return summary


def inspect_postgres_kb(database_url: str | None = None, schema: str = DEFAULT_DB_SCHEMA) -> dict[str, object]:
    """Return per-table row counts from the PostgreSQL knowledge base."""

    counts: dict[str, int] = {}
    with connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            for spec in TABLE_SPECS:
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{spec.table_name}")
                counts[spec.table_name] = int(cursor.fetchone()[0])
    return {"schema": schema, "table_counts": counts}


def month_filters_sql(
    field_sql: str,
    month_from: str | None,
    month_to: str | None,
    params: list[object],
) -> list[str]:
    """Build chronological month filter SQL fragments."""

    clauses: list[str] = []
    if month_from:
        clauses.append(f"{field_sql} >= %s")
        params.append(month_from)
    if month_to:
        clauses.append(f"{field_sql} <= %s")
        params.append(month_to)
    return clauses


def remote_filters_sql(
    field_sql: str,
    remote_status: str | None,
    params: list[object],
) -> list[str]:
    """Build remote-status SQL fragments."""

    clauses: list[str] = []
    if remote_status:
        clauses.append(f"{field_sql} = %s")
        params.append(remote_status)
    return clauses


def company_filters_sql(
    company_name: str | None,
    company_id_field_sql: str,
    company_name_field_sql: str,
    params: list[object],
) -> list[str]:
    """Build company filter SQL fragments."""

    if not company_name:
        return []
    params.extend([company_name, company_name, f"%{company_name}%"])
    return [
        "("
        f"lower({company_id_field_sql}) = lower(%s) "
        f"OR lower({company_name_field_sql}) = lower(%s) "
        f"OR {company_name_field_sql} ILIKE %s"
        ")"
    ]


def role_family_filters_sql(
    role_family: str | None,
    field_sql: str,
    params: list[object],
) -> list[str]:
    """Build role-family SQL fragments."""

    if not role_family:
        return []
    params.append(role_family)
    return [f"{field_sql} = %s"]


def search_posts_postgres(
    *,
    database_url: str | None = None,
    schema: str = DEFAULT_DB_SCHEMA,
    query: str | None = None,
    company_name: str | None = None,
    role_family: str | None = None,
    remote_status: str | None = None,
    month_from: str | None = None,
    month_to: str | None = None,
    limit: int = 20,
) -> dict[str, object]:
    """Search hiring posts with structured filters and optional full-text ranking."""

    select_params: list[object] = []
    where_params: list[object] = []
    where_clauses = ["p.is_hiring_post = TRUE"]
    where_clauses.extend(company_filters_sql(company_name, "p.company_id", "coalesce(c.company_name_observed_preferred, p.company_name_observed)", where_params))
    where_clauses.extend(remote_filters_sql("p.remote_status", remote_status, where_params))
    where_clauses.extend(month_filters_sql("t.thread_month", month_from, month_to, where_params))
    if role_family:
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {schema}.roles r2 WHERE r2.post_id = p.post_id AND r2.role_family = %s)"
        )
        where_params.append(role_family)

    rank_sql = "NULL::double precision AS text_rank"
    order_sql = "t.thread_month DESC, p.created_at_utc DESC, p.post_id"
    if query:
        where_clauses.append("p.post_search_tsv @@ websearch_to_tsquery('english', %s)")
        where_params.append(query)
        rank_sql = "ts_rank_cd(p.post_search_tsv, websearch_to_tsquery('english', %s)) AS text_rank"
        select_params.append(query)
        order_sql = "text_rank DESC NULLS LAST, t.thread_month DESC, p.created_at_utc DESC, p.post_id"

    params = [*select_params, *where_params, limit]
    sql = f"""
        SELECT
            p.post_id,
            p.thread_id,
            t.thread_month,
            t.thread_title,
            coalesce(c.company_name_observed_preferred, p.company_name_observed) AS company_name,
            p.company_id,
            p.remote_status,
            p.employment_type,
            p.location_text,
            p.compensation_text,
            p.funding,
            p.post_text_clean,
            rp.source_url,
            {rank_sql}
        FROM {schema}.posts p
        JOIN {schema}.threads t ON t.thread_id = p.thread_id
        JOIN {schema}.raw_posts rp ON rp.raw_post_id = p.raw_post_id
        LEFT JOIN {schema}.companies c ON c.company_id = p.company_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY {order_sql}
        LIMIT %s
    """
    with connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    return {
        "entity": "posts",
        "schema": schema,
        "filters": {
            "query": query,
            "company_name": company_name,
            "role_family": role_family,
            "remote_status": remote_status,
            "month_from": month_from,
            "month_to": month_to,
            "limit": limit,
        },
        "row_count": len(rows),
        "rows": rows,
    }


def search_roles_postgres(
    *,
    database_url: str | None = None,
    schema: str = DEFAULT_DB_SCHEMA,
    query: str | None = None,
    company_name: str | None = None,
    role_family: str | None = None,
    remote_status: str | None = None,
    month_from: str | None = None,
    month_to: str | None = None,
    limit: int = 20,
) -> dict[str, object]:
    """Search role rows with structured filters and optional full-text ranking."""

    select_params: list[object] = []
    where_params: list[object] = []
    where_clauses = ["p.is_hiring_post = TRUE"]
    where_clauses.extend(company_filters_sql(company_name, "r.company_id", "coalesce(c.company_name_observed_preferred, p.company_name_observed)", where_params))
    where_clauses.extend(role_family_filters_sql(role_family, "r.role_family", where_params))
    where_clauses.extend(remote_filters_sql("r.role_remote_status", remote_status, where_params))
    where_clauses.extend(month_filters_sql("t.thread_month", month_from, month_to, where_params))

    rank_sql = "NULL::double precision AS text_rank"
    order_sql = "t.thread_month DESC, r.role_id"
    if query:
        where_clauses.append("r.role_search_tsv @@ websearch_to_tsquery('english', %s)")
        where_params.append(query)
        rank_sql = "ts_rank_cd(r.role_search_tsv, websearch_to_tsquery('english', %s)) AS text_rank"
        select_params.append(query)
        order_sql = "text_rank DESC NULLS LAST, t.thread_month DESC, r.role_id"

    params = [*select_params, *where_params, limit]
    sql = f"""
        SELECT
            r.role_id,
            r.post_id,
            p.thread_id,
            t.thread_month,
            coalesce(c.company_name_observed_preferred, p.company_name_observed) AS company_name,
            r.company_id,
            r.role_title_observed,
            r.role_title_normalized,
            r.role_family,
            r.seniority,
            r.role_remote_status,
            r.role_location_text,
            p.compensation_text,
            rp.source_url,
            {rank_sql}
        FROM {schema}.roles r
        JOIN {schema}.posts p ON p.post_id = r.post_id
        JOIN {schema}.threads t ON t.thread_id = p.thread_id
        JOIN {schema}.raw_posts rp ON rp.raw_post_id = p.raw_post_id
        LEFT JOIN {schema}.companies c ON c.company_id = coalesce(r.company_id, p.company_id)
        WHERE {" AND ".join(where_clauses)}
        ORDER BY {order_sql}
        LIMIT %s
    """
    with connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
    return {
        "entity": "roles",
        "schema": schema,
        "filters": {
            "query": query,
            "company_name": company_name,
            "role_family": role_family,
            "remote_status": remote_status,
            "month_from": month_from,
            "month_to": month_to,
            "limit": limit,
        },
        "row_count": len(rows),
        "rows": rows,
    }


def summarize_search_result(result: dict[str, Any]) -> dict[str, object]:
    """Return a compact summary that is easier to scan from the CLI."""

    rows = list(result.get("rows", []))
    return {
        "entity": result.get("entity"),
        "schema": result.get("schema"),
        "filters": result.get("filters"),
        "row_count": result.get("row_count"),
        "sample_rows": rows[: min(5, len(rows))],
    }


def company_activity_timeline_postgres(
    *,
    company_name: str,
    database_url: str | None = None,
    schema: str = DEFAULT_DB_SCHEMA,
    month_from: str | None = None,
    month_to: str | None = None,
    limit_evidence: int = 10,
) -> dict[str, object]:
    """Return month-by-month company activity with evidence-linked posts."""

    params: list[object] = []
    where_clauses = ["p.is_hiring_post = TRUE"]
    where_clauses.extend(
        company_filters_sql(
            company_name,
            "p.company_id",
            "coalesce(c.company_name_observed_preferred, p.company_name_observed)",
            params,
        )
    )
    where_clauses.extend(month_filters_sql("t.thread_month", month_from, month_to, params))

    summary_sql = f"""
        SELECT
            t.thread_month,
            COUNT(*) AS post_count,
            COUNT(DISTINCT p.post_id) AS distinct_post_count,
            COUNT(DISTINCT r.role_id) AS role_count,
            COUNT(DISTINCT r.role_family) AS role_family_count,
            array_remove(array_agg(DISTINCT r.role_family ORDER BY r.role_family), NULL) AS role_families
        FROM {schema}.posts p
        JOIN {schema}.threads t ON t.thread_id = p.thread_id
        LEFT JOIN {schema}.companies c ON c.company_id = p.company_id
        LEFT JOIN {schema}.roles r ON r.post_id = p.post_id
        WHERE {" AND ".join(where_clauses)}
        GROUP BY t.thread_month
        ORDER BY t.thread_month
    """
    evidence_params = [*params, limit_evidence]
    evidence_sql = f"""
        SELECT
            t.thread_month,
            coalesce(c.company_name_observed_preferred, p.company_name_observed) AS company_name,
            p.remote_status,
            p.compensation_text,
            p.post_text_clean,
            rp.source_url
        FROM {schema}.posts p
        JOIN {schema}.threads t ON t.thread_id = p.thread_id
        JOIN {schema}.raw_posts rp ON rp.raw_post_id = p.raw_post_id
        LEFT JOIN {schema}.companies c ON c.company_id = p.company_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY t.thread_month DESC, p.created_at_utc DESC
        LIMIT %s
    """
    with connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(summary_sql, params)
            summary_columns = [desc[0] for desc in cursor.description]
            months = [dict(zip(summary_columns, row, strict=False)) for row in cursor.fetchall()]
            cursor.execute(evidence_sql, evidence_params)
            evidence_columns = [desc[0] for desc in cursor.description]
            evidence_rows = [dict(zip(evidence_columns, row, strict=False)) for row in cursor.fetchall()]
    return {
        "entity": "company_activity_timeline",
        "schema": schema,
        "filters": {
            "company_name": company_name,
            "month_from": month_from,
            "month_to": month_to,
            "limit_evidence": limit_evidence,
        },
        "active_month_count": len(months),
        "months": months,
        "evidence_rows": evidence_rows,
    }


def company_role_presence_postgres(
    *,
    company_name: str,
    database_url: str | None = None,
    schema: str = DEFAULT_DB_SCHEMA,
    query: str | None = None,
    role_family: str | None = None,
    month_from: str | None = None,
    month_to: str | None = None,
    limit_evidence: int = 10,
) -> dict[str, object]:
    """Return whether a company posted roles matching the requested role text/family in a range."""

    params: list[object] = []
    where_clauses = ["p.is_hiring_post = TRUE"]
    where_clauses.extend(
        company_filters_sql(
            company_name,
            "coalesce(r.company_id, p.company_id)",
            "coalesce(c.company_name_observed_preferred, p.company_name_observed)",
            params,
        )
    )
    where_clauses.extend(role_family_filters_sql(role_family, "r.role_family", params))
    where_clauses.extend(month_filters_sql("t.thread_month", month_from, month_to, params))
    if query:
        where_clauses.append("r.role_search_tsv @@ websearch_to_tsquery('english', %s)")
        params.append(query)

    summary_sql = f"""
        SELECT
            COUNT(*) AS matched_role_count,
            COUNT(DISTINCT t.thread_month) AS matched_month_count,
            array_remove(array_agg(DISTINCT t.thread_month ORDER BY t.thread_month), NULL) AS matched_months
        FROM {schema}.roles r
        JOIN {schema}.posts p ON p.post_id = r.post_id
        JOIN {schema}.threads t ON t.thread_id = p.thread_id
        LEFT JOIN {schema}.companies c ON c.company_id = coalesce(r.company_id, p.company_id)
        WHERE {" AND ".join(where_clauses)}
    """
    evidence_params = [*params, limit_evidence]
    evidence_sql = f"""
        SELECT
            t.thread_month,
            coalesce(c.company_name_observed_preferred, p.company_name_observed) AS company_name,
            r.role_title_observed,
            r.role_title_normalized,
            r.role_family,
            r.role_remote_status,
            p.post_text_clean,
            rp.source_url
        FROM {schema}.roles r
        JOIN {schema}.posts p ON p.post_id = r.post_id
        JOIN {schema}.threads t ON t.thread_id = p.thread_id
        JOIN {schema}.raw_posts rp ON rp.raw_post_id = p.raw_post_id
        LEFT JOIN {schema}.companies c ON c.company_id = coalesce(r.company_id, p.company_id)
        WHERE {" AND ".join(where_clauses)}
        ORDER BY t.thread_month DESC, r.role_id
        LIMIT %s
    """
    with connect_postgres(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(summary_sql, params)
            summary_columns = [desc[0] for desc in cursor.description]
            summary_row = dict(zip(summary_columns, cursor.fetchone(), strict=False))
            cursor.execute(evidence_sql, evidence_params)
            evidence_columns = [desc[0] for desc in cursor.description]
            evidence_rows = [dict(zip(evidence_columns, row, strict=False)) for row in cursor.fetchall()]
    matched_role_count = int(summary_row.get("matched_role_count") or 0)
    return {
        "entity": "company_role_presence",
        "schema": schema,
        "filters": {
            "company_name": company_name,
            "query": query,
            "role_family": role_family,
            "month_from": month_from,
            "month_to": month_to,
            "limit_evidence": limit_evidence,
        },
        "match_found": matched_role_count > 0,
        "matched_role_count": matched_role_count,
        "matched_month_count": int(summary_row.get("matched_month_count") or 0),
        "matched_months": summary_row.get("matched_months") or [],
        "evidence_rows": evidence_rows,
    }
