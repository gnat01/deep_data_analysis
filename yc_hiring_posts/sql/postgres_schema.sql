CREATE SCHEMA IF NOT EXISTS __SCHEMA__;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS __SCHEMA__.threads (
    thread_id TEXT PRIMARY KEY,
    thread_month TEXT NOT NULL,
    thread_date DATE,
    thread_title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_system TEXT NOT NULL,
    collection_timestamp_utc TIMESTAMPTZ NOT NULL,
    raw_payload_hash TEXT,
    raw_schema_version TEXT,
    source_payload_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.raw_posts (
    raw_post_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES __SCHEMA__.threads(thread_id),
    source_comment_id TEXT NOT NULL,
    author_handle TEXT,
    posted_at_utc TIMESTAMPTZ,
    edited_at_utc TIMESTAMPTZ,
    raw_text TEXT NOT NULL,
    source_url TEXT NOT NULL,
    collection_timestamp_utc TIMESTAMPTZ NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    is_dead BOOLEAN NOT NULL,
    raw_html TEXT,
    raw_payload_json JSONB,
    misc JSONB,
    raw_payload_hash TEXT,
    raw_schema_version TEXT,
    source_payload_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.companies (
    company_id TEXT PRIMARY KEY,
    company_name_observed_preferred TEXT NOT NULL,
    company_match_key TEXT NOT NULL,
    company_name_variants JSONB NOT NULL,
    company_website TEXT,
    yc_batch TEXT,
    entity_resolution_notes TEXT,
    first_seen_thread_month TEXT NOT NULL,
    last_seen_thread_month TEXT NOT NULL,
    misc JSONB,
    source_payload_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.posts (
    post_id TEXT PRIMARY KEY,
    raw_post_id TEXT NOT NULL REFERENCES __SCHEMA__.raw_posts(raw_post_id),
    thread_id TEXT NOT NULL REFERENCES __SCHEMA__.threads(thread_id),
    company_id TEXT REFERENCES __SCHEMA__.companies(company_id),
    company_name_observed TEXT,
    is_hiring_post BOOLEAN NOT NULL,
    location_text TEXT,
    remote_status TEXT NOT NULL,
    employment_type TEXT,
    visa_sponsorship_text TEXT,
    compensation_text TEXT,
    compensation_text_accuracy TEXT,
    funding TEXT,
    post_text_clean TEXT NOT NULL,
    misc JSONB,
    parser_version TEXT NOT NULL,
    parse_confidence DOUBLE PRECISION NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    source_payload_json JSONB NOT NULL,
    post_search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(post_text_clean, ''))
    ) STORED
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.roles (
    role_id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES __SCHEMA__.posts(post_id),
    company_id TEXT REFERENCES __SCHEMA__.companies(company_id),
    role_title_observed TEXT NOT NULL,
    role_title_normalized TEXT,
    role_family TEXT,
    role_subfamily TEXT,
    seniority TEXT,
    headcount_text TEXT,
    skills_text TEXT,
    responsibilities_text TEXT,
    requirements_text TEXT,
    role_location_text TEXT,
    role_remote_status TEXT NOT NULL,
    role_compensation_id TEXT,
    misc JSONB,
    source_payload_json JSONB NOT NULL,
    role_search_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'english',
            concat_ws(
                ' ',
                coalesce(role_title_observed, ''),
                coalesce(role_title_normalized, ''),
                coalesce(role_family, ''),
                coalesce(skills_text, ''),
                coalesce(requirements_text, ''),
                coalesce(responsibilities_text, '')
            )
        )
    ) STORED
);

CREATE INDEX IF NOT EXISTS idx_threads_thread_month ON __SCHEMA__.threads(thread_month);
CREATE INDEX IF NOT EXISTS idx_raw_posts_thread_id ON __SCHEMA__.raw_posts(thread_id);
CREATE INDEX IF NOT EXISTS idx_raw_posts_source_comment_id ON __SCHEMA__.raw_posts(source_comment_id);
CREATE INDEX IF NOT EXISTS idx_companies_company_match_key ON __SCHEMA__.companies(company_match_key);
CREATE INDEX IF NOT EXISTS idx_posts_thread_id ON __SCHEMA__.posts(thread_id);
CREATE INDEX IF NOT EXISTS idx_posts_company_id ON __SCHEMA__.posts(company_id);
CREATE INDEX IF NOT EXISTS idx_posts_hiring_remote_status ON __SCHEMA__.posts(is_hiring_post, remote_status);
CREATE INDEX IF NOT EXISTS idx_posts_post_search_tsv ON __SCHEMA__.posts USING GIN(post_search_tsv);
CREATE INDEX IF NOT EXISTS idx_roles_post_id ON __SCHEMA__.roles(post_id);
CREATE INDEX IF NOT EXISTS idx_roles_company_id ON __SCHEMA__.roles(company_id);
CREATE INDEX IF NOT EXISTS idx_roles_role_family ON __SCHEMA__.roles(role_family);
CREATE INDEX IF NOT EXISTS idx_roles_role_search_tsv ON __SCHEMA__.roles USING GIN(role_search_tsv);
