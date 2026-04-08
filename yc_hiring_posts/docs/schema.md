# YC Hiring Posts Draft Schema

## Design Principles

The schema should separate raw facts from normalized interpretations and from enriched analytical features.

For V1, the project should focus on top-level posts within each monthly thread. Replies to those posts do not need to be parsed or normalized in the first version because they are relatively sparse and add limited analytical value. The raw layer should capture the top-level thread posts first, and the normalized layer should decide which of them are actual hiring posts.

Recommended layers:

1. Raw layer: immutable source captures.
2. Normalized layer: cleaned, analysis-ready fields.
3. Enriched layer: derived attributes, similarity scores, and semantic annotations.

## `misc` Catchall Policy

Use `misc` as a JSON object whenever the parser or normalizer encounters useful information that does not yet fit a first-class column.

For V1, the parser should be loss-minimizing:

- do not silently discard useful structured information
- preserve unresolved, extra, or evolving fields in `misc`
- keep the original raw HTML or payloads available for auditability

## Core Entities

### 1. `threads`

One record per monthly YC hiring thread.

Suggested fields:

- `thread_id`
- `thread_month`
- `thread_date`
- `thread_title`
- `source_url`
- `source_system`
- `collection_timestamp_utc`
- `raw_payload_hash`

### 2. `raw_posts`

One record per fetched top-level post in a thread for V1.

Suggested fields:

- `raw_post_id`
- `thread_id`
- `source_comment_id`
- `author_handle`
- `posted_at_utc`
- `edited_at_utc`
- `raw_text`
- `raw_html` or `raw_payload_json`
- `source_url`
- `collection_timestamp_utc`
- `is_deleted`
- `is_dead`
- `misc`
- `raw_payload_hash`

### 3. `posts`

One normalized post record derived from `raw_posts`. This layer should classify whether the captured top-level post is actually a hiring post. In many cases this may map 1:1 to a raw post, but the model should also allow one normalized hiring post to expand into multiple role records later.

Before any database landing, the first materialized form of this layer can live as JSONL under `data/interim/<YYYY-MM>/`.

Suggested fields:

- `post_id`
- `raw_post_id`
- `thread_id`
- `company_id`
- `company_name_observed`
- `company_name_normalized`
- `is_hiring_post`
- `location_text`
- `remote_status`
- `employment_type`
- `visa_sponsorship_text`
- `compensation_text`
- `compensation_text_accuracy`
- `funding`
- `post_text_clean`
- `misc`
- `parser_version`
- `parse_confidence`
- `created_at_utc`

### 4. `roles`

One record per distinct role extracted from a normalized hiring post.

Suggested fields:

- `role_id`
- `post_id`
- `company_id`
- `role_title_observed`
- `role_title_normalized`
- `role_family`
- `role_subfamily`
- `seniority`
- `headcount_text`
- `skills_text`
- `responsibilities_text`
- `requirements_text`
- `role_location_text`
- `role_remote_status`
- `role_compensation_id`
- `misc`

### 5. `companies`

One normalized company dimension.

Suggested fields:

- `company_id`
- `company_name_canonical`
- `company_name_variants`
- `company_website`
- `company_description`
- `yc_batch`
- `entity_resolution_notes`
- `first_seen_thread_month`
- `last_seen_thread_month`

### 6. `compensation`

One normalized compensation record, optionally linked from `posts` and `roles`.

Suggested fields:

- `compensation_id`
- `compensation_text_raw`
- `currency_code`
- `min_amount`
- `max_amount`
- `period_unit`
- `equity_mentioned`
- `equity_text`
- `normalization_notes`
- `parse_confidence`

### 7. `post_similarity`

Pairwise or candidate duplicate analysis between postings.

Suggested fields:

- `similarity_id`
- `company_id`
- `post_id_left`
- `post_id_right`
- `same_role_family`
- `text_similarity_score`
- `embedding_similarity_score`
- `is_likely_duplicate`
- `duplicate_reason`
- `model_version`
- `computed_at_utc`

### 8. `role_skills`

Many-to-many role-to-skill mapping.

Suggested fields:

- `role_skill_id`
- `role_id`
- `skill_name`
- `skill_category`
- `extraction_method`
- `extraction_confidence`

## Grain And Modeling Notes

- `threads` is monthly-thread grain.
- `raw_posts` is top-level source-post grain for V1.
- `posts` is normalized top-level-post grain with hiring-post classification.
- `roles` is extracted-role grain for normalized hiring posts only.

This layered design avoids forcing every analytical question into a single oversized table.

Reply parsing can be added in a later version if it proves useful, but it should not shape the initial schema or ingestion workflow.

## Recommended Initial Output Tables

For a first useful version, the project only needs to reliably materialize:

1. `threads`
2. `raw_posts`
3. `posts`
4. `roles`
5. `companies`

The compensation and similarity layers can be phased in once extraction quality is acceptable.

## Example Analytical Views

Derived views that should be easy to build from the schema:

- monthly post count by company
- monthly role-family count
- remote vs on-site trend by month
- repeated-post count by company
- compensation distribution by role family and month
- top skills by role family and period

## Validation Rules

The data pipeline should check:

- every `raw_post` belongs to a valid `thread`
- every `post` links to exactly one `raw_post`
- every extracted `role` links to a valid hiring-classified `post`
- normalized company IDs are stable across months
- compensation parsing does not silently invent numeric values
- duplicate-post labels remain reproducible given the same inputs and model version

## Versioning Requirements

At minimum, the project should version:

- parser logic
- normalization rules
- role taxonomy mappings
- similarity model settings

This is necessary so results can be compared across pipeline revisions.
