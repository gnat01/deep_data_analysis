# Source Index Specification

## Purpose

The source index is the control table for YC hiring-post ingestion.

It should answer one question cleanly:

"For a given target month, what is the canonical YC hiring thread we should ingest?"

Without this file, scraping logic becomes too implicit and too hard to audit.

## V1 Scope

For V1, the source index only needs to track monthly threads and their ingestion status for top-level hiring posts.

It does not need to:

- track reply parsing
- store full fetch manifests
- encode normalization results
- serve as a substitute for raw source storage

## Recommended Format

Use a CSV file so it is:

- easy to inspect manually
- easy to edit during source verification
- easy to load with the Python standard library

Recommended path:

- `data/source_index.csv`

## Required Columns

- `thread_month`
  Format: `YYYY-MM`
- `thread_date`
  Format: `YYYY-MM-DD` when known
- `thread_title`
  Expected thread title
- `thread_id`
  Canonical source thread identifier
- `source_url`
  Canonical source URL
- `source_system`
  Example: `hacker_news`
- `status`
  Suggested values: `planned`, `verified`, `fetched`, `blocked`
- `notes`
  Free-text notes for anomalies or manual observations

## Optional Columns

- `expected_top_level_post_count`
- `last_checked_at_utc`
- `last_fetched_at_utc`

These should remain optional in V1 to avoid blocking initial progress.

## Status Semantics

- `planned`: month is in scope but source is not yet manually verified
- `verified`: source thread is confirmed and ready for ingestion
- `fetched`: raw ingestion has completed at least once
- `blocked`: source could not be resolved cleanly and needs manual intervention

## Validation Rules

The source index loader should validate:

- `thread_month` is unique
- months are formatted as `YYYY-MM`
- `status` is one of the allowed values
- `source_url` is populated for `verified` and `fetched` rows
- `thread_id` is populated for `verified` and `fetched` rows

## Practical Workflow

1. Manually seed the months in scope.
2. Generate discovery queries for each month such as `news.ycombinator hiring for January 2024`.
3. Verify the canonical source thread for each month from search results.
4. Record the confirmed thread metadata in the source index.
5. Mark the row `verified`.
6. Run ingestion using the verified rows only.
7. Update status to `fetched` once raw data lands successfully.

## Discovery Query Convention

For consistency, V1 should generate search queries using both of these forms:

- `news.ycombinator hiring for <MONTH> <YEAR>`
- `Hacker News hiring for <MONTH> <YEAR>`

This makes the discovery process easy to automate later while keeping the current workflow explicit and reviewable.

## Design Rationale

This looks slightly manual on purpose.

The thread-discovery problem is small, high leverage, and easy to get subtly wrong if fully automated too early. A lightweight source index keeps the ingestion workflow explicit and auditable.
