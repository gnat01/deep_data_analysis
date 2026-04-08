# YC Hiring Posts Build Plan

This document is the working execution plan for the YC hiring-posts project.

No implementation work should proceed beyond planning until this document is reviewed and approved.

## V1 Scope

Version 1 should focus on:

- monthly Y Combinator / Hacker News hiring threads
- January 2024 onward
- top-level posts only at raw ingestion
- no reply parsing
- no advanced NLP as part of the initial pipeline

The purpose of V1 is to build a trustworthy ingestion and normalization pipeline before adding higher-order analysis.

## Guiding Principles

- keep the pipeline auditable
- separate discovery from scraping
- preserve raw source material
- normalize only after raw capture is stable
- prefer explicit control tables over clever automation
- keep V1 conservative and deterministic
- preserve structured leftovers in `misc` rather than dropping them

## Ordered Build Steps

### 1. Lock The V1 Scope

Do not expand the first version into reply parsing, full semantic search, or broad labor-market inference.

The V1 target is a clean and reliable pipeline for:

- discovering the correct monthly thread
- capturing raw top-level posts
- classifying and normalizing the content into analysis-ready tables

### 2. Keep The Project Structure Clean

The repository layout should remain:

- `docs/` for supporting documentation
- `data/` for source index, raw data, and processed outputs
- `src/` for executable Python modules
- `tests/` for tests

This file sits directly in `yc_hiring_posts/` because it is the top-level execution reference for the whole subproject.

### 3. Finalize The Source Index Contract

Use `data/source_index.csv` as the control table for month-level ingestion.

For every month in scope, it should eventually record:

- month label
- thread date
- thread title
- thread ID
- source URL
- source system
- ingestion status
- notes

The source index is the control plane for all downstream work.

### 4. Generate Discovery Queries

For every target month, generate Google queries in these forms:

- `news.ycombinator hiring for <MONTH> <YEAR>`
- `Hacker News hiring for <MONTH> <YEAR>`

This is the intended V1 discovery method for locating the canonical monthly thread.

### 5. Verify Monthly Threads

For each `YYYY-MM` in scope:

1. run the search queries
2. identify the canonical Hacker News thread
3. capture the thread URL and thread ID
4. update `data/source_index.csv`
5. mark the row as `verified`

This is intentionally explicit and partly manual because the discovery problem is small and errors here would contaminate everything downstream.

### 6. Build The Raw Fetcher

Once rows are verified, build a fetcher that takes one verified thread and captures:

- thread metadata
- top-level posts only
- raw text and supporting raw payloads

This step should not do normalization.

### 7. Write Raw Data Deterministically

For each month, raw outputs should land in a dedicated month folder under `data/raw/`.

Expected artifacts:

- `data/raw/<YYYY-MM>/thread.json`
- `data/raw/<YYYY-MM>/posts.jsonl`

The raw layer should be append-friendly, auditable, and reproducible.

### 8. Parse Thread HTML Elegantly

Scraping should use BeautifulSoup or equivalent HTML parsing, not brittle string chopping.

The scraper should extract:

- thread metadata
- top-level post identifiers
- author
- timestamps when available
- raw post text
- source URL

Replies should be ignored in V1. Hiring-post classification happens later in normalization, not during raw capture.

If extracted information does not fit the current first-class raw-post fields, retain it in `misc`.

### 9. Add Raw Ingestion Validation

The ingestion stage should validate:

- every verified source row fetches cleanly
- output files are written in the expected place
- counts are plausible
- anomalies are recorded explicitly

The purpose of this layer is to establish trust in the raw corpus before normalization begins.

### 10. Freeze The Raw Schema

Once the raw layer is working, keep it stable.

Do not mix parsing assumptions into raw storage. Raw data should remain as close to source as practical.

### 11. Build The Normalized Post Parser

The first normalization pass should produce a `posts` layer with fields such as:

- company name as observed
- cleaned post text
- location text
- remote status
- compensation text
- hiring-post classification

This layer should be conservative and traceable back to raw source records.

At this stage, `posts` becomes the normalized layer that decides whether a captured top-level raw post is actually a hiring post.

If the normalized layer encounters useful leftovers that do not yet fit first-class fields, retain them in `misc`.

### 12. Build Role Extraction

Many top-level hiring posts contain multiple roles. The next step is to extract a `roles` layer from normalized hiring posts.

Target fields include:

- observed role title
- normalized role title
- role family
- seniority where visible
- role-level location or remote signals where available

### 13. Build Company Normalization

Create a stable company dimension so that spelling or formatting variants across months resolve to one company identity.

This is necessary for:

- month-over-month company activity
- repeated-post detection
- coherent trend analysis

### 14. Materialize The V1 Core Tables

The first production-worthy tables should be:

- `threads`
- `raw_posts`
- `posts`
- `roles`
- `companies`

Compensation normalization and similarity scoring can be layered in after these are trustworthy.

### 15. Add Parser And Normalization Tests

Tests should cover:

- multi-role posts
- inconsistent formatting
- remote / hybrid wording
- non-hiring top-level noise
- company-name normalization edge cases

The point is not maximal test count. The point is protecting the highest-risk transformations.

### 16. Build Core Analytical Outputs

Once the structured tables are stable, produce recurring outputs for:

- company posting counts by month
- remote vs non-remote trends
- role-family trends by month
- recurring-company hiring patterns

These outputs should answer the first generation of business questions directly from tables.

### 17. Add Repeated-Post Detection

Start simple.

Use deterministic or conservative similarity approaches first to find:

- repeated or near-duplicate posts by the same company
- recurring role families across adjacent months

This stage should produce reviewable evidence, not black-box claims.

### 18. Add Compensation Parsing

Compensation extraction should be phased in after the core structure is stable.

It should:

- preserve the raw compensation text
- normalize only clearly stated amounts
- avoid inventing precision where the source is vague

### 19. Build Reproducible Reporting

Create repeatable reporting outputs in CSV, JSON, SQL views, or equivalent artifacts so the dataset can support regular inspection and trend tracking.

The goal is to answer most early questions from deterministic outputs rather than ad hoc manual inspection.

### 20. Add NLP Only After The Structured Pipeline Is Trustworthy

Only after the structured data pipeline is stable should the project move into:

- skill extraction
- embeddings
- semantic retrieval
- grounded natural-language question answering

This prevents the project from jumping into high-variance NLP work before the base data is reliable.

## Immediate Next Steps After Approval

Once this document is approved, the first implementation steps should be:

1. verify and populate the first real rows in `data/source_index.csv`
2. build the raw fetcher for verified threads
3. write raw thread and top-level post artifacts into `data/raw/`

## Out Of Scope Until Further Notice

The following are explicitly not part of the approved V1 implementation unless the plan changes:

- reply parsing
- broad crawling outside the monthly hiring threads
- fully automated thread discovery without review
- advanced NLP before structured normalization is stable
- polished application UI work
