# YC Hiring Posts Data Sources

## Objective

Define how the project will source Y Combinator hiring-post data in a way that is reproducible, traceable, and maintainable over time.

## Primary Source Candidates

The likely primary corpus is the recurring Y Combinator "Who is hiring?" thread series, starting with threads from January 2024 onward.

Candidate source surfaces:

1. Hacker News thread pages for monthly YC hiring discussions.
2. Hacker News item APIs and comment APIs, where available.
3. Archived snapshots or mirrors, only if the canonical source is unavailable or incomplete.

The preferred ordering should be:

1. Canonical live source.
2. Canonical API or machine-readable endpoint.
3. Archived backup source with explicit provenance labeling.

## Unit Of Collection

For V1, the project should collect top-level hiring posts only and ignore reply chains unless they become necessary later.

The project should preserve three distinct collection units:

1. Thread
   - one monthly hiring discussion
   - includes metadata such as title, thread date, thread ID, source URL
2. Post
   - one top-level company hiring post within a thread
   - includes author metadata if available, timestamps, and source identifiers
3. Extraction artifact
   - one structured interpretation of a raw post
   - includes parser version, extraction confidence, and normalization decisions

## Required Provenance

Every collected record should retain:

- source system
- source URL
- source thread ID
- source comment ID
- collection timestamp
- raw payload hash
- parser / transformation version

This is required so later analyses can be audited and regenerated.

## Collection Requirements

The collection layer should:

- capture the full raw text of each post
- preserve thread-level context
- avoid silently overwriting source changes
- support incremental monthly refreshes
- be robust to deleted, edited, or unavailable records

## Recommended Raw Storage Strategy

Use append-only raw storage with one immutable record per fetched source object.

Recommended raw record categories:

- thread metadata snapshots
- top-level post payload snapshots
- collection manifests describing what was fetched and when

Potential storage formats:

- JSON Lines for raw API responses
- HTML snapshots when structure or rendering matters
- CSV only for derivative exports, not as the canonical raw layer

## Source Resolution Strategy

Before implementation, the project should create and maintain a source index containing:

- month label
- expected thread title
- thread URL
- thread ID
- collection status
- notes on missing or irregular months

This source index becomes the control plane for downstream ingestion.

## Data Quality Risks

Likely issues to plan for:

- posts with inconsistent formatting
- companies posting multiple roles in one comment
- edited or deleted top-level posts
- unclear location language
- ambiguous remote/hybrid wording
- missing compensation details
- non-company top-level comments mixed into the thread

## Inclusion Rules

The project should include:

- company hiring posts that meaningfully describe open roles
- multi-role posts from a single company
- repeated monthly postings as distinct historical records

The project should generally exclude or separately label:

- discussion-only top-level comments
- jokes, off-topic replies, or meta moderation comments
- top-level comments that cannot reasonably be mapped to a company hiring post

## Open Questions

These need validation during implementation:

- whether the HN API alone is sufficient for all required text and metadata
- whether edited top-level posts can be reliably captured after the fact
- how to distinguish top-level hiring posts from other top-level thread noise
- whether monthly thread discovery can be automated reliably from titles alone

## Recommended First Deliverables

1. A manually curated source index for January 2024 onward.
2. A fetcher that stores raw thread metadata and raw top-level posts without transformation.
3. A validation report showing fetched counts per month and obvious anomalies.
