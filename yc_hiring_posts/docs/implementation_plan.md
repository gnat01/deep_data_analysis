# YC Hiring Posts Implementation Plan

## Phase 1: Source Index And Raw Ingestion

Deliverables:

- manually verified monthly source index from January 2024 onward
- raw fetcher for threads and top-level posts
- append-only raw storage
- fetch validation report

Success criteria:

- each targeted month resolves to a source thread
- raw capture is reproducible
- source URLs and IDs are retained for every collected top-level post

## Phase 2: Parsing And Normalization

Deliverables:

- hiring-post classifier
- extraction logic for company, location, remote status, and compensation text
- normalized `posts`, `roles`, and `companies` tables

Success criteria:

- obvious non-hiring top-level comments are filtered or labeled
- multi-role posts are represented cleanly
- extraction outputs are auditable against source text

## Phase 3: Core Analytics

Deliverables:

- monthly company activity summaries
- remote vs non-remote trends
- role-family trend tables
- company persistence reports

Success criteria:

- standard trend questions can be answered from tables alone
- results are reproducible from raw inputs

## Phase 4: Similarity And Change Analysis

Deliverables:

- repeated-post detection
- wording-change analysis for recurring company posts
- compensation comparison across similar roles over time

Success criteria:

- likely duplicates can be reviewed with evidence
- comparisons can separate exact repeats from materially changed postings

## Phase 5: NLP And Query Layer

Deliverables:

- skill and topic extraction
- semantic indexing / retrieval layer
- source-grounded natural-language question answering

Success criteria:

- every answer can point to supporting records
- structured and inferred claims are clearly distinguished
