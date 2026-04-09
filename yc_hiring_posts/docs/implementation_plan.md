# YC Hiring Posts Implementation Plan

## Phase 1: Source Index And Raw Ingestion

Deliverables:

- manually verified monthly source index from January 2024 onward
- month-by-month Google discovery queries for canonical thread lookup
- raw fetcher for threads and top-level posts
- append-only raw storage
- fetch validation report

Success criteria:

- each targeted month resolves to a source thread
- the discovery process is explicit and repeatable
- raw capture is reproducible
- source URLs and IDs are retained for every collected top-level post

## Phase 2: Parsing And Normalization

Deliverables:

- hiring-post classifier
- extraction logic for company, location, remote status, and compensation text
- normalized `posts`, `roles`, and `companies` tables
- explicit `misc` preservation for unresolved or extra fields

Success criteria:

- raw top-level posts are classified cleanly into hiring vs non-hiring
- multi-role posts are represented cleanly
- extraction outputs are auditable against source text
- useful leftovers are preserved in `misc` rather than silently discarded

## Phase 3: Core Analytics

Deliverables:

- monthly company activity summaries
- remote vs non-remote trends
- role-family trend tables
- company persistence reports
- company-building theme summaries
- company-level semantic spread / sameness analysis
- interactive Streamlit exploration over processed analytics

Success criteria:

- standard trend questions can be answered from tables alone
- recurring companies can be ranked by how repetitive or varied their hiring narratives are
- results are reproducible from raw inputs

## Phase 4: Similarity And Change Analysis

Deliverables:

- company role-text spread
- post-vs-role spread comparison
- temporal embedding drift metrics
- changed-company ranking
- local projection PNGs / GIFs

Success criteria:

- variation can be separated into role-driven vs broader narrative-driven change
- recurring companies can be ranked by measurable change rather than vague impression
- selected companies can be inspected through both metrics and visuals

## Phase 5: Knowledge Base And Retrieval

Deliverables:

- PostgreSQL schema for the V1 core tables
- JSONB preservation for `misc` and source payloads
- knowledge-base loader from processed JSONL tables
- structured and full-text retrieval over posts and roles

Success criteria:

- the full processed corpus is queryable inside PostgreSQL
- filters and text search return evidence-linked records
- the storage design is stable enough to support a later natural-language layer

## Phase 6: Natural-Language Query Layer

Deliverables:

- natural-language query handling
- retrieval-backed answer synthesis
- clear source grounding in final answers

Success criteria:

- every answer can point to supporting records
- structured and inferred claims are clearly distinguished
