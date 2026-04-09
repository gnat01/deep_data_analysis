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

## Phase 5: NLP And Query Layer

Deliverables:

- skill and topic extraction
- semantic indexing / retrieval layer
- source-grounded natural-language question answering

Success criteria:

- every answer can point to supporting records
- structured and inferred claims are clearly distinguished
