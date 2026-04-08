# YC Hiring Posts Analysis Goals

## Purpose

Build a reliable, queryable dataset and analysis workflow around Y Combinator hiring posts, starting from January 2024 and extending forward over time. The immediate aim is to understand hiring activity patterns across companies and roles. The longer-term aim is to support precise natural-language questions over a structured knowledge base derived from the posts.

## Primary Objectives

1. Collect Y Combinator hiring post data from January 2024 onward.
2. Normalize the data into a consistent schema so posts can be compared across time.
3. Produce clear summaries of:
   - company names
   - remote vs non-remote roles
   - job families / job profiles
   - compensation, where stated
4. Build time-series views that show how hiring demand changes over time.
5. Detect repeated or near-duplicate postings by the same company across months.
6. Compare how role descriptions, hiring focus, and compensation evolve for similar jobs over time.
7. Support downstream NLP analysis on the full text of job postings.
8. Enable grounded natural-language querying over the resulting dataset and knowledge base.

## Scope

The project should treat each hiring thread as a historical snapshot and preserve enough structure to answer both simple counting questions and more nuanced trend questions.

Initial coverage should include:
- thread metadata
- company identity
- raw job post text
- normalized role labels
- location and remote status
- compensation details when available
- posting timestamps
- links or identifiers that allow traceability back to the source

## Key Questions To Answer

The system should eventually support questions such as:

- Which companies posted most frequently over the period?
- Which companies appear to repost substantially the same ad month after month?
- How has the mix of remote vs on-site roles changed over time?
- Which job families have grown or declined in relative share?
- Are some companies consistently hiring for the same functions?
- How do salary ranges for similar roles compare between January 2025 and later periods?
- How does the language used in job descriptions change as market conditions shift?
- Can we identify persistent demand clusters such as AI engineering, infra, security, product, or design?

## Data Product Goals

The project should produce a dataset that is:

- reproducible
- traceable to source material
- versionable over time
- suitable for both statistical analysis and text analysis
- structured enough to support deterministic answers where possible

Recommended output layers:

1. Raw scraped source data.
2. Cleaned and normalized tabular data.
3. Enriched analytical features such as role family, skill tags, compensation normalization, and duplicate-post indicators.
4. Query-ready knowledge artifacts for natural-language access.

## Analytical Extensions

Beyond the initial summaries, the project should support:

- longitudinal analysis of company hiring persistence
- cohorting companies by hiring behavior
- trend analysis by role family and seniority
- text similarity analysis across repeated posts
- compensation normalization by currency, range, and periodicity
- extraction of skills, tools, and domain keywords
- clustering of postings by thematic content
- change detection in wording for recurring roles at the same company

## NLP And Knowledge Base Goals

Once the data pipeline is stable, the next phase should:

1. Parse and annotate each job posting with structured entities and tags.
2. Derive embeddings or equivalent semantic representations for retrieval.
3. Store supporting evidence so every answer can point back to exact source records.
4. Distinguish between deterministic answers from structured fields and inferred answers from text analysis.
5. Minimize hallucination by grounding all user-facing responses in stored source material.

The target user experience is to ask natural-language questions and receive concise, correct, source-backed answers.

## Quality Requirements

The system should aim for:

- high extraction accuracy on company, role, remote status, and compensation
- explicit handling of ambiguity and missing data
- auditable transformations from raw text to normalized fields
- repeatable analyses as new monthly threads are added
- clear separation between observed facts and model-derived inferences

## Suggested Implementation Phases

1. Source discovery and scraping pipeline.
2. Raw data archival and schema definition.
3. Cleaning and normalization.
4. Exploratory summaries and time-series reporting.
5. Duplicate-post and repeated-company analysis.
6. NLP enrichment and semantic indexing.
7. Natural-language question answering over the knowledge base.

## Non-Goals For The First Phase

The first phase should not try to solve everything at once. It does not need to:

- build a polished end-user application
- perfectly classify every niche role taxonomy upfront
- solve every compensation-normalization edge case immediately
- answer broad labor-market questions outside the YC hiring corpus

## Deliverable Standard

At the end of the initial build-out, the project should be able to ingest the targeted YC hiring posts, generate trustworthy summary tables and trends, and provide a solid foundation for deeper NLP analysis and grounded question answering.
