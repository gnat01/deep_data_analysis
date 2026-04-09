# YC Hiring Posts

This subproject is the first concrete build-out inside the broader `deep_data_analysis` repository.

It is intended to:

- collect Y Combinator hiring-thread data from January 2024 onward
- preserve raw source material
- normalize job-post data into analysis-friendly tables
- support trend analysis, repeated-post detection, and compensation comparisons
- provide a foundation for grounded natural-language querying over the dataset

## Directory Layout

- `docs/goals.md`: formal project goals and intended outcomes
- `docs/data_sources.md`: source assumptions, collection strategy, and provenance requirements
- `docs/schema.md`: draft data model for raw, normalized, and enriched layers
- `docs/implementation_plan.md`: phased execution plan
- `docs/source_index.md`: contract for the monthly source index
- `data/source_index.csv`: manually curated month-to-thread control table
- `data/raw/`: immutable raw source captures
- `data/interim/`: normalized intermediate artifacts such as post-level and role-level JSONL
- `data/processed/`: analysis-ready outputs
- `app.py`: root-level Streamlit explorer launcher
- `src/`: flat Python module layout for scraping, parsing, normalization, and analysis
- `sql/`: analytical queries or warehouse-oriented transformations
- `tests/`: tests for parsers, normalizers, and analytical logic

## Initial Build Priorities

1. Identify the canonical YC hiring-thread sources and archival strategy.
2. Define a stable raw-record format.
3. Build extraction logic for company, role, remote status, and compensation.
4. Materialize normalized post and role layers that support time-series analysis.
5. Add duplicate-post detection and text-similarity analysis.

## Current Status

The repository now contains planning artifacts, a verified source index, raw thread fetch and parse logic, raw-schema validation, a normalized post parser, a first-pass role extractor, conservative company normalization, V1 core-table materialization, recurring analytical outputs, upgraded Step 16 visuals, company-level semantic spread analysis, Step 17 company-change analysis, local projection PNG/GIF artifacts, and an interactive Streamlit explorer with dedicated variation and change-analysis tabs.
