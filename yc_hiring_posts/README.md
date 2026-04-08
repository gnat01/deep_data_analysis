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
- `data/raw/`: immutable raw source captures
- `data/interim/`: cleaned intermediate artifacts
- `data/processed/`: analysis-ready outputs
- `src/yc_hiring_posts/`: Python package for scraping, parsing, normalization, and analysis
- `sql/`: analytical queries or warehouse-oriented transformations
- `tests/`: tests for parsers, normalizers, and analytical logic

## Initial Build Priorities

1. Identify the canonical YC hiring-thread sources and archival strategy.
2. Define a stable raw-record format.
3. Build extraction logic for company, role, remote status, and compensation.
4. Materialize normalized tables that support time-series analysis.
5. Add duplicate-post detection and text-similarity analysis.

## Current Status

The repository currently contains planning artifacts and a starter structure. No scraper or parser implementation has been added yet.
