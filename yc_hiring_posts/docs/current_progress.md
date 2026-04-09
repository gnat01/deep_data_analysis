# Current Progress

This file is the current handoff note for the YC / HN hiring-posts project.

## Where We Are

- **Step 16** is complete for V1.
  - analytics
  - visuals
  - Streamlit app
  - AI concept tracking
  - company/theme analysis
  - compensation hardening

- **Step 17** is complete.
  - company change analysis
  - post semantic spread
  - role semantic spread
  - drift metrics
  - changed-company ranking
  - PNG / GIF artifacts
  - Streamlit change-analysis views

- **Step 18** is complete.
  - PostgreSQL installed locally
  - PostgreSQL running
  - schema initialized
  - core YC tables loaded
  - config and docs saved

- **Step 19** is partially complete.
  - windowed 6-month company change analysis is built
  - Task 1 is done: raw PostgreSQL retrieval
  - Task 2 is done: first answer-oriented KB helpers
  - question-bank classification layer is built
  - Task 3 is underway with broad helper coverage now live

## Important Project Files

### Planning / Docs

- [`BUILD_PLAN.md`](../BUILD_PLAN.md)
- [`HOW_TO_RUN.md`](./HOW_TO_RUN.md)
- [`using_postgres.md`](./using_postgres.md)
- [`possible_qs.md`](./possible_qs.md)
- [`step17.md`](./step17.md)
- [`step19.md`](./step19.md)
- [`question_catalog.md`](./question_catalog.md)
- [`question_catalog.json`](./question_catalog.json)

### Core Recent Code

- [`src/postgres_kb.py`](./src/postgres_kb.py)
- [`src/question_catalog.py`](./src/question_catalog.py)
- [`src/cli.py`](./src/cli.py)
- [`src/explore_app.py`](./src/explore_app.py)
- [`src/analytics.py`](./src/analytics.py)

## PostgreSQL Status

Local DB is installed and loaded.

Current setup:

- database: `yc_hiring_posts`
- schema: `yc_hiring`
- DSN: `postgresql://gn@/yc_hiring_posts?host=/tmp`

Loaded core tables:

- `threads`
- `raw_posts`
- `posts`
- `roles`
- `companies`

Reference docs:

- [`using_postgres.md`](./using_postgres.md)
- [`postgres_local_setup.md`](./postgres_local_setup.md)
- [`config/postgres.local.env`](../config/postgres.local.env)

## Step 19 Progress

### Already Done

#### Windowed change analysis

- non-overlapping 6-month windows
- all-company windowed scatter
- binned robustness view
- company deep-dive windowed scatter in Streamlit

#### Task 1: raw retrieval layer

CLI commands:

- `search-postgres-posts`
- `search-postgres-roles`

These support:

- optional full-text query
- company filter
- role-family filter
- remote-status filter
- month range
- evidence-linked result rows

#### Task 2: first KB helpers

CLI commands:

- `company-activity-postgres`
- `company-role-presence-postgres`

These support:

- company timeline questions
- yes/no role-presence questions with matched months and evidence

#### Task 3: broader helper coverage now live

CLI commands:

- `month-summary-postgres`
- `role-family-timeline-postgres`
- `companies-for-role-postgres`
- `evidence-lookup-postgres`
- `remote-mix-postgres`
- `company-remote-change-postgres`
- `compensation-history-postgres`
- `ai-concept-timeline-postgres`
- `role-requirement-change-summary-postgres`
- `companies-every-month-postgres`
- `remote-first-companies-postgres`
- `companies-role-pair-postgres`
- `global-remote-share-postgres`
- `post-shape-summary-postgres`
- `company-post-length-consistency-postgres`
- `company-change-summary-postgres`
- `company-theme-history-postgres`

These now cover:

- month summaries
- role timelines
- company lookup by role
- evidence retrieval
- remote-mix questions
- company remote-status changes
- compensation history
- AI concept timelines
- requirement-change summarisation
- recurring-company patterns
- global-remote heuristics
- post-shape / consistency questions
- company-change rankings
- company-theme history / theme shifts

#### Question classification layer

Implemented in:

- [`src/question_catalog.py`](./src/question_catalog.py)

Generated outputs:

- [`docs/question_catalog.md`](./docs/question_catalog.md)
- [`docs/question_catalog.json`](./docs/question_catalog.json)

Purpose:

- classify the 50-question bank
- attach answer type
- attach evaluation mode
- attach likely KB helper
- drive Task 3 intelligently

## What Still Needs To Be Done

### Step 19 Task 3 Remaining Work

Task 3 is materially underway, but not complete.

Most important remaining areas:

- stronger comparative helpers
- tighter summarisation quality
- geography helpers only where data quality really supports them
- wider question-bank coverage against the still-uncovered families

### Step 19 Task 4

Potentially add a composition / routing layer over the helpers, still structured and grounded.

This is still before the real natural-language layer.

## Where We Want To Get To

Short-term:

- finish **Step 19** with broad helper coverage across the 50-question bank
- We also definitely need a dedicated streamlit app (NOT the current one) for the Q&A - both purely with KB helpers, and down the road using NLP + context + KB. This app will be finished in Step 20.
Then:

- move to **Step 20**
- build a natural-language interface over the helper-backed KB
- route questions into the right helper where possible
- use retrieval + synthesis only where necessary
- always keep answers grounded with evidence

## Tomorrow’s Restart Point

When restarting, the next concrete move is:

- continue **Step 19 Task 3**
- use [`docs/question_catalog.md`](./docs/question_catalog.md) as the spec

Priority order:

1. stronger comparative helpers
2. tighter summary helpers
3. question-family gaps from the catalog
4. helper composition patterns for Step 20


## Notes

- `spread_gap` was removed from the UI and analytics surfaces because it was not considered meaningful enough.
- `drift_score` is retained and documented as:
  - `mean(angle_from_first_deg) + mean(angle_from_previous_deg)`
- the app was cleaned up to reduce clutter in the change-analysis flows.
