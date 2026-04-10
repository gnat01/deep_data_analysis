# How To Run

This document explains exactly how to run **Steps 4 through 18** from the parent project directory:

```bash
cd /Users/gn/work/learn/python/deep_data_analysis/yc_hiring_posts
```

All commands below assume you are running from that directory.

## One-Time Convention

Use the CLI through:

```bash
PYTHONPATH=src python src/cli.py <command> ...
```

The main control file is:

```bash
data/source_index.csv
```

## Step 4: Generate Discovery Queries

Show the two Google search queries for one month:

```bash
PYTHONPATH=src python src/cli.py show-discovery-queries 2025-03
```

Show discovery queries for every row in the source index:

```bash
PYTHONPATH=src python src/cli.py show-all-discovery-queries --path data/source_index.csv
```

Optional: inspect the source index itself:

```bash
PYTHONPATH=src python src/cli.py show-source-index --path data/source_index.csv
```

## Step 5: Verify Monthly Threads

This step is partly manual.

Workflow:

1. Run Step 4 queries for a month.
2. Search Google manually for the canonical Hacker News thread.
3. Confirm the thread URL and HN thread ID.
4. Update `data/source_index.csv`.

Useful command to show only verified rows:

```bash
PYTHONPATH=src python src/cli.py show-source-index --path data/source_index.csv --verified-only
```

## Step 6: Fetch Raw Thread Artifacts

Fetch one verified month:

```bash
PYTHONPATH=src python src/cli.py fetch-thread-raw 2025-03 --path data/source_index.csv
```

This writes raw thread artifacts under:

```bash
data/raw/2025-03/
```

Typical files created:

```text
data/raw/2025-03/thread.html
data/raw/2025-03/thread.json
data/raw/2025-03/fetch_manifest.json
```

Repeat for any verified month:

```bash
PYTHONPATH=src python src/cli.py fetch-thread-raw 2024-11 --path data/source_index.csv
PYTHONPATH=src python src/cli.py fetch-thread-raw 2025-08 --path data/source_index.csv
```

## Step 7: Write Raw Data Deterministically

This is satisfied by Step 6 plus Step 8 output.

After running fetch + parse, each month should have deterministic raw artifacts under:

```text
data/raw/<YYYY-MM>/
```

You can inspect them with:

```bash
ls data/raw/2025-03
sed -n '1,40p' data/raw/2025-03/thread.json
```

## Step 8: Parse Thread HTML Into Top-Level Posts

Parse one fetched thread into raw top-level posts:

```bash
PYTHONPATH=src python src/cli.py parse-thread-posts 2025-03
```

This writes:

```text
data/raw/2025-03/posts.jsonl
```

Inspect a few rows:

```bash
sed -n '1,3p' data/raw/2025-03/posts.jsonl
```

Run for more months:

```bash
PYTHONPATH=src python src/cli.py parse-thread-posts 2024-11
PYTHONPATH=src python src/cli.py parse-thread-posts 2025-08
```

## Step 9: Validate Raw Ingestion

Validate one month:

```bash
PYTHONPATH=src python src/cli.py validate-thread-raw 2025-03
```

Validate multiple months:

```bash
PYTHONPATH=src python src/cli.py validate-many-thread-raw 2024-11 2025-03 2025-08
```

This checks:

- required raw files exist
- `thread.json` hash matches `thread.html`
- `posts.jsonl` parses cleanly
- top-level counts match HTML
- soft warnings such as low post counts

## Step 10: Freeze The Raw Schema

There is no separate CLI command for this step.

This step is represented by:

- raw schema versioning in code
- the raw artifact contract
- validation against the frozen raw shape

Practical checks:

```bash
PYTHONPATH=src python src/cli.py validate-thread-raw 2025-03
sed -n '1,3p' data/raw/2025-03/posts.jsonl
sed -n '1,40p' data/raw/2025-03/thread.json
```

## Step 11: Normalize Posts

Normalize one month from raw posts into the `posts` layer:

```bash
PYTHONPATH=src python src/cli.py normalize-thread-posts 2025-03
```

This writes:

```text
data/interim/2025-03/posts_normalized.jsonl
```

Inspect a few rows:

```bash
sed -n '1,3p' data/interim/2025-03/posts_normalized.jsonl
```

Run for more months:

```bash
PYTHONPATH=src python src/cli.py normalize-thread-posts 2024-11
PYTHONPATH=src python src/cli.py normalize-thread-posts 2025-08
```

## Step 12: Extract Roles

Extract role-level rows from normalized hiring posts:

```bash
PYTHONPATH=src python src/cli.py extract-thread-roles 2025-03
```

This writes:

```text
data/interim/2025-03/roles.jsonl
```

Inspect a few rows:

```bash
sed -n '1,5p' data/interim/2025-03/roles.jsonl
```

Run for more months:

```bash
PYTHONPATH=src python src/cli.py extract-thread-roles 2024-11
PYTHONPATH=src python src/cli.py extract-thread-roles 2025-08
```

## Step 13: Normalize Companies

Resolve conservative `company_id` values, write the company dimension, and backfill `company_id` into normalized posts and roles:

```bash
PYTHONPATH=src python src/cli.py normalize-thread-companies 2025-03
```

This writes:

```text
data/interim/2025-03/companies.jsonl
```

And rewrites:

```text
data/interim/2025-03/posts_normalized.jsonl
data/interim/2025-03/roles.jsonl
```

Inspect a few company rows:

```bash
sed -n '1,5p' data/interim/2025-03/companies.jsonl
```

Run for more months:

```bash
PYTHONPATH=src python src/cli.py normalize-thread-companies 2024-11
PYTHONPATH=src python src/cli.py normalize-thread-companies 2025-08
```

## Step 14: Materialize The V1 Core Tables

Materialize the consolidated V1 tables into `data/processed/`:

```bash
PYTHONPATH=src python src/cli.py materialize-v1-core-tables
```

This writes:

```text
data/processed/v1_core_tables/threads.jsonl
data/processed/v1_core_tables/raw_posts.jsonl
data/processed/v1_core_tables/posts.jsonl
data/processed/v1_core_tables/roles.jsonl
data/processed/v1_core_tables/companies.jsonl
data/processed/v1_core_tables_manifest.json
```

Inspect the processed outputs:

```bash
ls data/processed/v1_core_tables
sed -n '1,5p' data/processed/v1_core_tables/threads.jsonl
sed -n '1,40p' data/processed/v1_core_tables_manifest.json
```

## Step 16: Materialize Core Analytics

Write the recurring analytical outputs into `data/processed/analytics/`:

```bash
PYTHONPATH=src python src/cli.py materialize-core-analytics
```

This writes:

```text
data/processed/analytics/company_posting_counts_by_month.csv
data/processed/analytics/company_summary_by_month.csv
data/processed/analytics/remote_status_trends_by_month.csv
data/processed/analytics/remote_status_share_by_month.csv
data/processed/analytics/role_family_trends_by_month.csv
data/processed/analytics/distinct_roles_by_month.csv
data/processed/analytics/ai_concepts_by_month.csv
data/processed/analytics/ai_concepts_by_role_family.csv
data/processed/analytics/company_building_themes_by_month.csv
data/processed/analytics/company_semantic_spread.csv
data/processed/analytics/recurring_company_hiring_patterns.csv
data/processed/analytics/analytics_manifest.json
data/processed/analytics/visuals/
```

Inspect the outputs:

```bash
ls data/processed/analytics
sed -n '1,10p' data/processed/analytics/company_posting_counts_by_month.csv
sed -n '1,10p' data/processed/analytics/company_summary_by_month.csv
sed -n '1,12p' data/processed/analytics/remote_status_trends_by_month.csv
sed -n '1,12p' data/processed/analytics/remote_status_share_by_month.csv
sed -n '1,10p' data/processed/analytics/distinct_roles_by_month.csv
sed -n '1,14p' data/processed/analytics/ai_concepts_by_month.csv
sed -n '1,14p' data/processed/analytics/ai_concepts_by_role_family.csv
sed -n '1,14p' data/processed/analytics/company_building_themes_by_month.csv
sed -n '1,14p' data/processed/analytics/company_semantic_spread.csv
sed -n '1,12p' data/processed/analytics/recurring_company_hiring_patterns.csv
ls data/processed/analytics/visuals
```

The visuals directory now includes:

```text
data/processed/analytics/visuals/company_posting_counts_by_month.png
data/processed/analytics/visuals/company_summary_by_month.png
data/processed/analytics/visuals/remote_status_trends_by_month.png
data/processed/analytics/visuals/remote_status_share_by_month.png
data/processed/analytics/visuals/remote_status_share_timeseries.png
data/processed/analytics/visuals/role_family_trends_by_month.png
data/processed/analytics/visuals/role_family_trends_timeseries.png
data/processed/analytics/visuals/distinct_roles_by_month.png
data/processed/analytics/visuals/ai_concepts_by_month.png
data/processed/analytics/visuals/ai_concepts_share_by_month.png
data/processed/analytics/visuals/ai_concepts_by_role_family.png
data/processed/analytics/visuals/ai_concepts_by_role_family_share.png
data/processed/analytics/visuals/company_building_themes_2023.png
data/processed/analytics/visuals/company_building_themes_2024.png
data/processed/analytics/visuals/company_building_themes_2025.png
data/processed/analytics/visuals/company_building_themes_2026.png
data/processed/analytics/visuals/company_building_themes_timeseries.png
data/processed/analytics/visuals/company_semantic_spread.png
data/processed/analytics/visuals/company_role_semantic_spread.png
data/processed/analytics/visuals/company_post_vs_role_spread.png
data/processed/analytics/visuals/changed_companies_ranked.png
data/processed/analytics/visuals/company_variation_histograms/
data/processed/analytics/visuals/company_drift_projections/
data/processed/analytics/visuals/recurring_company_hiring_patterns.png
```

## Step 17: Company Change Analysis

Step 17 is produced by the same analytics materialization command:

```bash
PYTHONPATH=src python src/cli.py materialize-core-analytics
```

It adds:

```text
data/processed/analytics/company_role_semantic_spread.csv
data/processed/analytics/company_post_vs_role_spread.csv
data/processed/analytics/company_post_vs_role_spread_6m.csv
data/processed/analytics/company_embedding_drift.csv
data/processed/analytics/company_embedding_drift_monthly.csv
data/processed/analytics/changed_companies_ranked.csv
data/processed/analytics/visuals/company_role_semantic_spread.png
data/processed/analytics/visuals/company_post_vs_role_spread.png
data/processed/analytics/visuals/company_post_vs_role_spread_6m.png
data/processed/analytics/visuals/changed_companies_ranked.png
data/processed/analytics/visuals/company_drift_projections/
```

Inspect the outputs with:

```bash
sed -n '1,10p' data/processed/analytics/company_role_semantic_spread.csv
sed -n '1,10p' data/processed/analytics/company_post_vs_role_spread.csv
sed -n '1,10p' data/processed/analytics/company_post_vs_role_spread_6m.csv
sed -n '1,10p' data/processed/analytics/company_embedding_drift.csv
sed -n '1,10p' data/processed/analytics/changed_companies_ranked.csv
ls data/processed/analytics/visuals/company_drift_projections
```

## Interactive Explorer

Launch the Streamlit app from the project root:

```bash
streamlit run app.py
```

The explorer reads from `data/processed/v1_core_tables/` and lets you filter by:

- start month / end month
- companies
- role families
- remote status

It then shows:

- filtered hiring-post time series
- filtered remote-status mix over time
- filtered role-family time series
- filtered AI concept trends
- AI concepts by role family
- a what-companies-are-building summary
- a `Company Variation` tab with semantic spread metrics, pairwise-angle histograms, and company-level theme terms
- a `Change Analysis` tab with post-vs-role spread, changed-company ranking, company drift, and company projection views
- a windowed 6-month change layer inside `Change Analysis`, including all-company scatter, binned robustness boxplot, and same-company trajectories
- a searchable post preview table

Static histogram copies are also written locally under:

```text
data/processed/analytics/visuals/company_variation_histograms/
data/processed/analytics/visuals/company_drift_projections/
```

## Recommended End-To-End Order For One New Month

Example for `2025-03`:

```bash
PYTHONPATH=src python src/cli.py show-discovery-queries 2025-03
PYTHONPATH=src python src/cli.py fetch-thread-raw 2025-03 --path data/source_index.csv
PYTHONPATH=src python src/cli.py parse-thread-posts 2025-03
PYTHONPATH=src python src/cli.py validate-thread-raw 2025-03
PYTHONPATH=src python src/cli.py normalize-thread-posts 2025-03
PYTHONPATH=src python src/cli.py extract-thread-roles 2025-03
PYTHONPATH=src python src/cli.py normalize-thread-companies 2025-03
PYTHONPATH=src python src/cli.py materialize-v1-core-tables
PYTHONPATH=src python src/cli.py materialize-core-analytics
```

## Tests

Run the full current test suite:

```bash
python -m pytest tests/test_source_index.py tests/test_fetch.py tests/test_parse.py tests/test_validate.py tests/test_normalize.py tests/test_roles.py
python -m pytest tests/test_companies.py
python -m pytest tests/test_materialize.py
python -m pytest tests/test_analytics.py
```

Run only one area:

```bash
python -m pytest tests/test_normalize.py
python -m pytest tests/test_roles.py
```

## Useful File Locations

Source index:

```text
data/source_index.csv
```

Raw month outputs:

```text
data/raw/<YYYY-MM>/
```

Normalized month outputs:

```text
data/interim/<YYYY-MM>/posts_normalized.jsonl
data/interim/<YYYY-MM>/roles.jsonl
data/interim/<YYYY-MM>/companies.jsonl
```

Processed consolidated outputs:

```text
data/processed/v1_core_tables/
data/processed/v1_core_tables_manifest.json
data/processed/analytics/
```

## Notes

- `data/raw/`, `data/interim/`, and `data/processed/` are ignored by git.
- Steps 5 and part of 10 are intentionally not “one command” steps.
- If you delete generated data, rerun the relevant step commands to recreate it.
