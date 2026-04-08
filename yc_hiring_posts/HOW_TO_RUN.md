# How To Run

This document explains exactly how to run **Steps 4 through 12** from the parent project directory:

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

## Recommended End-To-End Order For One New Month

Example for `2025-03`:

```bash
PYTHONPATH=src python src/cli.py show-discovery-queries 2025-03
PYTHONPATH=src python src/cli.py fetch-thread-raw 2025-03 --path data/source_index.csv
PYTHONPATH=src python src/cli.py parse-thread-posts 2025-03
PYTHONPATH=src python src/cli.py validate-thread-raw 2025-03
PYTHONPATH=src python src/cli.py normalize-thread-posts 2025-03
PYTHONPATH=src python src/cli.py extract-thread-roles 2025-03
```

## Tests

Run the full current test suite:

```bash
python -m pytest tests/test_source_index.py tests/test_fetch.py tests/test_parse.py tests/test_validate.py tests/test_normalize.py tests/test_roles.py
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
```

## Notes

- `data/raw/`, `data/interim/`, and `data/processed/` are ignored by git.
- Steps 5 and part of 10 are intentionally not “one command” steps.
- If you delete generated data, rerun the relevant step commands to recreate it.
