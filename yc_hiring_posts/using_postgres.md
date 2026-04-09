# Using PostgreSQL

This project now has a local PostgreSQL 16 instance running for the YC hiring-posts knowledge base.

## Local Setup Summary

- service: `postgresql@16`
- database: `yc_hiring_posts`
- schema: `yc_hiring`
- local user: `gn`
- unix socket: `/tmp`

Working connection URL:

```text
postgresql://gn@/yc_hiring_posts?host=/tmp
```

Saved locally in:

```text
config/postgres.local.env
```

## Start The Instance

Start PostgreSQL with Homebrew:

```bash
brew services start postgresql@16
```

Check service status:

```bash
brew services list | grep postgresql
```

Stop it if needed:

```bash
brew services stop postgresql@16
```

## Log In

Connect with `psql`:

```bash
/opt/homebrew/opt/postgresql@16/bin/psql "postgresql://gn@/yc_hiring_posts?host=/tmp"
```

Or connect with explicit pieces:

```bash
/opt/homebrew/opt/postgresql@16/bin/psql -h /tmp -d yc_hiring_posts -U gn
```

## Useful psql Commands Once Inside

Show schemas:

```sql
\dn
```

List tables in the project schema:

```sql
\dt yc_hiring.*
```

Describe one table:

```sql
\d+ yc_hiring.posts
```

See row counts quickly:

```sql
SELECT 'threads' AS table_name, COUNT(*) FROM yc_hiring.threads
UNION ALL
SELECT 'raw_posts', COUNT(*) FROM yc_hiring.raw_posts
UNION ALL
SELECT 'companies', COUNT(*) FROM yc_hiring.companies
UNION ALL
SELECT 'posts', COUNT(*) FROM yc_hiring.posts
UNION ALL
SELECT 'roles', COUNT(*) FROM yc_hiring.roles;
```

Quit `psql`:

```sql
\q
```

## Example Queries

Hiring posts for one company:

```sql
SELECT t.thread_month, p.company_name_observed, p.remote_status, p.post_text_clean
FROM yc_hiring.posts p
JOIN yc_hiring.threads t ON t.thread_id = p.thread_id
WHERE p.company_name_observed ILIKE 'DuckDuckGo%'
  AND p.is_hiring_post = TRUE
ORDER BY t.thread_month;
```

Companies posting in each month:

```sql
SELECT t.thread_month, COUNT(DISTINCT p.company_id) AS company_count
FROM yc_hiring.posts p
JOIN yc_hiring.threads t ON t.thread_id = p.thread_id
WHERE p.is_hiring_post = TRUE
GROUP BY t.thread_month
ORDER BY t.thread_month;
```

Full-text search over posts:

```sql
SELECT t.thread_month, p.company_name_observed, p.post_text_clean
FROM yc_hiring.posts p
JOIN yc_hiring.threads t ON t.thread_id = p.thread_id
WHERE p.post_search_tsv @@ plainto_tsquery('english', 'agent orchestration')
ORDER BY t.thread_month;
```

Role-family search:

```sql
SELECT t.thread_month, c.company_name_observed_preferred, r.role_family, r.role_title_observed
FROM yc_hiring.roles r
JOIN yc_hiring.posts p ON p.post_id = r.post_id
JOIN yc_hiring.threads t ON t.thread_id = p.thread_id
LEFT JOIN yc_hiring.companies c ON c.company_id = r.company_id
WHERE r.role_family = 'data'
ORDER BY t.thread_month, c.company_name_observed_preferred;
```

## How To Store Intermediate Results

Use a separate schema for scratch work instead of polluting `yc_hiring`.

Create one:

```sql
CREATE SCHEMA IF NOT EXISTS yc_hiring_scratch;
```

Store a temporary analysis table:

```sql
CREATE TABLE yc_hiring_scratch.duckduckgo_posts AS
SELECT t.thread_month, p.company_id, p.company_name_observed, p.post_text_clean
FROM yc_hiring.posts p
JOIN yc_hiring.threads t ON t.thread_id = p.thread_id
WHERE p.is_hiring_post = TRUE
  AND p.company_name_observed ILIKE 'DuckDuckGo%';
```

Store an intermediate aggregated result:

```sql
CREATE TABLE yc_hiring_scratch.remote_mix_by_month AS
SELECT
    t.thread_month,
    p.remote_status,
    COUNT(*) AS post_count
FROM yc_hiring.posts p
JOIN yc_hiring.threads t ON t.thread_id = p.thread_id
WHERE p.is_hiring_post = TRUE
GROUP BY t.thread_month, p.remote_status
ORDER BY t.thread_month, p.remote_status;
```

Inspect your scratch objects:

```sql
\dt yc_hiring_scratch.*
```

Drop one when done:

```sql
DROP TABLE IF EXISTS yc_hiring_scratch.remote_mix_by_month;
```

## Recommended Convention

- keep core loaded data in `yc_hiring`
- keep ad hoc experiments in `yc_hiring_scratch`
- if an intermediate result becomes important and repeatable, move it into:
  - code
  - `sql/`
  - or a formal materialized view later

## Reinitialize / Reload From Project CLI

From the project root:

```bash
python src/cli.py init-postgres-kb --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp"
python src/cli.py load-postgres-kb --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp"
python src/cli.py inspect-postgres-kb --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp"
```

## Retrieval From The Project CLI

Search posts with structured filters and optional full-text search:

```bash
python src/cli.py search-postgres-posts \
  --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp" \
  --query "data science" \
  --company "DuckDuckGo" \
  --month-from 2024-12 \
  --month-to 2026-01 \
  --limit 10 \
  --summary-only
```

Search roles:

```bash
python src/cli.py search-postgres-roles \
  --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp" \
  --query "platform engineer" \
  --remote-status remote \
  --month-from 2025-01 \
  --month-to 2025-12 \
  --limit 10 \
  --summary-only
```

Remove `--summary-only` if you want the full result set.

Answer month-by-month company activity questions:

```bash
python src/cli.py company-activity-postgres \
  --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp" \
  --company "DuckDuckGo" \
  --month-from 2024-12 \
  --month-to 2026-01 \
  --limit-evidence 5
```

Check whether a company hired for a role query or family in a range:

```bash
python src/cli.py company-role-presence-postgres \
  --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp" \
  --company "DuckDuckGo" \
  --query "data science" \
  --month-from 2024-12 \
  --month-to 2026-01 \
  --limit-evidence 5
```
