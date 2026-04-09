# Local PostgreSQL Setup

This project now has a local PostgreSQL 16 instance installed and started via Homebrew.

## Service

- formula: `postgresql@16`
- service label: `homebrew.mxcl.postgresql@16`

## Local Server Details

- data directory: `/opt/homebrew/var/postgresql@16`
- `pg_hba.conf`: `/opt/homebrew/var/postgresql@16/pg_hba.conf`
- port: `5432`
- unix socket directory: `/tmp`

## Project Database

- database name: `yc_hiring_posts`
- schema name: `yc_hiring`
- local database user: `gn`

## Working Connection URL

Use the Unix-socket DSN:

```text
postgresql://gn@/yc_hiring_posts?host=/tmp
```

This is also saved in:

```text
config/postgres.local.env
```

## Access Notes

- local access is currently working through the Unix socket at `/tmp`
- commands that connect through PostgreSQL should use the socket DSN above unless you explicitly reconfigure host-based access
- Step 18 loader commands can be run with:

```bash
python src/cli.py init-postgres-kb --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp"
python src/cli.py load-postgres-kb --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp"
python src/cli.py inspect-postgres-kb --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp"
```
