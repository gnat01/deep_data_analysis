# Current Progress

This file is the current handoff note for the YC / HN hiring-posts project.

## Where We Are

- Steps 4 through 17 are the active project surface.
- The raw -> interim -> processed pipeline is intact.
- Step 16 analytics, visuals, and the main Streamlit explorer are intact.
- Step 17 company-change analysis is intact.
- Step 19 windowed post-vs-role analysis is also intentionally retained.
- The Step 18-20 PostgreSQL / KB / Q&A layer has been intentionally removed.

## What Is Solid

- source indexing and thread fetching
- parsing and validation
- post normalization
- role extraction
- company normalization
- processed core-table materialization
- analytics materialization
- time-series and change-analysis visuals
- Streamlit exploration app built on processed files

## Key Files

- [`HOW_TO_RUN.md`](./HOW_TO_RUN.md)
- [`step17.md`](./step17.md)
- [`step19.md`](./step19.md)
- [`possible_qs.md`](./possible_qs.md)
- [`IMPORTANT_FIELDS.md`](./IMPORTANT_FIELDS.md)
- [`schema.md`](./schema.md)
- [`src/cli.py`](../src/cli.py)
- [`src/analytics.py`](../src/analytics.py)
- [`src/explore_app.py`](../src/explore_app.py)
- [`src/materialize.py`](../src/materialize.py)

## Current Direction

The project is now back to the end-of-Step-17 boundary.

Any future knowledge-base or database work should be reconsidered from scratch rather than built on the removed Step 18-20 layer.
