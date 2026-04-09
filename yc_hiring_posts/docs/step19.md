# Step 19

Step 19 should turn the PostgreSQL knowledge base into a real retrieval layer and also deepen the company-change views with **windowed temporal structure**.

## Core Retrieval Goal

Add structured and text retrieval over the PostgreSQL-backed corpus so we can answer targeted questions quickly and reproducibly.

## Change-Analysis Extension That Must Be Added

The current `post_mean_angle_deg` vs `role_mean_angle_deg` comparison is useful, but it is still an approximation because it collapses time inside one selected window.

Step 19 must add a **windowed version** of this analysis.

### Required Design

Compute company change metrics over **non-overlapping 6-month windows**.

That means:

- one `post_mean_angle_deg` per company per 6-month window
- one `role_mean_angle_deg` per company per 6-month window

### Why This Matters

Without windowing:

- temporary pivots get averaged away
- return-to-template behavior gets hidden
- companies that change for a while and then revert look flatter than they really are

With 6-month windows:

- we can see all-company spread structure by time window
- we can track a single company across windows
- we can capture companies that start repetitive, pivot, then stabilize again

### Required Outputs

1. all-company scatter plots by 6-month window
   x = `role_mean_angle_deg`
   y = `post_mean_angle_deg`

2. binned robustness view by 6-month window
   x = role-angle bins
   y = post-angle distribution within each bin

3. same-company trajectory view across windows
   for a selected company, show how its windowed points move over time

4. windowed metrics table
   one row per `company x 6-month window`

### Streamlit Requirement

These must be added to the `Change Analysis` tab when Step 19 is built.

The app should support:

- choosing a company
- viewing its windowed trajectory across non-overlapping 6-month periods
- comparing that trajectory to the broader all-company scatter for each window

## Retrieval Deliverables

Step 19 should also add:

- structured filtering on company / month / role family / remote status
- full-text post retrieval
- role-level retrieval
- evidence-linked result sets coming out of PostgreSQL

### Task 1: PostgreSQL Retrieval Layer

The first retrieval task should expose CLI-level search over the live PostgreSQL KB for:

- `posts`
- `roles`

Minimum requirements:

- optional full-text query
- optional structured filters:
  - company
  - role family
  - remote status
  - month range
- evidence-linked rows that include:
  - thread month
  - company
  - source URL
  - matched post or role text
- a compact summary mode for quick inspection from the terminal

### Task 2: Answer-Oriented KB Helpers

The second task should turn raw retrieval into reusable question helpers.

Minimum helpers:

- company activity over time
  - which months did a company post?
  - how many posts / roles / role families per month?
- company role presence in a range
  - did a company hire for a given role query or role family between two dates?
  - return boolean + matched months + evidence rows

These helpers should stay structured and evidence-linked. Natural-language querying still belongs later.

### Task 3: Broader Helper Coverage

The third task should expand helper coverage across the largest remaining question families.

Current helper blocks now in place:

- month summaries and month rankings
- role-family timelines
- companies-for-role lookups
- evidence lookup helpers
- remote-mix helpers
- remote-change helpers
- compensation-history helpers
- AI concept timeline helpers
- first requirement-change summarisation helper
- recurring-company / post-shape helpers:
  - companies posting every month in a year
  - remote-first companies in a year
  - companies hiring across two role families in a range
  - global-remote share by year
  - post-length summary by year
  - company post-length consistency

These should stay:

- CLI accessible
- PostgreSQL backed
- evidence linked
- reusable by later orchestration and NL layers

### Task 3 Commands Now Available

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

### Remaining Task 3 Work

Task 3 is now well underway, but not complete.

Most likely remaining families:

- stronger comparative helpers
- geography-aware helpers only where the data truly supports them
- tighter summarisation quality for change questions
- helper composition patterns that can support Step 20 routing
