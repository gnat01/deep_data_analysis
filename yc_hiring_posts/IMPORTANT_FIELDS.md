# Important Fields

This document records field-level decisions that are easy to lose as the project grows.

It exists to prevent high-value raw or semi-structured fields from being accidentally removed, downgraded, or ignored in later stages.

## `misc.commtext_html`

Status:
- critical

Current location:
- `raw_posts.misc.commtext_html`

Why it matters:
- preserves the comment-body HTML without the rest of the surrounding row chrome
- retains paragraph structure better than flattened plain text
- keeps inline links in context
- is much more useful for downstream NLP and deep parsing than `raw_text` alone
- sits in the sweet spot between full-row `raw_html` and aggressively cleaned plain text

How to think about it:
- `raw_html` = full Hacker News row artifact
- `misc.commtext_html` = comment body HTML only
- `raw_text` = cleaned plain-text rendering of the comment body

Project rule:
- do not remove or stop populating `misc.commtext_html` without an explicit replacement plan
- if this field is ever promoted out of `misc`, preserve the same information content

## `raw_html`

Status:
- critical

Current location:
- `raw_posts.raw_html`

Why it matters:
- preserves the full source row exactly as captured
- supports auditability
- allows parser improvements later without refetching

## `raw_text`

Status:
- important

Current location:
- `raw_posts.raw_text`

Why it matters:
- useful for quick inspection, simple search, and lightweight analysis
- easier to work with than HTML for basic heuristics

Constraint:
- should not be treated as a full replacement for `misc.commtext_html`

## Maintenance Rule

When a field proves especially valuable during implementation, add it here with:

- current location
- why it matters
- whether it is critical, important, or optional
- any rule about preserving or promoting it later
