# Question Coverage

This note summarizes how much of the `50`-question bank is currently covered by the PostgreSQL KB helper layer.

## Coverage Summary

- strict primary-helper coverage: `38 / 50` = `76%`
- practical coverage with secondary-helper fallback: `49 / 50` = `98%`
- fully uncovered: `1 / 50`

## What The Coverage Numbers Mean

- `strict primary-helper coverage`
  The question's `primary_helper` in [`question_catalog.md`](./question_catalog.md) already exists as a real KB helper.

- `practical coverage with secondary-helper fallback`
  The catalog still points to a future helper or router, but an existing secondary helper can already answer the question reasonably well.

## Strictly Covered Questions

Covered directly by implemented primary helpers:

- `Q1, Q2`
- `Q6, Q7, Q8`
- `Q10, Q12, Q13, Q14, Q15`
- `Q18, Q19, Q20, Q21, Q22, Q23, Q24, Q25`
- `Q27, Q28, Q29, Q30, Q31, Q32, Q33`
- `Q35, Q36, Q37, Q38, Q39, Q40`
- `Q42, Q43, Q44`
- `Q47, Q48, Q49, Q50`

## Fallback-Covered Questions

These are not yet aligned to a real primary helper in the catalog, but they are practically answerable today through an implemented secondary helper or a simple composition around one.

### Q3

- question: Which companies posted hiring ads in every month of 2025?
- current practical path:
  `companies-every-month-postgres`

### Q4

- question: Which companies appeared for the first time in the HN hiring threads in 2026?
- current practical path:
  `month-summary-postgres` plus filtered company activity logic

### Q5

- question: Which companies returned to hiring after being absent for at least 6 months?
- current practical path:
  `company-activity-postgres` plus simple absence-gap logic

### Q9

- question: Which companies were remote-first in 2025?
- current practical path:
  `remote-first-companies-postgres`

### Q11

- question: Which companies explicitly mentioned visa sponsorship?
- current practical path:
  `search-postgres-posts`

### Q16

- question: Which companies had high role spread but low post spread?
- current practical path:
  `company-change-summary-postgres`

### Q17

- question: Which companies had high post spread but relatively low role spread?
- current practical path:
  `company-change-summary-postgres`

### Q26

- question: Which companies hired for both backend and ML roles in the same month?
- current practical path:
  `companies-role-pair-postgres`

### Q34

- question: Which companies changed from generic software hiring language to explicit AI-agent language over time?
- current practical path:
  `company-change-summary-postgres` plus `ai-concept-timeline-postgres`

### Q41

- question: Did any of the FAANG companies ever post on HN?
- current practical path:
  `search-postgres-posts`

### Q46

- question: What is the distribution of global remote roles to overall remote roles across the dataset, year by year?
- current practical path:
  `global-remote-share-postgres`

## Uncovered Question

### Q45

- question: Give the geographical distribution of where companies were headquartered for every year.
- status: genuinely uncovered
- reason:
  the current schema does not contain reliable company-headquarters data
- conclusion:
  this is a **data availability / data quality** gap, not just a missing helper

## Recommended Interpretation

For Step 19 Task 3:

- helper coverage is already strong enough to support most of the catalog
- the main remaining work is:
  - making fallback-covered questions first-class where useful
  - adding small composition logic
  - not pretending we can answer geography-headquarters questions without better source data
