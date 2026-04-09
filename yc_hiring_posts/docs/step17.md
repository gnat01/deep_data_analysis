# Step 17

Step 17 should focus on **company-level change analysis**, building on the company-variation groundwork already present in Step 16.

## Goal

Move from descriptive company variation into a clearer explanation of **why** companies differ:

- which companies hire for a wide variety of roles
- which companies mostly keep roles stable but change the surrounding narrative
- which companies materially drift in their hiring language over time
- which companies should rank highest as genuinely changed companies

## Core Outputs

### 1. Company role-text spread

For each recurring company, compute semantic spread on **role text** rather than full post text.

Target outputs:

- `company_role_semantic_spread.csv`
- role-spread ranking PNG

### 2. Post-vs-role spread comparison

Join company post spread and role spread side by side.

Fields should include:

- `post_mean_angle_deg`
- `role_mean_angle_deg`
- `post_median_angle_deg`
- `role_median_angle_deg`
- `post_p90_angle_deg`
- `role_p90_angle_deg`
- `spread_ratio`

Target outputs:

- `company_post_vs_role_spread.csv`
- scatter plot PNG

Interpretation:

- both low: highly repetitive company
- both high: company genuinely hires across varied themes and roles
- post high, role low: company framing changed more than actual role demand
- role high, post low: compact company template but varied role mix

### 3. Temporal embedding drift

For recurring companies, measure how company post embeddings move over time.

This should include:

- month-by-month centroid angle from the first month
- month-by-month centroid angle from the immediately previous month
- within-month spread
- total drift score per company

Current `drift_score` definition:

- `drift_score = mean(angle_from_first_deg) + mean(angle_from_previous_deg)`

where:

- `angle_from_first_deg` is the centroid angle for a month against the first active month
- `angle_from_previous_deg` is the centroid angle for a month against the immediately previous active month

This is a heuristic summary, not a canonical statistic. It is meant to combine:

- long-run drift away from the initial hiring narrative
- step-to-step drift as the company evolves over time

Target outputs:

- `company_embedding_drift.csv`
- per-company time-series PNGs
- per-company animated projection GIFs

Projection visuals should use:

- a stable 2D projection per selected company
- month coloring / month progression
- local PNG and GIF copies, not Streamlit-only views

### 4. Changed-companies ranking

Rank companies by overall change, combining:

- post spread
- role spread
- temporal embedding drift

Target outputs:

- `changed_companies_ranked.csv`
- changed-company ranking PNG

## Streamlit

Add a Step 17-focused tab to the app for:

- post-vs-role spread comparison
- changed-companies ranking
- selected-company drift metrics
- selected-company projection / time evolution

## Local Artifacts

Step 17 should write local copies of:

- CSV outputs
- comparison PNGs
- company-level PNGs
- company-level GIFs

These should sit under `data/processed/analytics/` and `data/processed/analytics/visuals/`.

## Recommended Build Order

1. company role-text spread
2. post-vs-role spread comparison
3. temporal embedding drift
4. changed-companies ranking
5. static PNG / GIF bundle
6. Streamlit Step 17 tab

## Success Criteria

Step 17 is complete when we can answer:

- whether company variation is mostly role-driven or broader than roles
- which companies changed most over time
- how that change looks in both metrics and visuals
