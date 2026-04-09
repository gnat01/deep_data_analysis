# Step 17

Step 17 should focus on **repeated-post detection and company-level change analysis**.

## Goal

Move from descriptive analytics into measurable sameness vs change:

- which companies keep posting nearly the same thing
- which companies materially changed their hiring narrative
- whether that change is driven by roles, or by broader company framing

## Core Outputs

### 1. Repeated-post detection

For recurring companies, compute:

- exact text reuse
- similarity to previous post
- similarity to closest historical post

Target outputs:

- `company_post_reuse.csv`
- `company_post_similarity.csv`

### 2. Company post-text spread

Formalize the current company-variation work as a Step 17 artifact:

- pairwise cosine similarity
- pairwise angle distribution
- mean / median / p90 / max angle
- exact reuse share

Target outputs:

- `company_semantic_spread.csv`
- static histogram bundle
- Streamlit company-variation tab

### 3. Company role-text spread

For each company, compute the same spread metrics on **role text** rather than full post text.

Target outputs:

- `company_role_semantic_spread.csv`

This will help separate:

- companies that truly hire across varied roles
- companies whose role demand is stable but whose narrative changed

### 4. Post-vs-role spread comparison

Join post spread and role spread into one comparison layer.

Fields should include:

- `post_mean_angle_deg`
- `role_mean_angle_deg`
- `post_median_angle_deg`
- `role_median_angle_deg`
- `post_p90_angle_deg`
- `role_p90_angle_deg`
- `spread_gap_deg`
- `spread_ratio`

Target outputs:

- `company_post_vs_role_spread.csv`
- scatter plot of post spread vs role spread

Interpretation:

- both low: highly repetitive company
- both high: company genuinely hires across varied themes/roles
- post high, role low: company framing changed more than actual role demand
- role high, post low: compact company template but varied role mix

### 5. Structured change deltas

For recurring companies, measure changes in structured fields:

- role-family changes
- AI-concept changes
- company-building-theme changes
- remote-status changes
- compensation changes

Target outputs:

- `company_post_changes.csv`
- `company_change_summary.csv`

### 6. Changed-companies ranking

Rank companies by overall narrative change, combining:

- semantic spread
- low exact reuse
- structured deltas
- role spread

Target output:

- `changed_companies_ranked.csv`

## Streamlit

Add a Step 17-focused surface to the app for:

- repeated companies
- changed companies
- post spread vs role spread
- selected-company similarity/change evidence

## Recommended Build Order

1. exact and near-duplicate reuse
2. previous-post and closest-historical similarity
3. role-level spread by company
4. post-vs-role spread comparison
5. structured change deltas
6. changed-company ranking
7. Streamlit Step 17 views

## Success Criteria

Step 17 is complete when we can answer:

- which companies are highly repetitive
- which companies changed meaningfully
- whether that change came from role diversity or broader narrative shifts
