# Step 19

Step 19 is intentionally retained for the **windowed post-vs-role change analysis**.

This is not part of the removed PostgreSQL / KB / Q&A layer.
It sits on top of the existing processed analytics stack and remains valuable.

## Goal

Take the company-level post-vs-role spread work from Step 17 and make it more temporally informative.

Instead of one aggregate spread number across the full history, divide the selected month range into non-overlapping 6-month windows and track:

- post spread by window
- role spread by window
- how these move together over time

## Core Outputs

### 1. Windowed post-vs-role table

Primary artifact:

- `data/processed/analytics/company_post_vs_role_spread_6m.csv`

Important fields include:

- `window_index`
- `window_start_month`
- `window_end_month`
- `window_label`
- `company_name`
- `post_mean_angle_deg`
- `role_mean_angle_deg`
- `spread_ratio`
- variance-related fields already produced by the analytics layer

### 2. Static visual

Primary PNG:

- `data/processed/analytics/visuals/company_post_vs_role_spread_6m.png`

This gives a compact window-by-window view across all companies.

### 3. Streamlit views

This layer remains part of the main exploration app and should stay visible in the change-analysis surface.

Key views:

- all-company windowed scatter
- binned robustness boxplot
- company-specific windowed trajectory
- company-specific windowed scatter

## Interpretation

This layer is useful because it preserves temporality.

Examples:

- low post spread and low role spread across windows: highly stable company
- rising post spread with flat role spread: wording or narrative changed more than hiring mix
- rising role spread with flat post spread: company template stayed tight while role mix changed
- both rising together in a window: genuine widening or pivot in hiring focus

## Why It Stays

This analysis is part of the pre-KB analytics stack.

It depends on:

- processed posts
- processed roles
- analytics embeddings / spread calculations
- the existing Streamlit explorer

It does **not** depend on:

- PostgreSQL
- the KB router
- the removed natural-language Q&A layer

## How To Rebuild

Recompute through the standard analytics command:

```bash
PYTHONPATH=src python src/cli.py materialize-core-analytics
```

Then inspect:

```bash
sed -n '1,10p' data/processed/analytics/company_post_vs_role_spread_6m.csv
ls data/processed/analytics/visuals/company_post_vs_role_spread_6m.png
streamlit run app.py
```
