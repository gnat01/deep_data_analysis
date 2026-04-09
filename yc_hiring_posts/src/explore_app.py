"""Interactive Streamlit explorer for YC hiring-post analytics."""

from __future__ import annotations

import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import streamlit as st
from sklearn.feature_extraction.text import TfidfVectorizer

from analytics import AI_CONCEPT_PATTERNS, PRODUCT_THEME_PATTERNS, pairwise_semantic_geometry, semantic_angle_metrics
from storage import processed_data_dir


MONTH_ABBREVIATIONS = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def load_table(name: str) -> pd.DataFrame:
    path = processed_data_dir() / "v1_core_tables" / name
    return pd.read_json(path, lines=True)


@st.cache_data(show_spinner=False)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    posts = load_table("posts.jsonl")
    roles = load_table("roles.jsonl")
    companies = load_table("companies.jsonl")
    threads = load_table("threads.jsonl")
    return posts, roles, companies, threads


def with_company_display(posts: pd.DataFrame, roles: pd.DataFrame, companies: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    company_name_by_id = dict(zip(companies["company_id"], companies["company_name_observed_preferred"], strict=False))
    posts = posts.copy()
    roles = roles.copy()
    posts["company_display"] = posts["company_id"].map(company_name_by_id).fillna(posts["company_name_observed"]).fillna("[unresolved]")
    roles["company_display"] = roles["company_id"].map(company_name_by_id).fillna("[unresolved]")
    return posts, roles


def with_thread_month(posts: pd.DataFrame, roles: pd.DataFrame, threads: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    month_by_thread_id = dict(zip(threads["thread_id"].astype(str), threads["thread_month"].astype(str), strict=False))
    posts = posts.copy()
    roles = roles.copy()
    posts["thread_id"] = posts["thread_id"].astype(str)
    roles["post_id"] = roles["post_id"].astype(str)
    posts["thread_month"] = posts["thread_id"].map(month_by_thread_id)
    post_month_by_post_id = dict(zip(posts["post_id"].astype(str), posts["thread_month"].astype(str), strict=False))
    roles["thread_month"] = roles["post_id"].map(post_month_by_post_id)
    return posts, roles


def app_style() -> None:
    st.set_page_config(page_title="YC Hiring Explorer", layout="wide")
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #f7f2e8 0%, #efe3cf 100%);
        }
        .block-container {
            padding-top: 1.4rem;
            padding-bottom: 2rem;
        }
        .hero {
            background: linear-gradient(135deg, #183a37 0%, #395b64 55%, #d5b26f 100%);
            border-radius: 22px;
            padding: 1.5rem 1.7rem;
            color: #fffaf0;
            margin-bottom: 1.1rem;
            box-shadow: 0 12px 30px rgba(24, 58, 55, 0.18);
        }
        .hero h1 {
            margin: 0 0 0.35rem 0;
            font-size: 2.1rem;
        }
        .hero p {
            margin: 0;
            font-size: 1.02rem;
            max-width: 54rem;
        }
        .metric-card {
            background: rgba(255, 250, 240, 0.78);
            border: 1px solid rgba(24, 58, 55, 0.12);
            border-radius: 16px;
            padding: 0.8rem 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def build_filtered_frames(
    posts: pd.DataFrame,
    roles: pd.DataFrame,
    companies: pd.DataFrame,
    start_month: str,
    end_month: str,
    selected_companies: list[str],
    selected_role_families: list[str],
    selected_remote_statuses: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    posts, roles = with_company_display(posts, roles, companies)

    filtered_posts = posts[posts["is_hiring_post"] == True].copy()
    filtered_posts = filtered_posts[
        (filtered_posts["thread_month"] >= start_month) & (filtered_posts["thread_month"] <= end_month)
    ]
    if selected_companies:
        filtered_posts = filtered_posts[filtered_posts["company_display"].isin(selected_companies)]
    if selected_remote_statuses:
        filtered_posts = filtered_posts[filtered_posts["remote_status"].fillna("unspecified").isin(selected_remote_statuses)]

    filtered_roles = roles.copy()
    filtered_roles = filtered_roles[filtered_roles["post_id"].astype(str).isin(set(filtered_posts["post_id"].astype(str)))]
    if selected_companies:
        filtered_roles = filtered_roles[filtered_roles["company_display"].isin(selected_companies)]
    if selected_role_families:
        filtered_roles = filtered_roles[filtered_roles["role_family"].fillna("unknown").isin(selected_role_families)]
    if selected_remote_statuses:
        allowed_post_ids = set(filtered_posts["post_id"].astype(str))
        filtered_roles = filtered_roles[filtered_roles["post_id"].astype(str).isin(allowed_post_ids)]

    if selected_role_families:
        allowed_post_ids = set(filtered_roles["post_id"].astype(str))
        filtered_posts = filtered_posts[filtered_posts["post_id"].astype(str).isin(allowed_post_ids)]

    return filtered_posts, filtered_roles


def month_counts(frame: pd.DataFrame, value_column: str, count_name: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["thread_month", value_column, count_name])
    grouped = (
        frame.groupby(["thread_month", value_column], dropna=False)
        .size()
        .reset_index(name=count_name)
        .sort_values(["thread_month", value_column])
    )
    return grouped


def pretty_month_label(thread_month: str) -> str:
    year_text, month_text = thread_month.split("-")
    month_number = int(month_text)
    month_name = MONTH_ABBREVIATIONS[month_number]
    if month_number == 1:
        return f"{month_name}\n{year_text}"
    return month_name


def apply_month_axis(ax, thread_months: list[str]) -> None:
    positions = list(range(len(thread_months)))
    ax.set_xticks(positions)
    ax.set_xticklabels([pretty_month_label(value) for value in thread_months])
    ax.tick_params(axis="x", labelrotation=90, labelsize=9, pad=6)
    for position, thread_month in enumerate(thread_months):
        if thread_month.endswith("-01"):
            ax.axvline(position, color="#d9ccb8", linewidth=1.0, alpha=0.45, zorder=0)


def line_chart(frame: pd.DataFrame, x_col: str, y_col: str, series_col: str, title: str, ylabel: str):
    fig, ax = plt.subplots(figsize=(12.8, 5.4))
    pivot = frame.pivot_table(index=x_col, columns=series_col, values=y_col, aggfunc="sum", fill_value=0)
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    for column in pivot.columns:
        ax.plot(positions, pivot[column], marker="o", linewidth=2.6, label=str(column).title())
    ax.set_title(title)
    ax.set_xlabel("Thread month")
    ax.set_ylabel(ylabel)
    apply_month_axis(ax, months)
    ax.legend(frameon=True, ncols=2)
    ax.grid(alpha=0.25, linestyle="--")
    return fig


def stacked_pct_chart(frame: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(12.8, 5.6))
    pivot = frame.pivot_table(index="thread_month", columns="remote_status", values="post_count", aggfunc="sum", fill_value=0)
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    total = pivot.sum(axis=1).replace(0, 1)
    share = pivot.div(total, axis=0) * 100.0
    order = [status for status in ["remote", "hybrid", "onsite", "unspecified"] if status in share.columns] + [
        status for status in share.columns if status not in {"remote", "hybrid", "onsite", "unspecified"}
    ]
    share = share[order]
    colors = {"remote": "#2a9d8f", "hybrid": "#e9c46a", "onsite": "#e76f51", "unspecified": "#8d99ae"}
    bottom = None
    for column in share.columns:
        values = share[column].tolist()
        ax.bar(positions, values, bottom=bottom, label=str(column).title(), color=colors.get(column, "#457b9d"))
        bottom = values if bottom is None else [l + r for l, r in zip(bottom, values)]
    ax.set_title("Remote Mix Over Time")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Share of selected posts (%)")
    apply_month_axis(ax, months)
    ax.set_ylim(0, 100)
    ax.legend(frameon=True, ncols=4, loc="upper center", bbox_to_anchor=(0.5, 1.16))
    return fig


def concept_rows(frame: pd.DataFrame, patterns: list[tuple[str, tuple[str, ...]]], count_field: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["thread_month", "concept_name", count_field, "month_total_hiring_posts", "share_pct"])
    month_totals = frame.groupby("thread_month").size().to_dict()
    counts: dict[tuple[str, str], int] = {}
    for _, row in frame.iterrows():
        month = str(row["thread_month"])
        text = str(row.get("post_text_clean") or "").lower()
        for concept_name, concept_patterns in patterns:
            if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in concept_patterns):
                key = (month, concept_name)
                counts[key] = counts.get(key, 0) + 1
    rows = []
    for (month, concept_name), count in sorted(counts.items()):
        total = month_totals[month]
        rows.append(
            {
                "thread_month": month,
                "concept_name": concept_name,
                count_field: count,
                "month_total_hiring_posts": total,
                "share_pct": round((count / total) * 100.0, 2) if total else 0.0,
            }
        )
    return pd.DataFrame(rows)


def concept_line_chart(frame: pd.DataFrame, y_col: str, title: str, ylabel: str, top_n: int = 6):
    if frame.empty:
        return None
    top_concepts = (
        frame.groupby("concept_name", as_index=False)[y_col]
        .sum()
        .sort_values([y_col, "concept_name"], ascending=[False, True])
        .head(top_n)["concept_name"]
        .tolist()
    )
    filtered = frame[frame["concept_name"].isin(top_concepts)]
    pivot = filtered.pivot_table(index="thread_month", columns="concept_name", values=y_col, aggfunc="sum", fill_value=0)
    months = pivot.index.tolist()
    positions = list(range(len(months)))
    fig, ax = plt.subplots(figsize=(13.2, 5.6))
    for column in pivot.columns:
        ax.plot(positions, pivot[column], marker="o", linewidth=2.6, label=str(column).replace("_", " "))
    ax.set_title(title)
    ax.set_xlabel("Thread month")
    ax.set_ylabel(ylabel)
    apply_month_axis(ax, months)
    ax.legend(frameon=True, ncols=2)
    ax.grid(alpha=0.25, linestyle="--")
    return fig


def theme_summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows = concept_rows(frame, PRODUCT_THEME_PATTERNS, "post_count")
    if rows.empty:
        return rows
    return (
        rows.groupby("concept_name", as_index=False)["post_count"]
        .sum()
        .sort_values(["post_count", "concept_name"], ascending=[False, True])
        .rename(columns={"concept_name": "building_theme"})
    )


def theme_rows(frame: pd.DataFrame) -> pd.DataFrame:
    rows = concept_rows(frame, PRODUCT_THEME_PATTERNS, "post_count")
    if rows.empty:
        return rows.rename(columns={"concept_name": "building_theme"})
    return rows.rename(columns={"concept_name": "building_theme"})


def theme_year_heatmap(theme_frame: pd.DataFrame, year: str):
    year_frame = theme_frame[theme_frame["thread_month"].astype(str).str.startswith(year)].copy()
    if year_frame.empty:
        return None
    top_themes = (
        year_frame.groupby("building_theme", as_index=False)["post_count"]
        .sum()
        .sort_values(["post_count", "building_theme"], ascending=[False, True])
        .head(8)
    )
    filtered = year_frame[year_frame["building_theme"].isin(top_themes["building_theme"])]
    pivot = filtered.pivot_table(index="building_theme", columns="thread_month", values="post_count", aggfunc="sum", fill_value=0)
    pivot = pivot.loc[top_themes["building_theme"]]
    pretty_columns = [pretty_month_label(value) for value in pivot.columns]
    pivot.columns = pretty_columns
    fig, ax = plt.subplots(figsize=(8.8, 4.8), constrained_layout=True)
    heatmap = sns.heatmap(
        pivot,
        cmap="YlGnBu",
        linewidths=0.7,
        linecolor="#ece4d8",
        cbar_kws={"label": "Posts"},
        annot=True,
        fmt="g",
        annot_kws={"fontsize": 9, "fontweight": "bold"},
        ax=ax,
    )
    threshold = float(pivot.to_numpy().max()) * 0.42 if not pivot.empty else 0.0
    for text, value in zip(heatmap.texts, pivot.to_numpy().flatten(), strict=False):
        text.set_color("#fffaf0" if float(value) >= threshold else "#102a43")
    ax.set_title(f"What Companies Are Building ({year})")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Theme")
    return fig


def ai_role_family_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["role_family", "concept_name", "role_count", "role_share_pct"])
    counts: dict[tuple[str, str], int] = {}
    totals: dict[str, int] = {}
    for _, row in frame.iterrows():
        role_family = str(row.get("role_family") or "unknown")
        totals[role_family] = totals.get(role_family, 0) + 1
        text = "\n".join(
            [
                str(row.get("role_title_observed") or ""),
                str(row.get("role_title_normalized") or ""),
                str(row.get("skills_text") or ""),
                str(row.get("requirements_text") or ""),
                str(row.get("responsibilities_text") or ""),
            ]
        ).lower()
        for concept_name, concept_patterns in AI_CONCEPT_PATTERNS:
            if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in concept_patterns):
                key = (role_family, concept_name)
                counts[key] = counts.get(key, 0) + 1
    rows = []
    for (role_family, concept_name), count in sorted(counts.items()):
        total = totals[role_family]
        rows.append(
            {
                "role_family": role_family,
                "concept_name": concept_name,
                "role_count": count,
                "role_share_pct": round((count / total) * 100.0, 2) if total else 0.0,
            }
        )
    return pd.DataFrame(rows)


def ai_role_family_heatmap(frame: pd.DataFrame, value_column: str, title: str, colorbar_label: str):
    if frame.empty:
        return None
    top_families = (
        frame.groupby("role_family", as_index=False)[value_column]
        .sum()
        .sort_values([value_column, "role_family"], ascending=[False, True])
        .head(8)["role_family"]
        .tolist()
    )
    top_concepts = (
        frame.groupby("concept_name", as_index=False)[value_column]
        .sum()
        .sort_values([value_column, "concept_name"], ascending=[False, True])
        .head(8)["concept_name"]
        .tolist()
    )
    filtered = frame[frame["role_family"].isin(top_families) & frame["concept_name"].isin(top_concepts)]
    pivot = filtered.pivot_table(index="role_family", columns="concept_name", values=value_column, aggfunc="sum", fill_value=0)
    fig, ax = plt.subplots(figsize=(13.5, 7.4), constrained_layout=True)
    heatmap = sns.heatmap(
        pivot,
        cmap="YlOrBr" if value_column == "role_count" else "crest",
        linewidths=0.6,
        linecolor="#ece4d8",
        annot=True,
        fmt="g" if value_column == "role_count" else ".1f",
        annot_kws={"fontsize": 9, "fontweight": "bold"},
        cbar_kws={"label": colorbar_label},
        ax=ax,
    )
    threshold = float(pivot.to_numpy().max()) * 0.45 if not pivot.empty else 0.0
    for text, value in zip(heatmap.texts, pivot.to_numpy().flatten(), strict=False):
        text.set_color("#fffaf0" if float(value) >= threshold else "#102a43")
    ax.set_title(title)
    ax.set_xlabel("AI concept")
    ax.set_ylabel("Role family")
    ax.tick_params(axis="x", labelrotation=0, labelsize=10)
    ax.tick_params(axis="y", labelsize=10)
    return fig


def sample_filtered_posts(frame: pd.DataFrame, max_rows: int = 18) -> pd.DataFrame:
    """Return a balanced sample of filtered posts across months."""

    if frame.empty:
        return frame
    months = sorted(frame["thread_month"].dropna().astype(str).unique().tolist())
    if not months:
        return frame.head(0)
    per_month = max(1, max_rows // len(months))
    sampled_parts = []
    for month in months:
        month_frame = frame[frame["thread_month"] == month].sort_values(["company_display", "location_text", "compensation_text"], na_position="last")
        sampled_parts.append(month_frame.head(per_month))
    sampled = pd.concat(sampled_parts, ignore_index=True)
    if len(sampled) < max_rows:
        used_post_ids = set(sampled["post_id"].astype(str))
        remainder = frame[~frame["post_id"].astype(str).isin(used_post_ids)].sort_values(
            ["thread_month", "company_display", "location_text"], na_position="last"
        )
        sampled = pd.concat([sampled, remainder.head(max_rows - len(sampled))], ignore_index=True)
    return sampled.head(max_rows)


def build_insights(filtered_posts: pd.DataFrame, filtered_roles: pd.DataFrame) -> list[str]:
    if filtered_posts.empty:
        return ["No hiring posts match the current filters."]
    insights = []
    monthly_counts = filtered_posts.groupby("thread_month").size().sort_index()
    top_month = monthly_counts.idxmax()
    insights.append(f"Peak filtered activity was in {top_month} with {int(monthly_counts.max())} hiring posts.")

    remote_mix = (
        filtered_posts["remote_status"].fillna("unspecified").value_counts(normalize=True).mul(100).round(1).sort_values(ascending=False)
    )
    if not remote_mix.empty:
        top_remote = remote_mix.index[0]
        insights.append(f"{top_remote.title()} dominates the selected slice at {remote_mix.iloc[0]:.1f}% of posts.")

    if not filtered_roles.empty:
        top_role = filtered_roles["role_family"].fillna("unknown").value_counts().idxmax()
        top_role_count = int(filtered_roles["role_family"].fillna("unknown").value_counts().max())
        insights.append(f"The most common role family is {top_role} with {top_role_count} extracted roles.")

    top_companies = filtered_posts["company_display"].value_counts().head(3)
    if not top_companies.empty:
        insights.append(
            "Top companies in the current slice: "
            + ", ".join(f"{company} ({count})" for company, count in top_companies.items())
            + "."
        )
    return insights


@st.cache_data(show_spinner=False)
def load_company_spread_table() -> pd.DataFrame:
    path = processed_data_dir() / "analytics" / "company_semantic_spread.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def company_variation_rows(frame: pd.DataFrame, top_n: int = 60) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "company_display",
                "post_count",
                "mean_pairwise_angle_deg",
                "median_pairwise_angle_deg",
                "p90_pairwise_angle_deg",
                "max_pairwise_angle_deg",
                "exact_reuse_share",
            ]
        )
    top_companies = frame["company_display"].value_counts().head(top_n).index.tolist()
    rows: list[dict[str, object]] = []
    for company_name in top_companies:
        company_posts = frame[frame["company_display"] == company_name].sort_values("thread_month")
        texts = company_posts["post_text_clean"].fillna("").astype(str).tolist()
        metrics = semantic_angle_metrics(texts)
        rows.append(
            {
                "company_display": company_name,
                "post_count": len(company_posts),
                "mean_pairwise_angle_deg": metrics["mean_pairwise_angle_deg"],
                "median_pairwise_angle_deg": metrics["median_pairwise_angle_deg"],
                "p90_pairwise_angle_deg": metrics["p90_pairwise_angle_deg"],
                "max_pairwise_angle_deg": metrics["max_pairwise_angle_deg"],
                "exact_reuse_share": metrics["exact_reuse_share"],
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["mean_pairwise_angle_deg", "post_count", "company_display"], ascending=[True, False, True]
    )


def company_theme_terms(texts: list[str], top_n: int = 12) -> list[str]:
    cleaned = [text.strip() for text in texts if text and text.strip()]
    if len(cleaned) < 2:
        return []
    vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), max_features=3000)
    try:
        matrix = vectorizer.fit_transform(cleaned)
    except ValueError:
        return []
    weights = np.asarray(matrix.mean(axis=0)).ravel()
    terms = vectorizer.get_feature_names_out()
    ranking = np.argsort(weights)[::-1]
    selected: list[str] = []
    for index in ranking:
        term = str(terms[index])
        if len(term) < 3:
            continue
        selected.append(term)
        if len(selected) == top_n:
            break
    return selected


def company_angle_histogram(company_posts: pd.DataFrame):
    texts = company_posts["post_text_clean"].fillna("").astype(str).tolist()
    angles, _ = pairwise_semantic_geometry(texts) if len(texts) >= 2 else ([], [])
    if not angles:
        return None
    fig, ax = plt.subplots(figsize=(12.5, 5.4))
    ax.hist(angles, bins=min(16, max(6, len(angles))), color="#7f5539", edgecolor="#1f1d1a", alpha=0.88)
    ax.set_title("Pairwise Semantic Angle Distribution")
    ax.set_xlabel("Angle between posts (degrees)")
    ax.set_ylabel("Pair count")
    ax.grid(alpha=0.22, linestyle="--")
    return fig


def render() -> None:
    app_style()
    posts, roles, companies, threads = load_data()
    posts, roles = with_thread_month(posts, roles, threads)
    posts, roles = with_company_display(posts, roles, companies)

    st.markdown(
        """
        <div class="hero">
          <h1>YC Hiring Explorer</h1>
          <p>Filter the processed dataset by company, role family, and remote mode, then watch the monthly hiring signal reshape itself in real time.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    company_options = sorted(posts.loc[posts["is_hiring_post"] == True, "company_display"].dropna().astype(str).unique().tolist())
    role_family_options = sorted(roles["role_family"].dropna().astype(str).unique().tolist())
    remote_options = ["remote", "hybrid", "onsite", "unspecified"]
    month_options = sorted(posts.loc[posts["is_hiring_post"] == True, "thread_month"].dropna().astype(str).unique().tolist())

    range_left, range_right = st.columns(2)
    with range_left:
        start_month = st.selectbox("Start month", month_options, index=0)
    with range_right:
        end_month = st.selectbox("End month", month_options, index=len(month_options) - 1)

    if start_month > end_month:
        st.error("Start month must be before or equal to end month.")
        return

    left, mid, right = st.columns([1.3, 1.1, 1.0])
    with left:
        selected_companies = st.multiselect("Companies", company_options, placeholder="All companies")
    with mid:
        selected_role_families = st.multiselect("Role families", role_family_options, placeholder="All role families")
    with right:
        selected_remote_statuses = st.multiselect("Remote status", remote_options, placeholder="All remote modes")

    filtered_posts, filtered_roles = build_filtered_frames(
        posts,
        roles,
        companies,
        start_month,
        end_month,
        selected_companies,
        selected_role_families,
        selected_remote_statuses,
    )

    tabs = st.tabs(["Slice Explorer", "Company Variation"])

    with tabs[0]:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Hiring posts", f"{len(filtered_posts):,}")
        c2.metric("Extracted roles", f"{len(filtered_roles):,}")
        c3.metric("Active months", f"{filtered_posts['thread_month'].nunique() if not filtered_posts.empty else 0}")
        c4.metric("Companies", f"{filtered_posts['company_display'].nunique() if not filtered_posts.empty else 0}")

        insights = build_insights(filtered_posts, filtered_roles)
        st.subheader("Insights")
        for insight in insights:
            st.write(f"- {insight}")

        post_trend = filtered_posts.groupby("thread_month").size().reset_index(name="post_count").sort_values("thread_month")
        role_trend = filtered_roles.groupby(["thread_month", "role_family"]).size().reset_index(name="role_count").sort_values(
            ["thread_month", "role_family"]
        )
        remote_trend = month_counts(filtered_posts.assign(remote_status=filtered_posts["remote_status"].fillna("unspecified")), "remote_status", "post_count")
        ai_trend = concept_rows(filtered_posts, AI_CONCEPT_PATTERNS, "mentioning_post_count")
        ai_role_family_trend = ai_role_family_rows(filtered_roles)
        building_theme_summary = theme_summary(filtered_posts)
        building_theme_rows = theme_rows(filtered_posts)

        chart_left, chart_right = st.columns(2)
        with chart_left:
            if not post_trend.empty:
                fig, ax = plt.subplots(figsize=(12.6, 5.4))
                months = post_trend["thread_month"].tolist()
                positions = list(range(len(months)))
                ax.plot(positions, post_trend["post_count"], marker="o", linewidth=3, color="#183a37")
                ax.fill_between(positions, post_trend["post_count"], color="#d5b26f", alpha=0.25)
                ax.set_title("Hiring Posts Over Time")
                ax.set_xlabel("Thread month")
                ax.set_ylabel("Post count")
                apply_month_axis(ax, months)
                ax.grid(alpha=0.25, linestyle="--")
                st.pyplot(fig, use_container_width=True)
            else:
                st.info("No hiring posts to plot for the current filters.")
        with chart_right:
            if not remote_trend.empty:
                st.pyplot(stacked_pct_chart(remote_trend), use_container_width=True)
            else:
                st.info("No remote-status trend available for the current filters.")

        st.subheader("Role Family Time Series")
        if not role_trend.empty:
            st.pyplot(
                line_chart(role_trend, "thread_month", "role_count", "role_family", "Role Families Over Time", "Role count"),
                use_container_width=True,
            )
        else:
            st.info("No role-family trend available for the current filters.")

        st.subheader("AI Concept Trends")
        ai_left, ai_right = st.columns(2)
        with ai_left:
            ai_count_chart = concept_line_chart(ai_trend, "mentioning_post_count", "AI Concepts Over Time", "Posts mentioning concept")
            if ai_count_chart is not None:
                st.pyplot(ai_count_chart, use_container_width=True)
            else:
                st.info("No AI concept trend available for the current filters.")
        with ai_right:
            ai_share_chart = concept_line_chart(ai_trend, "share_pct", "AI Concept Share Over Time", "Share of selected posts (%)")
            if ai_share_chart is not None:
                st.pyplot(ai_share_chart, use_container_width=True)
            else:
                st.info("No AI concept share trend available for the current filters.")

        st.subheader("AI Concepts By Role Family")
        role_ai_left, role_ai_right = st.columns(2)
        with role_ai_left:
            chart = ai_role_family_heatmap(
                ai_role_family_trend,
                "role_count",
                "AI Concepts By Role Family",
                "Role count",
            )
            if chart is not None:
                st.pyplot(chart, use_container_width=True)
            else:
                st.info("No AI role-family concept view available for the current filters.")
        with role_ai_right:
            chart = ai_role_family_heatmap(
                ai_role_family_trend,
                "role_share_pct",
                "AI Concept Share By Role Family",
                "Share of role family (%)",
            )
            if chart is not None:
                st.pyplot(chart, use_container_width=True)
            else:
                st.info("No AI role-family share view available for the current filters.")

        st.subheader("What Companies Are Building")
        if not building_theme_summary.empty:
            fig, ax = plt.subplots(figsize=(13.2, 5.6))
            top = building_theme_summary.head(8).iloc[::-1]
            ax.barh(
                top["building_theme"].str.replace("_", " ", regex=False),
                top["post_count"],
                color="#264653",
                edgecolor="#1b1a17",
            )
            ax.set_title("Top Product Themes In Current Slice")
            ax.set_xlabel("Hiring posts matching theme")
            ax.set_ylabel("Theme")
            st.pyplot(fig, use_container_width=True)

            year_values = sorted({value.split("-")[0] for value in filtered_posts["thread_month"].dropna().astype(str).tolist()})
            if year_values:
                st.markdown("Year-Sliced Theme Heatmaps")
                columns = st.columns(2)
                for index, year in enumerate(year_values):
                    chart = theme_year_heatmap(building_theme_rows, year)
                    if chart is None:
                        continue
                    with columns[index % 2]:
                        st.pyplot(chart, use_container_width=True)
        else:
            st.info("No product-theme signals available for the current filters.")

        st.subheader("Sample Of Filtered Posts")
        preview = sample_filtered_posts(filtered_posts)[
            [
                "thread_month",
                "company_display",
                "remote_status",
                "employment_type",
                "funding",
                "compensation_text",
                "location_text",
            ]
        ].rename(columns={"company_display": "company_name"})
        st.dataframe(preview, use_container_width=True, height=320)

    with tabs[1]:
        window_posts = posts[(posts["is_hiring_post"] == True) & (posts["thread_month"] >= start_month) & (posts["thread_month"] <= end_month)].copy()
        st.subheader("Company Narrative Variation")
        st.caption("This view measures how semantically tight or spread out a company's hiring posts are inside the selected month window.")

        variation_rows = company_variation_rows(window_posts, top_n=80)
        if variation_rows.empty:
            st.info("No company-level variation view is available for the selected month window.")
        else:
            company_options_variation = variation_rows.sort_values(
                ["mean_pairwise_angle_deg", "post_count", "company_display"], ascending=[True, False, True]
            )["company_display"].tolist()
            selected_company = st.selectbox("Company", company_options_variation, index=0)
            selected_rows = variation_rows[variation_rows["company_display"] == selected_company].iloc[0]
            selected_posts = (
                window_posts[window_posts["company_display"] == selected_company]
                .sort_values(["thread_month", "post_id"])
                .copy()
            )
            terms = company_theme_terms(selected_posts["post_text_clean"].fillna("").astype(str).tolist())

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Posts", int(selected_rows["post_count"]))
            m2.metric("Mean Angle", f"{float(selected_rows['mean_pairwise_angle_deg']):.1f}°")
            m3.metric("Median Angle", f"{float(selected_rows['median_pairwise_angle_deg']):.1f}°")
            m4.metric("Exact Reuse", f"{float(selected_rows['exact_reuse_share']) * 100:.1f}%")
            st.caption("Angle interpretation for this view: 0° means nearly identical text, 90° means semantically unrelated in this non-negative vector space.")

            left, right = st.columns([1.25, 1.0])
            with left:
                hist = company_angle_histogram(selected_posts)
                if hist is not None:
                    st.pyplot(hist, use_container_width=True)
                else:
                    st.info("Need at least two posts from the company in the selected month window to build a pairwise-angle histogram.")
            with right:
                timeline = selected_posts.groupby("thread_month").size().reset_index(name="post_count").sort_values("thread_month")
                if not timeline.empty:
                    fig, ax = plt.subplots(figsize=(12.2, 5.1))
                    months = timeline["thread_month"].tolist()
                    positions = list(range(len(months)))
                    ax.plot(positions, timeline["post_count"], marker="o", linewidth=2.8, color="#264653")
                    ax.set_title("Company Posts Over Time")
                    ax.set_xlabel("Thread month")
                    ax.set_ylabel("Post count")
                    apply_month_axis(ax, months)
                    ax.grid(alpha=0.25, linestyle="--")
                    st.pyplot(fig, use_container_width=True)

            st.markdown("Central Themes")
            if terms:
                st.write(", ".join(f"`{term}`" for term in terms))
            else:
                st.write("Not enough text variation to extract stable theme terms.")

            st.markdown("Sample Company Posts")
            company_sample = sample_filtered_posts(selected_posts, max_rows=6)[
                [
                    "thread_month",
                    "remote_status",
                    "employment_type",
                    "compensation_text",
                    "funding",
                    "location_text",
                    "post_text_clean",
                ]
            ].rename(columns={"post_text_clean": "post_text"})
            st.dataframe(company_sample, use_container_width=True, height=280)

            st.markdown("Companies Ordered By Variation In Current Window")
            st.dataframe(
                variation_rows[
                    [
                        "company_display",
                        "post_count",
                        "mean_pairwise_angle_deg",
                        "median_pairwise_angle_deg",
                        "p90_pairwise_angle_deg",
                        "exact_reuse_share",
                    ]
                ].rename(
                    columns={
                        "company_display": "company_name",
                        "mean_pairwise_angle_deg": "mean_angle_deg",
                        "median_pairwise_angle_deg": "median_angle_deg",
                        "p90_pairwise_angle_deg": "p90_angle_deg",
                    }
                ).head(20),
                use_container_width=True,
                height=320,
            )

            with st.expander("Full Selected Company Posts"):
                company_preview = selected_posts[
                    [
                        "thread_month",
                        "remote_status",
                        "employment_type",
                        "compensation_text",
                        "funding",
                        "location_text",
                        "post_text_clean",
                    ]
                ].rename(columns={"post_text_clean": "post_text"})
                st.dataframe(company_preview, use_container_width=True, height=320)


if __name__ == "__main__":
    render()
