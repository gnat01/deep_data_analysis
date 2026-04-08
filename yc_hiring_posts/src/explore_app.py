"""Interactive Streamlit explorer for YC hiring-post analytics."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from storage import processed_data_dir


def load_table(name: str) -> pd.DataFrame:
    path = processed_data_dir() / "v1_core_tables" / name
    return pd.read_json(path, lines=True)


@st.cache_data(show_spinner=False)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    posts = load_table("posts.jsonl")
    roles = load_table("roles.jsonl")
    companies = load_table("companies.jsonl")
    return posts, roles, companies


def with_company_display(posts: pd.DataFrame, roles: pd.DataFrame, companies: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    company_name_by_id = dict(zip(companies["company_id"], companies["company_name_observed_preferred"], strict=False))
    posts = posts.copy()
    roles = roles.copy()
    posts["company_display"] = posts["company_id"].map(company_name_by_id).fillna(posts["company_name_observed"]).fillna("[unresolved]")
    roles["company_display"] = roles["company_id"].map(company_name_by_id).fillna("[unresolved]")
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
    selected_companies: list[str],
    selected_role_families: list[str],
    selected_remote_statuses: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    posts, roles = with_company_display(posts, roles, companies)

    filtered_posts = posts[posts["is_hiring_post"] == True].copy()
    if selected_companies:
        filtered_posts = filtered_posts[filtered_posts["company_display"].isin(selected_companies)]
    if selected_remote_statuses:
        filtered_posts = filtered_posts[filtered_posts["remote_status"].fillna("unspecified").isin(selected_remote_statuses)]

    filtered_roles = roles.copy()
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


def line_chart(frame: pd.DataFrame, x_col: str, y_col: str, series_col: str, title: str, ylabel: str):
    fig, ax = plt.subplots(figsize=(10, 4.6))
    pivot = frame.pivot_table(index=x_col, columns=series_col, values=y_col, aggfunc="sum", fill_value=0)
    for column in pivot.columns:
        ax.plot(pivot.index, pivot[column], marker="o", linewidth=2.6, label=str(column).title())
    ax.set_title(title)
    ax.set_xlabel("Thread month")
    ax.set_ylabel(ylabel)
    ax.legend(frameon=True, ncols=2)
    ax.grid(alpha=0.25, linestyle="--")
    return fig


def stacked_pct_chart(frame: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 4.8))
    pivot = frame.pivot_table(index="thread_month", columns="remote_status", values="post_count", aggfunc="sum", fill_value=0)
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
        ax.bar(share.index, values, bottom=bottom, label=str(column).title(), color=colors.get(column, "#457b9d"))
        bottom = values if bottom is None else [l + r for l, r in zip(bottom, values)]
    ax.set_title("Remote Mix Over Time")
    ax.set_xlabel("Thread month")
    ax.set_ylabel("Share of selected posts (%)")
    ax.set_ylim(0, 100)
    ax.legend(frameon=True, ncols=4, loc="upper center", bbox_to_anchor=(0.5, 1.16))
    return fig


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


def render() -> None:
    app_style()
    posts, roles, companies = load_data()
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
        selected_companies,
        selected_role_families,
        selected_remote_statuses,
    )

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

    chart_left, chart_right = st.columns(2)
    with chart_left:
        if not post_trend.empty:
            fig, ax = plt.subplots(figsize=(9, 4.5))
            ax.plot(post_trend["thread_month"], post_trend["post_count"], marker="o", linewidth=3, color="#183a37")
            ax.fill_between(post_trend["thread_month"], post_trend["post_count"], color="#d5b26f", alpha=0.25)
            ax.set_title("Hiring Posts Over Time")
            ax.set_xlabel("Thread month")
            ax.set_ylabel("Post count")
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

    st.subheader("Filtered Posts")
    preview = filtered_posts[
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


if __name__ == "__main__":
    render()
