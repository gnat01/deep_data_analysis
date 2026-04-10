"""Dedicated Streamlit Q&A app for the PostgreSQL-backed knowledge base."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from qa_layer import answer_nl_question_postgres


DEFAULT_DB_URL = "postgresql://gn@/yc_hiring_posts?host=/tmp"


def app_style() -> None:
    st.set_page_config(page_title="YC Hiring Q&A", layout="wide")
    st.markdown(
        """
        <style>
        .stApp { background: linear-gradient(180deg, #f6f1e8 0%, #efe5d5 100%); }
        .block-container { padding-top: 1.25rem; padding-bottom: 2rem; }
        .hero {
            background: linear-gradient(135deg, #0f3d3e 0%, #1f5f66 58%, #d0a45c 100%);
            border-radius: 20px;
            padding: 1.35rem 1.6rem;
            color: #fff9f0;
            margin-bottom: 1rem;
        }
        .hero h1 { margin: 0 0 0.25rem 0; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _rows_to_frame(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def render() -> None:
    app_style()
    st.markdown(
        """
        <div class="hero">
          <h1>YC Hiring Q&amp;A</h1>
          <p>Ask a natural-language question. The app routes conservatively into the PostgreSQL KB, shows the helper used, and returns evidence-linked output rather than free-form guesses.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.subheader("Connection")
        database_url = st.text_input("PostgreSQL URL", value=DEFAULT_DB_URL)
        limit = st.slider("Result limit", min_value=3, max_value=25, value=10)
        limit_evidence = st.slider("Evidence rows", min_value=2, max_value=10, value=5)

    example_questions = [
        "Which companies posted hiring ads in every month of 2025?",
        "In 50 words or less, how did the requirements for an AI engineer change from 2024 - 2026?",
        "Which product themes were most common in 2023 versus 2025 versus 2026 YTD?",
        "What is the distribution of global remote roles to overall remote roles across the dataset, year by year?",
    ]
    example = st.selectbox("Example questions", [""] + example_questions)
    question = st.text_area(
        "Question",
        value=example,
        height=120,
        placeholder="Ask a grounded question about the YC / HN hiring corpus...",
    )

    if st.button("Ask", type="primary", width="stretch") and question.strip():
        with st.spinner("Routing question into the KB..."):
            result = answer_nl_question_postgres(
                question.strip(),
                database_url=database_url,
                limit=limit,
                limit_evidence=limit_evidence,
            )

        if result["status"] == "clarification_needed":
            st.error(result["reason"] or "Need clarification before answering safely.")
            parsed = result["parsed"]
            st.json(parsed)
            return

        st.subheader("Answer")
        st.write(result["summary"])

        routed = result["routed"]
        answer = routed["answer"]
        parsed = result["parsed"]

        col1, col2, col3 = st.columns(3)
        col1.metric("Routed Helper", routed["routed_helper"])
        col2.metric("Catalog Match", f"Q{parsed['question_id']}")
        col3.metric("Match Confidence", f"{parsed['confidence']:.2f}")

        with st.expander("Interpretation", expanded=False):
            st.json(parsed)

        rows = answer.get("rows")
        if isinstance(rows, list) and rows:
            st.subheader("Rows")
            st.dataframe(_rows_to_frame(rows), width="stretch")

        evidence_rows = answer.get("evidence_rows")
        if isinstance(evidence_rows, list) and evidence_rows:
            st.subheader("Evidence")
            for index, row in enumerate(evidence_rows, start=1):
                st.markdown(f"**Evidence {index}**")
                st.json(row)


if __name__ == "__main__":
    render()
