# Step 20

Step 20 adds a dedicated natural-language Q&A layer on top of the PostgreSQL knowledge base.

## Goal

Allow a user to ask a simple natural-language question and get back:

- the interpreted question match
- the routed helper
- a grounded answer
- supporting evidence rows where available

This layer should avoid hallucination by being conservative.

## Design Principles

- do **not** answer from free-form intuition
- first map the question to the closest catalog pattern
- extract concrete parameters such as:
  - company
  - month range
  - year
  - role family
  - AI concept
- only then route into the helper/router layer
- if confidence is weak or required parameters are missing, ask for clarification instead of guessing

## Current Step 20 v0

Implemented pieces:

- [`src/qa_layer.py`](../src/qa_layer.py)
  - conservative natural-language parsing
  - catalog-similarity matching
  - parameter extraction
  - grounded answer synthesis over routed helper outputs

- [`src/kb_router.py`](../src/kb_router.py)
  - structured routing over the helper library

- [`src/qa_app.py`](../src/qa_app.py)
  - dedicated Streamlit Q&A app

- [`qa_app.py`](../qa_app.py)
  - project-root Streamlit entrypoint

- CLI command:
  - `ask-postgres-kb`

## Current Behavior

The Q&A layer returns one of two states:

- `answered`
- `clarification_needed`

The `answered` state includes:

- parsed interpretation
- routed helper name
- helper output
- concise evidence-grounded summary

The `clarification_needed` state includes:

- reason for refusal
- parsed interpretation

## Example CLI Use

```bash
python src/cli.py ask-postgres-kb \
  --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp" \
  --question "Which companies posted hiring ads in every month of 2025?"
```

```bash
python src/cli.py ask-postgres-kb \
  --database-url "postgresql://gn@/yc_hiring_posts?host=/tmp" \
  --question "In 50 words or less, how did the requirements for an AI engineer change from 2024 - 2026?"
```

## Example App Use

```bash
streamlit run qa_app.py
```

## What This Is Not Yet

This is not yet:

- a general-purpose LLM chatbot
- a polished NL reasoning layer
- a substitute for direct evidence inspection

It is a **grounded routing-and-answering surface** over the KB.

## Likely Next Improvements

- tighten question parsing for more varied phrasing
- improve answer summaries
- broaden clarification logic
- make more fallback-covered questions first-class
- add richer source-link presentation in the Q&A app
