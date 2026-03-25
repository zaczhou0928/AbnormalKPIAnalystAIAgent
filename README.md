# Agentic KPI Root-Cause Analyst

An analytics workflow system that investigates abnormal KPI movements in e-commerce data. Built with LangGraph, DuckDB, and Python — designed for reproducibility, safety, and realistic business logic.

This is **not** a generic chatbot or toy RAG demo. It is a structured investigation pipeline with tool use, SQL safety enforcement, evidence verification, and evaluation against labeled anomaly cases.

## Architecture

```
Investigation Request
        │
   ┌────▼────┐
   │  Intake  │ ← Normalize request, set baseline window
   └────┬────┘
   ┌────▼──────────┐
   │  Context       │ ← BM25 retrieval of metric definitions
   │  Retrieval     │   and business rules
   └────┬──────────┘
   ┌────▼────┐
   │ Planner │ ← LLM creates investigation plan
   └────┬────┘
   ┌────▼─────────┐
   │ SQL Generator │ ← LLM generates safe SELECT queries
   └────┬─────────┘
   ┌────▼─────────────┐
   │ SQL Validator &   │ ← Validates safety, executes against DuckDB
   │ Executor          │
   └────┬─────────────┘
   ┌────▼──────────┐
   │ Python Analysis│ ← Contribution analysis, slice comparison,
   │                │   charting, evidence collection
   └────┬──────────┘
   ┌────▼────────┐
   │  Verifier   │ ← Checks evidence supports conclusions
   └────┬────────┘     (can loop back for another pass)
   ┌────▼────────────┐
   │  Human Review   │ ← Gates on low confidence / weak evidence
   └────┬────────────┘
   ┌────▼──────────────┐
   │ Report Generator  │ ← Markdown + HTML + JSON output
   └───────────────────┘
```

**Tech Stack:** Python 3.11, LangGraph, DuckDB, pandas, pydantic, Streamlit, plotly, Jinja2

## Quick Start

```bash
# Install
make install

# Generate synthetic data and initialize warehouse
make seed

# Run an investigation (mock LLM, no API key needed)
make run-case CASE_ID=ANO-005

# Launch the Streamlit app
make app

# Run evaluation pipeline
make eval

# Run tests
pytest
```

## LLM Modes

The system supports two modes:

### Mock Mode (default)
- No API key required
- Deterministic, reproducible outputs
- Generates realistic investigation plans, SQL queries, and conclusions
- Suitable for demos, testing, and development

### Real LLM Mode
To use OpenAI:
```bash
cp .env.example .env
# Edit .env:
#   OPENAI_API_KEY=sk-...
#   LLM_MODE=openai
```

The system uses a provider abstraction (`BaseLLMClient` → `OpenAIClient` / `MockLLMClient`). Adding new providers requires implementing `complete()` and `complete_json()`.

## SQL Safety

The SQL execution layer enforces strict safety rules:

- **SELECT only** — blocks INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, COPY, ATTACH
- **Approved tables only** — queries can only reference whitelisted tables and views
- **No multi-statement injection** — semicolons in query body are rejected
- **CTE-aware** — WITH/AS CTEs are correctly handled
- **Auto LIMIT** — adds row limit to queries that don't have one
- **String-literal-safe** — keywords inside string literals don't trigger false positives
- **Meaningful errors** — validation returns specific error messages

## Evidence Chain

Every conclusion includes:
- **Finding** — the identified root cause
- **Supporting evidence** — SQL results, contribution analysis, metric definitions
- **Confidence level** — high / medium / low / inconclusive
- **Caveats** — sample size warnings, methodological limitations
- **Source references** — query IDs, analysis types, document sources

If evidence is insufficient, the system explicitly states "inconclusive" instead of hallucinating.

## Human Review

The system triggers human review when:
- Confidence is below threshold (configurable)
- SQL validation fails repeatedly
- Investigation exceeds time budget
- Verifier marks conclusions as weakly supported

In mock/demo mode, human review is auto-approved. In the Streamlit app, approve/reject buttons are presented.

## Data

The system generates 12 months of synthetic e-commerce data (~90K orders) covering:
- Orders, items, products, customers, payments, refunds
- 6 channels, 4 regions, 6 categories, 5 payment types, 7 campaigns
- Deterministic random seed for reproducibility

### Anomaly Cases

16 labeled anomaly cases with injected realistic patterns:
- Channel conversion drops (landing page failure)
- Category refund spikes (defective batch)
- Regional AOV drops (pricing error)
- Payment gateway outages
- Campaign-driven GMV spikes
- Metric definition edge cases
- New customer acquisition surges
- Cancellation rate spikes

Each case includes expected root cause, recommended dimensions, and whether human review should trigger.

## Evaluation

The evaluation pipeline runs all labeled cases and computes:
- **Primary root-cause hit rate** — does the system identify the correct cause?
- **SQL execution success rate** — do generated queries run successfully?
- **Evidence sufficiency rate** — is there enough evidence to support conclusions?
- **Unsupported claim rate** — are there claims without backing evidence?
- **Human review trigger rate** — does the system correctly flag uncertain cases?
- **Average runtime per case**

Results are saved as JSON and markdown summaries in `outputs/evals/`.

## Project Structure

```
├── README.md
├── pyproject.toml
├── .env.example
├── Makefile
├── data/generated/           ← Synthetic data (parquet + DuckDB)
├── docs/knowledge/           ← Metric definitions, business rules, data dictionary
├── warehouse/
│   ├── schema.sql            ← Table definitions
│   ├── views.sql             ← Curated analytical views
│   └── seed_data.py          ← Synthetic data generator
├── src/agentic_kpi_analyst/
│   ├── config.py             ← Application settings
│   ├── models.py             ← Pydantic data models
│   ├── state.py              ← LangGraph workflow state
│   ├── logging_utils.py      ← Structured logging
│   ├── cli.py                ← CLI entry point
│   ├── llm/                  ← LLM provider abstraction
│   ├── retrieval/            ← BM25 knowledge retrieval
│   ├── warehouse/            ← DuckDB connection, SQL validator, executor
│   ├── analysis/             ← Contribution analysis, charting
│   ├── reports/              ← Jinja2 report renderer
│   ├── graph/                ← LangGraph nodes and workflow
│   ├── evals/                ← Evaluation pipeline
│   └── app/                  ← Streamlit UI
├── tests/                    ← 44 tests covering all modules
└── outputs/                  ← Reports, charts, eval results
```

## Curated Views

The LLM queries these views rather than raw tables:

| View | Description |
|------|-------------|
| `fact_orders_enriched` | Denormalized order data with refund and payment info |
| `daily_kpi_summary` | Daily aggregate KPIs across all dimensions |
| `channel_performance` | Daily KPIs by acquisition channel |
| `category_performance` | Daily KPIs by product category |
| `region_performance` | Daily KPIs by region |
| `customer_segment_performance` | Daily KPIs by customer type and channel |
| `refund_summary` | Refunds by date, category, region, channel, reason |

## Limitations and Future Work

- **Analysis is correlational, not causal** — the system uses contribution analysis and dimensional breakdowns, not causal inference methods
- **Mock LLM produces formulaic outputs** — real LLM mode provides more nuanced investigation
- **No session/clickstream data** — conversion rate is approximated from order patterns
- **Single-table joins only** — the mock SQL generator queries views, not complex multi-table joins
- **No real-time data** — designed for batch investigation of historical anomalies

Potential improvements:
- Add SHAP-based feature importance
- Support time-series decomposition (trend, seasonality, residual)
- Add anomaly detection (auto-detect cases instead of only investigating labeled ones)
- Multi-turn investigation with follow-up queries
- Integration with real data warehouses via configurable connectors
