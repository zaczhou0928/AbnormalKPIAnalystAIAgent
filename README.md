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

## Sample Investigation Output

Below is a condensed view of actual output produced by the pipeline for **ANO-005** — a simulated credit-card payment gateway outage that caused a 45% order-count drop. The entire report was generated in mock mode with no API keys.

**Executive summary (verbatim from generated report):**

> Evidence suggests the order_count anomaly is primarily driven by changes in 'credit_card' (payment_type). This slice accounts for 112.3% of the total movement. Confidence: high.

**Key evidence — contribution analysis by `payment_type`:**

| Slice | Baseline (daily) | Anomaly (daily) | Change | % Change | Contribution |
|-------|-------------------|-----------------|--------|----------|-------------|
| credit_card | 95.71 | 49.75 | -45.96 | -48.0% | 112.3% |
| apple_pay | 27.00 | 30.50 | +3.50 | +13.0% | -8.6% |
| debit_card | 48.00 | 49.62 | +1.62 | +3.4% | -4.0% |
| bank_transfer | 18.79 | 18.62 | -0.16 | -0.9% | 0.4% |
| paypal | 48.79 | 48.88 | +0.09 | +0.2% | -0.2% |

The `credit_card` slice alone explains >100% of the total decline (other payment types partially offset it), clearly isolating the payment gateway as the root cause.

**Verification checks (all passing):**

| Check | Result | Detail |
|-------|--------|--------|
| sql_evidence_exists | Pass | 3 successful SQL queries out of 3 |
| analysis_results_exist | Pass | 6 analysis results produced |
| supporting_evidence | Pass | 10 supporting evidence items |
| concentrated_driver_found | Pass | Concentrated driver found |
| sample_size_adequate | Pass | All slices have adequate sample sizes |

Each investigation produces a full report with sections for incident context, metric definitions retrieved via BM25, the investigation plan, SQL evidence, contribution tables across all priority dimensions, confidence assessment, and suggested follow-ups. Reports are saved as markdown, HTML, and machine-readable JSON.

## Representative Demo Outputs

Run `make demo` to deterministically generate complete investigation artifacts for three representative anomaly cases. No API keys are needed.

```bash
make demo
```

This produces the following in `outputs/demo/`:

```
outputs/demo/
  demo_summary.json           ← compact benchmark with case results
  ANO-001/                    ← conversion_rate drop (channel-driven)
    report_*.md / .html / .json
    charts/                   ← 7 interactive Plotly charts
  ANO-002/                    ← refund_rate spike (category-driven)
    report_*.md / .html / .json
    charts/
  ANO-005/                    ← order_count drop (payment-failure-driven)
    report_*.md / .html / .json
    charts/
```

**Demo summary (from actual `demo_summary.json`):**

| Case | Predicted Primary Cause | Confidence | Review Status |
|------|------------------------|------------|---------------|
| ANO-001 | `campaign='none'` dimension, -100.0% change, 778.4% contribution | high | approved |
| ANO-002 | `category='electronics'`, +183.2% change, 140.0% contribution | high | not_required |
| ANO-005 | `payment_type='credit_card'`, -48.0% change, 112.3% contribution | high | not_required |

Each case directory contains the full markdown/HTML/JSON report and 7 interactive charts (contribution bar charts per dimension + a time-series overview with highlighted baseline and anomaly windows).

To regenerate from scratch:

```bash
make seed   # generate synthetic data (deterministic)
make demo   # run 3 cases, save all artifacts
```

The outputs are fully deterministic — the same seed and mock LLM produce identical artifacts on every run.

## Quick Start

```bash
# Install
make install

# Generate synthetic data and initialize warehouse
make seed

# Run an investigation (mock LLM, no API key needed)
make run-case CASE_ID=ANO-005

# Generate demo artifacts for 3 representative cases
make demo

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
