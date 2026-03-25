"""Deterministic mock LLM client for offline testing and demos."""

from __future__ import annotations

import json
import re

from agentic_kpi_analyst.llm.base import BaseLLMClient, LLMResponse
from agentic_kpi_analyst.logging_utils import get_logger

logger = get_logger(__name__)


class MockLLMClient(BaseLLMClient):
    """Deterministic mock LLM that returns pre-built responses based on prompt analysis.

    Designed to produce realistic enough outputs for the investigation workflow
    without requiring any API keys.
    """

    def complete(self, prompt: str, system: str = "", temperature: float = 0.0) -> LLMResponse:
        """Route to appropriate mock handler based on prompt content."""
        prompt_lower = prompt.lower()

        if "investigation plan" in prompt_lower or "plan" in system.lower():
            content = self._mock_plan(prompt)
        elif "generate sql" in prompt_lower or "sql" in system.lower():
            content = self._mock_sql(prompt)
        elif "verify" in prompt_lower or "verif" in system.lower():
            content = self._mock_verification(prompt)
        elif "report" in prompt_lower or "report" in system.lower():
            content = self._mock_report(prompt)
        elif "root cause" in prompt_lower or "conclusion" in prompt_lower:
            content = self._mock_conclusion(prompt)
        else:
            content = self._mock_generic(prompt)

        logger.debug("mock_llm_response", prompt_len=len(prompt), response_len=len(content))
        return LLMResponse(content=content, model="mock-deterministic")

    def complete_json(self, prompt: str, system: str = "", temperature: float = 0.0) -> dict:
        """Generate mock JSON response."""
        response = self.complete(prompt, system=system, temperature=temperature)
        try:
            return json.loads(response.content)
        except json.JSONDecodeError:
            # Fallback: wrap in generic structure
            return {"content": response.content}

    def _extract_kpi(self, prompt: str) -> str:
        """Extract KPI name from prompt."""
        kpi_patterns = [
            "gmv", "order_count", "aov", "conversion_rate",
            "refund_rate", "new_customer_ratio", "revenue", "cancellation_rate",
        ]
        prompt_lower = prompt.lower()
        for kpi in kpi_patterns:
            if kpi in prompt_lower:
                return kpi
        return "gmv"

    def _extract_dimensions(self, prompt: str) -> list[str]:
        """Extract likely dimensions from prompt."""
        dims = []
        dim_patterns = ["channel", "region", "category", "customer_type", "payment_type", "campaign"]
        prompt_lower = prompt.lower()
        for d in dim_patterns:
            if d in prompt_lower:
                dims.append(d)
        return dims or ["channel", "region", "category"]

    def _mock_plan(self, prompt: str) -> str:
        """Generate a mock investigation plan."""
        kpi = self._extract_kpi(prompt)
        dims = self._extract_dimensions(prompt)

        plan = {
            "hypothesis": f"The {kpi} movement is likely driven by changes in one or more dimensional slices.",
            "priority_dimensions": dims,
            "steps": [
                {
                    "step_number": 1,
                    "action": "query_sql",
                    "description": f"Compare {kpi} baseline vs anomaly window at aggregate level",
                    "dimensions": [],
                    "sql_hint": f"SELECT date, {kpi} FROM daily_kpi_summary WHERE date BETWEEN ... GROUP BY date",
                },
                {
                    "step_number": 2,
                    "action": "query_sql",
                    "description": f"Break down {kpi} by {dims[0]} for both windows",
                    "dimensions": [dims[0]],
                    "sql_hint": f"SELECT {dims[0]}, SUM(...) FROM fact_orders_enriched GROUP BY {dims[0]}",
                },
                {
                    "step_number": 3,
                    "action": "analyze_dimension",
                    "description": f"Compute contribution analysis across {', '.join(dims)}",
                    "dimensions": dims,
                    "sql_hint": "",
                },
                {
                    "step_number": 4,
                    "action": "compare_baseline",
                    "description": "Compare top driver slices baseline vs anomaly",
                    "dimensions": dims[:2],
                    "sql_hint": "",
                },
            ],
        }
        return json.dumps(plan, indent=2)

    def _mock_sql(self, prompt: str) -> str:
        """Generate mock SQL queries."""
        kpi = self._extract_kpi(prompt)
        dims = self._extract_dimensions(prompt)

        # Detect date range from prompt
        date_match = re.findall(r"\d{4}-\d{2}-\d{2}", prompt)
        start = date_match[0] if len(date_match) > 0 else "2025-06-01"
        end = date_match[1] if len(date_match) > 1 else "2025-06-14"
        bl_start = date_match[2] if len(date_match) > 2 else "2025-05-18"
        bl_end = date_match[3] if len(date_match) > 3 else "2025-05-31"

        kpi_col_map = {
            "gmv": "SUM(order_total)",
            "order_count": "COUNT(*)",
            "aov": "AVG(order_total)",
            "conversion_rate": "AVG(conversion_rate)",
            "refund_rate": "SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END)::FLOAT / COUNT(*)",
            "revenue": "SUM(order_total)",
            "new_customer_ratio": "SUM(CASE WHEN customer_type='new' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)",
            "cancellation_rate": "SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)::FLOAT / COUNT(*)",
        }
        kpi_expr = kpi_col_map.get(kpi, "SUM(order_total)")

        queries = {
            "queries": [
                {
                    "query_id": "q_aggregate",
                    "purpose": f"Aggregate {kpi} for anomaly vs baseline window",
                    "target_view": "daily_kpi_summary",
                    "sql": (
                        f"SELECT CASE WHEN order_date BETWEEN '{start}' AND '{end}' THEN 'anomaly' "
                        f"ELSE 'baseline' END AS time_window, "
                        f"COUNT(*) as order_count, SUM(order_total) as gmv, AVG(order_total) as aov "
                        f"FROM fact_orders_enriched "
                        f"WHERE order_date BETWEEN '{bl_start}' AND '{end}' "
                        f"GROUP BY time_window"
                    ),
                },
                {
                    "query_id": f"q_by_{dims[0]}",
                    "purpose": f"Break down {kpi} by {dims[0]}",
                    "target_view": "fact_orders_enriched",
                    "sql": (
                        f"SELECT {dims[0]}, "
                        f"CASE WHEN order_date BETWEEN '{start}' AND '{end}' THEN 'anomaly' "
                        f"ELSE 'baseline' END AS time_window, "
                        f"COUNT(*) as order_count, SUM(order_total) as gmv, AVG(order_total) as aov "
                        f"FROM fact_orders_enriched "
                        f"WHERE order_date BETWEEN '{bl_start}' AND '{end}' "
                        f"GROUP BY {dims[0]}, time_window "
                        f"ORDER BY gmv DESC NULLS LAST"
                    ),
                },
            ]
        }

        if len(dims) > 1:
            queries["queries"].append({
                "query_id": f"q_by_{dims[1]}",
                "purpose": f"Break down {kpi} by {dims[1]}",
                "target_view": "fact_orders_enriched",
                "sql": (
                    f"SELECT {dims[1]}, "
                    f"CASE WHEN order_date BETWEEN '{start}' AND '{end}' THEN 'anomaly' "
                    f"ELSE 'baseline' END AS time_window, "
                    f"COUNT(*) as order_count, SUM(order_total) as gmv, AVG(order_total) as aov "
                    f"FROM fact_orders_enriched "
                    f"WHERE order_date BETWEEN '{bl_start}' AND '{end}' "
                    f"GROUP BY {dims[1]}, time_window "
                    f"ORDER BY gmv DESC NULLS LAST"
                ),
            })

        return json.dumps(queries, indent=2)

    def _mock_verification(self, prompt: str) -> str:
        """Generate mock verification result."""
        result = {
            "overall_supported": True,
            "checks": [
                {"check_name": "evidence_referenced", "passed": True, "detail": "All claims reference SQL results or analysis outputs."},
                {"check_name": "dimensions_in_results", "passed": True, "detail": "Cited dimensions appear in query results."},
                {"check_name": "contribution_support", "passed": True, "detail": "Top driver claims are backed by contribution analysis."},
                {"check_name": "caveats_present", "passed": True, "detail": "Report includes appropriate caveats."},
                {"check_name": "confidence_justified", "passed": True, "detail": "Confidence level matches evidence strength."},
            ],
            "unsupported_claims": [],
            "suggested_confidence": "medium",
            "needs_human_review": False,
            "review_reasons": [],
        }
        return json.dumps(result, indent=2)

    def _mock_conclusion(self, prompt: str) -> str:
        """Generate a mock root-cause conclusion."""
        kpi = self._extract_kpi(prompt)
        dims = self._extract_dimensions(prompt)
        return json.dumps({
            "primary_finding": f"The {kpi} movement is primarily driven by changes in the {dims[0]} dimension.",
            "confidence": "medium",
            "evidence_summary": f"Contribution analysis shows {dims[0]} explains the largest share of the observed {kpi} change.",
            "secondary_factors": [f"Minor shifts in {d}" for d in dims[1:]],
            "caveats": [
                "Analysis is based on observed correlations, not causal inference.",
                "Small sample sizes in some slices may affect reliability.",
            ],
        }, indent=2)

    def _mock_report(self, prompt: str) -> str:
        """Generate mock report content."""
        kpi = self._extract_kpi(prompt)
        return (
            f"# KPI Investigation Report: {kpi.upper()}\n\n"
            f"## Executive Summary\n"
            f"Investigation of {kpi} anomaly identified likely drivers through "
            f"dimensional contribution analysis.\n\n"
            f"## Confidence: MEDIUM\n"
            f"Evidence suggests the movement is real and attributable to identified drivers, "
            f"but causal confirmation would require further analysis.\n"
        )

    def _mock_generic(self, prompt: str) -> str:
        """Fallback mock response."""
        return json.dumps({"response": "Mock LLM response for unclassified prompt."})
