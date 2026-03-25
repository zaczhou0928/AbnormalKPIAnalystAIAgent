"""Load labeled anomaly cases for evaluation."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import AnomalyCase, KPIName

logger = get_logger(__name__)


def load_cases(cases_path: str | Path) -> list[AnomalyCase]:
    """Load anomaly cases from JSON file.

    Args:
        cases_path: Path to anomaly_cases.json.

    Returns:
        List of AnomalyCase objects.
    """
    cases_path = Path(cases_path)
    if not cases_path.exists():
        raise FileNotFoundError(f"Anomaly cases not found: {cases_path}. Run 'make seed' first.")

    with open(cases_path) as f:
        raw = json.load(f)

    cases: list[AnomalyCase] = []
    for c in raw:
        cases.append(AnomalyCase(
            case_id=c["case_id"],
            kpi_name=KPIName(c["kpi_name"]),
            affected_start_date=date.fromisoformat(c["affected_start_date"]),
            affected_end_date=date.fromisoformat(c["affected_end_date"]),
            description=c["description"],
            expected_primary_cause=c["expected_primary_cause"],
            expected_secondary_factors=c.get("expected_secondary_factors", []),
            recommended_dimensions=c.get("recommended_dimensions", []),
            should_trigger_human_review=c.get("should_trigger_human_review", False),
        ))

    logger.info("cases_loaded", count=len(cases))
    return cases
