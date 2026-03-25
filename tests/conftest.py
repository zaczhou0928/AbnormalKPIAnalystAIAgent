"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from agentic_kpi_analyst.config import Settings, PROJECT_ROOT
from agentic_kpi_analyst.logging_utils import setup_logging

setup_logging("WARNING")


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """Settings configured for testing with in-memory DuckDB."""
    return Settings(
        llm_mode="mock",
        warehouse_path=":memory:",
        data_dir="data/generated",
        output_dir="outputs",
        knowledge_dir="docs/knowledge",
        human_review_auto_approve=True,
    )


@pytest.fixture(scope="session")
def seeded_warehouse(test_settings: Settings):
    """A warehouse seeded with test data. Session-scoped for performance."""
    from agentic_kpi_analyst.warehouse.connection import WarehouseConnection

    # Check if parquet data exists
    data_dir = test_settings.data_abs_dir
    orders_parquet = data_dir / "orders.parquet"

    if not orders_parquet.exists():
        # Generate data
        import sys
        sys.path.insert(0, str(PROJECT_ROOT / "warehouse"))
        from warehouse.seed_data import generate_all
        generate_all(data_dir)

    wh = WarehouseConnection(test_settings)
    wh.initialize()
    yield wh
    wh.close()
