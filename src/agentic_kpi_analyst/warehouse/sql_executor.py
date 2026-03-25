"""Safe SQL executor that validates before running queries."""

from __future__ import annotations

import time

import pandas as pd

from agentic_kpi_analyst.config import Settings
from agentic_kpi_analyst.logging_utils import get_logger
from agentic_kpi_analyst.models import SQLExecutionResult, SQLQuery
from agentic_kpi_analyst.warehouse.connection import WarehouseConnection
from agentic_kpi_analyst.warehouse.sql_validator import validate_sql

logger = get_logger(__name__)


class SQLExecutor:
    """Validates and executes SQL queries safely against DuckDB."""

    def __init__(self, warehouse: WarehouseConnection, settings: Settings | None = None) -> None:
        self.warehouse = warehouse
        if settings is None:
            from agentic_kpi_analyst.config import get_settings
            settings = get_settings()
        self.settings = settings

    def execute(self, query: SQLQuery) -> SQLExecutionResult:
        """Validate and execute a SQL query, returning structured results."""
        start = time.monotonic()

        # Validate
        validation = validate_sql(query.sql, self.settings)
        if not validation.is_valid:
            logger.warning(
                "sql_validation_failed",
                query_id=query.query_id,
                errors=validation.errors,
            )
            return SQLExecutionResult(
                query_id=query.query_id,
                success=False,
                error=f"Validation failed: {'; '.join(validation.errors)}",
                execution_time_ms=(time.monotonic() - start) * 1000,
            )

        # Execute
        try:
            df = self.warehouse.execute_query(validation.sanitized_sql)
            elapsed_ms = (time.monotonic() - start) * 1000

            # Convert to list of dicts for serialization
            data = df.head(self.settings.max_sql_rows).to_dict(orient="records")
            # Convert any non-serializable types
            for row in data:
                for k, v in row.items():
                    if isinstance(v, pd.Timestamp):
                        row[k] = v.isoformat()
                    elif hasattr(v, "item"):  # numpy scalar
                        row[k] = v.item()

            logger.info(
                "sql_executed",
                query_id=query.query_id,
                rows=len(data),
                elapsed_ms=round(elapsed_ms, 1),
            )

            return SQLExecutionResult(
                query_id=query.query_id,
                success=True,
                row_count=len(data),
                columns=list(df.columns),
                data=data,
                execution_time_ms=elapsed_ms,
            )

        except Exception as e:
            elapsed_ms = (time.monotonic() - start) * 1000
            logger.error("sql_execution_error", query_id=query.query_id, error=str(e))
            return SQLExecutionResult(
                query_id=query.query_id,
                success=False,
                error=str(e),
                execution_time_ms=elapsed_ms,
            )

    def execute_batch(self, queries: list[SQLQuery]) -> list[SQLExecutionResult]:
        """Execute multiple queries and return all results."""
        return [self.execute(q) for q in queries]
