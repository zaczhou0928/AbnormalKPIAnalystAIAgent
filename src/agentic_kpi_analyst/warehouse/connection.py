"""DuckDB warehouse connection and setup."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from agentic_kpi_analyst.config import PROJECT_ROOT, Settings
from agentic_kpi_analyst.logging_utils import get_logger

logger = get_logger(__name__)

_SCHEMA_PATH = PROJECT_ROOT / "warehouse" / "schema.sql"
_VIEWS_PATH = PROJECT_ROOT / "warehouse" / "views.sql"


class WarehouseConnection:
    """Manages DuckDB connection and warehouse initialization."""

    def __init__(self, settings: Settings | None = None) -> None:
        from agentic_kpi_analyst.config import get_settings
        self.settings = settings or get_settings()
        self._conn: duckdb.DuckDBPyConnection | None = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Lazy connection property."""
        if self._conn is None:
            db_path = self.settings.warehouse_abs_path
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = duckdb.connect(str(db_path))
            logger.info("duckdb_connected", path=str(db_path))
        return self._conn

    def close(self) -> None:
        """Close the connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def setup_schema(self) -> None:
        """Create tables from schema.sql."""
        schema_sql = _SCHEMA_PATH.read_text()
        for statement in schema_sql.split(";"):
            stmt = statement.strip()
            if stmt:
                self.conn.execute(stmt)
        logger.info("schema_created")

    def create_views(self) -> None:
        """Create analytical views from views.sql."""
        views_sql = _VIEWS_PATH.read_text()
        for statement in views_sql.split(";"):
            stmt = statement.strip()
            if stmt:
                self.conn.execute(stmt)
        logger.info("views_created")

    def load_parquet_data(self, data_dir: Path | None = None) -> None:
        """Load parquet files into tables."""
        data_dir = data_dir or self.settings.data_abs_dir
        table_names = [
            "orders", "order_items", "products", "customers",
            "payments", "channels", "regions", "campaigns", "refunds",
        ]

        for table_name in table_names:
            parquet_path = data_dir / f"{table_name}.parquet"
            if parquet_path.exists():
                # Clear existing data and insert from parquet
                self.conn.execute(f"DELETE FROM {table_name}")
                self.conn.execute(
                    f"INSERT INTO {table_name} SELECT * FROM read_parquet('{parquet_path}')"
                )
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                logger.info("loaded_table", table=table_name, rows=count)
            else:
                logger.warning("parquet_not_found", table=table_name, path=str(parquet_path))

    def initialize(self, data_dir: Path | None = None) -> None:
        """Full warehouse initialization: schema + data + views."""
        self.setup_schema()
        self.load_parquet_data(data_dir)
        self.create_views()
        logger.info("warehouse_initialized")

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        return self.conn.execute(sql).fetchdf()

    def get_table_names(self) -> list[str]:
        """List all tables and views in the database."""
        result = self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchdf()
        return result["table_name"].tolist()

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Get column info for a table or view."""
        return self.conn.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name = '{table_name}' AND table_schema = 'main'"
        ).fetchdf()


def get_warehouse(settings: Settings | None = None) -> WarehouseConnection:
    """Factory function for warehouse connection."""
    return WarehouseConnection(settings)
