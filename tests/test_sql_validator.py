"""Tests for the SQL safety validator."""

import pytest

from agentic_kpi_analyst.config import Settings
from agentic_kpi_analyst.warehouse.sql_validator import validate_sql


@pytest.fixture
def settings() -> Settings:
    return Settings(llm_mode="mock")


class TestSQLValidator:
    """Tests for SQL validation safety rules."""

    def test_valid_select(self, settings: Settings) -> None:
        result = validate_sql("SELECT * FROM orders", settings)
        assert result.is_valid
        assert not result.errors
        assert "LIMIT" in result.sanitized_sql

    def test_valid_select_with_where(self, settings: Settings) -> None:
        result = validate_sql(
            "SELECT order_id, order_total FROM orders WHERE order_date > '2025-01-01'",
            settings,
        )
        assert result.is_valid

    def test_valid_cte(self, settings: Settings) -> None:
        sql = "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte"
        result = validate_sql(sql, settings)
        assert result.is_valid

    def test_valid_join(self, settings: Settings) -> None:
        sql = "SELECT o.order_id FROM orders o JOIN payments p ON o.order_id = p.order_id"
        result = validate_sql(sql, settings)
        assert result.is_valid

    def test_valid_view_query(self, settings: Settings) -> None:
        result = validate_sql("SELECT * FROM daily_kpi_summary", settings)
        assert result.is_valid

    def test_blocks_delete(self, settings: Settings) -> None:
        result = validate_sql("DELETE FROM orders WHERE 1=1", settings)
        assert not result.is_valid
        assert any("DELETE" in e for e in result.errors)

    def test_blocks_insert(self, settings: Settings) -> None:
        result = validate_sql("INSERT INTO orders VALUES (1, 2, 3)", settings)
        assert not result.is_valid

    def test_blocks_update(self, settings: Settings) -> None:
        result = validate_sql("UPDATE orders SET order_total = 0", settings)
        assert not result.is_valid

    def test_blocks_drop(self, settings: Settings) -> None:
        result = validate_sql("DROP TABLE orders", settings)
        assert not result.is_valid

    def test_blocks_create(self, settings: Settings) -> None:
        result = validate_sql("CREATE TABLE evil (id INT)", settings)
        assert not result.is_valid

    def test_blocks_alter(self, settings: Settings) -> None:
        result = validate_sql("ALTER TABLE orders ADD COLUMN hack VARCHAR", settings)
        assert not result.is_valid

    def test_blocks_unknown_table(self, settings: Settings) -> None:
        result = validate_sql("SELECT * FROM secret_data", settings)
        assert not result.is_valid
        assert any("secret_data" in e for e in result.errors)

    def test_blocks_semicolon_injection(self, settings: Settings) -> None:
        result = validate_sql("SELECT 1; DROP TABLE orders", settings)
        assert not result.is_valid

    def test_blocks_empty_query(self, settings: Settings) -> None:
        result = validate_sql("", settings)
        assert not result.is_valid

    def test_adds_limit(self, settings: Settings) -> None:
        result = validate_sql("SELECT * FROM orders", settings)
        assert result.is_valid
        assert "LIMIT" in result.sanitized_sql

    def test_preserves_existing_limit(self, settings: Settings) -> None:
        result = validate_sql("SELECT * FROM orders LIMIT 100", settings)
        assert result.is_valid
        # Should not add another LIMIT
        assert result.sanitized_sql.count("LIMIT") == 1

    def test_string_literal_with_keywords(self, settings: Settings) -> None:
        """Keywords inside string literals should not trigger validation errors."""
        result = validate_sql(
            "SELECT * FROM orders WHERE order_status = 'DELETE'",
            settings,
        )
        assert result.is_valid

    def test_multiple_views(self, settings: Settings) -> None:
        result = validate_sql(
            "SELECT * FROM channel_performance cp JOIN region_performance rp ON cp.order_date = rp.order_date",
            settings,
        )
        assert result.is_valid
