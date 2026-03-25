"""Application configuration using pydantic-settings."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    """Global application settings, loaded from env vars and .env file."""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # LLM
    llm_mode: Literal["mock", "openai"] = "mock"
    openai_api_key: str = ""
    openai_model: str = "gpt-4o"

    # Warehouse
    warehouse_path: str = "data/generated/warehouse.duckdb"

    # Logging
    log_level: str = "INFO"

    # SQL safety
    max_sql_rows: int = 10_000
    allowed_tables: list[str] = Field(default_factory=lambda: [
        "orders",
        "order_items",
        "products",
        "customers",
        "payments",
        "channels",
        "regions",
        "campaigns",
        "refunds",
        "daily_kpi_summary",
        "fact_orders_enriched",
        "refund_summary",
        "channel_performance",
        "category_performance",
        "region_performance",
        "customer_segment_performance",
    ])

    # Human review
    human_review_confidence_threshold: float = 0.6
    human_review_auto_approve: bool = True
    max_investigation_cost_seconds: float = 120.0

    # Paths
    data_dir: str = "data/generated"
    output_dir: str = "outputs"
    knowledge_dir: str = "docs/knowledge"

    @property
    def warehouse_abs_path(self) -> Path:
        return PROJECT_ROOT / self.warehouse_path

    @property
    def data_abs_dir(self) -> Path:
        return PROJECT_ROOT / self.data_dir

    @property
    def output_abs_dir(self) -> Path:
        return PROJECT_ROOT / self.output_dir

    @property
    def knowledge_abs_dir(self) -> Path:
        return PROJECT_ROOT / self.knowledge_dir


def get_settings(**overrides: object) -> Settings:
    """Create settings instance with optional overrides."""
    return Settings(**overrides)  # type: ignore[arg-type]
