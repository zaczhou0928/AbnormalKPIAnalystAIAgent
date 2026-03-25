"""LLM provider abstraction layer."""

from agentic_kpi_analyst.llm.base import BaseLLMClient, LLMResponse
from agentic_kpi_analyst.llm.mock_client import MockLLMClient

__all__ = ["BaseLLMClient", "LLMResponse", "MockLLMClient", "get_llm_client"]


def get_llm_client(mode: str = "mock", **kwargs: object) -> BaseLLMClient:
    """Factory to get the appropriate LLM client."""
    if mode == "openai":
        from agentic_kpi_analyst.llm.openai_client import OpenAIClient
        return OpenAIClient(**kwargs)  # type: ignore[arg-type]
    return MockLLMClient()
