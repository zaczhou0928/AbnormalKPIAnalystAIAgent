"""Base LLM client interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel


class LLMResponse(BaseModel):
    """Standardized LLM response."""
    content: str
    model: str = ""
    usage_prompt_tokens: int = 0
    usage_completion_tokens: int = 0


class BaseLLMClient(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    def complete(self, prompt: str, system: str = "", temperature: float = 0.0) -> LLMResponse:
        """Generate a completion from the LLM."""
        ...

    @abstractmethod
    def complete_json(self, prompt: str, system: str = "", temperature: float = 0.0) -> dict:
        """Generate a completion and parse as JSON."""
        ...
