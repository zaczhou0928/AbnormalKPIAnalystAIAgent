"""OpenAI LLM client implementation."""

from __future__ import annotations

import json

from openai import OpenAI

from agentic_kpi_analyst.llm.base import BaseLLMClient, LLMResponse
from agentic_kpi_analyst.logging_utils import get_logger

logger = get_logger(__name__)


class OpenAIClient(BaseLLMClient):
    """LLM client using the OpenAI API."""

    def __init__(self, api_key: str = "", model: str = "gpt-4o") -> None:
        self.model = model
        self.client = OpenAI(api_key=api_key) if api_key else OpenAI()

    def complete(self, prompt: str, system: str = "", temperature: float = 0.0) -> LLMResponse:
        """Generate a completion from OpenAI."""
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        logger.info("openai_request", model=self.model, prompt_len=len(prompt))
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,  # type: ignore[arg-type]
            temperature=temperature,
        )

        choice = response.choices[0]
        usage = response.usage
        return LLMResponse(
            content=choice.message.content or "",
            model=self.model,
            usage_prompt_tokens=usage.prompt_tokens if usage else 0,
            usage_completion_tokens=usage.completion_tokens if usage else 0,
        )

    def complete_json(self, prompt: str, system: str = "", temperature: float = 0.0) -> dict:
        """Generate a completion and parse as JSON."""
        response = self.complete(prompt, system=system, temperature=temperature)
        try:
            return json.loads(response.content)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks
            content = response.content
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            return json.loads(content)
