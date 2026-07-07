"""An OpenAI-SDK-shaped client backed by the codex ChatGPT-sub token.

Drop-in for the bits of the OpenAI Python SDK you actually use, at zero
marginal cost (reuses ~/.codex/auth.json — no API key). The call surface
matches `openai`:

    from aviary_sdk.openai import OpenAI          # instead of `from openai import OpenAI`
    client = OpenAI()

    r = client.responses.create(input="Summarize: ...")
    r.output_text                                 # the text
    r.usage.output_tokens
    r.model, r.status

    # structured output — same `text_format` / json_schema idea, strict:
    r = client.responses.create(input="Classify ...",
                                schema={"type": "object", ...})
    r.output_parsed                               # validated dict

Async: `from aviary_sdk.openai import AsyncOpenAI` → `await
client.responses.create(...)`. Models (fastest→): gpt-5.3-codex-spark,
gpt-5.4-mini, gpt-5.4, gpt-5.5 — everything else 400s on the ChatGPT sub.
Batteries: token auto-refresh, transient retry, service_tier=priority
("fast"). Subset mirror (message text + usage + structured output, not the
full item tree); `r.raw` is the response.completed event. Details:
aviary/docs/codex-chatgpt-backend.md.
"""

from __future__ import annotations

from .oneshot import oneshot as _oneshot, oneshot_async as _oneshot_async, models_available as _models_available
from ._response import Response  # noqa: F401  (re-export for callers)

DEFAULT_MODEL = "gpt-5.3-codex-spark"


class _Responses:
    def __init__(self, default_model: str):
        self._model = default_model

    def create(self, *, input: str, model: str | None = None,
               schema: dict | None = None, effort: str | None = None,
               instructions: str = "You are concise and precise. No preamble.",
               timeout: float = 90.0, retries: int = 2) -> Response:
        """Mirror of client.responses.create — `input` is the prompt."""
        return _oneshot(input, schema=schema, model=model or self._model,
                             effort=effort, instructions=instructions,
                             timeout=timeout, retries=retries)


class _AsyncResponses:
    def __init__(self, default_model: str):
        self._model = default_model

    async def create(self, *, input: str, model: str | None = None,
                     schema: dict | None = None, effort: str | None = None,
                     instructions: str = "You are concise and precise. No preamble.",
                     timeout: float = 90.0, retries: int = 2) -> Response:
        return await _oneshot_async(
            input, schema=schema, model=model or self._model, effort=effort,
            instructions=instructions, timeout=timeout, retries=retries)


class OpenAI:
    """Sync client. `OpenAI(model=...)` sets the default model."""

    def __init__(self, *, model: str = DEFAULT_MODEL, **_ignored):
        self.responses = _Responses(model)

    @staticmethod
    def models_available() -> list[str]:
        return _models_available()


class AsyncOpenAI:
    def __init__(self, *, model: str = DEFAULT_MODEL, **_ignored):
        self.responses = _AsyncResponses(model)
