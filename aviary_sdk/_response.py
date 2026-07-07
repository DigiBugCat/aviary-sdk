"""A drop-in shim for the OpenAI Responses API `Response` object.

Zero-dependency (no `openai` package). Mirrors the field names + access
patterns of `openai.types.responses.Response` closely enough that code
written against the OpenAI SDK works unchanged against aviary_sdk.oneshot:

    resp.output_text                       # the concatenated text
    resp.output[0].content[0].text         # the item tree
    resp.usage.output_tokens               # token accounting
    resp.model / resp.status / resp.id
    resp.output_parsed                      # parsed dict for schema calls (our add)
    resp.raw                                # the raw response.completed event

It's a SUBSET mirror: we surface message text, usage, and structured output —
not the full typed-item zoo (reasoning items, tool calls, etc.). If a caller
has the real `openai` installed and wants the genuine type,
`Response.model_validate(resp.raw)` reconstructs it from `.raw`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ResponseUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0

    @classmethod
    def _from(cls, d: dict | None) -> "ResponseUsage":
        d = d or {}
        return cls(input_tokens=d.get("input_tokens", 0),
                   output_tokens=d.get("output_tokens", 0),
                   total_tokens=d.get("total_tokens",
                                      d.get("input_tokens", 0) + d.get("output_tokens", 0)))


@dataclass
class ResponseOutputText:
    text: str
    type: str = "output_text"
    annotations: list = field(default_factory=list)


@dataclass
class ResponseOutputMessage:
    content: list[ResponseOutputText]
    role: str = "assistant"
    type: str = "message"
    id: str = ""
    status: str = "completed"


@dataclass
class Response:
    """OpenAI-Response-shaped result of a one-shot call."""
    id: str
    model: str
    output: list[ResponseOutputMessage]
    usage: ResponseUsage
    status: str = "completed"
    output_parsed: Any = None      # aviary add: parsed dict when schema= was used
    raw: dict = field(default_factory=dict)

    @property
    def output_text(self) -> str:
        parts = []
        for item in self.output:
            for c in getattr(item, "content", []):
                if getattr(c, "type", None) == "output_text":
                    parts.append(c.text)
        return "".join(parts)

    # let callers do `if resp:` / `str(resp)` naturally
    def __bool__(self) -> bool:
        return bool(self.output_text) or self.output_parsed is not None

    def __str__(self) -> str:
        return self.output_text


def build_response(text: str, model: str, *, parsed: Any = None,
                   completed_event: dict | None = None) -> Response:
    ev = completed_event or {}
    r = ev.get("response", {})
    msg = ResponseOutputMessage(content=[ResponseOutputText(text=text)])
    return Response(id=r.get("id", ""), model=r.get("model", model),
                    output=[msg], usage=ResponseUsage._from(r.get("usage")),
                    status=r.get("status", "completed"),
                    output_parsed=parsed, raw=ev)
