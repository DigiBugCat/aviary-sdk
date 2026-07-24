"""One-shot GPT calls on the ChatGPT sub via codex's OAuth token.

Zero-dependency (stdlib urllib) so it fits aviary-sdk's no-deps contract.
Reuses ~/.codex/auth.json — zero marginal cost, ~0.8-2s. Auto-refreshes the
token on 401 (writes it back atomically so codex and callers stay in sync).

    from aviary_sdk.oneshot import oneshot
    r = oneshot("Summarize: ...")            # → Response (OpenAI-shaped)
    r.output_text                            # the text
    r.usage.output_tokens                    # accounting
    v = oneshot("Classify ...", schema={...})
    v.output_parsed                          # the forced-shape dict

Speed: model choice is the real lever — gpt-5.3-codex-spark (~0.8s) fastest,
then gpt-5.4-mini, gpt-5.4, gpt-5.5. service_tier="priority" (codex's "fast"
on the wire; ASK_TIER env) tightens tail latency, same median. Structured
output uses text.format json_schema (strict, server-side) — NOT the
codex-exec flag name `output_schema`, which the backend rejects. Full parity
notes: aviary/docs/codex-chatgpt-backend.md.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from pathlib import Path

_URL = "https://chatgpt.com/backend-api/codex/responses"
_OAUTH = "https://auth.openai.com/oauth/token"
_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"

# Verified live on a ChatGPT sub (2026-07-07), fastest→slowest. Everything
# else (gpt-5, gpt-5-mini, *-codex-mini, o-series, gpt-4*) 400s. The exact
# spark slug is `-codex-spark`. Probe with models_available() — the codex
# catalog's supported_in_api flag is NOT authoritative for this endpoint.
MODELS = ("gpt-5.3-codex-spark", "gpt-5.4-mini", "gpt-5.4", "gpt-5.5")


class OneshotError(RuntimeError):
    """Raised on a non-retryable backend failure (bad model, bad request)."""


def _auth_path() -> Path:
    return Path(os.environ.get("ONESHOT_AUTH",
                               str(Path.home() / ".codex" / "auth.json")))


def _refresh() -> None:
    tok = json.loads(_auth_path().read_text())
    body = json.dumps({"client_id": _CLIENT_ID, "grant_type": "refresh_token",
                       "refresh_token": tok["tokens"]["refresh_token"],
                       "scope": "openid profile email offline_access"}).encode()
    req = urllib.request.Request(_OAUTH, data=body,
                                 headers={"Content-Type": "application/json"})
    d = json.loads(urllib.request.urlopen(req, timeout=30).read())
    tok["tokens"]["access_token"] = d["access_token"]
    if d.get("refresh_token"):
        tok["tokens"]["refresh_token"] = d["refresh_token"]
    tmp = _auth_path().with_suffix(".tmp")
    tmp.write_text(json.dumps(tok))
    tmp.rename(_auth_path())


def _parse_stream(raw_lines):
    """SSE parse: (text, completed_event) from an iterable of raw byte lines.

    Accumulates response.output_text.delta, stops at [DONE], keeps the
    response.completed event, and skips non-data / unparseable lines. Pure —
    no I/O — so the load-bearing wire-parse is testable without a live call."""
    out, completed = [], None
    for raw in raw_lines:
        line = raw.decode().strip()
        if not line.startswith("data:"):
            continue
        p = line[5:].strip()
        if p == "[DONE]":
            break
        try:
            ev = json.loads(p)
        except json.JSONDecodeError:
            continue
        et = ev.get("type")
        if et == "response.output_text.delta":
            out.append(ev["delta"])
        elif et == "response.completed":
            completed = ev
    return "".join(out).strip(), completed


def _post(payload: dict, timeout: float, _auth_retried: bool = False):
    """→ (text, completed_event). Refreshes on 401, flags transient 5xx/429."""
    tok = json.loads(_auth_path().read_text())["tokens"]
    headers = {"Authorization": f"Bearer {tok['access_token']}",
               "ChatGPT-Account-ID": tok["account_id"],
               "OpenAI-Beta": "responses=experimental",
               "originator": "codex_cli_rs", "session_id": str(uuid.uuid4()),
               "Accept": "text/event-stream", "Content-Type": "application/json",
               "User-Agent": "codex_cli_rs"}
    req = urllib.request.Request(_URL, data=json.dumps(payload).encode(),
                                 headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        detail = e.read()[:300].decode(errors="replace")
        if e.code == 401 and not _auth_retried:      # token expired → refresh once
            _refresh()
            return _post(payload, timeout, _auth_retried=True)
        if e.code in (429, 500, 502, 503, 529):       # transient → signal retry
            raise _Transient(f"{e.code}: {detail}")
        raise OneshotError(f"oneshot {e.code}: {detail}")   # 400/403 → caller's bug
    return _parse_stream(resp)


class _Transient(Exception):
    pass


def oneshot(prompt: str, *, schema: dict | None = None,
            model: str | None = None, effort: str | None = None,
            instructions: str = "You are concise and precise. No preamble.",
            timeout: float = 90.0, retries: int = 2) -> "Response":
    """One prompt → an OpenAI-Response-shaped object (drop-in for
    `client.responses.create`, at zero marginal cost on the ChatGPT sub).

        r = oneshot("Summarize: ...")
        r.output_text                      # the text
        r.usage.output_tokens              # accounting
        r.model, r.status

        v = oneshot("Classify ...", schema={...})
        v.output_parsed                    # the validated dict (strict, server-side)

    Batteries included: token auto-refresh on 401, retry-with-backoff on
    transient 429/5xx (`retries`), service_tier=priority ("fast") default.
    Raises OneshotError on a real request problem (bad model/body) or when
    transient retries are exhausted. Subset mirror of the Responses API
    (message text + usage + structured output, not the full item tree);
    `r.raw` holds the response.completed event. Details:
    aviary/docs/codex-chatgpt-backend.md.
    """
    import time
    from ._response import build_response
    model = model or os.environ.get("ASK_MODEL", "gpt-5.3-codex-spark")
    effort = effort or os.environ.get("ASK_EFFORT", "low")
    tier = os.environ.get("ASK_TIER", "priority")  # codex "fast"; "" disables
    payload = {"model": model, "reasoning": {"effort": effort},
               "instructions": instructions,
               "input": [{"type": "message", "role": "user",
                          "content": [{"type": "input_text", "text": prompt}]}],
               "tools": [], "tool_choice": "auto",
               "parallel_tool_calls": False, "store": False, "stream": True}
    if tier:
        payload["service_tier"] = tier
    if schema is not None:
        payload["text"] = {"format": {"type": "json_schema", "name": "response",
                                      "strict": True, "schema": schema}}
    last = None
    for attempt in range(retries + 1):
        try:
            text, completed = _post(payload, timeout)
            parsed = json.loads(text) if schema is not None and text else None
            return build_response(text, model, parsed=parsed,
                                  completed_event=completed)
        except _Transient as e:
            last = e
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
    raise OneshotError(f"oneshot transient, retries exhausted: {last}")


def models_available() -> list[str]:
    """Live-probe which MODELS this ChatGPT sub actually serves (a cheap 1-word
    call each). The authoritative check — the codex catalog's supported_in_api
    flag lies for this endpoint."""
    ok = []
    for m in MODELS:
        try:
            oneshot("hi", model=m, retries=0, timeout=30)
            ok.append(m)
        except Exception:
            pass
    return ok


async def oneshot_async(prompt: str, **kw):
    """Async variant — runs the sync call in a thread (stdlib-only, no httpx
    dep). Same signature/return as oneshot(). For callers already in an event
    loop (raven's read-side reasoner, ducklingd's digest)."""
    import asyncio
    return await asyncio.to_thread(oneshot, prompt, **kw)
