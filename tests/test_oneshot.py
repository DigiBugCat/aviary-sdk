"""Deterministic tests for the oneshot Response shim (no live API calls)."""
import pytest

import importlib

from aviary_sdk._response import build_response

oneshot_mod = importlib.import_module("aviary_sdk.oneshot")

MODELS = oneshot_mod.MODELS
OneshotError = oneshot_mod.OneshotError
_parse_stream = oneshot_mod._parse_stream
_Transient = oneshot_mod._Transient
oneshot = oneshot_mod.oneshot


def _sse(*lines):
    """Encode strings as the raw byte lines an SSE response iterates over."""
    return [ln.encode() for ln in lines]


def test_parse_stream_accumulates_deltas():
    text, completed = _parse_stream(_sse(
        'data: {"type": "response.output_text.delta", "delta": "hel"}',
        'data: {"type": "response.output_text.delta", "delta": "lo"}',
        'data: {"type": "response.completed", "response": {"id": "r1"}}',
        "data: [DONE]",
    ))
    assert text == "hello"
    assert completed == {"type": "response.completed", "response": {"id": "r1"}}


def test_parse_stream_stops_at_done():
    # A delta after [DONE] must not be consumed.
    text, _ = _parse_stream(_sse(
        'data: {"type": "response.output_text.delta", "delta": "kept"}',
        "data: [DONE]",
        'data: {"type": "response.output_text.delta", "delta": "dropped"}',
    ))
    assert text == "kept"


def test_parse_stream_skips_noise_and_bad_json():
    # Non-data lines, blanks, and unparseable data payloads are skipped.
    text, completed = _parse_stream(_sse(
        "",
        ": keepalive comment",
        "event: ping",
        "data: {not valid json",
        'data: {"type": "response.output_text.delta", "delta": "ok"}',
    ))
    assert text == "ok"
    assert completed is None


def test_oneshot_transient_exhausts_into_oneshot_error(monkeypatch):
    calls = []

    def boom(payload, timeout):
        calls.append(1)
        raise _Transient("503: down")

    import time as _time

    monkeypatch.setattr(oneshot_mod, "_post", boom)
    monkeypatch.setattr(_time, "sleep", lambda *_: None)
    with pytest.raises(OneshotError):
        oneshot("hi", retries=2)
    assert len(calls) == 3  # initial + 2 retries


def test_oneshot_returns_response_on_success(monkeypatch):
    monkeypatch.setattr(oneshot_mod, "_post", lambda p, t: ("hi there", None))
    r = oneshot("greet", retries=0)
    assert r.output_text == "hi there"


def test_response_output_text_and_tree():
    ev = {"type": "response.completed", "response": {
        "id": "resp_1", "model": "gpt-5.5", "status": "completed",
        "usage": {"input_tokens": 10, "output_tokens": 5}}}
    r = build_response("hello world", "gpt-5.5", completed_event=ev)
    assert r.output_text == "hello world"
    assert r.output[0].content[0].text == "hello world"      # item tree
    assert r.model == "gpt-5.5" and r.id == "resp_1"
    assert r.usage.input_tokens == 10 and r.usage.output_tokens == 5
    assert r.usage.total_tokens == 15
    assert bool(r) and str(r) == "hello world"
    assert r.raw is ev                                        # escape hatch


def test_response_parsed_for_schema():
    r = build_response('{"a": 1}', "gpt-5.4-mini", parsed={"a": 1})
    assert r.output_parsed == {"a": 1}
    assert bool(r)                                            # truthy via parsed


def test_response_empty_is_falsy():
    r = build_response("", "gpt-5.5")
    assert not bool(r) and r.usage.total_tokens == 0


def test_models_constant():
    assert MODELS[0] == "gpt-5.3-codex-spark"                # fastest first
    assert "gpt-5.5" in MODELS


def test_openai_client_surface():
    """The OpenAI-shaped client exposes .responses.create without a live call."""
    from aviary_sdk.openai import OpenAI, AsyncOpenAI
    c = OpenAI(model="gpt-5.4")
    assert c.responses._model == "gpt-5.4"           # default model set
    assert hasattr(c.responses, "create")
    assert hasattr(OpenAI, "models_available")
    ac = AsyncOpenAI()
    assert ac.responses._model == "gpt-5.3-codex-spark"   # default
