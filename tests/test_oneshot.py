"""Deterministic tests for the oneshot Response shim (no live API calls)."""
from aviary_sdk._response import build_response
from aviary_sdk.oneshot import MODELS


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
