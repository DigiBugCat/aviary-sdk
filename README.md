# aviary-sdk

Shared runtime for [aviary](https://github.com/DigiBugCat?tab=repositories)
birds. **Zero runtime dependencies** (stdlib only) — birds depend on it by path.

Two things live here:

1. **Bird faces** — a crop store + `/birdz`, `/mcp`, `/acc` HTTP faces off one
   op table (`bird.py`, `acc.py`). What gives a bird its plumbing for free.
2. **`aviary_sdk.openai`** — an OpenAI-SDK-shaped client backed by the local
   `codex` CLI's ChatGPT-subscription token. GPT-5 calls at **zero marginal
   cost**, drop-in for `client.responses.create`.

---

## `aviary_sdk.openai` — GPT on the ChatGPT sub, OpenAI-shaped

If you have the `codex` CLI logged in (`~/.codex/auth.json`), this reuses its
OAuth token to call the ChatGPT backend directly. No API key, no metered
billing — it rides your ChatGPT subscription. The call surface mirrors the
OpenAI Python SDK, so it's a one-line import swap:

```python
from aviary_sdk.openai import OpenAI          # was: from openai import OpenAI

client = OpenAI()                             # OpenAI(model="gpt-5.5") to set default
r = client.responses.create(input="Summarize this in one line: ...")
r.output_text                                 # "the summary"
r.usage.output_tokens                         # accounting
r.model, r.status
```

**Structured output** (strict, server-side — the reply is *forced* to the shape):

```python
r = client.responses.create(
    input="Classify the sentiment of: 'this rules'",
    schema={"type": "object",
            "properties": {"sentiment": {"type": "string", "enum": ["pos", "neg"]},
                           "score": {"type": "number"}},
            "required": ["sentiment", "score"], "additionalProperties": False})
r.output_parsed                               # {"sentiment": "pos", "score": 0.99}
```

**Async:**

```python
from aviary_sdk.openai import AsyncOpenAI
r = await AsyncOpenAI().responses.create(input="...")
```

**Low-level primitive** (what the client wraps — use directly if you prefer):

```python
from aviary_sdk import oneshot
r = oneshot("Summarize: ...", schema=None, model=None)   # → Response
```

### Models

Verified live on a ChatGPT sub — fastest to slowest:

| model | typical | note |
|---|---|---|
| `gpt-5.3-codex-spark` | ~0.8s | fastest; the default |
| `gpt-5.4-mini` | ~1.5s | cheap tier |
| `gpt-5.4` | ~1.7s | |
| `gpt-5.5` | ~1.8s | flagship |

Everything else (`gpt-5`, `gpt-5-mini`, `*-codex-mini`, o-series, `gpt-4*`)
returns `400 not supported when using Codex with a ChatGPT account`. Probe live
with `OpenAI.models_available()` — the codex catalog's `supported_in_api` flag
is **not** authoritative for this endpoint.

### What's included

Token auto-refresh on 401 (writes back atomically, stays in sync with codex),
retry-with-backoff on transient 429/5xx, `service_tier="priority"` (codex's
"fast", tightens tail latency), strict structured output via `schema`.

### Env knobs

`ASK_MODEL`, `ASK_EFFORT` (low/medium/high/xhigh), `ASK_TIER` (`priority`;
empty to disable), `ONESHOT_AUTH` (auth.json path).

### Honest scope

A **subset** mirror of the Responses API: message text + usage + structured
output — not tools, multi-turn `input` arrays, streaming callbacks, or the full
typed-item tree. A drop-in for **one-shot text/JSON** calls. `r.raw` holds the
raw `response.completed` event if you have the real `openai` installed and want
`Response.model_validate(r.raw)`.

> Undocumented private endpoint (codex's own). OpenAI can change the
> headers/allowlist or add attestation at any time. Grey-area ToS. Keep a
> fallback for anything load-bearing. Full reverse-engineering + parity notes:
> `../docs/codex-chatgpt-backend.md`.

---

## Install

Birds depend by path (no PyPI publish):

```toml
# pyproject.toml
[tool.uv.sources]
aviary-sdk = { path = "../sdk" }
```

or in a `uv run` script header:

```python
# dependencies = ["aviary-sdk @ file:///home/andrew/Obsidian/aviary/sdk"]
```

## Dev

```sh
uv pip install -e . --group dev   # or: PYTHONPATH=. python -m pytest tests/ -q
```

`_response.py` is the OpenAI-`Response`-shaped shim; `oneshot.py` the wire
implementation; `openai.py` the client surface; `bird.py` / `acc.py` the bird
faces.
