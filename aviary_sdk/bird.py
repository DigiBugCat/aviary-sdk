"""bird — the runtime shell (ARCHITECTURE.md §1, §3).

A bird is one process, one port, one sqlite crop, ONE op table, three faces:

  GET  /birdz                       identity/health {name, version, uptime, streams,
                                                     ref_mode, store_seq}
  POST /mcp                         streamable-HTTP MCP (acc_* tools + @bird.tool()s)
  /acc/*                            crop REST face (peacock envelope {ok:true,...})

Both /mcp and /acc funnel through the same OPS dispatch table (peacock.py line 478's
pattern) so each primitive is defined exactly once and the two faces cannot diverge.

REF middleware ships INERT at `soft` (§3.2 posture): loopback requests without a REF
pass as ident=local; non-loopback requests are logged (sub/jti parsed UNVERIFIED from
the token when present) and never blocked. `ref_mode = "on"` is REFUSED at
construction (unless AVIARY_REF_UNVERIFIED_ON=1) because this stub verifies no
signatures and matches no caps — real verification is ref.py's job (§5.3); enabling
'on' here would be an illusion of enforcement.

Zero dependencies: stdlib http.server + sqlite3. A bird can be one uv-run file.
"""
from __future__ import annotations

import asyncio
import base64
import inspect
import json
import os
import threading
import time
import tomllib
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .acc import Crop, Stream

_MCP_PROTOCOL = "2025-03-26"
_LOOPBACK = ("127.0.0.1", "::1", "::ffff:127.0.0.1")


def _b64url_json(part: str):
    try:
        pad = "=" * (-len(part) % 4)
        return json.loads(base64.urlsafe_b64decode(part + pad))
    except Exception:
        return None


def _run_coro(coro):
    """Run an async tool handler to completion from a sync dispatch thread.
    HTTP handler threads have no event loop, so asyncio.run just works; if this
    thread somehow IS inside a running loop (a bird embedded in an async host),
    run the coroutine on a fresh thread instead of deadlocking/double-entering."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    box: dict = {}

    def runner():
        try:
            box["r"] = asyncio.run(coro)
        except BaseException as e:  # propagate to the dispatching thread
            box["e"] = e

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    t.join()
    if "e" in box:
        raise box["e"]
    return box.get("r")


class Bird:
    """Bird(name, port, streams) — mounts /birdz, /mcp, /acc off one op table."""

    def __init__(self, name: str, port: int, streams=(), ref_mode: str = "soft",
                 host: str = "127.0.0.1", version: str = "0.1.0", desc: str = "",
                 root: str | None = None):
        if ref_mode not in ("off", "soft", "on"):
            raise ValueError(f"bad ref_mode: {ref_mode!r}")
        if ref_mode == "on" and not os.environ.get("AVIARY_REF_UNVERIFIED_ON"):
            # The REF stub parses tokens but verifies NOTHING (no signature, no
            # caps) — 'on' would be an illusion of enforcement (§5.3 ref.py is
            # the real thing). Refuse loudly rather than pretend.
            raise ValueError(
                "ref_mode='on' is not enforceable yet: REF verification is a stub "
                "(any 'ref1.' token passes unverified; no cap matching). Use 'soft', "
                "or set AVIARY_REF_UNVERIFIED_ON=1 to knowingly accept the stub's "
                "loopback-only gating.")
        self.name = name
        self.port = int(port)
        self.host = host
        self.version = version
        self.desc = desc
        self.ref_mode = ref_mode
        self.started = time.time()
        self.acc = Crop(name, streams, root=root)
        self._audit_path = os.path.join(self.acc.dir, "audit.ndjson")
        self._audit_lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._session_id = uuid.uuid4().hex

        def _acc_append(a: dict, i: dict):
            # Validation lives HERE (the shared op), not in one face's route —
            # otherwise REST 400s on a missing body while MCP appends null.
            if "body" not in a:
                raise ValueError("body required")
            return self.acc.append(a["stream"], a["body"], a.get("id"))

        # ── THE op table: one dispatch surface for REST and MCP alike ──
        self.ops: dict = {
            "acc_streams": lambda a, i: {"streams": self.acc.streams_info()},
            "acc_append":  _acc_append,
            "acc_read":    lambda a, i: self.acc.read(a["stream"],
                                                      int(a.get("since", 0)),
                                                      int(a.get("limit", 100))),
            "acc_peek":    lambda a, i: self.acc.peek(a["stream"],
                                                      int(a.get("limit", 10))),
            "acc_take":    lambda a, i: self.acc.take(a["stream"],
                                                      int(a.get("limit", 10)),
                                                      a.get("rid")),
            "guide":       lambda a, i: {"guide": self.guide()},
        }
        self._mutating = {"acc_append", "acc_take"}
        self._tool_meta: dict[str, dict] = dict(_ACC_TOOLS)
        self._tool_meta["guide"] = {
            "description": "This bird's operating manual (for fresh agent instances).",
            "inputSchema": {"type": "object", "properties": {}, "required": []},
        }

    # ── construction from a manifest ─────────────────────────────────────
    @classmethod
    def from_toml(cls, path: str, **overrides) -> "Bird":
        """Read a §3.4 bird.toml; registration = the file existing. `overrides`
        (port=, root=, host=, ...) win over the manifest — handy for tests."""
        with open(path, "rb") as f:
            m = tomllib.load(f)
        kw = {
            "name": m["name"],
            "port": m["port"],
            "streams": [Stream.parse(s) for s in m.get("streams", [])],
            "ref_mode": m.get("ref_mode", "soft"),
            "desc": m.get("desc", ""),
        }
        kw.update(overrides)
        return cls(**kw)

    # ── custom tools (the §4a @bird.tool() surface) ──────────────────────
    def tool(self, name: str | None = None, description: str | None = None):
        """Register a function as an op + MCP tool. Handler args come from the
        function signature; an `ident` parameter (if declared) receives the caller
        identity. Non-dict returns are wrapped as {ok, result}."""
        def deco(fn):
            op = name or fn.__name__
            wants_ident = "ident" in inspect.signature(fn).parameters

            def handler(a: dict, i: dict):
                kwargs = dict(a or {})
                if wants_ident:
                    kwargs["ident"] = i
                res = fn(**kwargs)
                if inspect.iscoroutine(res):   # async handlers actually RUN
                    res = _run_coro(res)       # (§4a's canonical bird is async)
                return res if isinstance(res, dict) else {"result": res}

            self.ops[op] = handler
            self._mutating.add(op)  # custom tools are assumed mutating (audited)
            self._tool_meta[op] = {
                "description": description or (fn.__doc__ or op).strip(),
                "inputSchema": _schema_for(fn),
            }
            return fn
        return deco

    # ── the single dispatch path (both faces call THIS) ──────────────────
    def dispatch(self, op: str, args: dict | None, ident: dict) -> dict:
        handler = self.ops.get(op)
        if handler is None:
            return {"ok": False, "error": f"unknown op: {op}"}
        try:
            res = handler(args or {}, ident)
        except KeyError as e:
            return {"ok": False, "error": f"missing/unknown: {e}"}
        except (ValueError, TypeError) as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:  # pragma: no cover — belt and braces
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}
        if op in self._mutating:
            self.audit(op, (args or {}).get("stream", "-"), ident)
        return {"ok": True, **res}

    # ── identity & audit (REF stub, §3.2 posture) ────────────────────────
    def ident_for(self, client_ip: str, headers, transport: str) -> dict | None:
        """Derive caller identity. Returns None => deny (403). soft mode never
        denies; loopback without a REF passes as ident=local."""
        loopback = client_ip in _LOOPBACK
        tok = headers.get("X-Aviary-Ref")
        if tok is None and loopback:
            auth = headers.get("Authorization", "")
            if auth.startswith("Bearer ref1."):  # loopback-only fallback (§3.2)
                tok = auth[len("Bearer "):]
        if tok and tok.startswith("ref1."):
            parts = tok.split(".")
            payload = _b64url_json(parts[1]) if len(parts) == 3 else None
            payload = payload if isinstance(payload, dict) else {}
            # STUB: parsed, not verified — ref.py (§5.3) supplies real verification.
            return {"sub": payload.get("sub", "ref?"), "jti": payload.get("jti"),
                    "transport": transport, "verified": False}
        if loopback:
            return {"sub": "local", "jti": None, "transport": transport,
                    "verified": False}
        if self.ref_mode == "on":
            return None  # non-loopback without a REF: denied when enforcing
        # off/soft: pass, identity is just the peer address (soft = log-only)
        return {"sub": f"anon@{client_ip}", "jti": None, "transport": transport,
                "verified": False}

    def audit(self, op: str, target: str, ident: dict, extra: dict | None = None):
        """One immutable ndjson row per mutation: ~/.aviary/<bird>/audit.ndjson."""
        rec = {"ts": time.time(),
               "iso": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
               "op": op, "target": target,
               "sub": ident.get("sub"), "jti": ident.get("jti"),
               "transport": ident.get("transport")}
        if extra:
            rec.update(extra)
        with self._audit_lock:
            try:
                with open(self._audit_path, "a") as f:
                    f.write(json.dumps(rec) + "\n")
            except OSError:
                pass

    # ── health & guide ───────────────────────────────────────────────────
    def birdz(self) -> dict:
        return {"name": self.name, "version": self.version,
                "uptime": round(time.time() - self.started, 3),
                "streams": self.acc.streams_info(),
                "ref_mode": self.ref_mode,
                "store_seq": self.acc.last_seq()}

    def guide(self) -> str:
        tools = ", ".join(sorted(self._tool_meta))
        streams = ", ".join(f"{s['name']}({s['class']})"
                            for s in self.acc.streams_info()) or "none"
        return (f"{self.name} — {self.desc or 'a bird'}\n"
                f"Faces: GET /birdz (health), POST /mcp (MCP tools: {tools}), "
                f"/acc/* (crop REST).\n"
                f"Streams: {streams}. Crop verbs: append (dedup by id), read "
                f"(your own cursor via `next`), peek, take (pass rid= to make "
                f"retries replay the same batch).\n"
                f"Poll, don't wait to be delivered to.")

    # ── serving ──────────────────────────────────────────────────────────
    def _make_server(self) -> ThreadingHTTPServer:
        handler = type("Handler", (_Handler,), {"bird": self})
        srv = ThreadingHTTPServer((self.host, self.port), handler)
        self.port = srv.server_address[1]  # resolve port=0 for tests
        return srv

    def start(self) -> int:
        """Serve in a background thread; returns the bound port."""
        self._server = self._make_server()
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        daemon=True)
        self._thread.start()
        return self.port

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        self.acc.close()

    def serve(self):
        """Blocking serve — the last line of a bird's __main__."""
        srv = self._make_server()
        self._server = srv
        print(f"{self.name} — /birdz /mcp /acc on http://{self.host}:{self.port}",
              flush=True)
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            srv.server_close()
            self.acc.close()

    # ── MCP (streamable-HTTP, minimal + stateless) ───────────────────────
    def mcp_message(self, msg: dict, ident: dict):
        """Handle one JSON-RPC message. Returns a response dict or None
        (notification: no reply)."""
        mid = msg.get("id")
        method = msg.get("method", "")
        params = msg.get("params") or {}
        if mid is None:  # notification (e.g. notifications/initialized)
            return None

        def ok(result):
            return {"jsonrpc": "2.0", "id": mid, "result": result}

        if method == "initialize":
            return ok({"protocolVersion":
                       params.get("protocolVersion", _MCP_PROTOCOL),
                       "capabilities": {"tools": {"listChanged": False}},
                       "serverInfo": {"name": self.name,
                                      "version": self.version}})
        if method == "ping":
            return ok({})
        if method == "tools/list":
            tools = [{"name": n,
                      "description": m["description"],
                      "inputSchema": m["inputSchema"]}
                     for n, m in sorted(self._tool_meta.items())]
            return ok({"tools": tools})
        if method == "tools/call":
            name = params.get("name", "")
            args = params.get("arguments") or {}
            res = self.dispatch(name, args, ident)   # ← same table as REST
            return ok({"content": [{"type": "text",
                                    "text": json.dumps(res, default=str)}],
                       "structuredContent": res,
                       "isError": not res.get("ok", False)})
        return {"jsonrpc": "2.0", "id": mid,
                "error": {"code": -32601, "message": f"method not found: {method}"}}


# ── tool metadata for the built-in acc_* surface ─────────────────────────
def _obj_schema(props: dict, required: list) -> dict:
    return {"type": "object", "properties": props, "required": required}


_STREAM_PROP = {"stream": {"type": "string", "description": "stream name"}}
_ACC_TOOLS = {
    "acc_streams": {
        "description": "List this bird's crop streams: {name, class, len, untaken, last_seq}.",
        "inputSchema": _obj_schema({}, []),
    },
    "acc_append": {
        "description": ("Append a record to a stream. `id` is the producer's dedup key: "
                        "an existing id returns the ORIGINAL seq with dup:true (body not "
                        "replaced). Omit id for a random never-dup UUID."),
        "inputSchema": _obj_schema({**_STREAM_PROP,
                                    "body": {"description": "any JSON payload"},
                                    "id": {"type": "string"}}, ["stream", "body"]),
    },
    "acc_read": {
        "description": ("Non-destructive log read: records with seq > since, oldest "
                        "first. Keep your own cursor: pass the returned `next` as the "
                        "following call's `since`."),
        "inputSchema": _obj_schema({**_STREAM_PROP,
                                    "since": {"type": "integer", "default": 0},
                                    "limit": {"type": "integer", "default": 100}},
                                   ["stream"]),
    },
    "acc_peek": {
        "description": "Oldest untaken records without claiming them ('is there work?').",
        "inputSchema": _obj_schema({**_STREAM_PROP,
                                    "limit": {"type": "integer", "default": 10}},
                                   ["stream"]),
    },
    "acc_take": {
        "description": ("Atomically take (mark consumed) the oldest untaken records. "
                        "At-most-once; pass a stable `rid` so a retried request replays "
                        "the SAME batch instead of dropping it."),
        "inputSchema": _obj_schema({**_STREAM_PROP,
                                    "limit": {"type": "integer", "default": 10},
                                    "rid": {"type": "string"}}, ["stream"]),
    },
}

_PY_JSON_TYPES = {int: "integer", float: "number", str: "string",
                  bool: "boolean", dict: "object", list: "array"}


def _schema_for(fn) -> dict:
    props, required = {}, []
    for p in inspect.signature(fn).parameters.values():
        if p.name in ("self", "ident"):
            continue
        t = _PY_JSON_TYPES.get(p.annotation)
        props[p.name] = {"type": t} if t else {}
        if p.default is inspect.Parameter.empty:
            required.append(p.name)
    return _obj_schema(props, required)


# ── the HTTP face ─────────────────────────────────────────────────────────
class _Handler(BaseHTTPRequestHandler):
    bird: Bird  # injected per server via type()
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # quiet by default; audit is the record
        pass

    # helpers ---------------------------------------------------------------
    def _json(self, status: int, obj, headers: dict | None = None):
        body = json.dumps(obj, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        try:
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n) or b"{}")
        except (ValueError, json.JSONDecodeError):
            return {}

    def _ident(self, transport: str) -> dict | None:
        ident = self.bird.ident_for(self.client_address[0], self.headers, transport)
        if ident is None:
            self._json(403, {"ok": False, "error": "X-Aviary-Ref required (ref_mode=on)"})
        return ident

    def _envelope(self, res: dict):
        self._json(200 if res.get("ok") else 400, res)

    # routes ------------------------------------------------------------------
    def do_GET(self):
        url = urlparse(self.path)
        q = parse_qs(url.query)
        parts = [p for p in url.path.split("/") if p]

        if url.path == "/birdz":
            return self._json(200, self.bird.birdz())
        ident = self._ident("http")
        if ident is None:
            return
        if url.path == "/acc":
            return self._envelope(self.bird.dispatch("acc_streams", {}, ident))
        if len(parts) == 2 and parts[0] == "acc":                 # GET /acc/{stream}
            args = {"stream": parts[1],
                    "since": q.get("since", ["0"])[0],
                    "limit": q.get("limit", ["100"])[0]}
            return self._envelope(self.bird.dispatch("acc_read", args, ident))
        if len(parts) == 3 and parts[0] == "acc" and parts[2] == "peek":
            args = {"stream": parts[1], "limit": q.get("limit", ["10"])[0]}
            return self._envelope(self.bird.dispatch("acc_peek", args, ident))
        if url.path == "/mcp":
            return self._json(405, {"ok": False, "error": "POST JSON-RPC to /mcp"},
                              {"Allow": "POST"})
        self._json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        url = urlparse(self.path)
        parts = [p for p in url.path.split("/") if p]

        if url.path == "/mcp":
            ident = self._ident("mcp")
            if ident is None:
                return
            msg = self._body()
            if isinstance(msg, list):  # batch — every element must be an object
                if not all(isinstance(m, dict) for m in msg):
                    return self._json(400, {"ok": False,
                                            "error": "JSON-RPC batch elements must be objects"})
                replies = [r for m in msg
                           if (r := self.bird.mcp_message(m, ident)) is not None]
                if not replies:
                    return self._accepted()
                return self._json(200, replies,
                                  {"Mcp-Session-Id": self.bird._session_id})
            if not isinstance(msg, dict):  # a JSON string/number is not a message
                return self._json(400, {"ok": False,
                                        "error": "JSON-RPC object or batch required"})
            reply = self.bird.mcp_message(msg, ident)
            if reply is None:  # notification
                return self._accepted()
            return self._json(200, reply,
                              {"Mcp-Session-Id": self.bird._session_id})

        ident = self._ident("http")
        if ident is None:
            return
        if len(parts) == 3 and parts[0] == "acc":
            b = self._body()
            if not isinstance(b, dict):   # [1,2,3] / "x" / 5 → envelope, not a crash
                return self._envelope({"ok": False, "error": "JSON object body required"})
            if parts[2] == "append":       # POST /acc/{stream}/append {body, id?}
                args = {"stream": parts[1], "id": b.get("id")}
                if "body" in b:            # 'body required' check lives in the shared op
                    args["body"] = b["body"]
                return self._envelope(self.bird.dispatch("acc_append", args, ident))
            if parts[2] == "take":         # POST /acc/{stream}/take {limit?, rid?}
                args = {"stream": parts[1], "limit": b.get("limit", 10),
                        "rid": b.get("rid")}
                return self._envelope(self.bird.dispatch("acc_take", args, ident))
        self._json(404, {"ok": False, "error": "not found"})

    def do_DELETE(self):
        if urlparse(self.path).path == "/mcp":  # stateless: session end is a no-op
            return self._json(200, {"ok": True})
        self._json(404, {"ok": False, "error": "not found"})

    def _accepted(self):
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()
