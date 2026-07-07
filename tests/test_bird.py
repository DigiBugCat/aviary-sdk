"""bird face tests — /birdz, /acc REST, /mcp MCP; both faces off ONE op table."""
import json
import tempfile
import unittest
import urllib.request
import urllib.error
from pathlib import Path

from aviary_sdk import Bird, Stream


def _req(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json",
                                          **(headers or {})})
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw else None)


class BirdFacesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bird = Bird("wren", port=0, root=cls.tmp.name, desc="test bird",
                        streams=[Stream("inbox", "queue"),
                                 Stream("digests", "ledger"),
                                 Stream("verbs", "queue")])
        cls.port = cls.bird.start()
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.bird.stop()
        cls.tmp.cleanup()

    # ── helpers ───────────────────────────────────────────────────────────
    def mcp(self, method, params=None, mid=1):
        status, reply = _req("POST", f"{self.base}/mcp",
                             {"jsonrpc": "2.0", "id": mid, "method": method,
                              "params": params or {}})
        self.assertEqual(status, 200)
        return reply

    def mcp_tool(self, name, arguments):
        reply = self.mcp("tools/call", {"name": name, "arguments": arguments})
        self.assertNotIn("error", reply)
        return reply["result"]["structuredContent"]

    # ── /birdz ────────────────────────────────────────────────────────────
    def test_birdz(self):
        status, b = _req("GET", f"{self.base}/birdz")
        self.assertEqual(status, 200)
        self.assertEqual(b["name"], "wren")
        self.assertEqual(b["ref_mode"], "soft")
        self.assertIn("uptime", b)
        self.assertIn("store_seq", b)
        self.assertEqual({s["name"] for s in b["streams"]},
                         {"inbox", "digests", "verbs"})

    # ── REST face: the four verbs + envelope ─────────────────────────────
    def test_rest_verbs_and_envelope(self):
        s, r = _req("POST", f"{self.base}/acc/verbs/append",
                    {"body": {"url": "x"}, "id": "g1"})
        self.assertEqual((s, r["ok"], r["dup"]), (200, True, False))
        seq = r["seq"]

        s, r2 = _req("POST", f"{self.base}/acc/verbs/append",
                     {"body": {"url": "CHANGED"}, "id": "g1"})
        self.assertEqual((r2["ok"], r2["dup"], r2["seq"]), (True, True, seq))

        s, rd = _req("GET", f"{self.base}/acc/verbs?since=0")
        self.assertTrue(rd["ok"])
        self.assertEqual(rd["records"][0]["body"], {"url": "x"})  # dup didn't replace
        self.assertEqual(rd["next"], seq)

        s, pk = _req("GET", f"{self.base}/acc/verbs/peek?limit=5")
        self.assertTrue(pk["ok"])
        self.assertEqual(len(pk["records"]), 1)

        s, tk = _req("POST", f"{self.base}/acc/verbs/take", {"limit": 5})
        self.assertTrue(tk["ok"])
        self.assertEqual([x["id"] for x in tk["records"]], ["g1"])

        s, pk2 = _req("GET", f"{self.base}/acc/verbs/peek")
        self.assertEqual(pk2["records"], [])  # consumed

    def test_rest_error_envelope(self):
        s, r = _req("GET", f"{self.base}/acc/nostream")
        self.assertEqual(s, 400)
        self.assertFalse(r["ok"])
        self.assertIn("nostream", r["error"])
        s, r = _req("POST", f"{self.base}/acc/inbox/append", {"id": "no-body"})
        self.assertEqual((s, r["ok"]), (400, False))

    def test_mcp_append_missing_body_errors_like_rest(self):
        """Faces must not diverge: the 'body required' check lives in the shared
        op, so MCP acc_append without a body errors exactly like REST."""
        reply = self.mcp("tools/call", {"name": "acc_append",
                                        "arguments": {"stream": "inbox",
                                                      "id": "no-body-mcp"}})
        res = reply["result"]["structuredContent"]
        self.assertFalse(res["ok"])
        self.assertIn("body required", res["error"])
        _, rd = _req("GET", f"{self.base}/acc/inbox?since=0&limit=1000")
        self.assertNotIn("no-body-mcp", [r["id"] for r in rd["records"]])

    def test_nondict_bodies_get_envelope_not_dropped_connection(self):
        s, r = _req("POST", f"{self.base}/acc/inbox/take", [1, 2, 3])
        self.assertEqual((s, r["ok"]), (400, False))
        s, r = _req("POST", f"{self.base}/acc/inbox/append", [1, 2, 3])
        self.assertEqual((s, r["ok"]), (400, False))
        s, r = _req("POST", f"{self.base}/mcp", "hello")
        self.assertEqual((s, r["ok"]), (400, False))
        s, r = _req("POST", f"{self.base}/mcp",
                    [{"jsonrpc": "2.0", "id": 9, "method": "ping"}, 5])
        self.assertEqual((s, r["ok"]), (400, False))
        # and the server is still alive afterwards
        s, _ = _req("GET", f"{self.base}/birdz")
        self.assertEqual(s, 200)

    def test_acc_index(self):
        s, r = _req("GET", f"{self.base}/acc")
        self.assertTrue(r["ok"])
        self.assertEqual({x["name"] for x in r["streams"]},
                         {"inbox", "digests", "verbs"})
        for x in r["streams"]:
            self.assertEqual(set(x), {"name", "class", "len", "untaken", "last_seq"})

    # ── MCP face ──────────────────────────────────────────────────────────
    def test_mcp_initialize_and_list(self):
        init = self.mcp("initialize", {"protocolVersion": "2025-03-26",
                                       "capabilities": {},
                                       "clientInfo": {"name": "t", "version": "0"}})
        self.assertEqual(init["result"]["serverInfo"]["name"], "wren")
        tools = self.mcp("tools/list")["result"]["tools"]
        names = {t["name"] for t in tools}
        self.assertTrue({"acc_streams", "acc_append", "acc_read",
                         "acc_peek", "acc_take", "guide"} <= names)
        for t in tools:
            self.assertIn("inputSchema", t)

    def test_mcp_notification_gets_202(self):
        status, _ = _req("POST", f"{self.base}/mcp",
                         {"jsonrpc": "2.0", "method": "notifications/initialized"})
        self.assertEqual(status, 202)

    # ── the invariant: both faces hit identical code paths/state ─────────
    def test_faces_share_state_mcp_write_rest_read(self):
        res = self.mcp_tool("acc_append", {"stream": "digests",
                                           "body": {"n": 1}, "id": "d1"})
        self.assertTrue(res["ok"])
        s, rd = _req("GET", f"{self.base}/acc/digests?since=0")
        self.assertIn("d1", [r["id"] for r in rd["records"]])

    def test_faces_share_state_rest_write_mcp_read(self):
        _req("POST", f"{self.base}/acc/digests/append",
             {"body": {"n": 2}, "id": "d2"})
        res = self.mcp_tool("acc_read", {"stream": "digests", "since": 0})
        self.assertIn("d2", [r["id"] for r in res["records"]])

    def test_faces_return_identical_state(self):
        _req("POST", f"{self.base}/acc/digests/append",
             {"body": {"n": 3}, "id": "d3"})
        _, rest = _req("GET", f"{self.base}/acc")
        mcp = self.mcp_tool("acc_streams", {})
        self.assertEqual(rest, mcp)  # same envelope, same payload, one op table
        _, rest_read = _req("GET", f"{self.base}/acc/digests?since=0&limit=100")
        mcp_read = self.mcp_tool("acc_read", {"stream": "digests",
                                              "since": 0, "limit": 100})
        self.assertEqual(rest_read, mcp_read)

    def test_rid_replay_across_faces(self):
        """A take started over REST and retried over MCP replays the SAME batch —
        proof the durable takes table sits under both faces."""
        for i in range(3):
            _req("POST", f"{self.base}/acc/inbox/append",
                 {"body": {"i": i}, "id": f"x{i}"})
        _, t1 = _req("POST", f"{self.base}/acc/inbox/take",
                     {"limit": 2, "rid": "cross-face"})
        t2 = self.mcp_tool("acc_take", {"stream": "inbox", "limit": 2,
                                        "rid": "cross-face"})
        self.assertTrue(t2["replayed"])
        self.assertEqual([r["seq"] for r in t1["records"]],
                         [r["seq"] for r in t2["records"]])

    def test_mcp_tool_error_is_enveloped(self):
        res = self.mcp_tool("acc_read", {"stream": "nope"})
        self.assertFalse(res["ok"])
        reply = self.mcp("tools/call", {"name": "acc_read",
                                        "arguments": {"stream": "nope"}})
        self.assertTrue(reply["result"]["isError"])

    # ── REF stub: soft mode, loopback passes ─────────────────────────────
    def test_ref_soft_loopback_passes_as_local(self):
        ident = self.bird.ident_for("127.0.0.1", {}, "http")
        self.assertEqual(ident["sub"], "local")

    def test_ref_soft_nonloopback_passes_logged(self):
        ident = self.bird.ident_for("192.168.1.50", {}, "http")
        self.assertIsNotNone(ident)  # soft mode never blocks
        self.assertEqual(ident["sub"], "anon@192.168.1.50")

    def test_ref_header_sub_parsed_unverified(self):
        import base64
        payload = base64.urlsafe_b64encode(
            json.dumps({"sub": "flock@hub", "jti": "r_1"}).encode()
        ).decode().rstrip("=")
        tok = f"ref1.{payload}.c2ln"
        ident = self.bird.ident_for("192.168.1.50", {"X-Aviary-Ref": tok}, "mcp")
        self.assertEqual((ident["sub"], ident["jti"], ident["verified"]),
                         ("flock@hub", "r_1", False))

    def test_ref_on_refused_at_construction(self):
        """The stub verifies nothing — ref_mode='on' must not offer an illusion
        of enforcement (refused until ref.py, §5.3, lands)."""
        with self.assertRaises(ValueError):
            Bird("strict", port=0, root=self.tmp.name, ref_mode="on",
                 streams=["s:queue"])

    def test_ref_on_denies_nonloopback_without_token(self):
        strict = Bird("strict", port=0, root=self.tmp.name, streams=["s:queue"])
        strict.ref_mode = "on"   # simulate enforced mode (constructor refuses the stub)
        try:
            self.assertIsNone(strict.ident_for("192.168.1.50", {}, "http"))
            self.assertIsNotNone(strict.ident_for("127.0.0.1", {}, "http"))
        finally:
            strict.acc.close()

    # ── audit ─────────────────────────────────────────────────────────────
    def test_mutations_audited(self):
        _req("POST", f"{self.base}/acc/inbox/append",
             {"body": {"a": 1}, "id": "audit-probe"})
        audit = Path(self.tmp.name, "wren", "audit.ndjson").read_text()
        rows = [json.loads(l) for l in audit.splitlines()]
        appends = [r for r in rows if r["op"] == "acc_append"]
        self.assertTrue(appends)
        self.assertEqual(appends[-1]["sub"], "local")


class BirdFromTomlTest(unittest.TestCase):
    def test_from_toml(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp, "bird.toml")
            manifest.write_text(
                'name = "heron"\n'
                'box = "pelican"\n'
                'port = 7331\n'
                'platforms = ["linux"]\n'
                'deps = ["uv"]\n'
                'launch = "uv run heron.py"\n'
                'health = "curl -sf http://127.0.0.1:7331/birdz"\n'
                'menubar = false\n'
                'ref_mode = "soft"\n'
                'streams = ["inbox:queue", "digests:ledger"]\n'
                'desc = "RSS digest producer"\n')
            bird = Bird.from_toml(str(manifest), port=0, root=tmp)
            try:
                self.assertEqual(bird.name, "heron")
                self.assertEqual(bird.ref_mode, "soft")
                info = {s["name"]: s["class"] for s in bird.acc.streams_info()}
                self.assertEqual(info, {"inbox": "queue", "digests": "ledger"})
                self.assertTrue(bird.acc.path.endswith("heron/crop.db"))
            finally:
                bird.acc.close()


class BirdToolDecoratorTest(unittest.TestCase):
    def test_custom_tool_both_faces(self):
        with tempfile.TemporaryDirectory() as tmp:
            bird = Bird("toolbird", port=0, root=tmp, streams=["inbox:queue"])

            @bird.tool(description="Pull feeds into the inbox.")
            def fetch_now(count: int = 1) -> str:
                n = 0
                for i in range(count):
                    r = bird.acc.append("inbox", {"i": i}, id=f"guid-{i}")
                    n += 0 if r["dup"] else 1
                return f"{n} new"

            port = bird.start()
            base = f"http://127.0.0.1:{port}"
            try:
                # via MCP
                _, reply = _req("POST", f"{base}/mcp",
                                {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                 "params": {"name": "fetch_now",
                                            "arguments": {"count": 3}}})
                res = reply["result"]["structuredContent"]
                self.assertEqual((res["ok"], res["result"]), (True, "3 new"))
                # rerun dedups (idempotent producer)
                _, reply = _req("POST", f"{base}/mcp",
                                {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                                 "params": {"name": "fetch_now",
                                            "arguments": {"count": 3}}})
                self.assertEqual(
                    reply["result"]["structuredContent"]["result"], "0 new")
                # listed as an MCP tool with a derived schema
                _, lst = _req("POST", f"{base}/mcp",
                              {"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
                tool = {t["name"]: t for t in lst["result"]["tools"]}["fetch_now"]
                self.assertEqual(tool["inputSchema"]["properties"]["count"]["type"],
                                 "integer")
                # state visible over REST — one store under everything
                _, rd = _req("GET", f"{base}/acc/inbox?since=0")
                self.assertEqual(len(rd["records"]), 3)
            finally:
                bird.stop()

    def test_async_tool_actually_runs(self):
        """§4a's canonical bird is async — an async handler must execute, not
        return an un-awaited coroutine wrapped in ok:true."""
        with tempfile.TemporaryDirectory() as tmp:
            bird = Bird("asyncbird", port=0, root=tmp, streams=["inbox:queue"])

            @bird.tool(description="Pull feeds (async).")
            async def fetch_now() -> str:
                r = bird.acc.append("inbox", {"k": 1}, id="g1")
                return f"{0 if r['dup'] else 1} new"

            port = bird.start()
            base = f"http://127.0.0.1:{port}"
            try:
                _, reply = _req("POST", f"{base}/mcp",
                                {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                 "params": {"name": "fetch_now", "arguments": {}}})
                res = reply["result"]["structuredContent"]
                self.assertEqual((res["ok"], res["result"]), (True, "1 new"))
                _, rd = _req("GET", f"{base}/acc/inbox?since=0")
                self.assertEqual(len(rd["records"]), 1)  # the body actually ran
            finally:
                bird.stop()


if __name__ == "__main__":
    unittest.main()
