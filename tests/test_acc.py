"""crop contract tests — ARCHITECTURE.md §3.1 verb semantics."""
import tempfile
import unittest

from aviary_sdk.acc import Crop, Stream


class CropTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.crop = Crop("testbird",
                         [Stream("inbox", "queue"), Stream("trades", "ledger")],
                         root=self.tmp.name)

    def tearDown(self):
        self.crop.close()
        self.tmp.cleanup()

    # ── append + dedup ────────────────────────────────────────────────────
    def test_append_returns_seq_id(self):
        r = self.crop.append("inbox", {"x": 1}, id="a")
        self.assertEqual(r, {"seq": 1, "id": "a", "dup": False})

    def test_dedup_same_id_returns_original_seq_body_not_replaced(self):
        r1 = self.crop.append("inbox", {"x": "original"}, id="a")
        r2 = self.crop.append("inbox", {"x": "REPLACEMENT"}, id="a")
        self.assertFalse(r1["dup"])
        self.assertTrue(r2["dup"])
        self.assertEqual(r2["seq"], r1["seq"])
        recs = self.crop.read("inbox")["records"]
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["body"], {"x": "original"})  # body NOT replaced

    def test_dedup_is_per_stream(self):
        r1 = self.crop.append("inbox", {"x": 1}, id="a")
        r2 = self.crop.append("trades", {"x": 1}, id="a")
        self.assertFalse(r2["dup"])
        self.assertNotEqual(r1["seq"], r2["seq"])

    def test_append_without_id_never_dups(self):
        r1 = self.crop.append("inbox", {"x": 1})
        r2 = self.crop.append("inbox", {"x": 1})
        self.assertFalse(r1["dup"] or r2["dup"])
        self.assertNotEqual(r1["id"], r2["id"])
        self.assertNotEqual(r1["seq"], r2["seq"])

    # ── read: consumer-owned cursors ──────────────────────────────────────
    def test_read_cursor_pagination(self):
        for i in range(5):
            self.crop.append("inbox", {"i": i}, id=f"r{i}")
        page1 = self.crop.read("inbox", since=0, limit=2)
        self.assertEqual([r["body"]["i"] for r in page1["records"]], [0, 1])
        page2 = self.crop.read("inbox", since=page1["next"], limit=2)
        self.assertEqual([r["body"]["i"] for r in page2["records"]], [2, 3])
        page3 = self.crop.read("inbox", since=page2["next"], limit=100)
        self.assertEqual([r["body"]["i"] for r in page3["records"]], [4])
        empty = self.crop.read("inbox", since=page3["next"])
        self.assertEqual(empty["records"], [])
        self.assertEqual(empty["next"], page3["next"])  # cursor stable when empty

    def test_cursor_isolation_two_consumers(self):
        """The server tracks no consumers — two callers with their own cursors
        each independently see every record."""
        for i in range(4):
            self.crop.append("inbox", {"i": i}, id=f"r{i}")
        a_cursor = b_cursor = 0
        a_seen, b_seen = [], []
        # consumer A reads everything, one at a time
        while True:
            r = self.crop.read("inbox", since=a_cursor, limit=1)
            if not r["records"]:
                break
            a_seen += [x["body"]["i"] for x in r["records"]]
            a_cursor = r["next"]
        # consumer B afterwards, in bigger pages — A's reads cost B nothing
        while True:
            r = self.crop.read("inbox", since=b_cursor, limit=3)
            if not r["records"]:
                break
            b_seen += [x["body"]["i"] for x in r["records"]]
            b_cursor = r["next"]
        self.assertEqual(a_seen, [0, 1, 2, 3])
        self.assertEqual(b_seen, [0, 1, 2, 3])

    def test_read_includes_taken_flag(self):
        self.crop.append("inbox", {"i": 0}, id="r0")
        self.crop.take("inbox", limit=1)
        recs = self.crop.read("inbox")["records"]
        self.assertTrue(recs[0]["taken"])  # read is non-destructive, flag visible

    # ── peek ──────────────────────────────────────────────────────────────
    def test_peek_no_claim(self):
        for i in range(3):
            self.crop.append("inbox", {"i": i}, id=f"r{i}")
        p1 = self.crop.peek("inbox", limit=2)
        p2 = self.crop.peek("inbox", limit=2)
        self.assertEqual(p1, p2)  # peeking claims nothing
        self.assertEqual([r["body"]["i"] for r in p1["records"]], [0, 1])

    def test_peek_skips_taken(self):
        for i in range(3):
            self.crop.append("inbox", {"i": i}, id=f"r{i}")
        self.crop.take("inbox", limit=1)
        p = self.crop.peek("inbox")
        self.assertEqual([r["body"]["i"] for r in p["records"]], [1, 2])

    # ── take + rid replay ─────────────────────────────────────────────────
    def test_take_marks_consumed_oldest_first(self):
        for i in range(3):
            self.crop.append("inbox", {"i": i}, id=f"r{i}")
        t = self.crop.take("inbox", limit=2)
        self.assertEqual([r["body"]["i"] for r in t["records"]], [0, 1])
        self.assertTrue(all(r["taken"] for r in t["records"]))
        t2 = self.crop.take("inbox", limit=10)
        self.assertEqual([r["body"]["i"] for r in t2["records"]], [2])

    def test_take_empty_stream(self):
        t = self.crop.take("inbox")
        self.assertEqual(t["records"], [])

    def test_rid_replay_same_batch(self):
        for i in range(4):
            self.crop.append("inbox", {"i": i}, id=f"r{i}")
        t1 = self.crop.take("inbox", limit=2, rid="run-1")
        self.assertFalse(t1["replayed"])
        t_retry = self.crop.take("inbox", limit=2, rid="run-1")
        self.assertTrue(t_retry["replayed"])
        self.assertEqual([r["seq"] for r in t_retry["records"]],
                         [r["seq"] for r in t1["records"]])  # SAME batch
        t2 = self.crop.take("inbox", limit=2, rid="run-2")   # new rid -> next batch
        self.assertEqual([r["body"]["i"] for r in t2["records"]], [2, 3])

    def test_rid_replay_survives_restart(self):
        """The takes table is durable — peacock's _RID_CACHE made real (§3.1)."""
        for i in range(3):
            self.crop.append("inbox", {"i": i}, id=f"r{i}")
        t1 = self.crop.take("inbox", limit=2, rid="run-1")
        self.crop.close()
        reopened = Crop("testbird", ["inbox:queue"], root=self.tmp.name)
        try:
            t_replay = reopened.take("inbox", limit=2, rid="run-1")
            self.assertTrue(t_replay["replayed"])
            self.assertEqual([r["seq"] for r in t_replay["records"]],
                             [r["seq"] for r in t1["records"]])
        finally:
            reopened.close()
        self.crop = Crop("testbird", ["inbox:queue"], root=self.tmp.name)  # for tearDown

    def test_rid_scoped_per_stream(self):
        self.crop.declare(Stream("inbox2", "queue"))
        self.crop.append("inbox", {"i": 0}, id="a")
        self.crop.append("inbox2", {"i": 9}, id="a")
        t1 = self.crop.take("inbox", rid="shared")
        t2 = self.crop.take("inbox2", rid="shared")
        self.assertFalse(t2["replayed"])
        self.assertNotEqual(t1["records"][0]["seq"], t2["records"][0]["seq"])

    def test_take_refused_on_ledger(self):
        """A ledger is append-only: consuming (and thereby making prunable) its
        records through a queue verb would erase the durable-record guarantee."""
        self.crop.append("trades", {"i": 0}, id="t0")
        with self.assertRaises(ValueError):
            self.crop.take("trades")
        self.assertEqual(len(self.crop.read("trades")["records"]), 1)

    def test_limit_clamped_no_unbounded_drain(self):
        """sqlite treats a negative LIMIT as unlimited — take(limit=-1) must not
        atomically drain the whole stream (at-most-once + crash = total loss)."""
        for i in range(5):
            self.crop.append("inbox", {"i": i}, id=f"r{i}")
        self.assertEqual(len(self.crop.take("inbox", limit=-1)["records"]), 1)
        self.assertEqual(len(self.crop.peek("inbox", limit=0)["records"]), 1)
        self.assertEqual(len(self.crop.read("inbox", limit=-5)["records"]), 1)

    def test_prune_ages_out_takes_rows(self):
        """The rid-replay table is an idempotency window, not forever — prune()
        must age it out or a 15-min flock cron accretes rows unboundedly."""
        crop = Crop("takesbird", [Stream("q", "queue", max_age_s=-1)],
                    root=self.tmp.name)
        try:
            for i in range(3):
                crop.append("q", {"i": i}, id=f"r{i}")
            crop.take("q", limit=1, rid="run-1")
            count = lambda: crop._db.execute(
                "SELECT COUNT(*) FROM takes").fetchone()[0]
            self.assertEqual(count(), 1)
            crop.prune()   # max_age_s=-1 -> everything is past the window
            self.assertEqual(count(), 0)
        finally:
            crop.close()

    def test_write_failure_rolls_back_open_txn(self):
        """A failed statement (e.g. a forbidden second opener holding the write
        lock) must not leave the shared connection inside an open transaction."""
        import sqlite3 as s3
        self.crop.append("inbox", {"i": 0}, id="r0")
        blocker = s3.connect(self.crop.path)
        try:
            blocker.execute("BEGIN IMMEDIATE")
            with self.assertRaises(s3.OperationalError):
                self.crop.take("inbox", limit=1)
            self.assertFalse(self.crop._db.in_transaction)  # rolled back, not pinned
        finally:
            blocker.rollback()
            blocker.close()
        t = self.crop.take("inbox", limit=1)   # store fully usable afterwards
        self.assertEqual(len(t["records"]), 1)

    def test_take_restores_synchronous_normal(self):
        """take commits at synchronous=FULL (durable at-most-once) but must leave
        the connection back at NORMAL for the append/read fast path."""
        self.crop.append("inbox", {"i": 0}, id="r0")
        self.crop.take("inbox", limit=1)
        self.assertEqual(
            self.crop._db.execute("PRAGMA synchronous").fetchone()[0], 1)

    # ── streams / classes / retention ─────────────────────────────────────
    def test_unknown_stream_errors(self):
        with self.assertRaises(KeyError):
            self.crop.append("nope", {})
        with self.assertRaises(KeyError):
            self.crop.read("nope")

    def test_class_change_refused(self):
        with self.assertRaises(ValueError):
            self.crop.declare(Stream("trades", "queue"))

    def test_streams_info(self):
        self.crop.append("inbox", {"i": 0}, id="a")
        self.crop.append("inbox", {"i": 1}, id="b")
        self.crop.take("inbox", limit=1)
        info = {s["name"]: s for s in self.crop.streams_info()}
        self.assertEqual(info["inbox"]["class"], "queue")
        self.assertEqual(info["trades"]["class"], "ledger")
        self.assertEqual(info["inbox"]["len"], 2)
        self.assertEqual(info["inbox"]["untaken"], 1)
        self.assertEqual(info["inbox"]["last_seq"], 2)

    def test_prune_keep_last_removes_only_taken(self):
        crop = Crop("prunebird", [Stream("q", "queue", keep_last=2)],
                    root=self.tmp.name)
        try:
            for i in range(5):
                crop.append("q", {"i": i}, id=f"r{i}")
            crop.take("q", limit=3)          # 0,1,2 taken
            removed = crop.prune()           # keep_last=2 -> taken older than 2-newest go
            self.assertEqual(removed, {"q": 3})
            left = crop.read("q")["records"]
            self.assertEqual([r["body"]["i"] for r in left], [3, 4])  # untaken kept
        finally:
            crop.close()

    def test_prune_never_touches_unconfigured_ledger(self):
        for i in range(3):
            self.crop.append("trades", {"i": i}, id=f"t{i}")
        removed = self.crop.prune()
        self.assertEqual(removed, {})
        self.assertEqual(len(self.crop.read("trades")["records"]), 3)

    def test_prune_gaps_keep_cursor_monotonic(self):
        crop = Crop("prunebird2", [Stream("q", "queue", keep_last=1)],
                    root=self.tmp.name)
        try:
            for i in range(3):
                crop.append("q", {"i": i}, id=f"r{i}")
            crop.take("q", limit=3)
            high = crop.last_seq()
            crop.prune()
            r = crop.append("q", {"i": 99}, id="r99")
            self.assertGreater(r["seq"], high)  # seq never reused after prune
            # a cursor parked before the gap still only sees newer records
            page = crop.read("q", since=1)
            self.assertTrue(all(rec["seq"] > 1 for rec in page["records"]))
        finally:
            crop.close()

    def test_store_path_convention(self):
        self.assertTrue(self.crop.path.endswith("testbird/crop.db"))
        self.assertTrue(self.crop.path.startswith(self.tmp.name))


if __name__ == "__main__":
    unittest.main()
