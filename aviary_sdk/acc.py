"""crop — the accumulator store (ARCHITECTURE.md §3.1).

One sqlite file (WAL) at ~/.aviary/<bird>/crop.db. A store holds named streams;
a stream is simultaneously a log (cursor reads) and a queue (take marks consumed).

Four verbs, exact semantics:
  append(stream, body, id?)          -> {seq, id, dup}
  read(stream, since=0, limit=100)   -> {records, next}     (server tracks NO consumers)
  peek(stream, limit=10)             -> {records}           (oldest untaken, no claim)
  take(stream, limit=10, rid?)       -> {records, replayed} (atomic, at-most-once;
                                          same rid replays the same batch — durable takes table)

Write-path concurrency rule (§3.1): exactly ONE process — the owning bird — opens
crop.db directly. Everyone else goes through /acc REST or acc_* MCP tools.

Extracted from peacock.py's proven patterns: single connection + RLock, WAL +
synchronous=NORMAL for appends/reads (take COMMITs at synchronous=FULL so a
consumed batch can't un-take across power loss — §3.1 at-most-once), global
monotonic seq (sqlite AUTOINCREMENT — pruned gaps never break `since` cursors),
rid idempotency (peacock's in-memory _RID_CACHE made durable).
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager

_NAME_RE = re.compile(r"[A-Za-z0-9._-]{1,64}")
CLASSES = ("queue", "ledger")
MAX_LIMIT = 1000          # hard cap on any caller-supplied limit (read/peek/take)
TAKES_MAX_AGE_S = 30 * 86400   # rid-replay idempotency window when a stream has no MaxAge


def _clamp_limit(limit, default: int) -> int:
    """Floor caller limits at 1 and cap at MAX_LIMIT. sqlite treats a negative
    LIMIT as unlimited — without this, take(limit=-1) atomically drains an entire
    stream in one call (and a crashed consumer then loses all of it)."""
    try:
        n = int(limit)
    except (TypeError, ValueError):
        n = default
    return max(1, min(n, MAX_LIMIT))


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Stream:
    """A declared stream: name + class (queue|ledger) + optional retention.

    - queue:  take-consumed work items; loss acceptable; snapshot-only backup.
    - ledger: append-only durable record; nightly backup, drilled restores.

    Retention (`keep_last`, `max_age_s`) applies only when set. Pruning removes only
    records that are taken OR past age, oldest first (§3.1). A ledger normally
    declares no retention and is never pruned.
    """

    def __init__(self, name: str, cls: str = "queue",
                 keep_last: int | None = None, max_age_s: float | None = None):
        if not _NAME_RE.fullmatch(name or ""):
            raise ValueError(f"bad stream name: {name!r}")
        if cls not in CLASSES:
            raise ValueError(f"bad stream class: {cls!r} (want queue|ledger)")
        self.name = name
        self.cls = cls
        self.keep_last = keep_last
        self.max_age_s = max_age_s

    @classmethod
    def parse(cls, spec: "Stream | str") -> "Stream":
        """Accept a Stream or a bird.toml-style 'name:class' string."""
        if isinstance(spec, Stream):
            return spec
        name, _, klass = str(spec).partition(":")
        return cls(name, klass or "queue")


class Crop:
    """The one store a bird owns: ~/.aviary/<bird>/crop.db."""

    def __init__(self, bird: str, streams=(), root: str | None = None):
        if not _NAME_RE.fullmatch(bird or ""):
            raise ValueError(f"bad bird name: {bird!r}")
        self.bird = bird
        self.root = root or os.path.expanduser("~/.aviary")
        self.dir = os.path.join(self.root, bird)
        os.makedirs(self.dir, exist_ok=True)
        self.path = os.path.join(self.dir, "crop.db")
        self._lock = threading.RLock()
        # timeout=1.0: a forbidden second opener holding a write txn should stall
        # this connection (and everyone queued on the RLock behind it, /birdz
        # included) for at most ~1s, not sqlite's 5s default.
        self._db = sqlite3.connect(self.path, check_same_thread=False, timeout=1.0)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
              seq    INTEGER PRIMARY KEY AUTOINCREMENT,
              stream TEXT NOT NULL,
              id     TEXT NOT NULL,
              body   TEXT NOT NULL,
              ts     TEXT NOT NULL,
              taken  INTEGER NOT NULL DEFAULT 0,
              UNIQUE (stream, id)
            );
            CREATE INDEX IF NOT EXISTS records_untaken ON records (stream, taken, seq);
            CREATE TABLE IF NOT EXISTS takes (
              stream TEXT NOT NULL,
              rid    TEXT NOT NULL,
              seqs   TEXT NOT NULL,
              ts     TEXT NOT NULL,
              PRIMARY KEY (stream, rid)
            );
            CREATE TABLE IF NOT EXISTS streams (
              name      TEXT PRIMARY KEY,
              class     TEXT NOT NULL,
              keep_last INTEGER,
              max_age_s REAL
            );
            """
        )
        self._db.commit()
        self.streams: dict[str, Stream] = {}
        # Re-adopt streams already declared in the db (previous runs).
        with self._lock:
            for name, klass, keep_last, max_age_s in self._db.execute(
                    "SELECT name, class, keep_last, max_age_s FROM streams"):
                self.streams[name] = Stream(name, klass, keep_last, max_age_s)
        for spec in streams:
            self.declare(Stream.parse(spec))

    # ── write transaction discipline ─────────────────────────────────────
    @contextmanager
    def _tx(self, durable: bool = False):
        """One serialized write txn: COMMIT on success, ROLLBACK on any failure —
        an error mid-verb (e.g. 'database is locked' from a forbidden second
        opener) must never leave the shared connection inside an open deferred
        transaction pinning the WAL snapshot. `durable=True` commits with
        synchronous=FULL: a take that returned a batch must not un-take across
        OS crash/power loss (§3.1 at-most-once)."""
        with self._lock:
            try:
                if durable:
                    self._db.execute("PRAGMA synchronous=FULL")
                yield self._db
                self._db.commit()
            except BaseException:
                try:
                    self._db.rollback()
                except sqlite3.Error:
                    pass
                raise
            finally:
                if durable:
                    try:
                        self._db.execute("PRAGMA synchronous=NORMAL")
                    except sqlite3.Error:
                        pass

    # ── declaration ──────────────────────────────────────────────────────
    def declare(self, stream: Stream) -> Stream:
        """Declare (or re-declare) a stream. Class changes are refused."""
        with self._tx() as db:
            prior = self.streams.get(stream.name)
            if prior is not None and prior.cls != stream.cls:
                raise ValueError(
                    f"stream {stream.name!r} is class {prior.cls!r}; refusing {stream.cls!r}")
            db.execute(
                "INSERT INTO streams (name, class, keep_last, max_age_s) VALUES (?,?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET keep_last=excluded.keep_last, "
                "max_age_s=excluded.max_age_s",
                (stream.name, stream.cls, stream.keep_last, stream.max_age_s))
            self.streams[stream.name] = stream
            return stream

    def _stream(self, name: str) -> Stream:
        s = self.streams.get(name)
        if s is None:
            raise KeyError(f"unknown stream: {name!r}")
        return s

    # ── the four verbs ───────────────────────────────────────────────────
    def append(self, stream: str, body, id: str | None = None) -> dict:
        """Append-only. Existing id -> ORIGINAL record's seq with dup:true (body NOT
        replaced; new data means a new id). No id -> random UUID, never dups."""
        self._stream(stream)
        rid = str(id) if id is not None and str(id) != "" else uuid.uuid4().hex
        payload = json.dumps(body, default=str)
        with self._tx() as db:
            cur = db.execute(
                "INSERT INTO records (stream, id, body, ts, taken) VALUES (?,?,?,?,0) "
                "ON CONFLICT(stream, id) DO NOTHING",
                (stream, rid, payload, _now_iso()))
            if cur.rowcount == 0:  # dup — return the original, untouched
                row = db.execute(
                    "SELECT seq FROM records WHERE stream=? AND id=?",
                    (stream, rid)).fetchone()
                return {"seq": row[0], "id": rid, "dup": True}
            seq = cur.lastrowid
        return {"seq": seq, "id": rid, "dup": False}

    def read(self, stream: str, since: int = 0, limit: int = 100) -> dict:
        """Non-destructive log read: seq > since, oldest first, taken flag included.
        The server tracks no consumers — each caller keeps its own cursor (next)."""
        self._stream(stream)
        with self._lock:
            rows = self._db.execute(
                "SELECT seq, stream, id, body, ts, taken FROM records "
                "WHERE stream=? AND seq>? ORDER BY seq LIMIT ?",
                (stream, int(since), _clamp_limit(limit, 100))).fetchall()
        records = [self._record(r) for r in rows]
        return {"records": records,
                "next": records[-1]["seq"] if records else int(since)}

    def peek(self, stream: str, limit: int = 10) -> dict:
        """Oldest untaken, no claim. 'Is there work?'"""
        self._stream(stream)
        with self._lock:
            rows = self._db.execute(
                "SELECT seq, stream, id, body, ts, taken FROM records "
                "WHERE stream=? AND taken=0 ORDER BY seq LIMIT ?",
                (stream, _clamp_limit(limit, 10))).fetchall()
        return {"records": [self._record(r) for r in rows]}

    def take(self, stream: str, limit: int = 10, rid: str | None = None) -> dict:
        """Atomically mark oldest untaken as taken and return them. At-most-once.
        Same rid replays the same batch (durable takes table), so a retried request
        is safe. No leases/acks in v1 (§6.4). Refused on ledger streams: a ledger
        is append-only and must not be consumable (or prunable) via a queue verb."""
        s = self._stream(stream)
        if s.cls == "ledger":
            raise ValueError(
                f"take refused: stream {stream!r} is a ledger (append-only) — use read")
        with self._tx(durable=True) as db:
            if rid is not None:
                row = db.execute(
                    "SELECT seqs FROM takes WHERE stream=? AND rid=?",
                    (stream, str(rid))).fetchone()
                if row is not None:  # replay: same batch, whatever of it survives
                    seqs = json.loads(row[0])
                    recs = self._records_by_seq(stream, seqs)
                    return {"records": recs, "replayed": True}
            rows = db.execute(
                "UPDATE records SET taken=1 WHERE seq IN ("
                "  SELECT seq FROM records WHERE stream=? AND taken=0 "
                "  ORDER BY seq LIMIT ?) "
                "RETURNING seq, stream, id, body, ts, taken",
                (stream, _clamp_limit(limit, 10))).fetchall()
            rows.sort(key=lambda r: r[0])
            if rid is not None:
                db.execute(
                    "INSERT INTO takes (stream, rid, seqs, ts) VALUES (?,?,?,?)",
                    (stream, str(rid), json.dumps([r[0] for r in rows]), _now_iso()))
        return {"records": [self._record(r) for r in rows], "replayed": False}

    # ── introspection / maintenance ──────────────────────────────────────
    def streams_info(self) -> list[dict]:
        """Per-stream {name, class, len, untaken, last_seq} — the GET /acc payload."""
        out = []
        with self._lock:
            for name in sorted(self.streams):
                s = self.streams[name]
                n, untaken, last = self._db.execute(
                    "SELECT COUNT(*), COALESCE(SUM(taken=0),0), COALESCE(MAX(seq),0) "
                    "FROM records WHERE stream=?", (name,)).fetchone()
                out.append({"name": name, "class": s.cls, "len": n,
                            "untaken": untaken, "last_seq": last})
        return out

    def last_seq(self) -> int:
        """Store-global high-water seq (survives pruning: sqlite AUTOINCREMENT)."""
        with self._lock:
            row = self._db.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='records'").fetchone()
        return row[0] if row else 0

    def prune(self) -> dict:
        """Apply per-stream retention. Removes only records that are taken OR past
        age, oldest first; never touches streams with no retention configured.
        Pruned gaps never break `since` cursors (AUTOINCREMENT never reuses seq).
        Also ages out `takes` rid-replay rows (stream MaxAge when set, else
        TAKES_MAX_AGE_S) — the idempotency window is finite, so the takes table
        must not grow without bound (a 15-min flock cron would accrete forever)."""
        removed: dict[str, int] = {}
        with self._tx() as db:
            for name, s in self.streams.items():
                n = 0
                if s.max_age_s is not None:
                    cutoff = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                           time.gmtime(time.time() - s.max_age_s))
                    n += db.execute(
                        "DELETE FROM records WHERE stream=? AND ts<?",
                        (name, cutoff)).rowcount
                if s.keep_last is not None:
                    row = db.execute(
                        "SELECT seq FROM records WHERE stream=? "
                        "ORDER BY seq DESC LIMIT 1 OFFSET ?",
                        (name, max(int(s.keep_last) - 1, 0))).fetchone()
                    if row is not None:  # nth-newest exists; drop TAKEN older than it
                        n += db.execute(
                            "DELETE FROM records WHERE stream=? AND taken=1 AND seq<?",
                            (name, row[0])).rowcount
                window = s.max_age_s if s.max_age_s is not None else TAKES_MAX_AGE_S
                tcut = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                     time.gmtime(time.time() - window))
                db.execute("DELETE FROM takes WHERE stream=? AND ts<?", (name, tcut))
                if n:
                    removed[name] = n
        return removed

    def close(self):
        with self._lock:
            self._db.close()

    # ── internals ────────────────────────────────────────────────────────
    @staticmethod
    def _record(row) -> dict:
        seq, stream, rid, body, ts, taken = row
        return {"seq": seq, "stream": stream, "id": rid,
                "body": json.loads(body), "ts": ts, "taken": bool(taken)}

    def _records_by_seq(self, stream: str, seqs: list) -> list[dict]:
        if not seqs:
            return []
        marks = ",".join("?" * len(seqs))
        rows = self._db.execute(
            f"SELECT seq, stream, id, body, ts, taken FROM records "
            f"WHERE stream=? AND seq IN ({marks}) ORDER BY seq",
            (stream, *seqs)).fetchall()
        return [self._record(r) for r in rows]
