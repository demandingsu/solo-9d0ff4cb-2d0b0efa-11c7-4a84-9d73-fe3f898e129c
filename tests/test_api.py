"""End-to-end tests for the exhibition visit-path API.

Covers the required self-test matrix:
  1. happy path with dwell calculation
  2. out-of-order / late open that re-sorts the path and changes dwell data
  3. missing close -> dwell unknown, visit still counted
  4. idempotent retries (same event_id, including different payload)
  5. consent withdrawal deletes the path and blocks re-uploads forever
  6. withdrawal interleaved with concurrent uploads/queries (no half paths)
  7. batch atomicity: one bad event rolls back the whole batch
  8. input validation (naive timestamps rejected, unknown visitor empty path)
  9. orphan close without an open is not shown as a visit
"""
from __future__ import annotations

import sqlite3
import threading

from fastapi.testclient import TestClient

from app import config, crud
from app.db import connect, write_transaction
from app.main import app
from app.models import EventIn


def _event(event_id, visitor, page_view, exhibit, event_type, event_time):
    return {
        "event_id": event_id,
        "visitor_id": visitor,
        "page_view_id": page_view,
        "exhibit_id": exhibit,
        "event_type": event_type,
        "event_time": event_time,
    }


def _post(client, **kwargs):
    return client.post("/events", json=_event(**kwargs))


# ---------------------------------------------------------------------------
# 1. Happy path: paired open/close produces ordered visits with dwell times.
# ---------------------------------------------------------------------------
def test_happy_path_pairs_and_orders_visits(client):
    v = "visitor-A"
    # Exhibit X: 08:00 -> 08:05 (300s); Exhibit Y: 09:00 -> 09:02 (120s).
    _post(client, event_id="e1", visitor=v, page_view="pv1",
          exhibit="X", event_type="open", event_time="2026-11-18T08:00:00Z")
    _post(client, event_id="e2", visitor=v, page_view="pv1",
          exhibit="X", event_type="close", event_time="2026-11-18T08:05:00Z")
    _post(client, event_id="e3", visitor=v, page_view="pv2",
          exhibit="Y", event_type="open", event_time="2026-11-18T09:00:00Z")
    _post(client, event_id="e4", visitor=v, page_view="pv2",
          exhibit="Y", event_type="close", event_time="2026-11-18T09:02:00Z")

    resp = client.get(f"/visitors/{v}/path")
    assert resp.status_code == 200
    path = resp.json()
    assert path["consent_withdrawn"] is False
    assert path["total_visits"] == 2
    first, second = path["visits"]
    assert (first["sequence"], first["exhibit_id"], first["dwell_seconds"],
            first["dwell_known"]) == (1, "X", 300.0, True)
    assert first["closed_at"] == "2026-11-18T08:05:00+00:00"
    assert (second["sequence"], second["exhibit_id"], second["dwell_seconds"],
            second["dwell_known"]) == (2, "Y", 120.0, True)


# ---------------------------------------------------------------------------
# 2. Out-of-order reporting: late events still pair, and a late earlier open
#    re-sorts the path on the next query.
# ---------------------------------------------------------------------------
def test_out_of_order_events_pair_and_resort_path(client):
    v = "visitor-B"
    # Close arrives before its open (network reordering).
    _post(client, event_id="c1", visitor=v, page_view="pv1",
          exhibit="X", event_type="close", event_time="2026-11-18T08:10:00Z")
    _post(client, event_id="o1", visitor=v, page_view="pv1",
          exhibit="X", event_type="open", event_time="2026-11-18T08:00:00Z")
    # A later visit is already fully present.
    _post(client, event_id="o2", visitor=v, page_view="pv2",
          exhibit="Y", event_type="open", event_time="2026-11-18T09:00:00Z")
    _post(client, event_id="c2", visitor=v, page_view="pv2",
          exhibit="Y", event_type="close", event_time="2026-11-18T09:30:00Z")

    path = client.get(f"/visitors/{v}/path").json()
    assert [visit["exhibit_id"] for visit in path["visits"]] == ["X", "Y"]
    assert path["visits"][0]["dwell_seconds"] == 600.0

    # Late backfill: an even earlier open (exhibit Z at 07:00). Its close was
    # already reported two days late. The path order must change.
    _post(client, event_id="c0", visitor=v, page_view="pv0",
          exhibit="Z", event_type="close", event_time="2026-11-18T20:00:00Z")
    _post(client, event_id="o0", visitor=v, page_view="pv0",
          exhibit="Z", event_type="open", event_time="2026-11-18T07:00:00Z")

    path = client.get(f"/visitors/{v}/path").json()
    assert path["total_visits"] == 3
    assert [visit["exhibit_id"] for visit in path["visits"]] == ["Z", "X", "Y"]
    assert [visit["sequence"] for visit in path["visits"]] == [1, 2, 3]
    z = path["visits"][0]
    assert z["dwell_seconds"] == 13 * 3600.0 and z["dwell_known"] is True


# ---------------------------------------------------------------------------
# 3. Missing close: visit counted, but dwell is unknown.
# ---------------------------------------------------------------------------
def test_missing_close_marks_dwell_unknown(client):
    v = "visitor-C"
    _post(client, event_id="o1", visitor=v, page_view="pv1",
          exhibit="X", event_type="open", event_time="2026-11-18T08:00:00Z")
    # Page never closed (tab abandoned). A complete visit exists alongside it.
    _post(client, event_id="o2", visitor=v, page_view="pv2",
          exhibit="Y", event_type="open", event_time="2026-11-18T09:00:00Z")
    _post(client, event_id="c2", visitor=v, page_view="pv2",
          exhibit="Y", event_type="close", event_time="2026-11-18T09:10:00Z")

    path = client.get(f"/visitors/{v}/path").json()
    assert path["total_visits"] == 2  # the unclosed page is still a visit
    x, y = path["visits"]
    assert x["exhibit_id"] == "X"
    assert x["closed_at"] is None and x["dwell_known"] is False
    assert x["dwell_seconds"] is None
    assert y["dwell_seconds"] == 600.0 and y["dwell_known"] is True

    # When the close finally arrives later, dwell becomes known.
    _post(client, event_id="c1", visitor=v, page_view="pv1",
          exhibit="X", event_type="close", event_time="2026-11-18T10:00:00Z")
    path = client.get(f"/visitors/{v}/path").json()
    x = path["visits"][0]
    assert x["closed_at"] == "2026-11-18T10:00:00+00:00"
    assert x["dwell_known"] is True and x["dwell_seconds"] == 7200.0


def test_orphan_close_without_open_is_not_a_visit(client):
    v = "visitor-orphan"
    _post(client, event_id="c1", visitor=v, page_view="pv9",
          exhibit="Q", event_type="close", event_time="2026-11-18T10:00:00Z")
    path = client.get(f"/visitors/{v}/path").json()
    assert path["total_visits"] == 0 and path["visits"] == []


# ---------------------------------------------------------------------------
# 4. Idempotent retries: same event_id never double-counted.
# ---------------------------------------------------------------------------
def test_duplicate_event_ids_are_idempotent(client):
    v = "visitor-D"
    payload = _event("dup1", v, "pv1", "X", "open", "2026-11-18T08:00:00Z")
    first = client.post("/events", json=payload)
    assert first.status_code == 201 and first.json()["status"] == "stored"

    # Exact replay. The HTTP status stays 201 (route-level); the response
    # body's status field distinguishes first store from idempotent replay.
    second = client.post("/events", json=payload)
    assert second.status_code == 201 and second.json()["status"] == "duplicate"

    # Replay with a *different* body under the same event_id: the event_id is
    # the identity, so it is still a duplicate and the original row wins.
    tampered = dict(payload)
    tampered["event_time"] = "2026-11-18T11:11:11Z"
    tampered["exhibit_id"] = "HACK"
    third = client.post("/events", json=tampered)
    assert third.json()["status"] == "duplicate"

    path = client.get(f"/visitors/{v}/path").json()
    assert path["total_visits"] == 1
    visit = path["visits"][0]
    assert visit["exhibit_id"] == "X"
    assert visit["opened_at"] == "2026-11-18T08:00:00+00:00"


def test_batch_endpoint_dedupes_and_counters(client):
    v = "visitor-batch"
    events = [
        _event("b1", v, "pv1", "X", "open", "2026-11-18T08:00:00Z"),
        _event("b2", v, "pv1", "X", "close", "2026-11-18T08:01:00Z"),
        _event("b1", v, "pv1", "X", "open", "2026-11-18T08:00:00Z"),  # dup
    ]
    resp = client.post("/events/batch", json={"events": events})
    assert resp.status_code == 201
    body = resp.json()
    assert (body["accepted"], body["duplicates"],
            body["rejected_withdrawn"]) == (2, 1, 0)


def test_batch_is_atomic_when_one_event_invalid(client):
    v = "visitor-bad-batch"
    good = _event("g1", v, "pv1", "X", "open", "2026-11-18T08:00:00Z")
    bad = _event("g2", v, "pv1", "X", "close", "not-a-timestamp")
    resp = client.post("/events/batch", json={"events": [good, bad]})
    assert resp.status_code == 400
    # The good event must have been rolled back too: no half batch.
    path = client.get(f"/visitors/{v}/path").json()
    assert path["total_visits"] == 0


# ---------------------------------------------------------------------------
# 5. Withdrawal: path disappears, later uploads and replays cannot restore it.
# ---------------------------------------------------------------------------
def test_withdrawal_erases_path_and_blocks_future_events(client):
    v = "visitor-E"
    _post(client, event_id="w1", visitor=v, page_view="pv1",
          exhibit="X", event_type="open", event_time="2026-11-18T08:00:00Z")
    _post(client, event_id="w2", visitor=v, page_view="pv1",
          exhibit="X", event_type="close", event_time="2026-11-18T08:05:00Z")
    assert client.get(f"/visitors/{v}/path").json()["total_visits"] == 1

    resp = client.post(f"/visitors/{v}/withdraw", json={"visitor_id": v})
    assert resp.status_code == 200
    body = resp.json()
    assert body["consent_withdrawn"] is True
    assert body["already_withdrawn"] is False
    assert body["events_deleted"] == 2

    # Existing path is gone.
    path = client.get(f"/visitors/{v}/path").json()
    assert path["consent_withdrawn"] is True
    assert path["visits"] == [] and path["total_visits"] == 0

    # Repeated withdrawal is idempotent.
    again = client.post(f"/visitors/{v}/withdraw", json={"visitor_id": v}).json()
    assert again["already_withdrawn"] is True and again["events_deleted"] == 0

    # New events are rejected and never stored.
    new_event = _event("w3", v, "pv2", "Y", "open", "2026-11-18T09:00:00Z")
    r = client.post("/events", json=new_event)
    assert r.status_code == 201 and r.json()["status"] == "rejected_withdrawn"

    # Replaying an old (pre-withdrawal) event cannot resurrect the path either.
    replay = _event("w1", v, "pv1", "X", "open", "2026-11-18T08:00:00Z")
    r = client.post("/events", json=replay)
    assert r.json()["status"] == "rejected_withdrawn"

    path = client.get(f"/visitors/{v}/path").json()
    assert path["total_visits"] == 0

    # Batch uploads likewise reject this visitor while accepting others.
    mixed = client.post("/events/batch", json={"events": [
        new_event,
        _event("ok1", "visitor-other", "pv1", "Z", "open",
               "2026-11-18T09:00:00Z"),
    ]}).json()
    assert mixed["rejected_withdrawn"] == 1 and mixed["accepted"] == 1


# ---------------------------------------------------------------------------
# 6. Concurrency: withdrawal interleaved with many uploads and queries.
#    Every observed query result must be internally consistent; after
#    withdrawal the path must stay empty no matter how uploads interleave.
# ---------------------------------------------------------------------------
def test_concurrent_withdrawal_never_shows_half_state(client):
    v = "visitor-concurrent"
    # TestClient uses an in-process ASGI transport (no TCP port), so each
    # thread gets its own client backed by the same SQLite file. That is the
    # same shape as multiple uvicorn workers sharing the volume.
    errors: list[str] = []
    barrier = threading.Barrier(4)

    def uploader(start: int) -> None:
        barrier.wait()
        try:
            with TestClient(app) as local:
                for i in range(start, start + 25):
                    pv = f"pv-{i}"
                    local.post("/events", json=_event(
                        f"e-{i}-o", v, pv, f"EX{i}", "open",
                        f"2026-11-18T08:{i % 60:02d}:00Z"))
                    local.post("/events", json=_event(
                        f"e-{i}-c", v, pv, f"EX{i}", "close",
                        f"2026-11-18T08:{i % 60:02d}:30Z"))
        except Exception as exc:  # pragma: no cover - failure reporting
            errors.append(f"uploader: {exc!r}")

    def reader() -> None:
        barrier.wait()
        try:
            with TestClient(app) as local:
                for _ in range(100):
                    body = local.get(f"/visitors/{v}/path").json()
                    # Invariant 1: never withdrawn -> every paired visit is
                    # either fully open+close or explicitly dwell-unknown.
                    # Invariant 2: withdrawn -> always empty.
                    if body["consent_withdrawn"]:
                        if body["visits"] or body["total_visits"] != 0:
                            errors.append("path visible after withdrawal")
                    else:
                        for visit in body["visits"]:
                            if visit["closed_at"] is None:
                                assert visit["dwell_known"] is False
                            else:
                                assert visit["dwell_known"] is True
                        sequences = [x["sequence"] for x in body["visits"]]
                        if sorted(sequences) != list(range(1, len(sequences) + 1)):
                            errors.append("non-contiguous sequence numbers")
        except Exception as exc:  # pragma: no cover
            errors.append(f"reader: {exc!r}")

    def withdrawer() -> None:
        barrier.wait()
        try:
            with TestClient(app) as local:
                local.post(f"/visitors/{v}/withdraw", json={"visitor_id": v})
        except Exception as exc:  # pragma: no cover
            errors.append(f"withdrawer: {exc!r}")

    threads = [
        threading.Thread(target=uploader, args=(0,)),
        threading.Thread(target=uploader, args=(100,)),
        threading.Thread(target=reader),
        threading.Thread(target=withdrawer),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], errors

    # Post-withdrawal steady state: all further attempts rejected, path empty.
    r = client.post("/events", json=_event(
        "late1", v, "pv-late", "LATE", "open", "2026-11-18T12:00:00Z"))
    assert r.json()["status"] == "rejected_withdrawn"
    final = client.get(f"/visitors/{v}/path").json()
    assert final == {
        "visitor_id": v,
        "consent_withdrawn": True,
        "visits": [],
        "total_visits": 0,
    }


# ---------------------------------------------------------------------------
# 7. Validation and storage-level guarantees.
# ---------------------------------------------------------------------------
def test_naive_timestamp_is_rejected(client):
    resp = client.post("/events", json=_event(
        "n1", "visitor-naive", "pv1", "X", "open", "2026-11-18T08:00:00"))
    assert resp.status_code == 400
    assert "UTC" in resp.json()["detail"]


def test_offset_timestamps_normalize_to_utc(client):
    v = "visitor-tz"
    # 09:00+01:00 == 08:00Z; fractional seconds are accepted.
    client.post("/events", json=_event(
        "tz1", v, "pv1", "X", "open", "2026-11-18T09:00:00+01:00"))
    client.post("/events", json=_event(
        "tz2", v, "pv1", "X", "close", "2026-11-18T08:00:30.500Z"))
    visit = client.get(f"/visitors/{v}/path").json()["visits"][0]
    assert visit["opened_at"] == "2026-11-18T08:00:00+00:00"
    assert visit["dwell_seconds"] == 30.5


def test_unknown_visitor_returns_empty_path(client):
    body = client.get("/visitors/nobody/path").json()
    assert body["consent_withdrawn"] is False
    assert body["total_visits"] == 0 and body["visits"] == []


def test_negative_dwell_is_flagged_unknown_but_close_kept(client):
    v = "visitor-anomaly"
    client.post("/events", json=_event(
        "a1", v, "pv1", "X", "open", "2026-11-18T10:00:00Z"))
    client.post("/events", json=_event(
        "a2", v, "pv1", "X", "close", "2026-11-18T09:59:00Z"))
    visit = client.get(f"/visitors/{v}/path").json()["visits"][0]
    assert visit["closed_at"] == "2026-11-18T09:59:00+00:00"
    assert visit["dwell_known"] is False and visit["dwell_seconds"] is None


def test_wal_mode_and_db_level_constraints(client):
    # Direct storage check: WAL journaling is enabled and event_id is the
    # primary key (the basis of idempotent replay).
    conn = connect(config.DB_PATH)
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"
        pk = conn.execute(
            "SELECT name FROM pragma_table_info('events') WHERE pk > 0"
        ).fetchall()
        assert [row[0] for row in pk] == ["event_id"]
    finally:
        conn.close()


def test_withdrawal_marker_survives_across_connections(client):
    # Simulate a replay arriving on a different connection/worker: the
    # withdrawal row is durable committed state, not in-memory.
    v = "visitor-persist"
    client.post("/events", json=_event(
        "p1", v, "pv1", "X", "open", "2026-11-18T08:00:00Z"))
    client.post(f"/visitors/{v}/withdraw", json={"visitor_id": v})

    conn = connect(config.DB_PATH)
    try:
        with write_transaction(conn):
            result = crud.ingest_event(conn, EventIn(**_event(
                "p1", v, "pv1", "X", "open", "2026-11-18T08:00:00Z")))
        assert result.status == "rejected_withdrawn"
        remaining = conn.execute(
            "SELECT COUNT(*) FROM events WHERE visitor_id = ?", (v,)
        ).fetchone()[0]
        assert remaining == 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 8. Scale: many duplicate close rows and orphan closes must not blow up
#    the query or exhaust memory. Only one row per page view is aggregated.
# ---------------------------------------------------------------------------
def _seed_duplicate_closes(db_path, visitor, page_views=200, closes_each=500):
    """Write a large, deliberately messy event history directly to SQLite.

    Every page view gets one open and ``closes_each`` duplicate closes
    (retries / re-renders), plus a few thousand orphan closes for a page
    view that was never opened. This is the shape that used to make the path
    query load every row into Python and die with 500.
    """
    conn = connect(db_path)
    try:
        with write_transaction(conn):
            for p in range(page_views):
                pv = f"pv-{p:04d}"
                base = f"2026-11-18T{8 + p // 60 % 12:02d}:{p % 60:02d}:00+00:00"
                conn.execute(
                    "INSERT INTO events VALUES (?,?,?,?,?,?,?)",
                    (f"{pv}-o", visitor, pv, f"EX{p}", "open", base, base),
                )
                conn.executemany(
                    "INSERT INTO events VALUES (?,?,?,?,?,?,?)",
                    [
                        (f"{pv}-c{i}", visitor, pv, f"EX{p}", "close",
                         f"2026-11-18T{8 + p // 60 % 12:02d}:{p % 60:02d}:30+00:00",
                         base)
                        for i in range(closes_each)
                    ],
                )
            conn.executemany(
                "INSERT INTO events VALUES (?,?,?,?,?,?,?)",
                [
                    (f"orphan-{i}", visitor, "pv-orphan", "ORPH", "close",
                     "2026-11-18T23:00:00+00:00", "2026-11-18T23:00:00+00:00")
                    for i in range(5000)
                ],
            )
    finally:
        conn.close()


def test_path_query_scales_with_visits_not_event_rows(client):
    v = "visitor-huge"
    _seed_duplicate_closes(config.DB_PATH, v)

    path = client.get(f"/visitors/{v}/path").json()
    assert path["total_visits"] == 200
    assert [x["sequence"] for x in path["visits"]] == list(range(1, 201))
    assert path["visits"][0]["page_view_id"] == "pv-0000"
    # Earliest close wins even though it was reported 500 times.
    assert path["visits"][0]["dwell_known"] is True
    assert path["visits"][0]["dwell_seconds"] == 30.0
    # Orphan closes (5000 of them, no open) still produce no visit.
    assert all(x["page_view_id"] != "pv-orphan" for x in path["visits"])


def test_path_query_does_not_fetch_raw_event_rows(client, monkeypatch):
    # Scale guard: the aggregate runs in SQLite, so the per-query application
    # footprint is one row per page view, independent of duplicate reports.
    v = "visitor-traced"
    _seed_duplicate_closes(config.DB_PATH, v, page_views=20, closes_each=50)

    seen: list[tuple[int, str]] = []

    class TracingCursor:
        """Cursor proxy that records how many rows each fetchall returned."""

        def __init__(self, inner, sql):
            self._inner = inner
            self._sql = " ".join(sql.split())

        def fetchall(self):
            rows = self._inner.fetchall()
            seen.append((len(rows), self._sql))
            return rows

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def __iter__(self):
            return iter(self._inner)

    class TracingConnection:
        """Connection proxy so the real query runs unmodified."""

        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def execute(self, sql, params=()):
            return TracingCursor(self._inner.execute(sql, params), sql)

    traced = TracingConnection(connect(config.DB_PATH))
    try:
        path = crud.get_visitor_path(traced, v)
    finally:
        traced._inner.close()

    assert path.total_visits == 20
    # Every SELECT fetched at most one row per page view (20), never the
    # 1000 close rows written above. This is the regression guard for the
    # out-of-memory / 500 failure on large reporters, including the orphan
    # page view whose 5000 closes must not be materialised.
    assert seen, "expected the traced connection to be used"
    assert max(size for size, _ in seen) <= 20, seen
    assert any("GROUP BY" in sql for _, sql in seen), seen


# ---------------------------------------------------------------------------
# 9. Legacy database: written by an older release, queried in place.
# ---------------------------------------------------------------------------
def test_legacy_database_is_queryable_and_upgraded(client, tmp_path):
    v = "visitor-legacy"
    db = tmp_path / "legacy.db"
    # Simulate the pre-fix schema: no covering index, timestamps stored in
    # the older "Z"-suffixed form, and duplicate closes present.
    legacy = sqlite3.connect(db)
    legacy.executescript(
        """
        CREATE TABLE events (
            event_id TEXT NOT NULL PRIMARY KEY,
            visitor_id TEXT NOT NULL,
            page_view_id TEXT NOT NULL,
            exhibit_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_time TEXT NOT NULL,
            received_at TEXT NOT NULL
        );
        CREATE TABLE withdrawals (
            visitor_id TEXT NOT NULL PRIMARY KEY,
            withdrawn_at TEXT NOT NULL
        );
        CREATE INDEX idx_events_visitor_time ON events (visitor_id, event_time);
        CREATE INDEX idx_events_visitor_page ON events (visitor_id, page_view_id);
        """
    )
    legacy.executemany(
        "INSERT INTO events VALUES (?,?,?,?,?,?,?)",
        [
            ("lo1", v, "pv1", "X", "open", "2026-11-18T08:00:00Z", "r"),
            ("lc1", v, "pv1", "X", "close", "2026-11-18T08:05:00Z", "r"),
            ("lc2", v, "pv1", "X", "close", "2026-11-18T08:06:00Z", "r"),
            ("lo2", v, "pv2", "Y", "open", "2026-11-18T09:00:00Z", "r"),
        ],
    )
    legacy.commit()
    legacy.close()

    # init_db() is what the container runs at startup: it must add the
    # missing covering index to the old file without touching rows.
    from app.db import init_db

    init_db(str(db))

    conn = connect(str(db))
    try:
        indexes = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'")}
        # Index added for the new aggregation.
        assert "idx_events_visitor_type_page_time" in indexes
        # Rows untouched.
        assert conn.execute(
            "SELECT COUNT(*) FROM events").fetchone()[0] == 4

        path = crud.get_visitor_path(conn, v)
        assert path.total_visits == 2
        assert path.visits[0].opened_at == "2026-11-18T08:00:00Z"
        assert path.visits[0].closed_at == "2026-11-18T08:05:00Z"
        assert path.visits[0].dwell_seconds == 300.0
        assert path.visits[1].dwell_known is False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 10. Late backfill and withdrawal: the whole path comes from one committed
#     snapshot, and a query after a successful withdrawal is empty.
# ---------------------------------------------------------------------------
def test_late_backfill_and_withdrawal_are_serialized_snapshots(client):
    v = "visitor-interleave"
    for pv, exhibit, opened in (
        ("pv1", "X", "2026-11-18T08:00:00Z"),
        ("pv2", "Y", "2026-11-18T09:00:00Z"),
    ):
        _post(client, event_id=f"{pv}-o", visitor=v, page_view=pv,
              exhibit=exhibit, event_type="open", event_time=opened)

    # A late backfill inserts an earlier visit and a missing close for the
    # first one, atomically as a batch. Before it commits, a query must not
    # see half of it; after it commits, the path is re-sorted by open time.
    batch = {
        "events": [
            _event("pv0-o", v, "pv0", "Z", "open", "2026-11-18T07:00:00Z"),
            _event("pv0-c", v, "pv0", "Z", "close", "2026-11-18T07:10:30Z"),
            _event("pv1-c", v, "pv1", "X", "close", "2026-11-18T08:02:00Z"),
        ]
    }
    assert client.post("/events/batch", json=batch).status_code == 201

    path = client.get(f"/visitors/{v}/path").json()
    assert [x["exhibit_id"] for x in path["visits"]] == ["Z", "X", "Y"]
    assert [x["sequence"] for x in path["visits"]] == [1, 2, 3]
    assert path["visits"][0]["dwell_seconds"] == 630.0
    assert path["visits"][1]["dwell_seconds"] == 120.0
    assert path["visits"][2]["dwell_known"] is False

    # Withdraw and immediately query on a fresh connection: no half-erased
    # path, and the post-withdrawal result is empty.
    w = client.post(f"/visitors/{v}/withdraw", json={"visitor_id": v})
    assert w.status_code == 200 and w.json()["already_withdrawn"] is False
    after = client.get(f"/visitors/{v}/path").json()
    assert after == {
        "visitor_id": v,
        "consent_withdrawn": True,
        "visits": [],
        "total_visits": 0,
    }


def test_reader_snapshot_is_stable_while_writer_commits(client, monkeypatch):
    # Directly exercise the snapshot boundary: a withdrawal committed between
    # the consent check and the aggregation must not leak an empty-but-not-
    # withdrawn or half-erased path into one response.
    v = "visitor-snapshot"
    _post(client, event_id="s1", visitor=v, page_view="pv1",
          exhibit="X", event_type="open", event_time="2026-11-18T08:00:00Z")
    _post(client, event_id="s2", visitor=v, page_view="pv1",
          exhibit="X", event_type="close", event_time="2026-11-18T08:05:00Z")

    reader = connect(config.DB_PATH)
    writer = connect(config.DB_PATH)
    try:
        real_is_withdrawn = crud.is_withdrawn
        injected = {"done": False}

        def is_withdrawn_then_race(conn, visitor_id):
            result = real_is_withdrawn(conn, visitor_id)
            if not injected["done"]:
                injected["done"] = True
                # Commit a withdrawal on another connection *after* this
                # snapshot's first read. The reader's transaction must keep
                # returning the complete pre-withdrawal path.
                with write_transaction(writer):
                    crud.withdraw_consent(writer, visitor_id)
            return result

        monkeypatch.setattr(crud, "is_withdrawn", is_withdrawn_then_race)
        path = crud.get_visitor_path(reader, v)
        monkeypatch.undo()

        assert path.consent_withdrawn is False
        assert path.total_visits == 1
        assert path.visits[0].closed_at == "2026-11-18T08:05:00+00:00"

        # Subsequent queries see the committed withdrawal: empty path.
        after = crud.get_visitor_path(reader, v)
        assert after.consent_withdrawn is True and after.visits == []
    finally:
        reader.close()
        writer.close()


def test_late_open_backfills_orphan_close_into_a_visit(client):
    v = "visitor-late-open"
    # The close is reported first and, until its open arrives, is an orphan
    # that produces no visit.
    _post(client, event_id="lo-c2", visitor=v, page_view="pv2",
          exhibit="Y", event_type="close", event_time="2026-11-18T09:02:00Z")
    path = client.get(f"/visitors/{v}/path").json()
    assert path["total_visits"] == 0

    # A complete visit already exists for a later page view.
    _post(client, event_id="lo-o3", visitor=v, page_view="pv3",
          exhibit="Z", event_type="open", event_time="2026-11-18T10:00:00Z")
    _post(client, event_id="lo-c3", visitor=v, page_view="pv3",
          exhibit="Z", event_type="close", event_time="2026-11-18T10:01:00Z")

    # The missing earlier open is backfilled late: the orphan close becomes a
    # full visit, ordered before the existing one, with its dwell computed.
    _post(client, event_id="lo-o2", visitor=v, page_view="pv2",
          exhibit="Y", event_type="open", event_time="2026-11-18T09:00:00Z")
    path = client.get(f"/visitors/{v}/path").json()
    assert [x["exhibit_id"] for x in path["visits"]] == ["Y", "Z"]
    assert [x["sequence"] for x in path["visits"]] == [1, 2]
    assert path["visits"][0]["dwell_seconds"] == 120.0
    assert path["visits"][0]["dwell_known"] is True
