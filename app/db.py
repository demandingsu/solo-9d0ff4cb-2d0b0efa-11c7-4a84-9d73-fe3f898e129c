"""SQLite connection handling and schema management.

Concurrency model
-----------------
SQLite in WAL mode allows multiple readers concurrently with one writer.
Every state-changing request runs inside an explicit ``BEGIN IMMEDIATE``
transaction, which acquires the reserved (write) lock up front. Combined
with the busy timeout, uploads and withdrawals are serialized against one
another and each sees a fully committed state. Reads use their own
``BEGIN DEFERRED`` snapshot transaction, so a query never observes a
half-written path: paths are *derived* from committed event rows at read
time, there is no separate path table that could be updated partially.

Path queries deliberately do **not** load every event row into Python:
the "earliest open / earliest close per page view" aggregation is pushed
into SQLite (see :func:`app.crud.get_visitor_path`), so a visitor with
millions of duplicate close rows costs one row per visit in the app.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT    NOT NULL PRIMARY KEY,
    visitor_id    TEXT    NOT NULL,
    page_view_id  TEXT    NOT NULL,
    exhibit_id    TEXT    NOT NULL,
    event_type    TEXT    NOT NULL CHECK (event_type IN ('open', 'close')),
    event_time    TEXT    NOT NULL,
    received_at   TEXT    NOT NULL
);

-- Hot lookup paths: pairing by page view, path ordering by open time,
-- and per-visitor scans / consent checks.
CREATE INDEX IF NOT EXISTS idx_events_visitor_time
    ON events (visitor_id, event_time);
CREATE INDEX IF NOT EXISTS idx_events_visitor_page
    ON events (visitor_id, page_view_id);

-- Covers the path-query aggregation: for one visitor and one event type,
-- entries are ordered by page view and then time, so both
-- ``GROUP BY page_view_id, MIN(event_time)`` and the per-page-view
-- "earliest open" lookup stream straight from the index. Keeping the
-- aggregation in SQLite is what makes query memory scale with visits
-- (one row per page view) instead of event rows. Existing databases get
-- this index added in place by init_db().
CREATE INDEX IF NOT EXISTS idx_events_visitor_type_page_time
    ON events (visitor_id, event_type, page_view_id, event_time);

CREATE TABLE IF NOT EXISTS withdrawals (
    visitor_id  TEXT    NOT NULL PRIMARY KEY,
    withdrawn_at TEXT   NOT NULL
);
"""

# Schema statements are idempotent (IF NOT EXISTS), so the same list is
# executed at startup for brand-new files and for databases written by an
# older release. init_db() itself never runs DDL inside an explicit
# transaction, so an old file that still lacks the covering index is
# upgraded in place on first boot.
SCHEMA_STATEMENTS: tuple[str, ...] = tuple(
    statement.strip()
    for statement in SCHEMA.split(";")
    if statement.strip()
)


def connect(db_path: str | None = None) -> sqlite3.Connection:
    """Open a connection with the pragmas required for safe concurrency."""
    # isolation_level=None disables sqlite3's implicit transaction
    # management: transactions are started explicitly by app.db (writes)
    # or app.crud (snapshot reads). Sync FastAPI endpoints run in uvicorn's
    # worker threadpool, so the connection created by the dependency may be
    # touched from a pool thread. Each request gets its own connection
    # (never shared), which makes cross-thread use safe; SQLite file
    # locking + BEGIN IMMEDIATE still serialize the actual writers.
    conn = sqlite3.connect(
        db_path or config.DB_PATH,
        timeout=config.BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {config.BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(db_path: str | None = None) -> None:
    """Create tables and indexes if they do not yet exist.

    Works on an existing database from an older release: every statement
    uses ``IF NOT EXISTS``, so only missing objects are added and existing
    rows are untouched. Statements run in autocommit mode (one implicit
    transaction each); building the covering index on a large legacy table
    is a one-time cost at first startup.
    """
    conn = connect(db_path)
    try:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
    finally:
        conn.close()


def get_connection() -> Iterator[sqlite3.Connection]:
    """FastAPI dependency: one connection per request, closed afterwards.

    FastAPI consumes this generator directly: the yielded connection is
    injected into the endpoint, and the ``finally`` block runs when the
    request finishes. Do not wrap it in ``@contextmanager`` — doing so hides
    the generator signature from FastAPI's dependency analysis.
    """
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Acquire the write lock immediately and commit/rollback atomically."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


@contextmanager
def snapshot_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run several reads against one committed snapshot.

    ``BEGIN DEFERRED`` does not take a snapshot by itself: SQLite pins the
    snapshot on the first read statement. Both statements inside this block
    are therefore guaranteed to see the same committed state, even if an
    upload or withdrawal commits in between. Nothing is ever written, so the
    block always rolls back.
    """
    # Transactions never leak between steps on a request connection
    # (write_transaction commits/rolls back, snapshot_transaction rolls
    # back), so a leftover transaction here would be a programming error;
    # let BEGIN fail loudly rather than silently discarding caller data.
    conn.execute("BEGIN DEFERRED")
    try:
        yield conn
    finally:
        conn.rollback()
