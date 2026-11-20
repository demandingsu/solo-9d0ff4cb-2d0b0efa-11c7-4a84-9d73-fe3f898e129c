"""Domain logic: event ingestion, path reconstruction, consent withdrawal.

All functions take an open :class:`sqlite3.Connection`; transaction
boundaries are controlled by the caller (see :mod:`app.db`) so that an
API request is always one atomic unit of work.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from .db import snapshot_transaction
from .models import (
    EventIn,
    IngestResult,
    Visit,
    VisitPath,
    WithdrawResult,
)


class BadEventTime(ValueError):
    """Raised when event_time is not a timezone-aware ISO-8601 timestamp."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and normalize it to UTC.

    Accepts the shapes that end up stored in the database (``+00:00``) and
    the ones clients send (``Z``, offsets, fractional seconds). The result
    round-trips through :meth:`datetime.isoformat`, so re-parsing a stored
    value — e.g. a very old SQLite row written by an earlier release —
    always succeeds.
    """
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BadEventTime(f"event_time 不是合法 ISO-8601 时间: {value!r}") from exc
    if dt.tzinfo is None:
        raise BadEventTime(f"event_time 必须带时区（UTC），收到无时区时间: {value!r}")
    return dt.astimezone(timezone.utc)


def is_withdrawn(conn: sqlite3.Connection, visitor_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM withdrawals WHERE visitor_id = ? LIMIT 1",
        (visitor_id,),
    ).fetchone()
    return row is not None


def ingest_event(conn: sqlite3.Connection, event: EventIn) -> IngestResult:
    """Store one event. Must be called inside a write transaction.

    Returns ``duplicate`` when ``event_id`` already exists (idempotent
    replay, payload differences are ignored), and ``rejected_withdrawn``
    when the visitor has withdrawn consent — such events are never stored
    and therefore can never resurrect a deleted path.
    """
    if is_withdrawn(conn, event.visitor_id):
        return IngestResult(event_id=event.event_id, status="rejected_withdrawn")

    existing = conn.execute(
        "SELECT 1 FROM events WHERE event_id = ? LIMIT 1",
        (event.event_id,),
    ).fetchone()
    if existing is not None:
        return IngestResult(event_id=event.event_id, status="duplicate")

    # Validated before INSERT so a bad timestamp fails the whole transaction
    # instead of leaving a partial write behind.
    normalized = parse_utc(event.event_time).isoformat()
    conn.execute(
        """
        INSERT INTO events
            (event_id, visitor_id, page_view_id, exhibit_id,
             event_type, event_time, received_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            event.visitor_id,
            event.page_view_id,
            event.exhibit_id,
            event.event_type.value,
            normalized,
            utc_now_iso(),
        ),
    )
    return IngestResult(event_id=event.event_id, status="stored")


# Path aggregation runs inside SQLite with GROUP BY + MIN, so only one row
# per page view crosses into the application (never one row per event).
# A visitor can accumulate tens of thousands of duplicate close rows for a
# single page view; loading those into Python is what used to exhaust
# memory and surface as HTTP 500. SQLite streams the aggregate through the
# covering index idx_events_visitor_type_page_time instead, so application
# memory grows with the number of visits, not with the number of reports.
#
# Earliest open and earliest close per page view are two grouped queries
# (rather than a self-join) because a page view may report several opens
# and closes; the grouped MIN keeps exactly the same "earliest wins"
# pairing semantics as before, including close-before-open anomalies.
_OPEN_ROWS_SQL = """
    SELECT page_view_id,
           MIN(event_time) AS opened_at,
           exhibit_id
    FROM events
    WHERE visitor_id = ? AND event_type = 'open'
    GROUP BY page_view_id
"""

# Closes are restricted to page views that actually have an open before
# aggregating. A visitor can have far more orphan closes (no matching open)
# than real visits, and aggregating those would materialise one row per
# orphan page view even though none can become a visit. The IN subquery is
# evaluated once from the same covering index and is bounded by the visit
# count; SQLite then seeks only the matching page views for the MIN.
_CLOSE_ROWS_SQL = """
    SELECT page_view_id,
           MIN(event_time) AS closed_at
    FROM events
    WHERE visitor_id = ? AND event_type = 'close'
      AND page_view_id IN (
          SELECT page_view_id
          FROM events
          WHERE visitor_id = ? AND event_type = 'open'
      )
    GROUP BY page_view_id
"""


def _pair_visits(opens: list[sqlite3.Row], closes: list[sqlite3.Row]) -> list[Visit]:
    """Combine per-page-view earliest open/close rows into the ordered path.

    ``opens`` and ``closes`` each hold at most one row per page view, so the
    dictionaries built here are bounded by the visitor's visit count.
    """
    closes_by_page = {row["page_view_id"]: row["closed_at"] for row in closes}

    visits: list[Visit] = []
    for open_row in opens:
        page_view_id = open_row["page_view_id"]
        opened_at = open_row["opened_at"]
        # A close without a matching open never reaches this loop, so orphan
        # closes still do not produce a visit.
        closed_at = closes_by_page.get(page_view_id)
        dwell_seconds: float | None = None
        dwell_known = False

        if closed_at is not None:
            open_dt = datetime.fromisoformat(opened_at)
            close_dt = datetime.fromisoformat(closed_at)
            delta = (close_dt - open_dt).total_seconds()
            if delta >= 0:
                dwell_seconds = round(delta, 3)
                dwell_known = True
            # delta < 0: close earlier than its own open — anomalous input,
            # closed_at is returned but stay duration is treated as unknown.

        visits.append(
            Visit(
                page_view_id=page_view_id,
                exhibit_id=open_row["exhibit_id"],
                opened_at=opened_at,
                closed_at=closed_at,
                dwell_seconds=dwell_seconds,
                dwell_known=dwell_known,
                sequence=0,  # assigned after sorting
            )
        )

    # Path order = open time. A late-arriving earlier open therefore simply
    # re-sorts the path on the next query; nothing about the order is stored.
    visits.sort(key=lambda v: (v.opened_at, v.page_view_id))
    for index, visit in enumerate(visits, start=1):
        visit.sequence = index
    return visits


def get_visitor_path(conn: sqlite3.Connection, visitor_id: str) -> VisitPath:
    """Reconstruct a visitor's path from committed events.

    Runs in a single read snapshot transaction so the withdrawal check and
    both aggregation queries observe one consistent committed state: a
    withdrawal committing concurrently can never yield a half-erased path.
    """
    with snapshot_transaction(conn):
        withdrawn = is_withdrawn(conn, visitor_id)
        if withdrawn:
            # Consent withdrawn: the path must disappear and stay empty.
            return VisitPath(
                visitor_id=visitor_id,
                consent_withdrawn=True,
                visits=[],
                total_visits=0,
            )

        opens = conn.execute(_OPEN_ROWS_SQL, (visitor_id,)).fetchall()
        closes = conn.execute(
            _CLOSE_ROWS_SQL, (visitor_id, visitor_id)
        ).fetchall()
        visits = _pair_visits(opens, closes)
        return VisitPath(
            visitor_id=visitor_id,
            consent_withdrawn=False,
            visits=visits,
            total_visits=len(visits),
        )


def withdraw_consent(conn: sqlite3.Connection, visitor_id: str) -> WithdrawResult:
    """Record withdrawal and delete the visitor's events atomically.

    Must be called inside a write transaction. The withdrawal marker
    persists, so any late upload or replayed old event is rejected
    afterwards and the path can never reappear.
    """
    already = is_withdrawn(conn, visitor_id)
    deleted = conn.execute(
        "DELETE FROM events WHERE visitor_id = ?", (visitor_id,)
    ).rowcount
    conn.execute(
        """
        INSERT INTO withdrawals (visitor_id, withdrawn_at)
        VALUES (?, ?)
        ON CONFLICT(visitor_id) DO UPDATE SET withdrawn_at = withdrawn_at
        """,
        (visitor_id, utc_now_iso()),
    )
    return WithdrawResult(
        visitor_id=visitor_id,
        consent_withdrawn=True,
        already_withdrawn=already,
        events_deleted=deleted,
    )
