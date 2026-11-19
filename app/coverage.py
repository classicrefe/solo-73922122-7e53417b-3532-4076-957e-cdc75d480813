"""Coverage-receipt verification.

A *coverage receipt* asserts that a channel's payment data for one account
is complete over a UTC interval. Data reviewers import receipts before
building a training set so that "no payments in the last 24h" can be
distinguished from "payment data missing".

Rules:

* Within one ``receipt_id`` the highest ``revision_id`` wins. A correction
  may move the receipt to another account or interval; ``revoked=true``
  cancels the receipt entirely.
* A report selects up to 200 already-imported samples and records, for
  each, whether ``[prediction_at - 24h, prediction_at)`` is fully covered
  by the union of that account's non-revoked receipts, plus every
  uncovered sub-interval (so missing stretches can be traced).
* A report is derived from one consistent set of committed receipts: only
  receipt batches committed before report creation are used. Batch import
  and report creation serialize on an advisory lock, so a concurrent
  upload can never be half-included.
* Any later batch that actually inserts a new receipt revision marks
  earlier reports stale (their original conclusions stay readable).
  Re-importing identical content inserts nothing and does not stale them.
* Report creation is a single transaction: a verification failure leaves
  no partial report behind.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from psycopg import Connection
from psycopg.types.json import Jsonb

from .imports import COVERAGE_LOCK_KEY

MAX_REPORT_SAMPLES = 200
COVERAGE_WINDOW = timedelta(hours=24)


class CoverageRequestError(ValueError):
    """Client-facing problem with a verification request."""


class MissingSamplesError(CoverageRequestError):
    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__(
            f"{len(missing)} sample_id(s) were not found among imported samples: "
            + ", ".join(missing[:10])
        )


class NoReceiptsError(CoverageRequestError):
    pass


class NoSamplesError(CoverageRequestError):
    pass


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _merge(intervals: Iterable[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            if end > merged[-1][1]:
                merged[-1] = (merged[-1][0], end)
        else:
            merged.append((start, end))
    return merged


def uncovered_ranges(
    window_from: datetime,
    window_to: datetime,
    covered: Iterable[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    """Complement of the covered intervals inside [window_from, window_to)."""
    clipped = [
        (max(start, window_from), min(end, window_to))
        for start, end in covered
        if start < window_to and end > window_from
    ]
    gaps: list[tuple[datetime, datetime]] = []
    cursor = window_from
    for start, end in _merge(clipped):
        if start > cursor:
            gaps.append((cursor, start))
        if end > cursor:
            cursor = end
    if cursor < window_to:
        gaps.append((cursor, window_to))
    return gaps


def _latest_receipts(
    conn: Connection, accounts: list[str], batch_id: int
) -> dict[str, list[tuple[datetime, datetime]]]:
    if not accounts:
        return {}
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT DISTINCT ON (receipt_id)
                   receipt_id, account_id, covered_from, covered_to, revoked
            FROM coverage_receipts
            WHERE batch_id <= %(batch_id)s
            ORDER BY receipt_id, revision_id DESC
        )
        SELECT account_id, covered_from, covered_to
        FROM latest
        WHERE NOT revoked
          AND account_id = ANY(%(accounts)s)
        """,
        {"batch_id": batch_id, "accounts": accounts},
    ).fetchall()
    out: dict[str, list[tuple[datetime, datetime]]] = {}
    for account_id, start, end in rows:
        out.setdefault(account_id, []).append((start, end))
    return out


def _select_samples(
    conn: Connection, sample_ids: Optional[list[str]]
) -> list[tuple[int, str, str, datetime]]:
    if sample_ids is not None:
        ids = list(sample_ids)
        if not ids:
            raise CoverageRequestError("sample_ids must contain at least one sample_id")
        if len(ids) > MAX_REPORT_SAMPLES:
            raise CoverageRequestError(
                f"at most {MAX_REPORT_SAMPLES} samples may be verified per report "
                f"(got {len(ids)})"
            )
        if len(set(ids)) != len(ids):
            raise CoverageRequestError("sample_ids contains duplicates")
        rows = conn.execute(
            """
            SELECT seq, sample_id, account_id, prediction_at
            FROM samples WHERE sample_id = ANY(%s) ORDER BY seq
            """,
            (ids,),
        ).fetchall()
        found = {r[1] for r in rows}
        missing = [sid for sid in ids if sid not in found]
        if missing:
            raise MissingSamplesError(missing)
        return [tuple(r) for r in rows]
    rows = conn.execute(
        """
        SELECT seq, sample_id, account_id, prediction_at
        FROM samples ORDER BY seq DESC LIMIT %s
        """,
        (MAX_REPORT_SAMPLES,),
    ).fetchall()
    if not rows:
        raise NoSamplesError("no samples have been imported yet")
    return [tuple(r) for r in reversed(rows)]


def create_report(
    conn: Connection, sample_ids: Optional[list[str]]
) -> dict[str, Any]:
    """Create a coverage report inside the caller's transaction.

    The caller is responsible for commit/rollback; any exception rolls the
    whole report back, so a failed verification leaves nothing behind.
    """
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (COVERAGE_LOCK_KEY,))
    batch_id = conn.execute(
        "SELECT max(batch_id) FROM coverage_batches"
    ).fetchone()[0]
    if batch_id is None:
        raise NoReceiptsError(
            "no coverage receipts have been imported yet; import a "
            "coverage-receipt batch before verifying samples"
        )
    samples = _select_samples(conn, sample_ids)
    accounts = sorted({s[2] for s in samples})
    receipts = _latest_receipts(conn, accounts, batch_id)

    items: list[dict[str, Any]] = []
    for seq, sample_id, account_id, prediction_at in samples:
        window_from = prediction_at - COVERAGE_WINDOW
        window_to = prediction_at
        gaps = uncovered_ranges(
            window_from, window_to, receipts.get(account_id, [])
        )
        items.append(
            {
                "seq": seq,
                "sample_id": sample_id,
                "account_id": account_id,
                "prediction_at": prediction_at,
                "window_from": window_from,
                "window_to": window_to,
                "fully_covered": not gaps,
                "uncovered": gaps,
            }
        )

    all_covered = all(item["fully_covered"] for item in items)
    watermark = max(s[0] for s in samples)
    report_id = conn.execute(
        """
        INSERT INTO coverage_reports
            (batch_id, watermark_seq, total_samples, all_covered)
        VALUES (%s, %s, %s, %s)
        RETURNING report_id
        """,
        (batch_id, watermark, len(items), all_covered),
    ).fetchone()[0]
    for item in items:
        conn.execute(
            """
            INSERT INTO coverage_report_items
                (report_id, sample_seq, sample_id, account_id, prediction_at,
                 from_utc, to_utc, fully_covered, uncovered_ranges)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                report_id,
                item["seq"],
                item["sample_id"],
                item["account_id"],
                item["prediction_at"],
                item["window_from"],
                item["window_to"],
                item["fully_covered"],
                Jsonb(
                    [
                        {"from_utc": _iso(start), "to_utc": _iso(end)}
                        for start, end in item["uncovered"]
                    ]
                ),
            ),
        )
    return {
        "report_id": report_id,
        "batch_id": batch_id,
        "watermark_seq": watermark,
        "total_samples": len(items),
        "covered_samples": sum(1 for item in items if item["fully_covered"]),
        "all_covered": all_covered,
        "created_at": conn.execute(
            "SELECT created_at FROM coverage_reports WHERE report_id = %s",
            (report_id,),
        ).fetchone()[0],
        "stale_at": None,
    }


def report_row(conn: Connection, report_id: int) -> Optional[tuple]:
    return conn.execute(
        """
        SELECT report_id, created_at, batch_id, watermark_seq, total_samples,
               all_covered, stale_at
        FROM coverage_reports WHERE report_id = %s
        """,
        (report_id,),
    ).fetchone()


def report_items(conn: Connection, report_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT sample_id, account_id, prediction_at, from_utc, to_utc,
               fully_covered, uncovered_ranges
        FROM coverage_report_items
        WHERE report_id = %s ORDER BY sample_seq
        """,
        (report_id,),
    ).fetchall()
    return [
        {
            "sample_id": r[0],
            "account_id": r[1],
            "prediction_at_utc": r[2],
            "window_from_utc": r[3],
            "window_to_utc": r[4],
            "fully_covered": r[5],
            "uncovered_ranges": [
                {
                    "from_utc": datetime.fromisoformat(rng["from_utc"]),
                    "to_utc": datetime.fromisoformat(rng["to_utc"]),
                }
                for rng in r[6]
            ],
        }
        for r in rows
    ]


def report_summary(row: tuple, covered_samples: int) -> dict[str, Any]:
    return {
        "report_id": row[0],
        "created_at": row[1],
        "batch_id": row[2],
        "watermark_seq": row[3],
        "total_samples": row[4],
        "covered_samples": covered_samples,
        "all_covered": row[5],
        "is_stale": row[6] is not None,
        "stale_at": row[6],
    }


def covered_count(conn: Connection, report_id: int) -> int:
    return conn.execute(
        "SELECT count(*) FROM coverage_report_items "
        "WHERE report_id = %s AND fully_covered",
        (report_id,),
    ).fetchone()[0]
