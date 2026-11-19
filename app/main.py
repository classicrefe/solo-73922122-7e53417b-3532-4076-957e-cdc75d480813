"""FastAPI application: payment/sample/coverage intake, training runs, coverage reports."""

from __future__ import annotations

import csv
import io
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

from .coverage import (
    MAX_REPORT_SAMPLES,
    MissingSamplesError,
    NoReceiptsError,
    NoSamplesError,
    CoverageRequestError,
    covered_count,
    create_report,
    report_items,
    report_row,
    report_summary,
)
from .csvio import (
    CsvError,
    parse_coverage_csv,
    parse_revision_csv,
    parse_sample_csv,
)
from .db import create_pool, init_schema
from .imports import (
    ACCEPTANCE_LOCK_KEY,
    ConflictError,
    import_coverage_batch,
    import_revision_batch,
    import_sample_batch,
)
from .runner import RunWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("api")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://app:app@localhost:5432/appdb"
)
RUN_CHUNK_SIZE = int(os.environ.get("RUN_CHUNK_SIZE", "200"))
RUN_CHUNK_DELAY_MS = int(os.environ.get("RUN_CHUNK_DELAY_MS", "0"))
RUNNER_IDLE_SLEEP_MS = int(os.environ.get("RUNNER_IDLE_SLEEP_MS", "500"))
MAX_CSV_BYTES = int(os.environ.get("MAX_CSV_BYTES", str(64 * 1024 * 1024)))


class BatchResult(BaseModel):
    batch_id: int
    rows_total: int
    rows_inserted: int
    rows_reused: int


class RunCreated(BaseModel):
    run_id: int
    status: str
    watermark_seq: int
    total_samples: int
    created_at: datetime


class RunStatus(BaseModel):
    run_id: int
    status: str
    watermark_seq: int
    total_samples: int
    done_samples: int
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error: Optional[str]


class CoverageBatchResult(BatchResult):
    reports_marked_stale: int


class CoverageRange(BaseModel):
    from_utc: datetime
    to_utc: datetime


class CoverageReportItem(BaseModel):
    sample_id: str
    account_id: str
    prediction_at_utc: datetime
    window_from_utc: datetime
    window_to_utc: datetime
    fully_covered: bool
    uncovered_ranges: list[CoverageRange]


class CoverageReportSummary(BaseModel):
    report_id: int
    created_at: datetime
    batch_id: int = Field(
        description="The single submitted receipt batch this report is derived from"
    )
    watermark_seq: int = Field(
        description="Highest sample seq selected; later-imported samples are never "
        "part of this report"
    )
    total_samples: int
    covered_samples: int
    all_covered: bool
    is_stale: bool = Field(
        description="A newer receipt batch inserted data after this report was "
        "created; the stored conclusions remain available but may be outdated"
    )
    stale_at: Optional[datetime]


class CoverageReport(CoverageReportSummary):
    items: list[CoverageReportItem]


class CoverageReportRequest(BaseModel):
    sample_ids: Optional[list[str]] = Field(
        default=None,
        description=(
            f"Up to {MAX_REPORT_SAMPLES} already-imported sample_ids to verify. "
            "Omit to verify the most recently imported samples (still capped "
            f"at {MAX_REPORT_SAMPLES})."
        ),
    )


_COVERAGE_REPORT_BODY = {
    "requestBody": {
        "required": False,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "sample_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": MAX_REPORT_SAMPLES,
                        }
                    },
                },
                "example": {"sample_ids": ["s1", "s2"]},
            },
            "application/x-www-form-urlencoded": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "sample_ids": {
                            "type": "string",
                            "description": "comma- or newline-separated sample ids",
                        }
                    },
                }
            },
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "sample_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "one field per sample id",
                        }
                    },
                }
            },
        },
    }
}


def _utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = create_pool(DATABASE_URL)
    init_schema(pool)
    stop_event = threading.Event()
    worker = RunWorker(
        pool,
        stop_event,
        chunk_size=RUN_CHUNK_SIZE,
        chunk_delay_s=RUN_CHUNK_DELAY_MS / 1000.0,
        idle_sleep_s=RUNNER_IDLE_SLEEP_MS / 1000.0,
    )
    worker.start()
    app.state.pool = pool
    app.state.worker = worker
    log.info("service started (chunk_size=%d)", RUN_CHUNK_SIZE)
    try:
        yield
    finally:
        stop_event.set()
        worker.join(timeout=15)
        pool.close()
        log.info("service stopped")


app = FastAPI(
    title="Point-in-time Payment Feature Builder",
    version="1.0.0",
    description=(
        "Rebuilds the last-24h payment amount per sample exactly as knowable "
        "at prediction time, so late-arriving corrections never leak the "
        "future into training data. Upload payment-revision CSVs and "
        "prediction-sample CSVs, then create a training run and download the "
        "feature dataset once the run is complete. Coverage-receipt CSVs can "
        "be imported and verified against samples so missing payment data is "
        "never mistaken for zero payments."
    ),
    lifespan=lifespan,
)


def get_pool(request: Request) -> ConnectionPool:
    return request.app.state.pool


# ---------------------------------------------------------------- uploads


async def _read_csv_body(request: Request) -> str:
    """Read the CSV payload either as raw body or as multipart file field."""
    ctype = request.headers.get("content-type", "")
    if ctype.startswith("multipart/form-data"):
        form = await request.form()
        upload = form.get("file")
        if upload is None or not hasattr(upload, "read"):
            raise HTTPException(
                status_code=400,
                detail="multipart form must contain a 'file' field with the CSV",
            )
        raw = await upload.read()
    else:
        raw = await request.body()
    if not raw:
        raise HTTPException(status_code=400, detail="empty request body: CSV expected")
    if len(raw) > MAX_CSV_BYTES:
        raise HTTPException(
            status_code=413, detail=f"CSV too large (limit {MAX_CSV_BYTES} bytes)"
        )
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="body must be UTF-8 encoded CSV")


# Documents the upload body in /docs without letting FastAPI consume the
# stream (the endpoint reads it manually to support both raw and multipart).
_UPLOAD_OPENAPI_EXTRA = {
    "requestBody": {
        "required": True,
        "content": {
            "text/csv": {
                "schema": {"type": "string", "format": "binary"},
            },
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "properties": {"file": {"type": "string", "format": "binary"}},
                    "required": ["file"],
                }
            },
        },
    }
}


@app.post(
    "/payment-revision-batches",
    status_code=201,
    response_model=BatchResult,
    summary="Import a payment-revision CSV batch",
    description=(
        "Columns: `payment_id,revision_id,account_id,occurred_at_utc,"
        "visible_at_utc,amount_cents,revoked`. Idempotent: a key "
        "(payment_id, revision_id) re-uploaded with identical content is "
        "reused; the same key with different content rejects the ENTIRE "
        "batch (409, nothing stored). Send the CSV as raw body "
        "(`Content-Type: text/csv`) or as multipart file."
    ),
    openapi_extra=_UPLOAD_OPENAPI_EXTRA,
)
async def upload_revision_batch(request: Request):
    text = await _read_csv_body(request)
    try:
        rows, in_file_dups = parse_revision_csv(text)
    except CsvError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    pool = get_pool(request)
    try:
        with pool.connection() as conn:
            batch_id, inserted, reused = import_revision_batch(conn, rows, in_file_dups)
    except ConflictError as exc:
        return JSONResponse(
            status_code=409,
            content={"detail": str(exc), "conflicts": exc.conflicts[:20]},
        )
    return BatchResult(
        batch_id=batch_id,
        rows_total=len(rows) + in_file_dups,
        rows_inserted=inserted,
        rows_reused=reused,
    )


@app.post(
    "/sample-batches",
    status_code=201,
    response_model=BatchResult,
    summary="Import a prediction-sample CSV batch",
    description=(
        "Columns: `sample_id,account_id,prediction_at_utc`. Idempotent per "
        "sample_id (identical re-upload is reused, conflicting content "
        "rejects the entire batch with 409). Batches accepted while a "
        "training run is in progress belong to the NEXT run."
    ),
    openapi_extra=_UPLOAD_OPENAPI_EXTRA,
)
async def upload_sample_batch(request: Request):
    text = await _read_csv_body(request)
    try:
        rows, in_file_dups = parse_sample_csv(text)
    except CsvError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    pool = get_pool(request)
    try:
        with pool.connection() as conn:
            batch_id, inserted, reused = import_sample_batch(conn, rows, in_file_dups)
    except ConflictError as exc:
        return JSONResponse(
            status_code=409,
            content={"detail": str(exc), "conflicts": exc.conflicts[:20]},
        )
    return BatchResult(
        batch_id=batch_id,
        rows_total=len(rows) + in_file_dups,
        rows_inserted=inserted,
        rows_reused=reused,
    )


# ---------------------------------------------------------------- receipts


@app.post(
    "/coverage-receipt-batches",
    status_code=201,
    response_model=CoverageBatchResult,
    summary="Import a coverage-receipt CSV batch",
    description=(
        "Columns: `receipt_id,revision_id,account_id,covered_from_utc,"
        "covered_to_utc,revoked`. A receipt asserts that an account's payment "
        "data is complete on `[covered_from_utc, covered_to_utc)`. Within one "
        "`receipt_id` the highest `revision_id` wins; corrections may move the "
        "account or interval, and `revoked=true` cancels the receipt. "
        "Idempotent per (receipt_id, revision_id): identical re-upload is "
        "reused, different content rejects the ENTIRE batch (409). If the "
        "batch inserts a new receipt revision, existing coverage reports are "
        "marked stale (their stored conclusions stay readable)."
    ),
    openapi_extra=_UPLOAD_OPENAPI_EXTRA,
)
async def upload_coverage_batch(request: Request):
    text = await _read_csv_body(request)
    try:
        rows, in_file_dups = parse_coverage_csv(text)
    except CsvError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    pool = get_pool(request)
    try:
        with pool.connection() as conn:
            batch_id, inserted, reused, stale = import_coverage_batch(
                conn, rows, in_file_dups
            )
    except ConflictError as exc:
        return JSONResponse(
            status_code=409,
            content={"detail": str(exc), "conflicts": exc.conflicts[:20]},
        )
    return CoverageBatchResult(
        batch_id=batch_id,
        rows_total=len(rows) + in_file_dups,
        rows_inserted=inserted,
        rows_reused=reused,
        reports_marked_stale=stale,
    )


def _split_form_ids(value: str) -> list[str]:
    return [part.strip() for part in value.replace("\n", ",").split(",") if part.strip()]


@app.post(
    "/coverage-reports",
    status_code=201,
    response_model=CoverageReport,
    summary="Verify sample coverage against committed receipts",
    description=(
        "Selects up to 200 already-imported samples (explicit `sample_ids`, or "
        "the most recently imported samples when omitted) and records for each "
        "whether `[prediction_at_utc - 24h, prediction_at_utc)` is fully "
        "covered by that account's receipts, plus every uncovered UTC "
        "sub-interval. A report is derived from exactly one submitted receipt "
        "batch, so a concurrent upload can never be half-included. Creation is "
        "atomic: on failure no partial report is stored."
    ),
    openapi_extra=_COVERAGE_REPORT_BODY,
)
async def create_coverage_report(request: Request):
    sample_ids: Optional[list[str]] = None
    ctype = request.headers.get("content-type", "")
    if ctype.startswith("application/json"):
        raw = await request.body()
        if raw.strip():
            try:
                payload = CoverageReportRequest.model_validate_json(raw)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            sample_ids = payload.sample_ids
    elif ctype.startswith("application/x-www-form-urlencoded"):
        form = await request.form()
        value = form.get("sample_ids")
        if value is not None:
            sample_ids = _split_form_ids(str(value))
    elif ctype.startswith("multipart/form-data"):
        form = await request.form()
        sample_ids = []
        for value in form.getlist("sample_ids"):
            sample_ids.extend(_split_form_ids(str(value)))
        if not sample_ids:
            raise HTTPException(
                status_code=400,
                detail="multipart form must contain 'sample_ids' field(s)",
            )
    elif (await request.body()).strip():
        raise HTTPException(
            status_code=400,
            detail="send JSON {\"sample_ids\": [...]}, a form field, or an empty "
            "body to verify the most recently imported samples",
        )
    if sample_ids is not None and len(sample_ids) > MAX_REPORT_SAMPLES:
        raise HTTPException(
            status_code=422,
            detail=f"at most {MAX_REPORT_SAMPLES} samples may be verified per "
            f"report (got {len(sample_ids)})",
        )
    pool = get_pool(request)
    try:
        with pool.connection() as conn:
            report = create_report(conn, sample_ids)
            items = report_items(conn, report["report_id"])
    except MissingSamplesError as exc:
        return JSONResponse(
            status_code=422,
            content={"detail": str(exc), "missing_sample_ids": exc.missing[:200]},
        )
    except NoReceiptsError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except NoSamplesError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except CoverageRequestError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return _report_payload(report, items)


def _report_payload(report: dict[str, Any], items: list[dict[str, Any]]) -> dict:
    return {
        "report_id": report["report_id"],
        "created_at": report["created_at"],
        "batch_id": report["batch_id"],
        "watermark_seq": report["watermark_seq"],
        "total_samples": report["total_samples"],
        "covered_samples": report.get(
            "covered_samples", sum(1 for item in items if item["fully_covered"])
        ),
        "all_covered": report["all_covered"],
        "is_stale": report.get("is_stale", False),
        "stale_at": report.get("stale_at"),
        "items": items,
    }


@app.get(
    "/coverage-reports",
    response_model=list[CoverageReportSummary],
    summary="List coverage reports (stale ones included)",
)
def list_coverage_reports(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        rows = conn.execute(
            """
            SELECT r.report_id, r.created_at, r.batch_id, r.watermark_seq,
                   r.total_samples, r.all_covered, r.stale_at,
                   (SELECT count(*) FROM coverage_report_items i
                    WHERE i.report_id = r.report_id AND i.fully_covered)
            FROM coverage_reports r ORDER BY r.report_id
            """
        ).fetchall()
    return [report_summary(r[:7], r[7]) for r in rows]


@app.get(
    "/coverage-reports/{report_id}",
    response_model=CoverageReport,
    summary="Get a stored coverage report, including stale ones",
    description=(
        "Stale reports remain readable: `is_stale`/`stale_at` flag that newer "
        "receipts were imported after creation, while the original per-sample "
        "conclusions are returned unchanged."
    ),
)
def get_coverage_report(report_id: int, request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        row = report_row(conn, report_id)
        if not row:
            raise HTTPException(
                status_code=404, detail=f"coverage report {report_id} not found"
            )
        covered = covered_count(conn, report_id)
        items = report_items(conn, report_id)
    return _report_payload(report_summary(row, covered), items)


@app.get(
    "/coverage-reports/{report_id}/report.csv",
    summary="Download a coverage report as CSV",
    description="One row per sample; uncovered intervals in `uncovered_ranges`.",
)
def download_coverage_report(report_id: int, request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        row = report_row(conn, report_id)
        if not row:
            raise HTTPException(
                status_code=404, detail=f"coverage report {report_id} not found"
            )
        items = report_items(conn, report_id)
        summary = report_summary(row, covered_count(conn, report_id))
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(
        [
            "sample_id",
            "account_id",
            "prediction_at_utc",
            "window_from_utc",
            "window_to_utc",
            "fully_covered",
            "uncovered_ranges",
        ]
    )
    for item in items:
        writer.writerow(
            [
                item["sample_id"],
                item["account_id"],
                _utc(item["prediction_at_utc"]),
                _utc(item["window_from_utc"]),
                _utc(item["window_to_utc"]),
                "true" if item["fully_covered"] else "false",
                "; ".join(
                    f"[{_utc(rng['from_utc'])},{_utc(rng['to_utc'])})"
                    for rng in item["uncovered_ranges"]
                ),
            ]
        )
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="coverage_report_{report_id}.csv"',
            "X-Coverage-Report-Stale": "true" if summary["is_stale"] else "false",
        },
    )


@app.get(
    "/coverage-receipt-batches", summary="List imported coverage-receipt batches"
)
def list_coverage_batches(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT batch_id, received_at, rows_total, rows_inserted, rows_reused "
            "FROM coverage_batches ORDER BY batch_id"
        ).fetchall()
    return [
        {
            "batch_id": r[0],
            "received_at": _utc(r[1]),
            "rows_total": r[2],
            "rows_inserted": r[3],
            "rows_reused": r[4],
        }
        for r in rows
    ]


# ---------------------------------------------------------------- runs


@app.post(
    "/training-runs",
    status_code=201,
    response_model=RunCreated,
    summary="Create a training run over all samples accepted so far",
    description=(
        "Freezes the acceptance watermark: exactly the sample batches "
        "committed before this call are included. Only one active "
        "(pending/running) run is allowed; 409 otherwise."
    ),
)
def create_training_run(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (ACCEPTANCE_LOCK_KEY,))
        active = conn.execute(
            "SELECT run_id, status FROM training_runs "
            "WHERE status IN ('pending', 'running') LIMIT 1"
        ).fetchone()
        if active:
            raise HTTPException(
                status_code=409,
                detail=f"training run {active[0]} is still {active[1]}; "
                "wait for it to finish before creating a new one",
            )
        watermark = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM samples"
        ).fetchone()[0]
        total = conn.execute(
            "SELECT count(*) FROM samples WHERE seq <= %s", (watermark,)
        ).fetchone()[0]
        row = conn.execute(
            "INSERT INTO training_runs (watermark_seq, total_samples) "
            "VALUES (%s, %s) RETURNING run_id, status, created_at",
            (watermark, total),
        ).fetchone()
    return RunCreated(
        run_id=row[0],
        status=row[1],
        watermark_seq=watermark,
        total_samples=total,
        created_at=row[2],
    )


def _run_status_row(row) -> RunStatus:
    return RunStatus(
        run_id=row[0],
        status=row[1],
        watermark_seq=row[2],
        total_samples=row[3],
        done_samples=row[4],
        created_at=row[5],
        started_at=row[6],
        finished_at=row[7],
        error=row[8],
    )


_RUN_COLS = (
    "run_id, status, watermark_seq, total_samples, done_samples, "
    "created_at, started_at, finished_at, error"
)


@app.get("/training-runs", response_model=list[RunStatus], summary="List training runs")
def list_training_runs(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        rows = conn.execute(
            f"SELECT {_RUN_COLS} FROM training_runs ORDER BY run_id"
        ).fetchall()
    return [_run_status_row(r) for r in rows]


@app.get(
    "/training-runs/{run_id}",
    response_model=RunStatus,
    summary="Get training-run status and progress",
)
def get_training_run(run_id: int, request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        row = conn.execute(
            f"SELECT {_RUN_COLS} FROM training_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"training run {run_id} not found")
    return _run_status_row(row)


@app.get(
    "/training-runs/{run_id}/dataset.csv",
    summary="Download the feature dataset CSV",
    description=(
        "Available only once the run is completed — every sample's amount "
        "and the payment revisions it used are settled before anything is "
        "served. Returns 409 while the run is pending/running/failed."
    ),
)
def download_dataset(run_id: int, request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT status FROM training_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"training run {run_id} not found")
    if row[0] != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"training run {run_id} is '{row[0]}'; the dataset is "
            "downloadable only after the run completes",
        )

    def generate():
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(
            [
                "sample_id",
                "account_id",
                "prediction_at_utc",
                "amount_last_24h_cents",
                "payment_count",
            ]
        )
        yield buf.getvalue()
        with pool.connection() as conn:
            with conn.cursor(name=f"dataset_{run_id}") as cur:
                cur.itersize = 1000
                cur.execute(
                    "SELECT sample_id, account_id, prediction_at, amount_cents, "
                    "payment_count FROM run_results WHERE run_id = %s "
                    "ORDER BY sample_seq",
                    (run_id,),
                )
                for sample_id, account_id, prediction_at, amount, count in cur:
                    buf.seek(0)
                    buf.truncate(0)
                    writer.writerow(
                        [sample_id, account_id, _utc(prediction_at), amount, count]
                    )
                    yield buf.getvalue()

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="training_run_{run_id}.csv"'
        },
    )


# ---------------------------------------------------------------- introspection


@app.get("/sample-batches", summary="List accepted sample batches")
def list_sample_batches(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT batch_id, accepted_at, rows_total, rows_inserted, rows_reused "
            "FROM sample_batches ORDER BY batch_id"
        ).fetchall()
    return [
        {
            "batch_id": r[0],
            "accepted_at": _utc(r[1]),
            "rows_total": r[2],
            "rows_inserted": r[3],
            "rows_reused": r[4],
        }
        for r in rows
    ]


@app.get("/payment-revision-batches", summary="List imported revision batches")
def list_revision_batches(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT batch_id, received_at, rows_total, rows_inserted, rows_reused "
            "FROM revision_batches ORDER BY batch_id"
        ).fetchall()
    return [
        {
            "batch_id": r[0],
            "received_at": _utc(r[1]),
            "rows_total": r[2],
            "rows_inserted": r[3],
            "rows_reused": r[4],
        }
        for r in rows
    ]


@app.get("/stats", summary="Row counts of the main tables")
def stats(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        out: dict[str, Any] = {}
        for table in (
            "payment_revisions",
            "samples",
            "revision_batches",
            "sample_batches",
            "training_runs",
            "run_results",
            "coverage_receipts",
            "coverage_batches",
            "coverage_reports",
            "coverage_report_items",
        ):
            out[table] = conn.execute(
                f"SELECT count(*) FROM {table}"  # noqa: S608 - fixed names
            ).fetchone()[0]
    return out


@app.get("/health", summary="Liveness/readiness probe")
def health(request: Request):
    pool = get_pool(request)
    with pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/", summary="Service overview")
def root():
    return {
        "service": "point-in-time payment feature builder",
        "docs": "/docs",
        "endpoints": [
            "POST /payment-revision-batches",
            "POST /sample-batches",
            "POST /coverage-receipt-batches",
            "POST /coverage-reports",
            "GET /coverage-reports",
            "GET /coverage-reports/{report_id}",
            "GET /coverage-reports/{report_id}/report.csv",
            "GET /coverage-receipt-batches",
            "POST /training-runs",
            "GET /training-runs",
            "GET /training-runs/{run_id}",
            "GET /training-runs/{run_id}/dataset.csv",
            "GET /sample-batches",
            "GET /payment-revision-batches",
            "GET /stats",
            "GET /health",
        ],
    }
