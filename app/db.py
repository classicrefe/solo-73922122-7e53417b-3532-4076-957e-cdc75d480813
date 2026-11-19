"""Database schema and connection pool.

Design constraint honored here: the service never holds a long-lived
snapshot transaction and never copies the source revision history for a
training run. All historical reconstruction is done per sample, in short
read transactions, using the temporal predicate ``visible_at <=
prediction_at`` against the live ``payment_revisions`` table.
"""

from __future__ import annotations

from psycopg_pool import ConnectionPool

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS payment_revisions (
    payment_id    text        NOT NULL,
    revision_id   integer     NOT NULL,
    account_id    text        NOT NULL,
    occurred_at   timestamptz NOT NULL,
    visible_at    timestamptz NOT NULL,
    amount_cents  bigint      NOT NULL,
    revoked       boolean     NOT NULL,
    imported_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (payment_id, revision_id)
);
CREATE INDEX IF NOT EXISTS ix_payment_revisions_visible
    ON payment_revisions (visible_at);
CREATE INDEX IF NOT EXISTS ix_payment_revisions_account_occurred
    ON payment_revisions (account_id, occurred_at);

CREATE TABLE IF NOT EXISTS revision_batches (
    batch_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at   timestamptz NOT NULL DEFAULT now(),
    rows_total    integer NOT NULL,
    rows_inserted integer NOT NULL,
    rows_reused   integer NOT NULL
);

CREATE TABLE IF NOT EXISTS sample_batches (
    batch_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    accepted_at   timestamptz NOT NULL DEFAULT now(),
    rows_total    integer NOT NULL,
    rows_inserted integer NOT NULL,
    rows_reused   integer NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
    seq           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id      bigint NOT NULL REFERENCES sample_batches(batch_id),
    sample_id     text NOT NULL UNIQUE,
    account_id    text NOT NULL,
    prediction_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS training_runs (
    run_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at     timestamptz NOT NULL DEFAULT now(),
    status         text NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    watermark_seq  bigint NOT NULL,
    total_samples  integer NOT NULL,
    done_samples   integer NOT NULL DEFAULT 0,
    started_at     timestamptz,
    finished_at    timestamptz,
    error          text
);

CREATE TABLE IF NOT EXISTS run_results (
    run_id         bigint NOT NULL REFERENCES training_runs(run_id),
    sample_seq     bigint NOT NULL REFERENCES samples(seq),
    sample_id      text NOT NULL,
    account_id     text NOT NULL,
    prediction_at  timestamptz NOT NULL,
    amount_cents   bigint NOT NULL,
    payment_count  integer NOT NULL,
    computed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, sample_seq)
);

-- ------------------------------------------------------ coverage receipts
-- A receipt asserts "account X's payment data is complete on [from, to)".
-- Revisions of one receipt supersede each other; a revoked revision
-- cancels the receipt. Each imported batch is a submission; a report is
-- built from exactly one submitted batch (never a mix of concurrent ones).
CREATE TABLE IF NOT EXISTS coverage_receipts (
    receipt_id     text        NOT NULL,
    revision_id    integer     NOT NULL,
    account_id     text        NOT NULL,
    covered_from   timestamptz NOT NULL,
    covered_to     timestamptz NOT NULL,
    revoked        boolean     NOT NULL,
    batch_id       bigint      NOT NULL,
    imported_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (receipt_id, revision_id),
    CHECK (covered_from < covered_to)
);
CREATE INDEX IF NOT EXISTS ix_coverage_receipts_account
    ON coverage_receipts (account_id, covered_from, covered_to);

CREATE TABLE IF NOT EXISTS coverage_batches (
    batch_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at   timestamptz NOT NULL DEFAULT now(),
    rows_total    integer NOT NULL,
    rows_inserted integer NOT NULL,
    rows_reused   integer NOT NULL
);

-- One verification report. batch_id pins the exact submitted receipt
-- batch the report was derived from; watermark_seq caps the samples to
-- those accepted by report creation time. Both are frozen at creation.
CREATE TABLE IF NOT EXISTS coverage_reports (
    report_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at    timestamptz NOT NULL DEFAULT now(),
    batch_id      bigint NOT NULL REFERENCES coverage_batches(batch_id),
    watermark_seq bigint NOT NULL,
    total_samples integer NOT NULL,
    all_covered   boolean NOT NULL,
    stale_at      timestamptz
);

CREATE TABLE IF NOT EXISTS coverage_report_items (
    report_id         bigint NOT NULL REFERENCES coverage_reports(report_id),
    sample_seq        bigint NOT NULL REFERENCES samples(seq),
    sample_id         text NOT NULL,
    account_id        text NOT NULL,
    prediction_at     timestamptz NOT NULL,
    from_utc          timestamptz NOT NULL,
    to_utc            timestamptz NOT NULL,
    fully_covered     boolean NOT NULL,
    uncovered_ranges  jsonb NOT NULL,
    PRIMARY KEY (report_id, sample_seq)
);
"""


def create_pool(database_url: str, max_size: int = 10) -> ConnectionPool:
    pool = ConnectionPool(database_url, min_size=1, max_size=max_size, open=True)
    return pool


def init_schema(pool: ConnectionPool) -> None:
    with pool.connection() as conn:
        conn.execute(SCHEMA_SQL)
