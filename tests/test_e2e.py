"""End-to-end tests against a real PostgreSQL, driving the ASGI app in-process.

Requires a reachable PostgreSQL (see TEST_DATABASE_URL). The database is
recreated empty for the test module. Run with:

    TEST_DATABASE_URL=postgresql://app@127.0.0.1:5544/appdb_test2 \
    TEST_ADMIN_URL=postgresql://app@127.0.0.1:5544/postgres \
        pytest tests/test_e2e.py
"""

from __future__ import annotations

import csv
import io
import os
import time
import unittest

os.environ["DATABASE_URL"] = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://app@127.0.0.1:5544/appdb_test2"
)
os.environ.setdefault("RUN_CHUNK_SIZE", "100")
os.environ.setdefault("RUN_CHUNK_DELAY_MS", "50")
os.environ.setdefault("RUNNER_IDLE_SLEEP_MS", "100")

import psycopg  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

ADMIN_URL = os.environ.get("TEST_ADMIN_URL", "postgresql://app@127.0.0.1:5544/postgres")
DB_NAME = os.environ["DATABASE_URL"].rstrip("/").split("/")[-1]

REV_HEADER = (
    "payment_id,revision_id,account_id,occurred_at_utc,visible_at_utc,"
    "amount_cents,revoked\n"
)
SMP_HEADER = "sample_id,account_id,prediction_at_utc\n"


def make_db() -> None:
    try:
        with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{DB_NAME}"')
    except psycopg.errors.DuplicateDatabase:
        pass
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")


def post_csv(client: TestClient, path: str, text: str):
    return client.post(
        path, content=text.encode(), headers={"Content-Type": "text/csv"}
    )


def wait_run(client: TestClient, run_id: int, timeout: float = 120.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/training-runs/{run_id}")
        assert r.status_code == 200, r.text
        body = r.json()
        if body["status"] in ("completed", "failed"):
            return body
        time.sleep(0.2)
    raise AssertionError(f"run {run_id} did not finish in {timeout}s")


def dataset_rows(client: TestClient, run_id: int) -> list[dict]:
    r = client.get(f"/training-runs/{run_id}/dataset.csv")
    assert r.status_code == 200, r.text
    return list(csv.DictReader(io.StringIO(r.text)))


class E2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        make_db()

    def test_01_feature_semantics_and_boundaries(self):
        """Window edges, visibility edge, revocation, account/time corrections."""
        revs = [
            # occurred exactly T-24h -> inside window (lower bound inclusive)
            "bdy-b1,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T11:00:00Z,100,false",
            # occurred exactly T -> outside window (upper bound exclusive)
            "bdy-b2,1,acct-A,2026-11-15T12:00:00Z,2026-11-15T11:00:00Z,200,false",
            # visible exactly at T -> visible (visibility is inclusive)
            "bdy-b3,1,acct-A,2026-11-15T11:59:59Z,2026-11-15T12:00:00Z,300,false",
            # visible 1s after T -> not visible
            "bdy-b4,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T12:00:01Z,400,false",
            # revoked top revision: payment gone, old revision NOT resurrected
            "bdy-b5,1,acct-A,2026-11-15T10:00:00Z,2026-11-15T09:00:00Z,500,false",
            "bdy-b5,2,acct-A,2026-11-15T10:00:00Z,2026-11-15T11:00:00Z,500,true",
            # correction moves payment to another account
            "bdy-b6,1,acct-A,2026-11-15T10:00:00Z,2026-11-15T09:00:00Z,600,false",
            "bdy-b6,2,acct-B,2026-11-15T10:00:00Z,2026-11-15T11:00:00Z,600,false",
            # correction moves occurred_at INTO the window
            "bdy-b7,1,acct-A,2026-10-16T12:00:00Z,2026-11-15T07:00:00Z,700,false",
            "bdy-b7,2,acct-A,2026-11-15T10:00:00Z,2026-11-15T11:00:00Z,700,false",
            # correction moves occurred_at OUT of the window
            "bdy-b8,1,acct-A,2026-11-15T10:00:00Z,2026-11-15T09:00:00Z,800,false",
            "bdy-b8,2,acct-A,2026-10-16T12:00:00Z,2026-11-15T11:00:00Z,800,false",
        ]
        samples = [
            "bdy-t1,acct-A,2026-11-15T12:00:00Z",
            "bdy-t2,acct-B,2026-11-15T12:00:00Z",
            "bdy-t3,acct-NOBODY,2026-11-15T12:00:00Z",
        ]
        with TestClient(app) as client:
            r = post_csv(client, "/payment-revision-batches", REV_HEADER + "\n".join(revs) + "\n")
            self.assertEqual(r.status_code, 201, r.text)
            self.assertEqual(r.json()["rows_inserted"], len(revs))
            r = post_csv(client, "/sample-batches", SMP_HEADER + "\n".join(samples) + "\n")
            self.assertEqual(r.status_code, 201, r.text)
            run = client.post("/training-runs").json()
            final = wait_run(client, run["run_id"])
            self.assertEqual(final["status"], "completed", final)
            rows = {r["sample_id"]: r for r in dataset_rows(client, run["run_id"])}
        # acct-A: b1(100) + b3(300) + b7(700) = 1100, 3 payments
        self.assertEqual(rows["bdy-t1"]["amount_last_24h_cents"], "1100")
        self.assertEqual(rows["bdy-t1"]["payment_count"], "3")
        # acct-B: only b6 after correction
        self.assertEqual(rows["bdy-t2"]["amount_last_24h_cents"], "600")
        self.assertEqual(rows["bdy-t2"]["payment_count"], "1")
        # unknown account: zero row still present
        self.assertEqual(rows["bdy-t3"]["amount_last_24h_cents"], "0")
        self.assertEqual(rows["bdy-t3"]["payment_count"], "0")

    def test_02_idempotent_reuse_and_conflict(self):
        body = REV_HEADER + (
            "idem-p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,1000,false\n"
            "idem-p2,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,2000,false\n"
        )
        with TestClient(app) as client:
            r1 = post_csv(client, "/payment-revision-batches", body)
            self.assertEqual(r1.status_code, 201, r1.text)
            self.assertEqual((r1.json()["rows_inserted"], r1.json()["rows_reused"]), (2, 0))
            # identical re-upload -> everything reused
            r2 = post_csv(client, "/payment-revision-batches", body)
            self.assertEqual(r2.status_code, 201, r2.text)
            self.assertEqual((r2.json()["rows_inserted"], r2.json()["rows_reused"]), (0, 2))
            # same key, different content -> 409, whole batch rejected
            conflict = REV_HEADER + (
                "idem-p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,9999,false\n"
                "idem-p3,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,5,false\n"
            )
            r3 = post_csv(client, "/payment-revision-batches", conflict)
            self.assertEqual(r3.status_code, 409, r3.text)
            stats = client.get("/stats").json()
            # idem-p3 must not exist: batch rolled back
            self.assertEqual(stats["payment_revisions"], 12 + 2)  # 12 boundary + 2 here
            # malformed CSV -> 400
            bad = SMP_HEADER + "sx,acct-A,not-a-timestamp\n"
            r4 = post_csv(client, "/sample-batches", bad)
            self.assertEqual(r4.status_code, 400, r4.text)

    def test_03_watermark_excludes_concurrent_uploads(self):
        """Batches uploaded while a run is in progress belong to the next run."""
        smp1 = SMP_HEADER + "".join(
            f"wm-a{i:04d},acct-A,2026-11-15T12:00:00Z\n" for i in range(3000)
        )
        smp2 = SMP_HEADER + "".join(
            f"wm-b{i:03d},acct-A,2026-11-15T12:00:00Z\n" for i in range(40)
        )
        with TestClient(app) as client:
            before = client.get("/stats").json()["samples"]
            self.assertEqual(post_csv(client, "/sample-batches", smp1).status_code, 201)
            run = client.post("/training-runs").json()
            self.assertEqual(run["total_samples"], before + 3000)
            run_id = run["run_id"]
            # upload while the run is active -> accepted but excluded
            self.assertEqual(post_csv(client, "/sample-batches", smp2).status_code, 201)
            active = client.post("/training-runs")
            self.assertIn(active.status_code, (409, 201))
            if active.status_code == 201:
                # run already finished (fast machine): the "second" run is a
                # new run and must contain the late batch instead
                second_run_id = active.json()["run_id"]
                wait_run(client, second_run_id)
                rows2 = dataset_rows(client, second_run_id)
                self.assertEqual(sum(r["sample_id"].startswith("wm-b") for r in rows2), 40)
            # dataset gated until completion
            final = wait_run(client, run_id)
            self.assertEqual(final["status"], "completed")
            self.assertEqual(final["total_samples"], before + 3000)
            rows = dataset_rows(client, run_id)
            self.assertEqual(sum(r["sample_id"].startswith("wm-a") for r in rows), 3000)
            self.assertEqual(sum(r["sample_id"].startswith("wm-b") for r in rows), 0)
            # next run picks up the late batch
            run2 = client.post("/training-runs").json()
            if run2.get("status") == "pending" or "run_id" in run2:
                final2 = wait_run(client, run2["run_id"])
                self.assertEqual(final2["status"], "completed")
                rows2 = dataset_rows(client, run2["run_id"])
                self.assertEqual(
                    sum(r["sample_id"].startswith("wm-b") for r in rows2), 40
                )

    def test_04_download_gating(self):
        with TestClient(app) as client:
            smp = SMP_HEADER + "".join(
                f"gate-{i:04d},acct-A,2026-11-15T12:00:00Z\n" for i in range(2000)
            )
            self.assertEqual(post_csv(client, "/sample-batches", smp).status_code, 201)
            run = client.post("/training-runs").json()
            run_id = run["run_id"]
            # while pending/running the dataset is not downloadable
            early = client.get(f"/training-runs/{run_id}/dataset.csv")
            if early.status_code != 200:
                self.assertEqual(early.status_code, 409)
            final = wait_run(client, run_id)
            self.assertEqual(final["status"], "completed")
            rows = dataset_rows(client, run_id)
            self.assertEqual(sum(r["sample_id"].startswith("gate-") for r in rows), 2000)

    def test_05_restart_resume_graceful_and_crash(self):
        """Graceful stop mid-run resumes; stale 'running' state is re-queued."""
        smp = SMP_HEADER + "".join(
            f"rs-{i:05d},acct-A,2026-11-15T12:00:00Z\n" for i in range(5000)
        )
        with TestClient(app) as client:
            self.assertEqual(post_csv(client, "/sample-batches", smp).status_code, 201)
            run = client.post("/training-runs").json()
            run_id, total = run["run_id"], run["total_samples"]
            # wait until partially done
            deadline = time.time() + 60
            done = 0
            while time.time() < deadline:
                body = client.get(f"/training-runs/{run_id}").json()
                done = body["done_samples"]
                if 0 < done < total:
                    break
                if body["status"] == "completed":
                    self.fail("run finished before we could observe partial progress")
                time.sleep(0.1)
            self.assertGreater(done, 0)
            self.assertLess(done, total)
        # exiting the client = graceful shutdown (like `docker compose restart`)
        with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as conn:
            status, persisted = conn.execute(
                "SELECT status, done_samples FROM training_runs WHERE run_id = %s",
                (run_id,),
            ).fetchone()
            self.assertEqual(status, "pending")  # handed back for resume
            self.assertGreaterEqual(persisted, done)
            # simulate a hard crash: run claimed by a dead worker
            conn.execute(
                "UPDATE training_runs SET status = 'running' WHERE run_id = %s", (run_id,)
            )
        with TestClient(app) as client2:
            body = None
            deadline = time.time() + 30
            while time.time() < deadline:
                body = client2.get(f"/training-runs/{run_id}").json()
                if body["status"] in ("running", "completed"):
                    break
                time.sleep(0.2)
            self.assertIn(body["status"], ("running", "completed"))
            self.assertGreaterEqual(body["done_samples"], persisted)  # no regression
            final = wait_run(client2, run_id)
            self.assertEqual(final["status"], "completed")
            self.assertEqual(final["done_samples"], total)
            rows = dataset_rows(client2, run_id)
            self.assertEqual(len(rows), total)
            self.assertEqual(len({r["sample_id"] for r in rows}), total)  # no duplicates
            self.assertEqual(sum(r["sample_id"].startswith("rs-") for r in rows), 5000)


COV_HEADER = (
    "receipt_id,revision_id,account_id,covered_from_utc,covered_to_utc,revoked\n"
)


def post_coverage(client: TestClient, text: str):
    return post_csv(client, "/coverage-receipt-batches", text)


class CoverageE2E(unittest.TestCase):
    """Coverage receipts: revisions, idempotency, report isolation, staleness."""

    @classmethod
    def setUpClass(cls) -> None:
        make_db()

    def _samples(self, rows: list[str]):
        make_db()
        with TestClient(app) as client:
            r = post_csv(
                client,
                "/sample-batches",
                SMP_HEADER + "\n".join(rows) + "\n",
            )
            self.assertEqual(r.status_code, 201, r.text)

    def test_01_coverage_gaps_and_revision_semantics(self):
        make_db()
        # T = 2026-11-15T12:00:00Z, window = [T-24h, T)
        samples = [
            "cov-full,acct-A,2026-11-15T12:00:00Z",
            "cov-gap,acct-A,2026-11-15T12:00:00Z",
            "cov-other,acct-B,2026-11-15T12:00:00Z",
            "cov-revoked,acct-C,2026-11-15T12:00:00Z",
            "cov-moved,acct-D,2026-11-15T12:00:00Z",
        ]
        receipts = [
            # acct-A: two adjacent receipts tile part of the window, leaving
            # [T-6h, T) uncovered once the overlapping r3 is revoked.
            "cov-r1,1,acct-A,2026-11-14T00:00:00Z,2026-11-14T18:00:00Z,false",
            "cov-r2,1,acct-A,2026-11-14T18:00:00Z,2026-11-15T06:00:00Z,false",
            # covers the tail; revision 2 revokes it -> gap reappears
            "cov-r3,1,acct-A,2026-11-15T06:00:00Z,2026-11-15T12:00:00Z,false",
            "cov-r3,2,acct-A,2026-11-15T06:00:00Z,2026-11-15T12:00:00Z,true",
            # acct-B: full cover
            "cov-r4,1,acct-B,2026-11-14T00:00:00Z,2026-11-16T00:00:00Z,false",
            # acct-C: revoked top revision cancels coverage entirely
            "cov-r5,1,acct-C,2026-11-14T00:00:00Z,2026-11-16T00:00:00Z,false",
            "cov-r5,2,acct-C,2026-11-14T00:00:00Z,2026-11-16T00:00:00Z,true",
            # acct-D: correction (higher revision) moves coverage from D to
            # the full-cover receipt's account; D then has no coverage.
            "cov-r6,1,acct-D,2026-11-14T00:00:00Z,2026-11-16T00:00:00Z,false",
            "cov-r6,2,acct-E,2026-11-14T00:00:00Z,2026-11-16T00:00:00Z,false",
        ]
        with TestClient(app) as client:
            r = post_csv(client, "/sample-batches", SMP_HEADER + "\n".join(samples) + "\n")
            self.assertEqual(r.status_code, 201, r.text)
            r = post_coverage(client, COV_HEADER + "\n".join(receipts) + "\n")
            self.assertEqual(r.status_code, 201, r.text)
            self.assertEqual(r.json()["rows_inserted"], len(receipts))

            r = client.post(
                "/coverage-reports",
                json={"sample_ids": [s.split(",")[0] for s in samples]},
            )
            self.assertEqual(r.status_code, 201, r.text)
            report = r.json()

        items = {i["sample_id"]: i for i in report["items"]}
        self.assertEqual(report["total_samples"], 5)
        self.assertFalse(report["all_covered"])
        self.assertEqual(report["covered_samples"], 1)

        # acct-A: covered [T-24h, T-6h); gap [T-6h, T)
        gap = items["cov-gap"]
        self.assertFalse(gap["fully_covered"])
        self.assertEqual(
            [(r["from_utc"], r["to_utc"]) for r in gap["uncovered_ranges"]],
            [("2026-11-15T06:00:00Z", "2026-11-15T12:00:00Z")],
        )
        self.assertEqual(gap["window_from_utc"], "2026-11-14T12:00:00Z")
        self.assertEqual(gap["window_to_utc"], "2026-11-15T12:00:00Z")

        self.assertTrue(items["cov-other"]["fully_covered"])
        self.assertEqual(items["cov-other"]["uncovered_ranges"], [])

        # revoked receipt -> whole window uncovered
        self.assertFalse(items["cov-revoked"]["fully_covered"])
        self.assertEqual(
            items["cov-revoked"]["uncovered_ranges"],
            [
                {
                    "from_utc": "2026-11-14T12:00:00Z",
                    "to_utc": "2026-11-15T12:00:00Z",
                }
            ],
        )
        # correction moved the receipt away from acct-D
        self.assertFalse(items["cov-moved"]["fully_covered"])
        self.assertEqual(len(items["cov-moved"]["uncovered_ranges"]), 1)

        # CSV export carries the same uncovered interval
        with TestClient(app) as client:
            r = client.get(f"/coverage-reports/{report['report_id']}/report.csv")
            self.assertEqual(r.status_code, 200, r.text)
            csv_rows = {row["sample_id"]: row for row in csv.DictReader(
                io.StringIO(r.text)
            )}
        self.assertEqual(csv_rows["cov-gap"]["fully_covered"], "false")
        self.assertEqual(
            csv_rows["cov-gap"]["uncovered_ranges"],
            "[2026-11-15T06:00:00Z,2026-11-15T12:00:00Z)",
        )
        self.assertEqual(csv_rows["cov-other"]["fully_covered"], "true")
        self.assertEqual(csv_rows["cov-other"]["uncovered_ranges"], "")

    def test_02_idempotency_conflict_and_staleness(self):
        make_db()
        with TestClient(app) as client:
            post_csv(
                client,
                "/sample-batches",
                SMP_HEADER + "cov-s1,acct-A,2026-11-15T12:00:00Z\n",
            )
            base = COV_HEADER + (
                "cov-p1,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n"
            )
            r = post_coverage(client, base)
            self.assertEqual(r.status_code, 201, r.text)
            self.assertEqual(r.json()["rows_inserted"], 1)

            # identical re-upload -> reused, and no report exists yet
            r = post_coverage(client, base)
            self.assertEqual(
                (r.json()["rows_inserted"], r.json()["rows_reused"]), (0, 1)
            )
            self.assertEqual(r.json()["reports_marked_stale"], 0)

            r = client.post("/coverage-reports", json={"sample_ids": ["cov-s1"]})
            self.assertEqual(r.status_code, 201, r.text)
            self.assertTrue(r.json()["all_covered"])
            rid = r.json()["report_id"]

            # Export the report via HTTP too (CSV paths go through JSONB
            # round-trips that the JSON endpoint would not catch).
            r = client.get(f"/coverage-reports/{rid}/report.csv")
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.headers["x-coverage-report-stale"], "false")
            self.assertEqual(
                list(csv.DictReader(io.StringIO(r.text)))[0]["fully_covered"], "true"
            )

            # same key, different content -> 409, entire batch rolled back
            conflict = COV_HEADER + (
                "cov-p1,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,true\n"
                "cov-p9,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n"
            )
            r = post_coverage(client, conflict)
            self.assertEqual(r.status_code, 409, r.text)
            # the conflicting batch was rolled back: no new batch bookkeeping
            self.assertEqual(len(client.get("/coverage-receipt-batches").json()), 2)
            # rollback means the report is NOT stale (nothing inserted)
            self.assertFalse(
                client.get(f"/coverage-reports/{rid}").json()["is_stale"]
            )

            # identical re-import of the base batch must not stale the report
            r = post_coverage(client, base)
            self.assertEqual(r.json()["rows_inserted"], 0)
            self.assertFalse(
                client.get(f"/coverage-reports/{rid}").json()["is_stale"]
            )

            # a genuinely new revision -> report becomes stale but stays readable
            newer = COV_HEADER + (
                "cov-p2,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n"
            )
            r = post_coverage(client, newer)
            self.assertEqual(r.json()["rows_inserted"], 1)
            self.assertEqual(r.json()["reports_marked_stale"], 1)
            got = client.get(f"/coverage-reports/{rid}").json()
            self.assertTrue(got["is_stale"])
            self.assertIsNotNone(got["stale_at"])
            self.assertTrue(got["all_covered"])  # original conclusion preserved
            self.assertEqual(got["items"][0]["sample_id"], "cov-s1")
            # listing includes the stale report and flags it
            listed = client.get("/coverage-reports").json()
            self.assertEqual([x["report_id"] for x in listed], [rid])
            self.assertTrue(listed[0]["is_stale"])
            # CSV export still works and flags staleness
            csv_resp = client.get(f"/coverage-reports/{rid}/report.csv")
            self.assertEqual(csv_resp.status_code, 200, csv_resp.text)
            self.assertEqual(csv_resp.headers["x-coverage-report-stale"], "true")

    def test_03_report_uses_one_committed_batch(self):
        """A report pins exactly the latest committed receipt batch."""
        make_db()
        with TestClient(app) as client:
            post_csv(
                client,
                "/sample-batches",
                SMP_HEADER
                + "cov-b1,acct-A,2026-11-15T12:00:00Z\n"
                + "cov-b2,acct-B,2026-11-15T12:00:00Z\n",
            )
            first = post_coverage(
                client,
                COV_HEADER + "cov-q1,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n",
            ).json()
            second = post_coverage(
                client,
                COV_HEADER + "cov-q2,1,acct-B,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n",
            ).json()
            self.assertEqual(first["batch_id"] + 1, second["batch_id"])

            report = client.post(
                "/coverage-reports", json={"sample_ids": ["cov-b1", "cov-b2"]}
            ).json()
            # derived from the latest committed batch (the second one)
            self.assertEqual(report["batch_id"], second["batch_id"])
            items = {i["sample_id"]: i for i in report["items"]}
            # both samples are covered: receipts from every committed batch
            # are considered, while batch_id pins the submission the report
            # was derived from
            self.assertTrue(items["cov-b1"]["fully_covered"])
            self.assertTrue(items["cov-b2"]["fully_covered"])
            self.assertTrue(report["all_covered"])

    def test_04_bad_requests_leave_no_partial_report(self):
        make_db()
        with TestClient(app) as client:
            post_csv(
                client,
                "/sample-batches",
                SMP_HEADER + "".join(
                    f"cov-n{i},acct-A,2026-11-15T12:00:00Z\n" for i in range(5)
                ),
            )
            # no receipts yet -> 409
            r = client.post("/coverage-reports", json={})
            self.assertEqual(r.status_code, 409, r.text)
            post_coverage(
                client,
                COV_HEADER + "cov-z,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n",
            )
            # unknown sample id -> 422, nothing stored
            r = client.post(
                "/coverage-reports",
                json={"sample_ids": ["cov-n0", "does-not-exist"]},
            )
            self.assertEqual(r.status_code, 422, r.text)
            self.assertEqual(r.json()["missing_sample_ids"], ["does-not-exist"])
            # more than 200 samples -> 422 (cap checked before existence)
            r = client.post(
                "/coverage-reports",
                json={"sample_ids": [f"cov-n{i % 5}" for i in range(201)]},
            )
            self.assertEqual(r.status_code, 422, r.text)
            r = client.post(
                "/coverage-reports",
                json={"sample_ids": [f"x{i}" for i in range(201)]},
            )
            self.assertEqual(r.status_code, 422, r.text)
            self.assertIn("at most 200", r.json()["detail"])
            self.assertEqual(client.get("/coverage-reports").json(), [])
            stats = client.get("/stats").json()
            self.assertEqual(stats["coverage_reports"], 0)
            self.assertEqual(stats["coverage_report_items"], 0)

            # form-encoded input (usable from /docs without JSON editing)
            r = client.post(
                "/coverage-reports",
                data={"sample_ids": "cov-n0,cov-n1"},
            )
            self.assertEqual(r.status_code, 201, r.text)
            self.assertEqual(r.json()["total_samples"], 2)
            # no body -> most recent samples (all 5 here)
            r = client.post("/coverage-reports")
            self.assertEqual(r.status_code, 201, r.text)
            self.assertEqual(r.json()["total_samples"], 5)
            self.assertTrue(r.json()["all_covered"])

    def test_05_reports_survive_restart(self):
        make_db()
        with TestClient(app) as client:
            post_csv(
                client,
                "/sample-batches",
                SMP_HEADER + "cov-r,acct-A,2026-11-15T12:00:00Z\n",
            )
            post_coverage(
                client,
                COV_HEADER + "cov-rs,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n",
            )
            rid = client.post(
                "/coverage-reports", json={"sample_ids": ["cov-r"]}
            ).json()["report_id"]
        with TestClient(app) as client2:
            got = client2.get(f"/coverage-reports/{rid}")
            self.assertEqual(got.status_code, 200, got.text)
            self.assertTrue(got.json()["all_covered"])
            post_coverage(
                client2,
                COV_HEADER + "cov-rs2,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n",
            )
            got = client2.get(f"/coverage-reports/{rid}").json()
            self.assertTrue(got["is_stale"])

    def test_06_report_never_sees_uncommitted_batch(self):
        """A report concurrent with an in-flight import uses committed data only."""
        import threading

        make_db()
        with TestClient(app) as client:
            post_csv(
                client,
                "/sample-batches",
                SMP_HEADER + "cov-c,acct-A,2026-11-15T12:00:00Z\n",
            )
            post_coverage(
                client,
                COV_HEADER + "cov-cr,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T06:00:00Z,false\n",
            )
            # Hold an import transaction open with the coverage advisory lock
            # held and rows already inserted but not committed.
            holder = psycopg.connect(os.environ["DATABASE_URL"])
            holder.execute("SELECT pg_advisory_xact_lock(730002)")
            holder.execute(
                "INSERT INTO coverage_batches (rows_total, rows_inserted, rows_reused) "
                "VALUES (1, 1, 0) RETURNING batch_id"
            ).fetchone()
            holder.execute(
                "INSERT INTO coverage_receipts (receipt_id, revision_id, account_id, "
                "covered_from, covered_to, revoked, batch_id) VALUES "
                "('cov-cr', 2, 'acct-A', '2026-11-14T12:00:00Z', "
                "'2026-11-15T12:00:00Z', false, 2)"
            )
            result: dict = {}

            def run_report():
                r = client.post("/coverage-reports", json={"sample_ids": ["cov-c"]})
                result["status"] = r.status_code
                result["body"] = r.json()

            t = threading.Thread(target=run_report)
            t.start()
            # The report must block on the advisory lock while the batch is
            # uncommitted; it cannot observe the half-applied revision.
            t.join(timeout=1.0)
            blocked = t.is_alive()
            holder.commit()
            holder.close()
            t.join(timeout=30)
            self.assertFalse(t.is_alive(), "report creation did not finish")
            self.assertTrue(blocked, "report did not wait for the in-flight batch")
            self.assertEqual(result["status"], 201, result["body"])
            # The report was produced after the batch committed, so it sees
            # the completed receipt: s1 fully covered. A half-applied read
            # would have reported the [T-6h, T) gap.
            self.assertTrue(result["body"]["all_covered"], result["body"])
            self.assertEqual(
                result["body"]["batch_id"], 2, "report pinned the older committed batch"
            )

    def test_07_failed_import_does_not_stale_reports(self):
        """A rejected (rolled-back) conflicted batch never marks reports stale."""
        make_db()
        with TestClient(app) as client:
            post_csv(
                client,
                "/sample-batches",
                SMP_HEADER + "cov-f,acct-A,2026-11-15T12:00:00Z\n",
            )
            base = COV_HEADER + (
                "cov-fr,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n"
            )
            self.assertEqual(post_coverage(client, base).status_code, 201)
            rid = client.post(
                "/coverage-reports", json={"sample_ids": ["cov-f"]}
            ).json()["report_id"]
            conflict = COV_HEADER + (
                "cov-fr,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,true\n"
                "cov-new,1,acct-A,2026-11-14T12:00:00Z,2026-11-15T12:00:00Z,false\n"
            )
            self.assertEqual(post_coverage(client, conflict).status_code, 409)
            got = client.get(f"/coverage-reports/{rid}").json()
            self.assertFalse(got["is_stale"])
            self.assertTrue(got["all_covered"])
            stats = client.get("/stats").json()
            self.assertEqual(stats["coverage_receipts"], 1)
            self.assertEqual(stats["coverage_batches"], 1)


if __name__ == "__main__":
    unittest.main()
