"""Unit tests for CSV parsing/validation (no server needed)."""

import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.coverage import uncovered_ranges

from app.csvio import (
    CsvError,
    parse_coverage_csv,
    parse_revision_csv,
    parse_sample_csv,
)

REV_HEADER = (
    "payment_id,revision_id,account_id,occurred_at_utc,visible_at_utc,"
    "amount_cents,revoked\n"
)
REV_ROW = "p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,1000,false\n"


class RevisionCsvTest(unittest.TestCase):
    def test_valid_row(self):
        rows, dups = parse_revision_csv(REV_HEADER + REV_ROW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(dups, 0)
        r = rows[0]
        self.assertEqual((r.payment_id, r.revision_id, r.account_id), ("p1", 1, "acct-A"))
        self.assertEqual(r.amount_cents, 1000)
        self.assertFalse(r.revoked)
        self.assertEqual(r.occurred_at.isoformat(), "2026-11-15T11:00:00+00:00")

    def test_offset_timezone_normalized_to_utc(self):
        rows, _ = parse_revision_csv(
            REV_HEADER + "p1,1,a,2026-11-15T19:00:00+08:00,2026-11-15T10:00:00Z,5,false\n"
        )
        self.assertEqual(rows[0].occurred_at.isoformat(), "2026-11-15T11:00:00+00:00")

    def test_identical_duplicate_in_file_collapses(self):
        rows, dups = parse_revision_csv(REV_HEADER + REV_ROW + REV_ROW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(dups, 1)

    def test_conflicting_duplicate_in_file_rejected(self):
        other = REV_ROW.replace(",1000,", ",1001,")
        with self.assertRaises(CsvError):
            parse_revision_csv(REV_HEADER + REV_ROW + other)

    def test_missing_column(self):
        with self.assertRaisesRegex(CsvError, "missing required column"):
            parse_revision_csv("payment_id,revision_id\np1,1\n")

    def test_empty_file(self):
        with self.assertRaisesRegex(CsvError, "header"):
            parse_revision_csv("")

    def test_bad_boolean(self):
        with self.assertRaisesRegex(CsvError, "boolean"):
            parse_revision_csv(REV_HEADER + REV_ROW.replace("false", "maybe"))

    def test_boolean_spellings(self):
        for token, expect in [("TRUE", True), ("0", False), ("yes", True), ("No", False)]:
            rows, _ = parse_revision_csv(REV_HEADER + REV_ROW.replace("false", token))
            self.assertEqual(rows[0].revoked, expect, token)

    def test_bad_integer(self):
        with self.assertRaisesRegex(CsvError, "integer"):
            parse_revision_csv(REV_HEADER + REV_ROW.replace(",1000,", ",10.5,"))

    def test_negative_amount_allowed(self):
        rows, _ = parse_revision_csv(REV_HEADER + REV_ROW.replace(",1000,", ",-250,"))
        self.assertEqual(rows[0].amount_cents, -250)

    def test_naive_timestamp_rejected(self):
        with self.assertRaisesRegex(CsvError, "timezone"):
            parse_revision_csv(
                REV_HEADER + REV_ROW.replace("2026-11-15T11:00:00Z", "2026-11-15 11:00:00")
            )

    def test_bad_revision_id(self):
        with self.assertRaisesRegex(CsvError, "integer"):
            parse_revision_csv(REV_HEADER + REV_ROW.replace("p1,1,", "p1,one,"))

    def test_negative_revision_id_rejected(self):
        with self.assertRaisesRegex(CsvError, ">= 0"):
            parse_revision_csv(REV_HEADER + REV_ROW.replace("p1,1,", "p1,-1,"))


class SampleCsvTest(unittest.TestCase):
    HEADER = "sample_id,account_id,prediction_at_utc\n"
    ROW = "s1,acct-A,2026-11-15T12:00:00Z\n"

    def test_valid(self):
        rows, dups = parse_sample_csv(self.HEADER + self.ROW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].sample_id, "s1")

    def test_conflicting_duplicate_in_file_rejected(self):
        with self.assertRaises(CsvError):
            parse_sample_csv(self.HEADER + self.ROW + "s1,acct-B,2026-11-15T12:00:00Z\n")

    def test_identical_duplicate_collapses(self):
        rows, dups = parse_sample_csv(self.HEADER + self.ROW + self.ROW)
        self.assertEqual((len(rows), dups), (1, 1))

    def test_empty_id_rejected(self):
        with self.assertRaisesRegex(CsvError, "empty"):
            parse_sample_csv(self.HEADER + ",acct-A,2026-11-15T12:00:00Z\n")


class CoverageCsvTest(unittest.TestCase):
    HEADER = (
        "receipt_id,revision_id,account_id,covered_from_utc,covered_to_utc,revoked\n"
    )
    ROW = "r1,1,acct-A,2026-11-14T00:00:00Z,2026-11-16T00:00:00Z,false\n"

    def test_valid(self):
        rows, dups = parse_coverage_csv(self.HEADER + self.ROW)
        self.assertEqual(len(rows), 1)
        self.assertEqual(dups, 0)
        r = rows[0]
        self.assertEqual((r.receipt_id, r.revision_id, r.account_id), ("r1", 1, "acct-A"))
        self.assertFalse(r.revoked)
        self.assertEqual(r.covered_from.isoformat(), "2026-11-14T00:00:00+00:00")

    def test_revoked_and_offset(self):
        rows, _ = parse_coverage_csv(
            self.HEADER
            + "r1,2,acct-A,2026-11-14T08:00:00+08:00,2026-11-16T00:00:00Z,TRUE\n"
        )
        self.assertTrue(rows[0].revoked)
        self.assertEqual(rows[0].covered_from.isoformat(), "2026-11-14T00:00:00+00:00")

    def test_identical_duplicate_collapses(self):
        rows, dups = parse_coverage_csv(self.HEADER + self.ROW + self.ROW)
        self.assertEqual((len(rows), dups), (1, 1))

    def test_conflicting_duplicate_rejected(self):
        with self.assertRaisesRegex(CsvError, "conflicting duplicate"):
            parse_coverage_csv(
                self.HEADER
                + self.ROW
                + self.ROW.replace("acct-A", "acct-B")
            )

    def test_missing_column(self):
        with self.assertRaisesRegex(CsvError, "missing required column"):
            parse_coverage_csv("receipt_id,revision_id\nr1,1\n")

    def test_naive_timestamp_rejected(self):
        with self.assertRaisesRegex(CsvError, "timezone"):
            parse_coverage_csv(
                self.HEADER
                + self.ROW.replace("2026-11-14T00:00:00Z", "2026-11-14 00:00:00")
            )

    def test_empty_interval_rejected(self):
        with self.assertRaisesRegex(CsvError, "strictly before"):
            parse_coverage_csv(
                self.HEADER
                + "r1,1,a,2026-11-16T00:00:00Z,2026-11-16T00:00:00Z,false\n"
            )
        with self.assertRaisesRegex(CsvError, "strictly before"):
            parse_coverage_csv(
                self.HEADER
                + "r1,1,a,2026-11-17T00:00:00Z,2026-11-16T00:00:00Z,false\n"
            )

    def test_negative_revision_rejected(self):
        with self.assertRaisesRegex(CsvError, ">= 0"):
            parse_coverage_csv(self.HEADER + self.ROW.replace("r1,1,", "r1,-1,"))

    def test_empty_receipt_id_rejected(self):
        with self.assertRaisesRegex(CsvError, "empty"):
            parse_coverage_csv(self.HEADER + self.ROW.replace("r1,1,", ",1,"))


class UncoveredRangesTest(unittest.TestCase):
    def _dt(self, s: str):
        return datetime.fromisoformat(s)

    def test_full_cover(self):
        start, end = self._dt("2026-11-14T12:00:00+00:00"), self._dt(
            "2026-11-15T12:00:00+00:00"
        )
        self.assertEqual(uncovered_ranges(start, end, [(start, end)]), [])

    def test_no_receipts_covers_whole_window(self):
        start, end = self._dt("2026-11-14T12:00:00+00:00"), self._dt(
            "2026-11-15T12:00:00+00:00"
        )
        self.assertEqual(uncovered_ranges(start, end, []), [(start, end)])

    def test_gap_in_middle_and_overlapping_receipts(self):
        start, end = self._dt("2026-11-14T12:00:00+00:00"), self._dt(
            "2026-11-15T12:00:00+00:00"
        )
        covered = [
            (self._dt("2026-11-14T00:00:00+00:00"), self._dt("2026-11-14T18:00:00+00:00")),
            (self._dt("2026-11-14T16:00:00+00:00"), self._dt("2026-11-15T06:00:00+00:00")),
        ]
        self.assertEqual(
            uncovered_ranges(start, end, covered),
            [
                (
                    self._dt("2026-11-15T06:00:00+00:00"),
                    self._dt("2026-11-15T12:00:00+00:00"),
                )
            ],
        )

    def test_adjacent_receipts_merge_without_gap(self):
        start, end = self._dt("2026-11-14T12:00:00+00:00"), self._dt(
            "2026-11-15T12:00:00+00:00"
        )
        covered = [
            (self._dt("2026-11-14T12:00:00+00:00"), self._dt("2026-11-15T00:00:00+00:00")),
            (self._dt("2026-11-15T00:00:00+00:00"), self._dt("2026-11-15T12:00:00+00:00")),
        ]
        self.assertEqual(uncovered_ranges(start, end, covered), [])


if __name__ == "__main__":
    unittest.main()
