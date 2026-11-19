#!/usr/bin/env bash
# Example 4 — coverage receipts and sample coverage verification.
#
# Timeline (all UTC, T = 2026-11-15T12:00:00Z, window = [T-24h, T)):
#   * samples s1(acct-A@T), s2(acct-B@T) are imported
#   * receipts: acct-A covered [T-36h, T-3h) and [T-2h, T), leaving
#     [T-3h, T-2h) missing; acct-B fully covered [T-36h, T+12h]
#   * report 1: s1 has gap [T-3h, T-2h) = [09:00, 10:00); s2 is covered
#   * a later revision revokes crc-2 -> report 1 turns STALE while still
#     showing its original conclusion; report 2 sees the bigger gap
#   * re-importing the identical first receipt file inserts 0 rows and
#     creates no further staleness
set -euo pipefail
cd "$(dirname "$0")"
source ./_common.sh
require_app

echo "== 1. import samples =="
post_csv /sample-batches data/samples_1.csv; echo

echo "== 2. import coverage receipts (acct-A has a 1h hole, acct-B full) =="
post_csv /coverage-receipt-batches data/coverage_receipts.csv; echo

echo "== 3. create coverage report for s1, s2 =="
REPORT1=$(curl -sf -X POST "$BASE/coverage-reports" -H 'Content-Type: application/json' \
  -d '{"sample_ids":["s1","s2"]}' | python3 -c "import sys,json;print(json.load(sys.stdin)['report_id'])")
echo "report $REPORT1:"
curl -sf "$BASE/coverage-reports/$REPORT1" | python3 -m json.tool

echo "== 4. late receipt revision revokes crc-2 =="
post_csv /coverage-receipt-batches data/coverage_receipts_late.csv; echo

echo "== 5. report 1 is stale but still readable; create report 2 =="
curl -sf "$BASE/coverage-reports/$REPORT1" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print('stale:', d['is_stale'], '| original all_covered:', d['all_covered'], '| original gap:', d['items'][0]['uncovered_ranges'])"
REPORT2=$(curl -sf -X POST "$BASE/coverage-reports" -H 'Content-Type: application/json' \
  -d '{"sample_ids":["s1","s2"]}' | python3 -c "import sys,json;print(json.load(sys.stdin)['report_id'])")
echo "report $REPORT2:"
curl -sf "$BASE/coverage-reports/$REPORT2/report.csv"

echo "== 6. re-import identical receipts (0 rows inserted, reports unchanged) =="
post_csv /coverage-receipt-batches data/coverage_receipts.csv; echo
curl -sf "$BASE/coverage-reports" | python3 -c \
  "import sys,json; print([(r['report_id'], 'stale' if r['is_stale'] else 'fresh') for r in json.load(sys.stdin)])"

cat <<EOF

Reading the numbers (T = 2026-11-15T12:00:00Z):
  report 1  s1 gap [2026-11-15T09:00:00Z, 2026-11-15T10:00:00Z)  -> not fully covered
            s2 fully covered -> report all_covered=false
  step 4    crc-2#2 revoked -> report 1 marked stale, original items preserved
  report 2  s1 gap now [2026-11-15T09:00:00Z, 2026-11-15T12:00:00Z)
  step 6    identical re-import (rows_reused) does not stale report 2
EOF
