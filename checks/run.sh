#!/bin/sh
# Runs every standalone payments check (no Postgres, no network). Exit 1 on any failure.
set -e
cd "$(dirname "$0")/.."
fail=0
for f in checks/payments_*.exs; do
  if mix run "$f" >/tmp/payments-check.out 2>&1; then
    echo "ok   $f"
  else
    echo "FAIL $f"
    tail -20 /tmp/payments-check.out
    fail=1
  fi
done
exit $fail
