#!/bin/sh
# Runs every standalone payments check (no Postgres, no network). Exit 1 on any failure.
set -e
cd "$(dirname "$0")/.."
fail=0
for f in checks/payments_*.exs; do
  out=$(mktemp /tmp/payments-check.XXXXXX)
  if mix run "$f" >"$out" 2>&1; then
    echo "ok   $f"
  else
    echo "FAIL $f"
    tail -20 "$out"
    fail=1
  fi
  rm -f "$out"
done
exit $fail
