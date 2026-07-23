#!/bin/sh
# Cross-package end-to-end harness: REAL genswarms-payments + REAL
# genswarms-llm-proxy in one BEAM (no Postgres, no network — loopback only).
# The proxy checkout is located via LLM_PROXY_PATH (absolute path recommended;
# a relative one is resolved against your current directory), defaulting to
# the sibling checkout ../genswarms-llm-proxy of the payments repo root.
# Exit 1 on any failure — including a missing proxy checkout: an e2e that
# silently skips is worthless.
set -e
caller_dir=$(pwd)
cd "$(dirname "$0")"

proxy_path="${LLM_PROXY_PATH:-$(pwd)/../../genswarms-llm-proxy}"
case "$proxy_path" in
  /*) : ;;
  *) proxy_path="$caller_dir/$proxy_path" ;;
esac

if [ ! -f "$proxy_path/mix.exs" ]; then
  echo "FAIL e2e: proxy checkout not found — set LLM_PROXY_PATH to a genswarms-llm-proxy checkout (looked at: $proxy_path)"
  exit 1
fi
LLM_PROXY_PATH="$proxy_path"
export LLM_PROXY_PATH

mix deps.get >/dev/null

fail=0
for f in *_e2e_test.exs; do
  out=$(mktemp /tmp/payments-e2e.XXXXXX)
  if mix run "$f" >"$out" 2>&1; then
    echo "ok   e2e/$f"
  else
    echo "FAIL e2e/$f"
    tail -40 "$out"
    fail=1
  fi
  rm -f "$out"
done
exit $fail
