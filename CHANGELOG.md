# Changelog

## 0.1.0 — unreleased

- Initial release: settlement hub core (stable HD deposit addresses from a
  watch-only xpub, fail-closed idempotent settlement ledger, stamped
  payment_confirmed delivery to allowlisted targets), Method behaviour for
  pluggable modalities, in-tree USDC watcher (multi-chain, reorg-safe,
  chunked getLogs, curl JSON-RPC with keyed-URL protection).

### Hardening (pre-release audit)

- Store seams (`init_bindings`, `store_result`, `store_write`) now
  `catch kind, reason` instead of `rescue`, so an EXIT-shaped store failure
  (GenServer call timeout, dead Ecto pool) degrades/holds instead of
  crashing `init/1` or a settlement tick.
- Settlement dedup (`payment_seen?`) and `payment_status` (`list_payments`)
  now distinguish a NOT-EXPORTED callback (falls back to memory, same as a
  nil store) from an EXPORTED-BUT-ERRORED one (fails closed) — a
  coherence-legal bindings-only store no longer freezes all settlement
  forever.
- The cursor-read seam's fallback default changed from `{:ok, nil}` to
  `{:error, _}`, so a raising/exiting cursor store is skipped that round
  instead of silently rescanning from `start_block` every tick.
- `deliver_one` now only treats a literal `:ok` return from `deliver_fn` as
  delivered — an `{:error, _}` return is queued for retry exactly like a
  raise or an EXIT, instead of being silently counted as delivered.
- Added coverage pinning the cursor fail-closed invariant and the
  untrusted-`tick` trust gate (both mutation-tested; no code change was
  needed for either).
- `Genswarms.Payments.Rpc` unifies URL scrubbing in `call/4` for both the
  success and error runner paths (deleted the fragile config-reparse scrub
  in `run_curl`); `rpc_url` is now required on every chain (raises
  `ArgumentError` at init instead of a runtime `KeyError`); the RPC config
  tempfile now uses a random Base16 suffix opened exclusively
  (`File.open!/2` with `:exclusive`).
- `checks/run.sh` uses a unique `mktemp` output file per check instead of a
  fixed `/tmp` path.
- `payment_status` now refuses instead of failing open: `{"ok": false,
  "error": "degraded_boot"}` during degraded boot, `{"ok": false, "error":
  "store_unavailable"}` when a configured `list_payments/1`
  errors/raises/exits, and a new `durable` field (`true`/`false`) on
  successful replies so a memory-mode empty list is distinguishable from a
  store actually answering.
