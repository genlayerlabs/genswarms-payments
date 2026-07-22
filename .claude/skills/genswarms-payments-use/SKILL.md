---
name: genswarms-payments-use
description: >-
  Wire the genswarms-payments object into a swarm: stable HD deposit
  addresses from a watch-only xpub, an idempotent settlement ledger, stamped
  payment_confirmed delivery to allowlisted targets, and the in-tree
  multi-chain USDC watcher. Use when adding payment settlement to a swarm, or
  debugging "address not credited" (unwatched chain / cursor held by a store
  failure / below confirmations / wrong contract), "deposit_address
  refused" (store down — fail closed — or untrusted source, or
  degraded_boot), "tick does nothing" (source not in trusted_sources, or the
  store's cursor read failing), or "everything refused / poll does nothing"
  (degraded_boot from a store outage at init — restart once the store
  recovers). Importer's guide — for internals read the README and checks/.
---

# genswarms-payments — using the package

Settlement hub object: one object owns beneficiary↔address identity, the
settlement ledger, and delivery — any number of payment modalities plug into
it via `Genswarms.Payments.Method`. v1 ships USDC in-tree; future modalities
(Stripe, x402) are sibling packages.

## Wiring

Declare the object (see README for the full config block and every default):

- `xpub` — required. Watch-only BIP32 public key. **Never** put an xprv
  (private key) here or anywhere near this object — it only needs to watch,
  never spend.
- `trusted_sources` — required for anything to work. Fail-closed: empty ⇒
  every `deposit_address` / `payment_status` / `tick` / `ingest_event`
  message is silently ignored.
- `targets` — required for anyone to get credited. Fail-closed: empty ⇒
  settlements still record durably but nobody is ever delivered
  `payment_confirmed`.
- `store_mod` — optional, any subset of `Genswarms.Payments.Store` (8
  optional callbacks) — but two groups must be all-or-nothing or `init/1`
  raises: `{put_address_binding, list_address_bindings}` and
  `{payment_seen?, record_payment, get_last_scanned_block,
  put_last_scanned_block}`. Without a store: memory-only, resets on restart
  — fine in dev, not in prod.
- `chains` — one map per EVM chain to watch (see README for every field);
  only relevant if `methods` includes the USDC watcher (the default).
  `rpc_url` is validated at init — a quote, backslash, or control character
  raises (it rides a curl `--config` tempfile; see below).

A trusted source sends `{"action":"deposit_address","beneficiary":"..."}` to
mint/fetch a stable address, and `{"action":"tick"}` to run one watch round.
**There is no internal scheduler** — `auto_tick`/`poll_interval_ms` are
accepted but inert. Wire a scheduler object (e.g. genswarms-cron) as a
trusted source that delivers `tick` on an interval.

## Durable accounting

Without `store_mod`, bindings and the settlement ledger are in-memory:
addresses and dedup state reset on restart (dev only). For production pass
`store_mod: MyApp.PaymentsStore` implementing any subset of the Store
contract; missing callbacks fall back to memory. Unlike budget reads in
sibling packages, settlement **writes** here fail closed — a store error on
the dedup read or the record write holds that settlement for the next round
rather than risk a double-credit or silent loss. The host owns the
schema/migrations.

## Gotchas

- **"address not credited"** — check, in order: is the chain in `chains` at
  all (unwatched chain never gets scanned); is the store's cursor stuck
  because a *previous* round's settlement was held back by a store failure
  (fail-closed keeps the cursor from advancing — check logs for "FAIL
  CLOSED, holding settlement" / "cursor write failed"); is the deposit still
  below `confirmations` deep (reorg-safe by design — it will settle once the
  chain advances); does the log's contract address actually match
  `usdc_contract` for that chain (a Transfer-shaped log from an unrelated
  contract is rejected by design, even from an otherwise-trusted RPC).
- **"deposit_address refused"** (`{"ok": false, ...}`) — either the store is
  down and *this* allocation fails closed (`"error": "store_unavailable"` —
  never hand out an address whose binding isn't durably persisted), the
  object is in `degraded_boot` (`"error": "degraded_boot"` — see below), or
  the source isn't in `trusted_sources` (in which case there's no reply at
  all, not even a refusal).
- **"tick does nothing"** — the sending source isn't in `trusted_sources`
  (silently ignored, same as any other untrusted message), the object is in
  `degraded_boot` (poll is a no-op — see below), or the store's
  `get_last_scanned_block` read is failing (poll proceeds but each chain's
  scan can't compute its `from`, so nothing new is fetched that round).
- **"everything refused / poll does nothing"** — check `{"action":
  "health"}` for `"degraded_boot": true`. It means the CONFIGURED store's
  `list_address_bindings/0` errored or raised at boot, so `init/1` couldn't
  trust the true watched-address set or next HD index and refused to guess
  — `poll/1` is a no-op and `deposit_address` is refused until the object is
  **restarted** (it does not self-heal on its own; that's deliberate, so a
  transient DB blip at boot doesn't crash-loop the object instead).
- Delivery of `payment_confirmed` is at-least-once for transient per-target
  failures (a raise, an EXIT, a throw) — a failing target is queued and
  retried at the start of every subsequent `tick`, but the queue is
  in-memory only, so a process crash between recording and delivering can
  still drop a delivery (the settlement itself is never re-presented, since
  it's already recorded — dedup by `idempotency_key`). Reconcile via
  `payment_status`, not delivery receipt.
- The RPC URL may embed a provider API key — it rides a chmod-600 tempfile,
  never argv, and is scrubbed from both success and error output. Don't
  "fix" logging or argument-passing around it.
- `namespace` has no meaning inside this package beyond being stamped on
  bindings and deliveries — it's a caller-defined tag (e.g. which budget or
  tenant a beneficiary belongs to).
- Push modalities (`ingest_event`) are a real callback on `Method` but
  nothing wires to it yet — the core action always replies
  `{"ok": false, "error": "no_push_methods"}`. Adding a push method means
  implementing `Method.ingest_event/2` with its OWN signature verification —
  the core trusts whatever settlements a method hands back.

## Verification

`./checks/run.sh` — every `checks/payments_*.exs` (no Postgres, no network;
injected seams: fake store, injected `rpc_fn`/`now_fn`/`deliver_fn`).
