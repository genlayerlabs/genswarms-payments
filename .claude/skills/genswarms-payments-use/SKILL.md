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

## Authorization lane (entry A: EIP-3009 → treasury)

A sibling package (e.g. `genswarms-wallet-bridge`) can have users sign
EIP-3009 authorizations whose USDC lands directly in a shared TREASURY
wallet instead of a per-beneficiary deposit address. This hub owns the
issued-authorization registry end to end and is the ONLY thing standing
between "a signed authorization" and "a credited payment":

- Configure `treasury_address` on the chain(s) that use this lane. It is
  NOT added to `bindings`/the watched-address map — the treasury has no
  single beneficiary, so its `Transfer`s are resolved by nonce correlation,
  never by address lookup.
- A trusted source calls `issue_authorization` with
  `{nonce, order_ref, beneficiary, amount_usd, valid_before}` BEFORE the
  authorization is ever submitted on chain. **Order matters and there is no
  safe reverse**: issue in THIS hub first, THEN hand the nonce to whatever
  signs/submits it (a keeper, a wallet-bridge order). Registering the order
  with a downstream keeper BEFORE this hub has issued the nonce means a
  Transfer can land in the treasury and get scanned before
  `issued_authorization/1` can ever resolve it — the credit rule (below)
  refuses anything it cannot look up, so that money is recorded
  `unrecognised`, not credited, and needs a manual operator reissue/release
  to fix. Issue first, always.
- `store_mod` needs 5 more optional callbacks for this lane to do anything:
  `record_issued_authorization/1`, `issued_authorization/1`,
  `live_authorization_nonces/1`, `mark_authorization_consumed/1`,
  `record_unrecognised_inflow/1`. Like every other callback group in this
  package, a store implementing only PART of the issuance round trip
  (`record_issued_authorization` + `issued_authorization`) or the nonce-filter
  round trip (`live_authorization_nonces` + `mark_authorization_consumed`)
  is refused at `init/1` — worse than implementing neither.
- **The credit rule (spec §4.4), the one line in this whole package money
  literally depends on**: a treasury `Transfer` settles ONLY when it
  correlates (by `tx_hash`) to an `AuthorizationUsed` log whose nonce this
  hub's own `issued_authorization/1` can resolve. No correlation, or a
  nonce this hub never issued, is recorded as an unrecognised inflow
  (`payments_unrecognised_inflow`) and never credited. Without this rule a
  future deposit-sweep collection landing in the SAME treasury wallet would
  read as a user payment and get credited a second time for money already
  credited once.
- The host implements storage only — issuance, lookup, the nonce filter,
  and consumption marking are all `Store` callbacks with host-owned schema,
  exactly like bindings and settlements. This object never writes an
  issued-authorization row except through `issue_authorization`, and never
  credits a treasury inflow except through the rule above.

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
- **`issue_authorization` without a store implementing `issued_authorization/1`
  is memory-only and functionally inert for crediting**: `issue_authorization`
  still answers `ok: true` (dev/memory mode, same stance as every other write
  in this package), but nothing written that way can ever be looked back up
  by the credit rule, so every treasury inflow correlating to it is recorded
  `unrecognised` instead of settled. Configure a durable store for this lane
  before relying on it for real money.

## Verification

`./checks/run.sh` — every `checks/payments_*.exs` (no Postgres, no network;
injected seams: fake store, injected `rpc_fn`/`now_fn`/`deliver_fn`).
