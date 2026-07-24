# Changelog

## 0.2.0 — Unreleased

- USDC settlement rows now preserve the raw chain facts needed to re-verify
  and recompute credited money: `raw_amount`, `decimals`, `token_contract`,
  `chain`, `chain_id`, `block_number`, `log_index`, `tx_hash`, and
  `from_address`, alongside the existing derived `amount_usd` and settlement
  fields.
- Every chain config now requires an integer `chain_id`. New settlements use
  `"#{chain_id}:#{tx_hash}:#{log_index}"` as the idempotency key, preventing a
  mutable chain-name rename from re-crediting history. Previously recorded
  old-format keys remain valid because dedup is string equality.
- `Genswarms.Payments.Store.record_payment/1` may return either legacy `:ok`
  or `{:ok, positive_seq}`. Store-assigned sequences are retained as
  `outbox_seq` on the settlement mirror; the memory fallback mints its own
  monotone sequence. Error, raise, exit, and invalid-return paths remain
  fail-closed, and held rows receive no sequence.
- A hub with non-empty `targets` now refuses to boot unless its effective
  store exports durable `payment_seen?/1` and `record_payment/1`. Ephemeral
  dev/test use requires the explicit `allow_ephemeral: true` opt-out because
  memory mode can re-mint addresses and re-credit history after restart.
- Added a contract-shape check that derives every action string from the
  handler source and pins `init/1` plus every message path to the real
  `Genswarms.Objects.ObjectHandler` return shapes and JSON reply contract.

## 0.1.1 — 2026-07-24

- FIX (engine contract): `init/1` now returns `{:ok, state}` as
  `Genswarms.Objects.ObjectHandler` requires — v0.1.0 returned the bare state
  map, which crash-looped the object at real swarm boot (ObjectServer matches
  on the tuple). Caught on the first live engine boot; every direct-call check
  and the cross-package e2e had bypassed ObjectServer. New `init!/1` returns
  the bare state for tests/embedders; checks pin the engine shape.

## 0.1.0 — 2026-07-23

- Initial release: settlement hub core (stable HD deposit addresses from a
  watch-only xpub, fail-closed idempotent settlement ledger, stamped
  payment_confirmed delivery to allowlisted targets), Method behaviour for
  pluggable modalities, in-tree USDC watcher (multi-chain, reorg-safe,
  chunked getLogs, curl JSON-RPC with keyed-URL protection).
- Cross-package end-to-end harness (`e2e/`): boots this hub and the REAL
  `genswarms-llm-proxy` in one BEAM and drives the full USDC → credit →
  spend story across the live `deliver_fn` seam (deposit address, budget
  block with hub-provided top-up hint, settlement + credit over the
  strings-only wire, exact debit math, duplicate-redelivery and
  retryable-NACK outage retry). `sh e2e/run.sh`; proxy checkout located via
  `LLM_PROXY_PATH` (defaults to sibling `../genswarms-llm-proxy`), fails
  loudly when absent. Still hermetic — canned JSON-RPC, loopback only.

### Hardening (pre-release audit)

- A log whose `topics` don't decode as a 3-binary-topic Transfer (nil
  to-topic, wrong arity, non-list) now fails CLOSED — the chain is held
  with the cursor unmoved, like a missing `transactionHash` — instead of
  being silently skipped while the cursor advanced past a possibly real
  payment with mangled topics.
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
- Settlement dedup's `payment_seen?` match is now strict (`{:ok, bool} when
  is_boolean(bool)`): a store returning `{:ok, nil}` (the `Repo.one`
  no-row shape) or any other non-boolean fails closed — held, cursor
  unmoved — instead of escaping `settle_one` as a `CaseClauseError` and
  crash-looping the object every tick.
- RPC output scrubbing is path-aware: `scrub/2` redacts the full `rpc_url`
  AND its path, userinfo, and query fragments, so a provider error body
  echoing only the URL path (where Alchemy/Infura-style API keys live) no
  longer leaks the key into logged `{:error, _}` tuples.
- The shipped default `deliver_fn` no longer hardcodes `:ok`: the
  `ObjectServer.deliver_message` return flows through
  `map_peer_delivery_result/1` (`:ok`/`{:ok, _}` = delivered; `{:error, _}`
  passes through; anything else becomes `{:error, {:bad_return, _}}`), so
  an error-shaped peer RETURN is queued for retry instead of being silently
  counted as delivered.
- The USDC watcher skips zero-value Transfer logs (no settlement, no
  ledger write, no delivery; cursor still advances) — `transfer(victim, 0)`
  costs an attacker only gas and would otherwise grow the ledger, seen-set,
  and delivery fan-out for free.
- The USDC watcher fails CLOSED on a log missing `transactionHash` (chain
  held, cursor unmoved — consistent with every other malformed field)
  instead of settling under the nil-interpolated dedup key
  `"<chain>::<logIndex>"`, which a durable store then remembered forever,
  silently swallowing every future colliding hash-less log while the
  cursor advanced.
- `HD.parse_xpub` verifies the embedded pubkey is actually ON secp256k1
  (y² ≡ x³ + 7 mod p, and x < p) and rejects off-curve keys as
  `{:error, :not_on_curve}` — a validly-checksummed off-curve xpub
  previously derived valid-looking EIP-55 addresses no private key
  controls (one bad config value = a permanently unspendable address
  tree).
- All USDC watcher hex comparisons are case-insensitive (topic0, to-address
  topic, contract address, watched-address matching) — an uppercase-hex
  provider's Transfer logs were silently missed while the cursor advanced.
- `removed: true` reorg-marker logs are skipped entirely (no settlement,
  no delivery; the cursor advances normally — the log is officially
  retracted).
- The RPC config-tempfile hardening (exclusive create + chmod 600 before
  the secret write) is extracted to `Rpc.open_config_exclusively!/1` and
  pinned by checks — it was previously an unpinned mutation survivor.

### Known limitations (v1 riders)

- Namespace is config-level, not per-requesting-source: the design's
  "namespace defaults to the requesting source" is not implemented, because
  binding identity (state map, store contract, `payment_status`) is keyed by
  the bare beneficiary string — per-source namespaces require re-keying
  bindings by (namespace, beneficiary) across the store contract and host
  schemas. Equivalent while a hub serves one trusted consumer; run one hub
  namespace per consumer until then.
- Settlements recorded while `targets: []` are never re-delivered after
  targets are wired later (durable dedup blocks re-presentation) — wire
  targets before announcing deposit addresses, or reconcile manually via
  `payment_status`.
- Outgoing `fromBlock`/`toBlock` quantities are uppercase hex (`"0xC8"`);
  the canonical JSON-RPC quantity encoding is lowercase and a strict
  provider/validator could reject them.
- URL scrubbing does not redact API keys embedded in the HOSTNAME
  (`https://<key>.provider.com/`); full URL, path, userinfo, and query
  fragments are covered.
