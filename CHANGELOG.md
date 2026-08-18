# Changelog

## 0.3.0 — 2026-08-18

- Added an explicit authorization-only mode with
  `deposit_addresses_enabled: false`. In this mode an xpub is not required,
  historical HD bindings are not loaded or watched, `deposit_address` and
  `sweep_report` fail closed with `deposit_addresses_disabled`, and the
  EIP-3009 treasury watcher, issuance registry, reconciliation and settlement
  paths remain active. The default is still `true`, preserving the existing
  xpub-required behavior for deposit-address deployments.
- Health replies now expose `deposit_addresses_enabled`, making custody mode
  visible without disclosing any key material.

## 0.2.1 — 2026-07-29

- Deposits dashboard tile: the "sweep lane" metric now names the real
  mechanism ("manual — operator /payments sweep") instead of internal
  plan jargon. Label only; no behavior change.

## 0.2.0 — 2026-07-27

- Added `Genswarms.Payments.Dashboard` — the package's schema-1 dashboard
  page (top-ups + the §4.4 unrecognised-inflow audit trail), read entirely
  through the host's Store adapter via two new optional list callbacks
  (`list_issued_authorizations/1`, `list_unrecognised_inflows/1`). A host
  opts in with one probed line; no compile dependency in either direction.
  The inflows table is always present when the page is — an empty audit
  trail renders as "none seen", never as a hidden section. Conformance
  covers both reads when exported.
- Added `Genswarms.Payments.StoreConformance` — an executable conformance
  suite a host runs against its REAL store + throwaway database
  (`StoreConformance.run!(MyStore)`). Pins the semantics the hub and the
  presenter rely on: issuance idempotent by order_ref echoing the row OF
  RECORD, exact-hit-plus-honest-miss on every lookup (the
  answers-anything-store defect class), the live-nonce window, consume
  idempotence, settlement dedup walls, the settlement→authorization joins,
  per-chain cursors, and inflow rescan dedupe. Sections skip when their
  optional callbacks are absent; a host adopting the authorization lane or
  TopupAck must see no skips in those sections. The first host runs it
  inside its throwaway-PG gate; mutation-verified (an answers-anything
  `authorization_by_order_ref` turns it red).
- Added `priv/reference_schema.sql` — the PostgreSQL DDL the contract's
  semantics were proven against (all five Store-contract tables, with the
  money-bearing column notes inline). A reference for host migrations, not
  a migration runner; the conformance suite remains the authority.
- Added `Genswarms.Payments.TopupAck` — the presenter half of the
  authorization lane, moved here from the first host so a second host wires
  seams instead of rewriting logic. Owns the orchestration (keeper
  `result_fn` and credit `credit_notice_fn`: order_ref/settlement → issued
  row → conversation → edit-the-card-or-send) and the default English copy
  for every terminal, including the two `{:refused, reason}` shapes (the
  insufficient-balance refusal gets its actionable sentence; the raw
  contract revert string never reaches a chat). Hosts inject `:store`,
  `:conversation_fn` and `:deliver_fn`, and may override copy via
  `:text_fn`/`:credit_text_fn`. Best-effort throughout: any fault costs the
  one message, never the calling process.
- `Genswarms.Payments.Store` gains two optional read callbacks the presenter
  resolves through: `authorization_by_order_ref/1` and
  `authorization_by_settlement/2` (exact nonce-facts join only; never
  raises). Issued-authorization rows may now carry the optional
  `card_chat_id`/`card_message_id` columns recorded by the host's delivery
  effect — nil means "send, don't edit".

- Documented **key custody** in the usage guide: the seed never touches the
  host, only the xpub is configured, `allow_test_xpub` is a local-rig opt-in
  and never production, physical seed backup, who-may-sign decided in advance,
  and how to collect deposit-address balances by signing EIP-3009
  authorizations offline while a gas-only relayer submits them. Also states
  plainly what a compromised host does and does not cost you (addresses and
  balances exposed; nothing spendable) and that this guarantee is void if an
  xprv or seed is ever placed near the host.
- Added the authorization lane (entry A): a new `treasury_address` per chain
  and the hub action `issue_authorization` (trusted-source gated, like
  `deposit_address`) that owns the issued-authorization registry end to end.
  A trusted source submits `{nonce, order_ref, beneficiary, amount_usd,
  valid_before}`; the hub validates shape (32-byte hex nonce, positive
  `@money_pattern` amount, non-empty beneficiary/order_ref), SEALS the row
  under its own namespace regardless of what the caller sent, and persists it
  idempotently by `order_ref` via the new `Store.record_issued_authorization/1`.
  Five new optional `Store` callbacks: `record_issued_authorization/1`,
  `issued_authorization/1`, `live_authorization_nonces/1` (issued ∧
  unconsumed ∧ unexpired — bounded by construction, since every authorization
  expires), `mark_authorization_consumed/1`, and
  `record_unrecognised_inflow/1`. Coherence-gated at `init/1` like every
  other multi-callback group: implementing half of either round trip
  (issuance or the nonce filter) raises.
  The USDC watcher's `getLogs` now runs a SECOND query per chain with a
  `treasury_address` configured — `AuthorizationUsed(address,bytes32)`
  events filtered to the live nonce set, correlated to the matching
  `Transfer` by `{tx_hash, authorizer}` (the event's indexed `authorizer`
  MUST equal the `Transfer`'s `from`, which is exactly what EIP-3009's
  `receiveWithAuthorization` guarantees; a transaction can carry many
  authorizations and anyone may submit them, so log position decides
  nothing) — and credits a treasury inflow under settlement
  method `"usdc_authorization"` (same idempotency key shape as every other
  USDC settlement, `"#{chain_id}:#{tx_hash}:#{log_index}"`, amount = what
  actually moved, which may be less than what was issued) ONLY when its
  nonce was issued by THIS hub and is still resolvable — spec §4.4's credit
  rule. Everything else landing in the treasury (no correlated nonce, or a
  nonce this hub never issued — the exact shape a future deposit-sweep
  collection into the same wallet would otherwise be double-credited under)
  is recorded via `record_unrecognised_inflow/1` and metered
  (`payments_unrecognised_inflow`), never held against the chain's cursor.
  The `AuthorizationUsed` topic0 is computed at compile time from its
  signature string (never hand-typed hex) and pinned against an
  independently-verified frozen literal in
  `checks/payments_keccak_test.exs`.
  Money-safety properties of this lane, each with its own regression test:
  - **Ambiguity fails closed, it never guesses.** A `Transfer` that cannot be
    matched to EXACTLY ONE issued authorization in its own transaction (zero
    matches; two issued authorizations whose `authorizer` is this Transfer's
    sender; two treasury Transfers in the transaction sharing a sender) is
    recorded unrecognised with a `reason`, never credited.
  - **Consumption is marked only AFTER the settlement is durably recorded.**
    A held settlement (store blip on the dedup read, the record, or the cap
    evaluation) leaves the nonce in `live_authorization_nonces/1`, so the
    re-presented `Transfer` still correlates on the next scan. Retiring it
    first turned every held authorization settlement into a permanent
    under-credit.
  - **The credited amount is capped by what was authorized**
    (`min(moved, issued)`). A `receiveWithAuthorization` moves exactly the
    signed value, so `moved > issued` is evidence of mis-correlation: the row
    carries `moved_amount_usd` + `credit_capped` and emits
    `payments_authorization_overpay`.
  - **`issue_authorization` is idempotent by the STORED row.** A duplicate
    `order_ref` answers with the row on record, so a caller that retried with
    a freshly minted nonce is never handed an ok:true naming a nonce nobody
    registered. `Store.record_issued_authorization/1`'s duplicate shape is
    now `{:ok, :duplicate, row}`; a row-less `{:ok, :duplicate}` is a store
    defect and the action is refused.
  - **The window is bounded at issue time.** A `valid_before` that is absent,
    non-integer, already past, or further out than the new
    `max_authorization_window_seconds` (default 3600) is refused — this is
    what makes the live-nonce `getLogs` filter bounded by construction rather
    than by caller goodwill.
  - **An inert authorization lane refuses to boot.** A chain configuring
    `treasury_address` requires a store exporting all five authorization
    callbacks, behind the same explicit `allow_ephemeral: true` opt-out the
    settlement ledger uses — a hub that answers `issue_authorization` with
    ok:true and can never credit the resulting payment is refused at `init/1`,
    not discovered in production.
  - **The treasury address can never be a watched binding.** A binding whose
    address equals a chain's `treasury_address` refuses boot (a watched
    binding would resolve one beneficiary for every user's payment and skip
    the credit rule entirely), and the watcher resolves the treasury branch
    BEFORE the watched map for any collision written after boot.
  - The live-nonce filter is re-checked against the returned logs, so a
    provider that ignores `topics[2]` cannot inject a correlation for a nonce
    this hub did not ask about (e.g. an already-consumed one).
- `binding_conflict` now ADOPTS the durable binding instead of refusing. When
  this beneficiary is already bound to an address this process does not have in
  memory (a peer instance wrote it, or this one booted before the write), the
  hub calls `Store.get_address_binding/1` — the contract callback that until now
  had no caller — and serves the STORED address, repairing `state.bindings` so
  the next `/topup` is a plain hit. It never re-derives and never rebinds, so a
  beneficiary still cannot end up with two deposit addresses; the refusal that
  remains is for a binding under a foreign namespace (`namespace_mismatch`, its
  settlements would be HELD) and for a conflict the store cannot resolve. The
  old behaviour was a permanent break, not a race: it was reported as
  "the store is down" and every later `/topup` by that user on that instance
  failed identically for the life of the process.
- `sweep_report` is now bounded in TIME as well as count. Each address is one
  sequential synchronous RPC inside the hub's own callback, so the previous
  200-address cap was a mailbox stall measured in minutes on an operator
  keystroke — taken, by construction, when the RPC endpoint is already degraded.
  The hard cap is now 25 (default page 10) with a wall-clock budget
  (`sweep_budget_ms`, default 20s); an exhausted budget returns a PARTIAL report
  with `complete: false`, `remaining`, `budget_spent: true` and a
  `payments_sweep_truncated` metric, rather than holding every `/topup` and
  every chain tick behind it.
- The `degraded_boot` `payment_status` refusal now carries `action` and echoes
  the beneficiary, like every other operator reply. It was the one reply with
  neither, which made it indistinguishable on the wire from the untagged
  `deposit_address` refusal a host routes to an END USER. The `store_unavailable`
  `payment_status` refusal and the `degraded_boot` `release_payment` /
  `quarantined` refusals now echo too, so a caller correlates exactly instead of
  guessing "most recent".
- A released row that cannot be pushed is reported `creditable: false`. Such a
  row is missing the fields the credit key is built from, so the consumer's poll
  validates it exactly as the push did and will refuse it too — telling the
  operator "the poll will credit it" was fabricated success generated by the
  branch that detected the problem.
- The `quarantined` queue reports `amounts_unparsable`: an amount this hub
  cannot parse is counted, never folded into the money total as zero (the stance
  the sweep already takes with an unreadable balance).

- Added the D3 operator surface, gated by a NEW `operator_sources` allowlist
  that is separate from `trusted_sources` and defaults to `[]`.
  `release_payment` is the only path that turns a quarantined row back into
  creditable money: the new optional `Store.release_quarantined_payment/2`
  flips `status` to `"settled"` and mints a FRESH `outbox_seq` at release
  time in one namespace-scoped atomic statement, which is what puts the row
  above every consumer cursor; the hub then emits the SAME
  `payment_confirmed` a normal settlement emits, so a release credits through
  the consumer's ordinary validating path and never through a bypass.
  Releasing an already-settled row is an idempotent no-op success (no second
  sequence, no second push). Refusals are distinct: `unknown_key`,
  `not_quarantined`, `namespace_mismatch`, `no_release_store`,
  `degraded_boot`, `store_unavailable`. Added the `quarantined` action (the
  operator's held-money queue, via the new optional
  `Store.list_quarantined_payments/3`), extended `payment_status` with a
  `held`/`held_durable` view of the same rows, and started persisting the
  quarantine `reason` with the row (as an audit fact) so the queue can answer
  "why is this held?" days later.
- Added the D4 `sweep_report` action: a bounded, READ-ONLY per-address ERC-20
  `balanceOf` measurement (how many derived addresses hold a balance, the
  total, the largest) so consolidation economics become data-driven. It never
  moves funds, and an unreadable balance is reported as `unreadable` rather
  than folded into zero.
- Fixed an address-allocation LIVELOCK under two hubs on one database (any
  rolling restart). `put_address_binding/1` now distinguishes
  `{:error, :index_taken}` (another beneficiary owns that HD index/address)
  from `{:error, :binding_conflict}` (THIS beneficiary is already bound to a
  different address). The hub advances its index and retries on the former
  (bounded at 25 attempts) instead of re-offering the same permanently-taken
  index to every subsequent new beneficiary forever; the latter is still
  refused outright, so a rebind never strands money sent to the first
  address. A store that cannot distinguish the two keeps the old behaviour.

- Added per-settlement and aggregate issuance caps. `max_payment_usd`
  (default `"10000"`) bounds one settlement; `max_issuance_per_window_usd`
  (default `nil` = disabled) bounds the settled total inside a trailing
  `issuance_window_hours` window (default 24). A settlement past either cap is
  recorded with `status: "quarantined"` and a NULL `outbox_seq` instead of
  being settled: durable, deduped by idempotency key, excluded from the outbox
  read and from `payment_confirmed` delivery, alarmed as
  `payments_quarantined`, and announced to targets as a one-shot best-effort
  `payment_held` cast (the user-visible hold hook) carrying the same stamp as
  `payment_confirmed` — `method`, `ref`, `namespace` and ISO8601 `at` — plus
  the quarantine `reason`, so a consumer can refuse a foreign-namespace hold
  and key it under the very `"<method>:<ref>"` string the eventual release
  credits under. The aggregate cap carries a
  per-beneficiary `small_topup_usd` carve-out (default `"5"`) so one large
  payment cannot deny everyone else's small top-ups for the rest of the
  window; the carve-out never overrides `max_payment_usd`. Releasing a held row
  is an operator action, not implemented in this release — the row shape
  defines it as `status: "settled"` plus a FRESH release-time `outbox_seq`.
- Added the optional `Store.issuance_totals_since/3` callback (trailing-window
  settled totals per namespace and beneficiary). `init/1` refuses a hub that
  sets `max_issuance_per_window_usd` over a durable settlement store that does
  not export it, because the cap could then only hold every settlement.
  Memory-mode hubs compute the window from their settlement mirror. A store
  read failure, an invalid store result, or `{:error, :unsupported_status}`
  from `record_payment/1` HOLDS the settlement (cursor unmoved, retried next
  tick) and emits `payments_store_version_skew`; a store that mints an
  `outbox_seq` for a quarantined row is refused the same way rather than
  crediting what the cap just declined.
- Separated the two chain depths that were previously conflated. The CREDIT
  leg uses each chain's new `fast_credit_depth` (default: that chain's
  `confirmations`, so existing configs keep their depth) — shallow and fast on
  purpose, bounded by the caps above. FINALITY is now queried on the
  RECONCILE leg: per-chain `finality: :finalized` (default) asks the reconcile
  endpoint for the `finalized` block tag, `{:confirmations, n}` serves chains
  without the tag. `reconcile` gained `unfinalized` (rows above the finality
  head — informational, never a credit reversal) and `finality_unverifiable`
  counts, with `payments_reconcile_unfinalized` and
  `payments_reconcile_finality_unverifiable` metrics. A null, absent, or
  unparseable answer is never treated as finalized. The head is fetched at most
  once per chain per run.
- Added the first-tick on-chain self-check per chain: `eth_chainId` must equal
  the configured `chain_id` and, for a chain with a token contract, its
  `decimals()` must equal the configured `decimals`. A mismatch or an
  unverifiable answer holds THAT chain (no scanning, no settling) with a
  `payments_chain_self_check_failed` alarm and is retried next tick, so a healed
  RPC recovers by itself; a pass is cached per chain. It deliberately runs at
  the first tick rather than at boot: an endpoint that is merely down at boot
  must not crash-loop the object.
- Added the namespace-coherence gate: bindings loaded at boot under a namespace
  other than the hub's are logged, metered as
  `payments_namespace_mismatch`, and kept in the watched set, but their
  settlements are HELD — money is never silently re-namespaced, and the
  chain's cursor stays back until an operator repairs the binding.
- Added a compiled-in denylist of publicly known test xpubs (the BIP32
  `abandon abandon … about` key at `m/44'/60'/0'`). Booting one raises unless
  `allow_test_xpub: true` is set explicitly; mainnet hubs must never set it.
  `allow_test_xpub` and `allow_ephemeral` are now validated as booleans rather
  than read as truthy.
- Money configuration is parsed strictly: plain non-negative decimal strings
  only, exponent forms rejected, positivity enforced where required, and
  `issuance_window_hours`, `confirmations`, `fast_credit_depth`, `decimals`,
  `finality`, and non-map/non-list `chains` entries all validated at `init/1`.
- Added the optional `Store.list_settlements_since/2` transactional-outbox
  read contract, the synchronous `Genswarms.Payments.settlements_since/3`
  host seam, and the trusted-target message action. Reads are namespace
  filtered, carry whole-table `max_seq`, and expose an unfiltered-page
  `next_seq`/`complete` cursor so foreign namespaces cannot stall consumers.
  Action limits clamp to 1..500, malformed params and inconsistent store
  sequence bounds refuse distinctly, and poisoned row values cannot crash
  reply encoding. Explicit ephemeral hubs can serve the same action from
  their sequenced settlement mirror.
- Removed the in-memory `undelivered` queue and tick-time redelivery
  machinery. `payment_confirmed` is now a one-shot best-effort latency path:
  target failures are isolated, logged, and metered, while the durable
  sequenced outbox is the authoritative recovery path.
- Added the trusted `reconcile` action and optional per-chain
  `reconcile_rpc_url`, validated and called through the same scrubbed,
  tempfile-hardened RPC path as the primary endpoint. Recent full-fact rows
  are checked against independent receipts/logs, including the stored sender
  address. Drift, unverifiable chains, legacy pre-0.2.0 rows, and incomplete
  0.2.0-era rows are reported without automatically reversing credits.
  Reconcile caps receipt RPC calls to the action limit and reports elapsed
  milliseconds.
- Added the isolated `metrics_fn` seam (default `Logger`) for settlements,
  fail-closed holds, failed pushes, refused reads, reconciliation drift,
  incomplete rows, and unverifiable reconciliation. Raising/exiting telemetry
  cannot affect a settlement or other money path.
- Updated the cross-package e2e lost-ack and proxy-store-outage scenarios:
  both now recover by reading `settlements_since` and applying the returned
  row through the proxy's real validating ingress, rather than relying on
  the deleted hub retry queue.
- USDC settlement rows now preserve the raw chain facts needed to re-verify
  and recompute credited money: `raw_amount`, `decimals`, `token_contract`,
  `chain`, `chain_id`, `block_number`, `log_index`, `tx_hash`, and
  `from_address`, alongside the existing derived `amount_usd` and settlement
  fields.
- Every chain config now requires a unique positive integer `chain_id`, and
  chain names must also be unique because scan cursors are keyed by name. New
  settlements use `"#{chain_id}:#{tx_hash}:#{log_index}"` as the idempotency
  key, preventing a mutable chain-name rename from re-keying new rows. Dedup
  remains exact string equality, so already-recorded old keys keep deduping
  matching old-key inputs, while new watcher rows always use the new format;
  safe transition relies on the scan cursor never rolling back, not on
  cross-format key equivalence.
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
- Invalid configuration no longer escapes `init/1` as a raise: it returns
  `{:error, term}`, which the engine logs while leaving the object unstarted,
  instead of crash-looping it. `init!/1` retains the raising contract for
  tests and embedders.

### Hardening (adversarial review of the caps/quarantine phase)

- The durable outbox READ now enforces creditability itself: any row a store
  returns whose `status` is present and is not `"settled"` is dropped from
  `settlements_since` (message action, host seam, and reconciliation alike),
  logged, and metered as `payments_outbox_poisoned_row` with the idempotency
  key. Previously the hub only refused to PUBLISH such a row at record time —
  a store that had already persisted a sequence on a quarantined row would
  have served it to the consumer on every subsequent page. Paging bookkeeping
  (`next_seq`, `max_seq`, `complete`, and the internal scan cursor) is still
  computed over the raw store page, so a dropped row can neither end a page
  early nor hide the rows behind it. Status-less pre-0.2.0 rows keep flowing.
- The known-test-xpub denylist now matches the decoded 33-byte compressed
  public key rather than only the published base58 string. Re-serializing the
  denylisted key under a different chain code produced a different string that
  used to boot without an opt-out, while its child private keys stay derivable
  from the same publicly known parent. The string list remains the source of
  truth and is decoded at `init/1`.
- `settle/2` now HOLDS a non-positive `amount_usd` as `invalid_amount`
  instead of settling it. A negative Decimal cleared both caps (never `:gt`)
  and, once recorded `"settled"`, subtracted from the trailing-window totals,
  widening the window for a later over-credit.
- `deposit_address` now refuses a beneficiary whose binding was loaded under a
  foreign namespace with `{"ok": false, "error": "namespace_mismatch"}` and a
  `payments_namespace_mismatch` alarm (`stage: "deposit_address"`). It used to
  re-serve the address while every settlement to it would be held — and the
  hold freezes that chain's cursor.
- The first-tick decimals check now distinguishes "the endpoint answered
  nothing readable" (`decimals_unverifiable`, with the method and the raw
  answer) from "the token reports another number" (`decimals_mismatch`, now
  also carrying `observed_decimals`). Both still hold the chain.
- A reconcile row whose own `block_number` cannot be parsed now emits
  `payments_reconcile_finality_unverifiable` (reason
  `block_number_unparseable`) instead of only incrementing the reply counter.
- A chain configured `finality: {:confirmations, n}` with no
  `reconcile_rpc_url` no longer emits the per-run
  `payments_reconcile_finality_unverifiable` alarm: it opted out of the
  finality leg rather than failing it. The reply still counts those rows as
  `finality_unverifiable` — they are never called finalized — and chains that
  were actually asked and could not answer keep alarming.

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
- Settlements recorded without an `outbox_seq` (legacy adapters returning
  plain `:ok`) are not visible to `settlements_since`; deploy a store that
  assigns positive sequences before relying on pull recovery.
- Outgoing `fromBlock`/`toBlock` quantities are uppercase hex (`"0xC8"`);
  the canonical JSON-RPC quantity encoding is lowercase and a strict
  provider/validator could reject them.
- URL scrubbing does not redact API keys embedded in the HOSTNAME
  (`https://<key>.provider.com/`); full URL, path, userinfo, and query
  fragments are covered.
- `settlement_mirror` is append-only and grows without bound for the life of
  the hub in EVERY mode (durable-store hubs append too; only the outbox
  fallback read is ephemeral-mode-specific). Phase 2 will decide the reader
  and retention owner; this release does not bound it.
