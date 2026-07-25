# genswarms-payments

Payment settlement hub object for [genswarms](https://github.com/genlayerlabs/genswarms)
swarms. It owns beneficiary identity (a stable HD deposit address per
beneficiary), the idempotent settlement ledger, and stamped
`payment_confirmed` delivery to allowlisted downstream targets. Payment
modalities (in-tree USDC today; Stripe, x402, etc. as sibling packages
tomorrow) implement `Genswarms.Payments.Method` and plug into the same core —
a hub-and-adapter design: one settlement ledger, one delivery path, any
number of ways money can arrive.

## Trust model

Every capability is fail-closed, gated by two allowlists:

- **`trusted_sources`** — who may talk to the object at all. An untrusted
  `deposit_address`, `payment_status`, `settlements_since`, `reconcile`,
  `tick`, or `ingest_event` message gets silent `{:noreply, _}` (only
  `health` is unauthenticated). Empty `trusted_sources` means nobody can act.
- **`operator_sources`** — who may take a VALUE-AFFECTING or operator-scope
  action: `release_payment` (turning quarantined money back into creditable
  money), `quarantined` (the held-money queue) and `sweep_report` (every
  beneficiary's address and balance). Deliberately NOT `trusted_sources`, and
  it defaults to `[]`: the cron that ticks the watcher and the consumer that
  reads the outbox are trusted, and neither has any business releasing money.
  Both gates apply — an operator source that is not also trusted can never
  act, and is warned about at boot.

  Read this for what it is: a SOURCE-IDENTITY gate. Against objects that are
  not on it (a cron, an outbox consumer) it is a real barrier. Against an
  object that IS on it and also relays ordinary end-user traffic, it is not a
  second factor at all — every message that object sends carries the same
  source identity, so this hub cannot tell an operator-authorized action from
  any other action that object was talked into sending. In that shape the real
  control is the caller-side operator gate, and this list only narrows WHICH
  object holds it. Two independent factors need either a dedicated
  operator-only object between the glue and this hub, or a config-injected
  shared secret in the action payload validated here; neither is invented for
  the host by this package.
- **`targets`** — who may receive `payment_confirmed`. Empty `targets` means
  nobody is ever credited, even though settlement still records durably.
  The `settlements_since` action additionally requires its authenticated
  caller to be a target; a trusted non-target receives `not_a_consumer`.

Push modalities add a second gate before settlement ever sees a payload: the
`Method.ingest_event/2` callback must verify the payload's authenticity
(webhook signature, facilitator signature, ...) itself — the core trusts
whatever settlements a method returns, so an unverified method is a hole. No
push method ships yet; `ingest_event` currently always replies
`{"ok": false, "error": "no_push_methods"}`.

## Config

```elixir
%{
  name: :payments,                    # object name, stamped on every delivered message (default :payments)
  swarm_name: "my_swarm",             # used by the default deliver_fn (default "swarm")
  xpub: System.fetch_env!("PAYMENTS_XPUB"),   # required — watch-only, see Custody below
  allow_test_xpub: false,             # explicit opt-out for a publicly known test xpub (default false)
  trusted_sources: ["telegram_ingress", "cron"],  # required for anything to work (default [])
  operator_sources: ["commands"],     # SEPARATE allowlist for value-affecting actions (default [] = nobody)
  targets: ["downstream_object"],     # required for anyone to get credited (default [])
  allow_ephemeral: false,             # explicit dev-only opt-out when targets are non-empty (default false)
  namespace: "default",               # stamped on bindings/deliveries; caller-defined meaning (default "default")
  store_mod: MyApp.PaymentsStore,     # optional — see Store contract (default nil = memory)
  max_payment_usd: "10000",           # per-settlement cap; above it a row is QUARANTINED (default "10000")
  max_issuance_per_window_usd: nil,   # aggregate cap over the window; nil = disabled (default nil)
  issuance_window_hours: 24,          # trailing window for the aggregate cap (default 24)
  small_topup_usd: "5",               # per-beneficiary carve-out from the aggregate cap (default "5")
  chains: [
    %{
      name: "base",
      chain_id: 8453,                 # required integer; immutable on-chain identity
      rpc_url: System.fetch_env!("BASE_RPC_URL"),
      reconcile_rpc_url: System.get_env("BASE_RECONCILE_RPC_URL"), # optional independent endpoint
      usdc_contract: "0x...",
      confirmations: 12,              # default 12 — the fast_credit_depth default
      fast_credit_depth: 12,          # CREDIT leg depth (default: this chain's confirmations)
      finality: :finalized,           # RECONCILE leg: :finalized | {:confirmations, n} (default :finalized)
      decimals: 6,                    # default 6 — asserted against the contract at the first tick
      start_block: 0,                 # default 0 — cold-start scan floor
      max_block_range: 2000,          # default 2000 — cap per poll round
      address_chunk: 200              # default 200 — addresses per eth_getLogs call
    }
  ],
  methods: [Genswarms.Payments.Usdc], # pluggable modalities (default [Genswarms.Payments.Usdc])
  deliver_fn: fn target, from, content -> :ok | {:error, term()} end,  # default dispatches via the host ObjectServer
  metrics_fn: fn event, meta -> :ok end, # optional; default logs through Logger
  now_fn: &DateTime.utc_now/0,        # injection seam for checks (default)
  rpc_fn: &Genswarms.Payments.Rpc.call/3,  # injection seam for checks (default)
  auto_tick: true,                    # currently INERT — see below (default true)
  poll_interval_ms: 60_000            # currently INERT — see below (default 60_000)
}
```

When `targets` is non-empty, `init/1` requires the effective store to export
both `payment_seen?/1` and `record_payment/1`. Without durable settlement
dedup, a restart can re-mint addresses and re-credit payment history. Local
or test configurations may accept that risk only by setting
`allow_ephemeral: true` explicitly. Empty-target observers may still use
memory mode without the opt-out because they cannot credit anyone.

Every money amount above is a plain non-negative decimal STRING, parsed
strictly: no floats, no integers, no exponent forms (`"1e6"` is refused, not
silently read as a million). `max_payment_usd` and
`max_issuance_per_window_usd` must be greater than zero;
`max_issuance_per_window_usd` may be `nil` to disable the aggregate cap;
`small_topup_usd: "0"` disables the carve-out.

## Caps and quarantine (C1)

The cheapest control in the system: it turns every unbounded over-credit mode
(a lying RPC oracle, a reorg, a decimals misconfiguration) into a bounded,
loud one.

- A settlement above `max_payment_usd` is **quarantined**.
- A settlement that would push the trailing window's settled total past
  `max_issuance_per_window_usd` is **quarantined**, *unless* it fits the
  beneficiary's own first `small_topup_usd` in that window — a whale must not
  deny everyone else's small top-ups for the rest of the window. The carve-out
  never overrides `max_payment_usd`.

A quarantined settlement is:

- **recorded durably** with `status: "quarantined"` and `outbox_seq: nil`;
- **deduped** exactly like a settled one (`payment_seen?` answers true), so a
  re-presented log neither re-alarms nor re-notifies;
- **never delivered as `payment_confirmed` and never visible to
  `settlements_since`** — the sequence marks *creditable*, not *recorded*.
  Both ends are defended: the hub refuses to publish a quarantined row, and
  the outbox read drops any row a store returns whose `status` is present and
  is not `"settled"` (alarmed as `payments_outbox_poisoned_row`; paging is
  computed over the raw store page, so the drop cannot hide the rows behind
  it). Status-less pre-0.2.0 rows keep flowing;
- **alarmed** through `metrics_fn` as `payments_quarantined` with the
  idempotency key, beneficiary, amount, and reason (`max_payment` |
  `aggregate`);
- **notified** to every target as a one-shot best-effort
  `{"action": "payment_held", "beneficiary": ..., "amount_usd": ...,
  "method": ..., "ref": ..., "namespace": ..., "at": ..., "reason": ...}`
  cast — the same stamp `payment_confirmed` carries, plus the reason. That is
  the user-visible hold hook: a silent hold on money the user watched leave
  their wallet is a support incident by design. A lost cast is acceptable —
  the operator queue is the authoritative record. The stamp is not decoration:
  `namespace` lets a consumer refuse a hold that is not its own money, and
  `method` lets it key the hold under the very `"<method>:<ref>"` string the
  eventual release will credit under.

Releasing a held payment is an operator action and is **not implemented yet**
(phase 4). The row shape already defines it: set `status` to `"settled"` and
mint a fresh `outbox_seq` at release time, so the released row appears at the
head of every consumer's outbox.

The window total comes from the store's `issuance_totals_since/3` when a
durable store is configured; `init/1` refuses a hub that sets
`max_issuance_per_window_usd` over a durable store lacking that callback,
because the cap could then only ever hold every settlement. Memory-mode hubs
compute the window from their in-memory mirror. A store read that fails, or a
store that cannot record a `"quarantined"` status (`{:error,
:unsupported_status}`), HOLDS the settlement — fail closed, cursor unmoved,
retried next tick — and emits `payments_store_version_skew`.

## Credit depth vs finality (C2)

Two different depths, deliberately named apart:

| Leg | Config | Typical Base latency | What it is for |
|---|---|---|---|
| Credit | `fast_credit_depth` (per chain, defaults to `confirmations`) | seconds to a minute | the fast path a blocked user is waiting on; the caps above bound what its shallowness can cost |
| Reconcile | `finality: :finalized` (per chain; `{:confirmations, n}` for chains without the tag) | ~10-20 minutes | the truth, queried from the chain — never a small confirmations number *pretending* to be finality |

Crediting is never gated on finality: putting a quarter-hour wall in front of
"unblock me now" would defeat the feature. Instead the `reconcile` action
re-checks recent settled rows against the `finalized` head and reports
`unfinalized` counts (informational — it never reverses a credit), while a
reorged-out row shows up on the existing receipt leg as drift. A null, absent,
or unparseable answer to the `finalized` tag — or a row whose own
`block_number` cannot be parsed — is reported as `finality_unverifiable` and
alarmed; it is never treated as finalized. The head is fetched at most once
per chain per reconcile run. One exception to the alarm: a chain configured
`finality: {:confirmations, n}` with no `reconcile_rpc_url` opted out of the
finality leg rather than failing it, so it is still counted
`finality_unverifiable` but does not alarm every run.

## Boot and first-tick gates

- **Known test xpubs (D7).** A publicly known test xpub — the BIP32
  `abandon abandon … about` key at `m/44'/60'/0'` — is refused at `init/1`,
  because its private key is in every tutorial: watching it means crediting
  deposits anyone can sweep. The gate matches the decoded 33-byte compressed
  public key, not just the published base58 string, so re-serializing that key
  under a different chain code does not slip past it (its children are still
  derived from the same publicly known parent private key).
  `allow_test_xpub: true` is the explicit opt-out for a local testnet rig; a
  mainnet hub must never set it. The list is a floor, not a guarantee — a
  leaked key of your own belongs in your own refusal path.
- **Namespace coherence (D2).** Every binding loaded at boot whose
  `namespace` differs from the hub's is logged, metered
  (`payments_namespace_mismatch`), and kept in the watched set — but its
  settlements are **held**, never credited under the hub's namespace and never
  silently re-namespaced. Held means held: that chain's cursor does not
  advance past such a settlement and the alarm repeats every tick until an
  operator repairs the binding (or points the hub at the right namespace).
  `deposit_address` refuses such a beneficiary with `namespace_mismatch` for
  the same reason: handing the address back would invite a deposit that can
  only ever be held.
- **On-chain self-check (D4).** Before a chain is scanned for the first time,
  the endpoint must prove it is the configured chain: `eth_chainId` equal to
  `chain_id`, and — for a chain with a token contract — that contract's
  `decimals()` equal to the configured `decimals`. A mismatch, an RPC error, or
  an unparseable answer holds **that chain only** (no scanning, no settling)
  with a `payments_chain_self_check_failed` alarm whose `reason` separates the
  two repairs — `chain_id_mismatch` / `decimals_mismatch` (wrong config) from
  `unverifiable` / `decimals_unverifiable` (the endpoint never answered
  readably) — and is retried next tick, so
  a healed RPC recovers by itself. A pass is cached per chain. This runs at the
  first tick, not at boot, because an endpoint that is merely down at boot must
  not crash-loop the object.

`auto_tick` and `poll_interval_ms` are accepted and stored but nothing in
this package reads them to schedule anything — a poll round only happens
when a trusted source sends `{"action": "tick"}`. In practice that means
wiring a scheduler object (e.g. genswarms-cron) to deliver `tick` on an
interval; this package owns the settlement/watch logic, not the clock.

## Object protocol

- `{"action": "health"}` — unauthenticated; `{"ok": true, "bindings": N,
  "degraded_boot": bool}`.
- `{"action": "tick"}` — trusted only; runs one poll round (every configured
  method scans, settlements settle, cursors advance per the fail-closed rule
  below). No reply. A no-op while `degraded_boot` (see below).
- `{"action": "deposit_address", "beneficiary": "..."}` — trusted only;
  returns the beneficiary's stable address, minting one on first ask.
  Refused with `{"ok": false, "error": "namespace_mismatch"}` for a
  beneficiary whose binding was loaded under a foreign namespace (its
  settlements would be held — see D2 below).
  Refused with `{"ok": false, "error": "degraded_boot"}` while
  `degraded_boot` (distinct from `{"ok": false, "error": "store_unavailable"}`,
  which means boot was fine but *this* allocation's write just failed).
- `{"action": "payment_status", "beneficiary": "..."}` — trusted only;
  returns the address plus recorded payments, plus a `durable` flag: `true`
  when a configured store actually answered, `false` when there's no store
  configured or it doesn't implement `list_payments/1` (memory mode — a
  genuinely empty list, not a masked failure). Refuses rather than fail
  open in two cases: `{"ok": false, "error": "degraded_boot"}` while
  `degraded_boot` (see below — init never learned the true payment history),
  and `{"ok": false, "error": "store_unavailable"}` when a **configured**
  `list_payments/1` errors, raises, or exits (an empty list here would be
  indistinguishable from "no payments" — see Reconciliation below).
- `{"action": "settlements_since", "after_seq": N, "limit": M}` — trusted
  target only. `after_seq` defaults to 0; `limit` defaults to 100 and clamps
  to 1..500. Present values must be non-negative integers or the action
  refuses with `bad_request`. Returns namespace-filtered, ascending outbox rows
  with `action: "settlements_since"`, `next_seq`, whole-table `max_seq`, and
  `complete`; the action key keeps a routed response visible to a consumer's
  dispatcher. It refuses distinctly on degraded boot, store failure, or a
  non-ephemeral store without the outbox callback. Explicit ephemeral mode
  reads the in-memory settlement mirror.
- `{"action": "reconcile", "limit": M}` — trusted only; defaults to 50 and
  clamps to 1..200; a present non-integer or negative limit is `bad_request`.
  Re-fetches recent full-fact rows through each chain's
  independent `reconcile_rpc_url`, reporting checked rows, drift keys,
  unverifiable rows, legacy rows, incomplete 0.2.0-era rows, `unfinalized`
  rows, `finality_unverifiable` rows, and `elapsed_ms`.
  A run makes at most `limit` sequential receipt RPCs plus one finality-head
  call per chain; the default RPC seam's
  existing 20-second curl timeout bounds endpoint delay accordingly.
  Detection alarms only; it never reverses a credit.
- `{"action": "ingest_event", ...}` — trusted only; reserved for future push
  methods, currently always refuses.

### Operator actions (`operator_sources`)

- `{"action": "release_payment", "idempotency_key": "..."}` — the ONLY thing
  that turns a quarantined row back into creditable money. The store flips
  `status` to `"settled"` and mints a FRESH `outbox_seq` at release time in one
  atomic statement, so the row lands at the HEAD of the outbox — above every
  consumer cursor, including one that already advanced past the position the
  row would have had when it was recorded. It then emits exactly the
  `payment_confirmed` a normal settlement emits, so the consumer credits
  through its own validating, deduping path; there is no release-specific
  credit message anywhere. Idempotent: an already-settled row answers
  `{"ok": true, "released": false, "already": "settled"}` with no second
  sequence and no second push. Distinct refusals: `unknown_key`,
  `not_quarantined` (plus the row's `status`), `namespace_mismatch`,
  `no_release_store` (the store exports no `release_quarantined_payment/2`),
  `degraded_boot`, `store_unavailable`, `bad_request`.
- `{"action": "quarantined", "beneficiary": "...", "limit": M}` — the
  operator's held-money queue for this namespace, newest first;
  `beneficiary` is optional, `limit` defaults to 20 and clamps to 1..100.
  Reports `count`, `total_usd` and the rows (key, beneficiary, amount, method,
  ref, quarantine reason, `at`). Refuses with `no_quarantine_store` rather
  than reporting an empty queue it cannot see.
- `{"action": "sweep_report", "chain": "...", "limit": M}` — D4 measurement:
  how many derived addresses hold a balance and how much. One ERC-20
  `balanceOf` (`eth_call`, selector `0x70a08231`) per address against the
  chain's configured token, bounded by `limit` (default 10, clamps to 1..25,
  addresses walked in HD-index order) AND by a wall-clock budget
  (`sweep_budget_ms`, default 20_000). Every call is sequential and synchronous
  inside this object's callback, so the time bound is the one that matters: an
  exhausted budget returns a PARTIAL report (`complete: false`, `remaining`,
  `budget_spent: true`) instead of holding the hub's mailbox while every
  `deposit_address` and chain tick queues behind it. Reports `nonzero`,
  `total_usd`, `largest`, up to 50 non-zero rows, `unreadable` and `complete`.
  It NEVER
  moves funds — this object holds an xPUB, not an xprv — and an unreadable
  balance is reported as `unreadable`, never folded into zero. With several
  chains configured and no `chain` argument it refuses (`chain_required`)
  rather than guessing which token to measure.

`payment_status` also gains a held view: alongside `payments` (settled money
only) it returns `held` (this beneficiary's quarantined rows, capped at 20) and
`held_durable`. Either leg failing refuses the whole answer — "no held rows"
and "I could not read held rows" are different sentences.

The primary single-BEAM consumer seam is synchronous and does not depend on
object routing:

```elixir
Genswarms.Payments.settlements_since(
  %{store_mod: MyApp.PaymentsStore, namespace: "default"},
  after_seq,
  limit
)
```

It returns the store error unchanged, returns `:no_outbox_store` when the
optional callback is absent, and never converts a failed read into an empty
success. Its `next_seq` is the highest sequence in the unfiltered store page
(`after_seq` for an empty page), and `complete` says whether that raw page was
last. Consumers must advance by `next_seq`, not by the filtered rows, so a page
containing only another namespace cannot stall polling.

## Degraded boot

`init/1` needs `list_address_bindings/0` to succeed to know the true
watched-address set and the next free HD index. If a **configured** store's
`list_address_bindings/0` errors or raises, `init/1` doesn't guess — it sets
`degraded_boot: true` on the state rather than falling back to an empty set
(which would silently drop every in-flight deposit under an empty watched
set, and reissue an already-handed-out address from index 0). While
degraded: `poll/1` is a no-op (logs an error, changes nothing), and
`deposit_address` is refused. This is fail-*flagged*, not fail-crashed, on
purpose — a transient DB blip at pod boot shouldn't crash-loop the object —
but it also means it does **not** self-heal on its own: recovering requires
restarting the object once the store is healthy again. `health` reports the
flag so operators can detect it externally.

## Custody model

The object holds an **xpub only** — watch-only BIP32 public derivation
(`Genswarms.Payments.HD`), pure Elixir, no NIFs. It can compute deposit
addresses and watch them; it can never sign a transaction, because it never
sees or accepts an xprv (a private key). If the host ever misconfigures the
wrong chain for a contract, funds sent are still recoverable — the address
itself is a standard EIP-55 Ethereum account controlled by whoever holds the
matching xprv offline, not something this object can lose custody of by
misbehaving.

## Store contract (`Genswarms.Payments.Store`)

Every callback is optional. Settlement/binding/cursor seams use in-memory
mirrors where documented (fine in dev, lost on restart). The durable outbox
read is intentionally different: a missing `list_settlements_since/2`
refuses unless the hub explicitly booted in ephemeral mode.

| Callback | Purpose |
|---|---|
| `put_address_binding/1` | persist `%{beneficiary, index, address, namespace}`; `{:error, :index_taken}` when another beneficiary owns that index/address (the hub advances and retries), `{:error, :binding_conflict}` when THIS beneficiary is already bound to a different one (never retried, never rebound — the hub reads `get_address_binding/1` and serves the address the store already committed to) |
| `get_address_binding/1` | fetch a binding by beneficiary; the hub calls it on `{:error, :binding_conflict}` and ADOPTS the stored address (never re-derives, never rebinds) — a store that answers `binding_conflict` should export this, or that beneficiary is permanently refused on that instance |
| `list_address_bindings/0` | boot: rebuild the watched set + next index |
| `payment_seen?/1` | settlement dedup by idempotency key — must be durable in prod |
| `record_payment/1` | record one settlement (`status` `"settled"` or `"quarantined"`); return `:ok`, `{:ok, positive_seq}` (settled rows only), or `{:error, term}` — `{:error, :unsupported_status}` for a status the schema does not know |
| `issuance_totals_since/3` | trailing-window settled totals for C1's aggregate cap (required when that cap is set over a durable store) |
| `get_last_scanned_block/1` | last fully-settled block for a chain |
| `put_last_scanned_block/2` | advance a chain's scan cursor |
| `list_payments/1` | settled payments for a beneficiary, newest first |
| `list_settlements_since/2` | ascending sequenced outbox page plus whole-table `max_seq` |
| `release_quarantined_payment/2` | operator release: ONE atomic namespace-scoped flip to `"settled"` with a FRESH release-time `outbox_seq`; `{:ok, :released, row}` / `{:ok, :already_settled, row}` / `{:error, :not_found}` / `{:error, {:not_releasable, status}}` |
| `list_quarantined_payments/3` | the operator's held-money queue: quarantined rows for a namespace (optionally one beneficiary), newest first, capped |

Unlike budget *reads* in sibling packages, settlement **writes** fail closed:
if a configured store errors on the dedup read or the record write, the
round holds that settlement rather than risk crediting it twice or losing
it. No store at all is a legitimate dev mode — memory dedup still works
within a single run, but a hub with non-empty `targets` refuses that mode
unless `allow_ephemeral: true` is explicit.

This fail-closed rule is keyed on whether the callback is **exported**, not
on whether `store_mod` is `nil`. A store that implements the bindings group
but none of the settlement group (`payment_seen?/1`, `record_payment/1`, ...)
is coherence-legal (see below) — for those NOT-EXPORTED callbacks it is
treated exactly like a nil store: settlement falls back to in-memory dedup,
never frozen. Only a callback that **is** exported and then raises, exits, or
returns `{:error, _}` holds the settlement closed.

**Coherence requirement**: `init/1` validates two callback groups —
`{put_address_binding/1, list_address_bindings/0}` and `{payment_seen?/1,
record_payment/1, get_last_scanned_block/1, put_last_scanned_block/2}` —
and returns `{:error, %ArgumentError{}}` if a store implements only part of
either group (`init!/1` raises the same error). A store that persists
bindings but can never list them forgets the
watched set (and reuses HD indices) on every restart; a store that can
write settlements but never check `payment_seen?` (or vice versa) always
looks unseen and double-credits. Implement all of a group's callbacks or
none of them. `list_payments/1`, `list_settlements_since/2`, and
`get_address_binding/1` are independent read callbacks, not part of either
group, as are the two operator callbacks — a store without them makes the
operator actions refuse distinctly (`no_release_store`, `no_quarantine_store`)
rather than answer an empty or fabricated success.

## Settlement fail-closed rule and the cursor invariant

`settle/2` durably dedups each settlement before recording it, then delivers
`payment_confirmed` to every target. A settlement is skipped (never
recorded, never delivered) only when the store errors on the dedup read or
the write — the watcher will re-present it next round.

`poll/1` advances a chain's scan cursor (`put_last_scanned_block`) **only**
when the round found something to advance to (a non-nil `safe_to`) **and**
every settlement scanned for that chain actually settled (recorded, or
already-seen — dedup counts). If even one of that chain's settlements was
held back by a store failure, the cursor stays put, so the next `tick`
re-scans and re-presents it. This is the invariant that makes the whole
pipeline safe against a flaky store: nothing is ever double-credited, and
nothing is ever silently skipped.

USDC settlement rows retain the raw chain evidence used to compute credit:
`raw_amount`, `decimals`, `token_contract`, `chain`, `chain_id`,
`block_number`, `log_index`, `tx_hash`, and `from_address`, alongside the
existing derived `amount_usd` and settlement fields. New idempotency keys are
`"#{chain_id}:#{tx_hash}:#{log_index}"`, so renaming a mutable chain label
cannot re-key and re-credit history. Existing old-format keys remain valid
because dedup compares the stored strings as-is.

`record_payment/1` may return `{:ok, seq}` with a positive store-assigned
sequence or the legacy `:ok`. A returned sequence is retained as
`outbox_seq` on the in-memory settlement mirror; legacy durable adapters
remain valid with `outbox_seq: nil`. When settlement storage is absent, the
memory fallback assigns its own monotone sequence for the life of the hub.
Store failures and invalid return values still hold the settlement closed,
and held settlements receive no sequence.

## Delivery guarantee

`deliver_fn`'s return contract is `:ok | {:error, term()}`. Only a literal
`:ok` counts as delivered — an `{:error, _}` return is treated exactly like
a raise or an EXIT: logged and emitted as `payments_push_failed`. Each target
is isolated under `catch kind, reason`, so one failed push never blocks the
others or crashes settlement.

Push is deliberately **one-shot best-effort**. There is no in-memory
undelivered queue and `tick` never redelivers. The sequenced outbox is the
recovery and correctness path: a consumer reads `settlements_since`, applies
each full row through its normal validating/idempotent credit path, and
advances its cursor. A dropped push changes only latency; the row remains
durable and readable.

## Reconciliation and metrics

The chain reconciliation action reads the most recent namespace rows, treats
pre-0.2.0 rows without the complete chain-fact set as `legacy`, and counts a
0.2.0-era row carrying `chain_id` but missing another required fact as
`incomplete`. It uses the configured chain's optional `reconcile_rpc_url` as a
second endpoint, fetches `eth_getTransactionReceipt`, locates the stored log
index, and compares raw amount, token contract, stored sender address, bound
destination address, block number, log index, and transaction hash. Missing
independent endpoints and RPC failures are counted as unverifiable; mismatches
and incomplete rows are logged and metered. Each complete row is additionally
checked against its chain's finality head (see above). No result automatically
changes credited money.

`metrics_fn` receives `payments_settled`, `payments_quarantined`,
`payments_hold`, `payments_namespace_mismatch`,
`payments_chain_self_check_failed`, `payments_store_version_skew`,
`payments_push_failed`, `payments_read_refused`,
`payments_outbox_poisoned_row`,
`payments_reconcile_drift`, `payments_reconcile_incomplete`,
`payments_reconcile_unverifiable`, `payments_reconcile_unfinalized`, and
`payments_reconcile_finality_unverifiable`. Every invocation is isolated with
`try/catch`; telemetry failure cannot affect settlement or another money path.
The default implementation logs through `Logger`.

## In-tree USDC watcher

`Genswarms.Payments.Usdc` is a pull method: per `tick`, per configured
chain, it fetches `eth_blockNumber`, computes `safe_to = latest -
fast_credit_depth` (the credit leg's explicitly-labelled shallow depth,
defaulting to the chain's `confirmations`), and pulls `eth_getLogs` for the
ERC-20 `Transfer` topic
against the chain's `usdc_contract`, chunked over watched addresses
(`address_chunk`) and capped in range (`max_block_range`) so a cold start
never issues an unbounded query. Two client-side defenses run even though
the RPC is asked to filter: logs are re-filtered by `blockNumber <= to`
(never trust a provider to honor `toBlock`) and by exact contract address
match (never trust a `Transfer`-shaped log to actually be USDC — a
misbehaving or compromised RPC could hand back logs from an unrelated
contract). A chain's whole scan is also wrapped so a malformed RPC response
shape (e.g. a provider returning `{:ok, nil}` for `eth_blockNumber` instead
of a hex string) can't crash the tick — that one chain's round is skipped
(cursor untouched, retried next `tick`) while every other configured chain
still proceeds. `Genswarms.Payments.Rpc` shells out to `curl` (the engine
has no `:inets`); the RPC URL — which may embed a provider API key — rides
a chmod-600, exclusively-created `--config` tempfile (random suffix, never
reused), never argv where `ps` would expose it, and is scrubbed from both
successful and error output (unified in `call/4`, not reparsed out of the
config file) before it's logged. `init/1` requires `rpc_url` on every
configured chain (returning an `ArgumentError` tuple if the key is missing,
rather than booting and hitting a `KeyError` the first time a poll round
runs) and also rejects any chain `rpc_url` containing a quote, backslash, or
control character. Optional `reconcile_rpc_url` receives the same validation
and is passed through the same scrubbed tempfile RPC implementation.
`init!/1` raises those validation errors. The URL is
written into that tempfile as `url = "#{rpc_url}"`, where an unsanitized
value could close the string early and inject config directives.

## Method behaviour

Future modalities (Stripe, x402, ...) ship as sibling packages implementing
`Genswarms.Payments.Method`: `id/0`, `capabilities/0`, and either `poll/2`
(pull: scan and return `{chain, settlements, safe_to}` per configured
chain) or `ingest_event/2` (push: verify then return settlements). Both
callbacks are optional so a method can be pull-only or push-only.

## Verification

```sh
mix deps.get
./checks/run.sh        # every checks/payments_*.exs — no Postgres, no network
```

## End-to-end tests (`e2e/`)

`e2e/` boots this hub together with the REAL `genswarms-llm-proxy` in one
BEAM and drives the full USDC → credit → spend story across the live seam:
deposit address (ADDR0, stable), free-budget exhaustion over real HTTP, the
block notice carrying a hub-provided top-up hint, a canned on-chain USDC
Transfer settling and crediting the proxy (strings-only wire), credit-funded
spending with exact debit math, a lost-push row recovered through
`settlements_since` (proxy answers `duplicate` when it already applied the
push), and a retryable-NACK outage recovered by outbox application after the
proxy's credit store heals. Still hermetic: canned JSON-RPC, loopback HTTP
only, no Postgres. Its canned chain answers the D4 self-check truthfully, and
its config sets `allow_test_xpub: true` — the harness derives from the public
BIP32 test key, exactly the case the gate exists to catch in production.

```sh
sh e2e/run.sh          # needs a genswarms-llm-proxy checkout:
                       #   defaults to the sibling ../genswarms-llm-proxy,
                       #   or set LLM_PROXY_PATH=/path/to/genswarms-llm-proxy
```

The runner fails (exit 1) when the proxy checkout is missing — the e2e never
silently skips.
