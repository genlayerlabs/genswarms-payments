defmodule Genswarms.Payments.Store do
  @moduledoc """
  The OPTIONAL durable seam (host-owned schema). Every callback is optional.
  Settlement/binding/cursor callbacks have documented memory fallbacks; the
  outbox read refuses when absent unless the hub explicitly booted in
  ephemeral mode. Unlike budget reads in sibling packages, settlement WRITES
  fail CLOSED when a configured store errors: without durable dedup there is
  no safe way to guarantee a payment is credited exactly once. No store at
  all uses the memory fallback; with non-empty targets, that requires the
  explicit `allow_ephemeral: true` boot opt-out.

  The authorization lane has its OWN instance of that same gate: a chain with
  `treasury_address` configured requires a store exporting all six
  authorization callbacks below, behind the same explicit opt-out. A hub that
  answers `issue_authorization` with `ok: true` and cannot register the nonce
  acknowledges money it can never credit — every treasury Transfer it later
  sees lands as an unrecognised inflow.

  The fail-closed rule keys off whether the callback is *exported*, not
  whether `store_mod` is nil: a coherence-legal store that implements the
  bindings group but not the settlement group (`payment_seen?/1`,
  `record_payment/1`, ...) is NOT-EXPORTED for those callbacks, and is
  treated exactly like a nil store — memory dedup, not a permanent hold. Only
  an EXPORTED callback that raises, exits, or returns `{:error, _}` fails
  closed.

  Money is `Decimal`. Addresses are EIP-55 checksummed strings.
  """

  @doc """
  Persist a beneficiary↔address binding: %{beneficiary, index, address, namespace}.

  Identity is immutable: a replay carrying the SAME beneficiary/index/address
  is `:ok`, and one that would move an existing beneficiary to a different
  index/address is refused.

  Two refusals are DISTINCT, and the difference is load-bearing under
  concurrency (two hubs on one database, e.g. a rolling restart):

  - `{:error, :index_taken}` — the hd_index (or the address derived from it)
    is already owned by ANOTHER beneficiary. The hub reads this as "that slot
    is gone", advances its allocation index and retries, so a concurrent
    allocator can never wedge it. The write is idempotent and the store MUST
    NOT have mutated anything.
  - `{:error, :binding_conflict}` — THIS beneficiary is already bound to a
    different index/address. Never retried and never rebound: the address a
    user was told to pay is permanent.

  A store that cannot tell the two apart may answer `:binding_conflict` for
  both; the hub then behaves exactly as it did before this distinction
  existed (refuse, no retry) — it just cannot self-heal the concurrent case.
  """
  @callback put_address_binding(map()) :: :ok | {:error, term()}

  @doc """
  Fetch a binding by beneficiary string; `{:ok, nil}` when unbound.

  Rows carry `beneficiary`, `index` (or `hd_index`), `address` and `namespace`.

  This is the read the hub makes on `{:error, :binding_conflict}`: the
  beneficiary is bound durably to an address this hub process does not have in
  memory (a peer instance wrote it, or this process booted before the write).
  The hub ADOPTS what this callback returns and serves that address — it never
  re-derives, never rebinds, and refuses outright when this read is missing,
  errors, or cannot say which address the beneficiary is bound to. So a store
  that answers `:binding_conflict` should export this too; without it a
  conflict is a permanent refusal for that beneficiary on that instance.
  """
  @callback get_address_binding(String.t()) :: {:ok, map() | nil} | {:error, term()}

  @doc "All bindings (boot: builds the watched address set + next index)."
  @callback list_address_bindings() :: {:ok, [map()]} | {:error, term()}

  @doc "Has idempotency_key already settled? Settlement dedup — MUST be durable in prod."
  @callback payment_seen?(String.t()) :: {:ok, boolean()} | {:error, term()}

  @doc """
  Record one settlement with the existing settlement fields, `status`,
  `outbox_seq`, and the full method-supplied audit facts (`raw_amount`,
  `decimals`, `token_contract`, `chain`, `chain_id`, `block_number`,
  `log_index`, `tx_hash`, and `from_address` for USDC).

  `status` is `"settled"` (creditable) or `"quarantined"` (recorded by a C1
  cap, deduped like any other row, NEVER creditable). The distinction is
  carried by the SEQUENCE, not only by the status column:

  - `"settled"` — a store may return `{:ok, seq}` with its positive monotone
    insertion sequence. Plain `:ok` remains valid for adapters that do not
    assign a sequence yet.
  - `"quarantined"` — the row MUST persist with `outbox_seq` NULL and MUST
    answer plain `:ok`. A sequence here would publish the row to the outbox
    read and credit exactly what the cap refused; the hub treats a sequence on
    a quarantined row as a store defect (alarm + hold).

  A store that has no column for a status it does not know should answer
  `{:error, :unsupported_status}`. The hub then HOLDS the settlement — fail
  closed, re-presented next tick — and emits a `payments_store_version_skew`
  metric so the operator sees the hub/store version gap instead of losing the
  payment.

  Releasing a quarantined row is an operator action (phase 4, not implemented
  here) and is defined as exactly two field changes on the existing row:
  set `status` to `"settled"` and mint a FRESH `outbox_seq` at release time.
  A release-time sequence is what makes the released row appear at the head of
  every consumer's outbox; a record-time sequence would sit forever below an
  already-advanced cursor and never be credited.
  """
  @callback record_payment(map()) :: :ok | {:ok, pos_integer()} | {:error, term()}

  @doc """
  Trailing-window issuance totals for C1's aggregate cap, in USD `Decimal`s
  over rows whose `status` is `"settled"` (quarantined rows are excluded by
  definition — they were never credited):

      %{total_usd: Σ amount_usd for the namespace,
        beneficiary_usd: Σ amount_usd for this beneficiary in the namespace}

  `since` is an inclusive lower bound on the row's `at`. Required — and
  asserted at `init/1` — whenever `max_issuance_per_window_usd` is configured
  over a durable settlement store: without it the cap could only ever hold
  every settlement. Hubs with no durable store compute the window from their
  in-memory settlement mirror instead.
  """
  @callback issuance_totals_since(
              namespace :: String.t(),
              beneficiary :: String.t(),
              since :: DateTime.t()
            ) :: {:ok, %{total_usd: Decimal.t(), beneficiary_usd: Decimal.t()}} | {:error, term()}

  @doc "Last fully-settled block for a chain name; {:ok, nil} when never scanned."
  @callback get_last_scanned_block(String.t()) ::
              {:ok, non_neg_integer() | nil} | {:error, term()}

  @doc "Advance the scan cursor for a chain (only after all its settlements recorded)."
  @callback put_last_scanned_block(String.t(), non_neg_integer()) :: :ok | {:error, term()}

  @doc """
  SETTLED payments for a beneficiary, newest first. Quarantined rows must not
  appear here — `payment_status` reports this list as money the beneficiary
  received. Surfacing held rows is the operator surface's job (phase 4), not
  this callback's.
  """
  @callback list_payments(String.t()) :: {:ok, [map()]} | {:error, term()}

  @doc """
  Return the outbox page whose rows satisfy:

      outbox_seq IS NOT NULL AND outbox_seq > after_seq

  Rows are ordered by `outbox_seq` ascending and limited to `limit`.
  `max_seq` is `COALESCE(MAX(outbox_seq), 0)` over the whole settlements
  table, not merely the returned page.

  The `IS NOT NULL` clause is what keeps quarantined rows out of the credit
  path: they carry no sequence until an operator releases them.
  """
  @callback list_settlements_since(after_seq :: non_neg_integer(), limit :: pos_integer()) ::
              {:ok, %{settlements: [map()], max_seq: non_neg_integer()}} | {:error, term()}

  @doc """
  Release ONE quarantined settlement (the phase-4 operator action).

  Exactly the two field changes the quarantine contract above names, applied
  in ONE atomic statement on the existing row:

      UPDATE <settlements>
         SET status = 'settled',
             outbox_seq = <fresh sequence from the same monotone generator
                           every settled row is minted from>
       WHERE idempotency_key = $2 AND namespace = $1 AND status = 'quarantined'

  Three properties the store MUST hold, each of which is a money bug if it
  does not:

  1. The sequence is minted AT RELEASE TIME from the same generator, so the
     released row lands ABOVE every consumer cursor and appears at the head
     of the next poll. A record-time sequence would sit below an advanced
     cursor forever and never be credited.
  2. `namespace` scopes the statement. One hub must not be able to release
     another hub's money; a row under a foreign namespace answers
     `{:error, :not_found}` here, exactly like a key that does not exist.
  3. It is IDEMPOTENT. A row already `"settled"` is reported as
     `{:ok, :already_settled, row}` — never re-sequenced, never re-credited.

  Returns:

  - `{:ok, :released, row}` — this call flipped it. `row` carries the row's
    settlement fields with the FRESH `outbox_seq` and `status: "settled"`.
  - `{:ok, :already_settled, row}` — nothing changed; the row was settled
    before this call (a replay, or a second operator). The hub answers
    success and pushes nothing: the outbox already carries it.
  - `{:error, :not_found}` — no row with that key in this namespace.
  - `{:error, {:not_releasable, status}}` — the row exists but is in a status
    that is neither `"quarantined"` nor `"settled"`.
  - `{:error, term()}` — anything else. The hub refuses; it never reports a
    release it cannot prove.

  Optional. A store without it makes `release_payment` refuse with a distinct
  `no_release_store` — never a silent success.
  """
  @callback release_quarantined_payment(
              namespace :: String.t(),
              idempotency_key :: String.t()
            ) ::
              {:ok, :released, map()}
              | {:ok, :already_settled, map()}
              | {:error, :not_found}
              | {:error, {:not_releasable, String.t()}}
              | {:error, term()}

  @doc """
  QUARANTINED rows, newest first — the operator's held-money queue.

  `beneficiary` is `nil` for the whole namespace, or a beneficiary string to
  scope to one. `limit` bounds the returned rows (the caller clamps it).
  Scoped to `namespace` for the same reason the release is: another hub's
  held money is not this operator surface's business.

  Read-only and optional; `payment_status` and `quarantined` degrade to
  "this store cannot answer" rather than reporting an empty queue, because
  "no held money" and "I cannot see held money" are different sentences.
  """
  @callback list_quarantined_payments(
              namespace :: String.t(),
              beneficiary :: String.t() | nil,
              limit :: pos_integer()
            ) :: {:ok, [map()]} | {:error, term()}

  @doc """
  Persist one issued authorization: `%{nonce_hex, order_ref, beneficiary,
  amount_usd, namespace, valid_before, issued_at}`.

  Identity is the ORDER_REF, not the nonce: the hub's `issue_authorization`
  action is idempotent by `order_ref`, so a replay of the same issuance
  request must return the SAME row rather than mint a second one.

  - `:ok` — a fresh row was written.
  - `{:ok, :duplicate, stored_row}` — `order_ref` was already recorded;
    nothing new happened, and `stored_row` is THE ROW ON RECORD (at least
    `nonce_hex`, `order_ref`, `beneficiary`, `amount_usd`, `valid_before`,
    `namespace`). The hub answers success by echoing THAT row, never the
    request that lost the race — which is what makes issuance idempotent from
    the caller's point of view. Returning the stored row is MANDATORY, not a
    convenience: a caller that timed out on the first reply retries the order
    with a freshly minted nonce, and echoing the request would ack a nonce
    that was never registered, is never in the `AuthorizationUsed` getLogs
    filter, and buries the user's payment as an unrecognised inflow. A bare
    `{:ok, :duplicate}` is therefore treated as a STORE DEFECT (the hub has no
    order_ref lookup to read the row back with, so it cannot repair it): the
    action is refused with `store_unavailable` rather than answered with
    fields nobody can prove are on record.
  - `{:error, term()}` — the hub refuses the action and reports
    `store_unavailable`; it never fabricates success it cannot prove.

  The `namespace` field is SEALED by the hub before this callback is ever
  invoked — whatever namespace the caller's message carried (if any) never
  reaches here. A store that enforces uniqueness on `order_ref` scoped to
  `namespace` is correct; a process-global uniqueness constraint is equally
  fine, since one hub process serves exactly one namespace.
  """
  @callback record_issued_authorization(row :: map()) ::
              :ok | {:ok, :duplicate, map()} | {:error, term()}

  @doc """
  Look up an issued authorization by its nonce, keyed EXACTLY like the
  `AuthorizationUsed` log's indexed `nonce` topic (lowercase hex, `"0x"` plus
  64 hex characters) — both the USDC watcher and `issue_authorization` itself
  normalize to that form before this callback is ever called or queried.

  `nil` answers "this hub never issued this nonce" and is the single most
  safety-critical answer this store gives: it is what makes the credit rule
  refuse a stranger's Transfer into the treasury wallet — including a
  same-wallet inflow from a DIFFERENT domain's own on-chain authorization
  (e.g. a future deposit-sweep collection correlating to the same treasury
  address) — exactly as firmly as an unbound deposit address refuses to
  settle.

  There is no error-TUPLE return, but a store MAY signal that it cannot
  currently answer at all by raising or exiting — the hub catches that and
  HOLDS the settlement (nonce stays live, cursor stays put, the store is
  asked again next tick) rather than treating the fault as "unrecognised".
  Earlier revisions of this doc required "behave as if every nonce is
  unrecognised" on any failure, on the theory that under-crediting is always
  the safer direction; that was itself a bug (F2): it made a transient store
  fault indistinguishable from a genuinely foreign nonce, so a real payment
  this hub had a row for got buried as unrecognised and its cursor advanced
  past it — a fabricated non-beneficiary, and a permanent one, since
  `mark_authorization_consumed/1` never even ran to keep the nonce alive for
  a retry. Holding fabricates no beneficiary either and loses nothing: the
  nonce is still live (see C1), so recovery costs nothing but a later tick.
  NOT-EXPORTED is unaffected by any of this — it is the ordinary
  memory-fallback path (`nil`, same as a store that has never heard of the
  nonce), not a fault.

  The returned row carries at least `beneficiary` and `amount_usd` (and, for
  audit purposes, the `namespace` it was originally sealed under) — the hub
  always stamps the SETTLEMENT it credits with its OWN current namespace,
  never this row's, so a row surviving a namespace rename is only ever an
  audit curiosity, never a re-namespacing risk.

  Both fields are LOAD-BEARING and both fail closed when unusable (a missing
  or empty `beneficiary`, a non-positive or unparsable `amount_usd`): the
  inflow is held as unrecognised rather than credited.

  - `beneficiary` is the ONLY source of who gets paid. It is never taken from
    the Transfer's `from`, never from log position within the transaction.
  - `amount_usd` is the CEILING on what may be credited. A correct
    `receiveWithAuthorization` moves exactly the signed value, so a Transfer
    that moved MORE than this row authorizes is evidence of a mis-correlation,
    not a windfall: the hub credits this amount, records the moved amount
    alongside it (`moved_amount_usd`, `credit_capped`), and emits
    `payments_authorization_overpay`.
  """
  @callback issued_authorization(nonce_hex :: String.t()) :: map() | nil

  @doc """
  Every nonce this hub has issued that is still UNCONSUMED and not yet past
  its `valid_before` (compared against `now`, a unix timestamp — the same
  clock `DateTime.to_unix(now_fn.())` produces).

  Excluding the expired ones is NOT optional bookkeeping — it is half of the
  bound below, and a store that returns them anyway grows this filter without
  limit.

  This is the `topics[2]` filter for the watcher's second `eth_getLogs` query
  (`AuthorizationUsed`), so its SIZE is the RPC filter's size. **Bounded by
  construction**, and the construction is enforced at BOTH ends: the hub
  refuses at `issue_authorization` any `valid_before` that is already past or
  further out than `max_authorization_window_seconds` (default 3600, the
  plan's 1-hour window), and this callback drops every nonce whose window has
  closed. So the set cannot grow without bound even if
  `mark_authorization_consumed/1` failed on every single call ever made —
  expiry alone eventually retires every nonce from this list, independent of
  whether consumption was ever recorded.

  `{:error, term()}` FAILS the whole chain's scan round for this tick, fail
  closed exactly like `get_last_scanned_block/1` erroring (cursor unmoved,
  retried next tick): a store that cannot say which nonces are live must
  never let the watcher silently ask for none (that would quietly stop
  crediting the entire authorization lane while looking healthy) nor for
  every nonce ever issued (that would defeat the bound this callback exists
  to provide).
  """
  @callback live_authorization_nonces(now :: integer()) :: [String.t()] | {:error, term()}

  @doc """
  Retire a nonce from `live_authorization_nonces/1` once the hub has observed
  its `AuthorizationUsed` log on chain AND durably resolved the linked
  Transfer.

  Called INDEPENDENTLY of whether that Transfer settled or was quarantined by
  this hub's own caps — the nonce is consumed on-chain the moment the token
  contract fires the event, and the hub's cap policy is a separate decision
  made afterwards that must not gate this call. Both outcomes are recorded
  rows the dedup recognises forever, so neither can be re-presented.

  It is NOT called when the settlement is HELD (a store blip on the dedup
  read, the record, or the cap evaluation). A hold means the chain's cursor
  stays put and the Transfer is re-presented on a later tick — but
  re-presentation is only lossless while this nonce is still in the filter
  above, because that is the only query that recovers the correlation.
  Retiring it first turns every held authorization settlement into a
  permanent UNDER-credit: the retry arrives with no nonce at all and is buried
  as an unrecognised inflow while the cursor advances past it.

  A failure here is logged and NOT fail-closed: the worst case is the nonce
  stays in the live filter and gets asked about again next tick until it
  naturally expires (see the bound above). Note what this failure does NOT
  rely on for safety: `idempotency_key` dedup only prevents *re-observing the
  same* `AuthorizationUsed` log twice (same `tx_hash`) — it does nothing for a
  genuine SECOND on-chain use of this nonce arriving in a DIFFERENT
  transaction, which is exactly the shape a failed call here leaves open. The
  actual guard against that is `authorization_settled?/1` below: the credit
  rule refuses any nonce that already has a SETTLED settlement on record,
  independent of whether this callback ever succeeded.
  """
  @callback mark_authorization_consumed(nonce_hex :: String.t()) :: :ok | {:error, term()}

  @doc """
  Has a settlement with status `"settled"` already been recorded for this
  nonce?

  This is the guard that makes one-credit-per-nonce a HUB-STATE fact rather
  than a borrowed assumption about the token contract. `mark_authorization_consumed/1`
  is best-effort (see its doc above): if it fails on a settled row, the nonce
  stays in `live_authorization_nonces/1` and a genuinely later
  `AuthorizationUsed` reusing the same nonce — a different `tx_hash`, so a
  different `idempotency_key`, so untouched by ordinary settlement dedup —
  would otherwise correlate and credit a second time. `issued_authorization/1`
  is not gated on consumption either, so without this callback the second
  Transfer would resolve the SAME issued row and credit again.

  Answer `true` ONLY for a durably `"settled"` row. A `"quarantined"` row
  never credited in the first place (see `settle/2`'s doc — quarantine is
  "durable and resolved, but never credited"), so it must NOT count here: the
  whole point is to refuse a SECOND credit, not to refuse crediting something
  that was never credited once. A row later released from quarantine goes
  through the ordinary settlement path (and its own `idempotency_key` dedup),
  independent of this callback.

  `{:error, term()}` FAILS the settlement closed exactly like
  `issued_authorization/1` raising: the hub cannot tell "never settled" from
  "store fault" and holds rather than guessing either way.
  """
  @callback authorization_settled?(nonce_hex :: String.t()) ::
              {:ok, boolean()} | {:error, term()}

  @doc """
  Record a Transfer into the treasury wallet that the credit rule refused to
  credit. The row carries a `reason` naming which refusal it was, plus the
  `nonce_candidates` the hub was choosing between:

  - `not_issued` — no `AuthorizationUsed` in the same transaction had this
    Transfer's sender as its `authorizer`, or the one that did carries a nonce
    `issued_authorization/1` never heard of (a different domain's on-chain
    authorization landing in the same wallet — the exact double-credit shape
    spec §4.4 warns about, e.g. a future deposit-sweep collection correlating
    to this same treasury address).
  - `ambiguous_correlation` — the correlation was genuinely undecidable: two
    issued authorizations in the transaction share this Transfer's sender as
    authorizer, or two treasury Transfers in it share a sender. Log order
    inside a batched transaction is attacker-controlled (anyone may submit an
    EIP-3009 authorization), so the hub refuses to guess. THIS ROW IS AN
    OPERATOR SIGNAL, not routine noise.
  - `issued_row_unusable` — the nonce resolved, but the stored row could not
    supply a usable `beneficiary` or `amount_usd`. A store defect, held rather
    than credited to `nil` or to an unbounded amount.
  - `authorization_already_settled` — the nonce resolved to a real issued row,
    but `authorization_settled?/1` says a `"settled"` settlement already
    exists for it. A genuinely SECOND on-chain use of the same nonce (see that
    callback's doc): a different `tx_hash` means a different `idempotency_key`,
    so ordinary settlement dedup does not catch it — this is the only wall
    that does. THIS ROW IS AN OPERATOR SIGNAL: it means either
    `mark_authorization_consumed/1` failed earlier, or something replayed a
    spent nonce.

  Never blocks the scan: a write failure here is logged, but the chain's
  cursor still advances normally, because nothing in this row was ever
  creditable to begin with — losing the audit row is a visibility gap, not a
  money bug. The operator still sees the row's characterizing metric
  (`payments_unrecognised_inflow`) either way.
  """
  @callback record_unrecognised_inflow(row :: map()) :: :ok | {:error, term()}

  @doc """
  The issued authorization for an ORDER ref, or nil.

  The nonce-keyed `issued_authorization/1` answers the watcher, which only
  ever sees nonces on chain. This one answers the keeper's result callback
  (`Genswarms.Payments.TopupAck`), which knows an order ref and nothing else
  — it is how a submitted-and-mined (or refused/reverted) order finds the
  person waiting for it.

  MUST NEVER raise: it runs inside the keeper's own process, on a
  best-effort acknowledgement path. A store fault must cost a chat message,
  never the keeper. Return nil on any fault.

  The returned map carries at least `beneficiary` and `amount_usd`, plus
  the optional card columns `card_chat_id`/`card_message_id` (nil when the
  original card's delivery never recorded an id — the ack then sends a new
  message instead of editing).
  """
  @callback authorization_by_order_ref(order_ref :: String.t()) :: map() | nil

  @doc """
  The issued authorization behind a SETTLEMENT (`method` + `ref`), or nil.

  Answers the credit notice: a landed credit knows its settlement, the
  settlement's facts carry the nonce, the nonce identifies the issued row —
  and with it the card to edit into its final state. Exact join only (the
  first host: `topup_authorizations.nonce_hex = settlement facts nonce_hex`);
  a fuzzy match here could edit a stranger's card. Same never-raise stance
  as `authorization_by_order_ref/1`.
  """
  @callback authorization_by_settlement(method :: String.t(), ref :: String.t()) ::
              map() | nil

  @doc """
  Most-recent issued authorizations, newest first, for the dashboard page
  (`Genswarms.Payments.Dashboard`). Read-only projection; `{:ok, []}` when
  none. Rows carry at least `order_ref`, `beneficiary`, `amount_usd`,
  `valid_before`, `created_at`, `consumed_at`; a store MAY enrich each row
  with `tx_ref` (the settlement ref, when the authorization settled) — the
  page shows it when present and shows nothing when not.
  """
  @callback list_issued_authorizations(limit :: pos_integer()) ::
              {:ok, [map()]} | {:error, term()}

  @doc """
  Most-recent unrecognised treasury inflows, newest first, for the dashboard
  page. Read-only projection of the §4.4 audit trail; `{:ok, []}` when none
  — which the page renders as its own answer (\"none seen\"), never as an
  absent section. Rows carry `chain`, `tx_hash`, `from_addr`, `amount_usd`,
  `reason`, `seen_at`.
  """
  @callback list_unrecognised_inflows(limit :: pos_integer()) ::
              {:ok, [map()]} | {:error, term()}

  @doc """
  Per-deposit-address collection view for the dashboard's Deposits page and
  the sweep preparer (plan 3 / spec §7). Newest activity first; `{:ok, []}`
  when no bindings exist.

  Rows carry `beneficiary`, `address`, `hd_index`, `received_usd` (settled
  entry-B deposits — authorization-lane settlements go straight to the
  treasury and are excluded), `swept_usd` (recognized sweeps; 0 until C3
  lands), and `last_at`. This is a STORE-derived ESTIMATE of what sits
  uncollected — the sweep executor reads the chain's own balances before
  signing anything; the page must label it as such.

  Attribution (2026-07-27, found live): receipts attach by ADDRESS — a
  settlement whose facts carry `to_address` counts only toward the binding
  with that exact address (case-insensitive; the scanner stamps it on every
  deposit settlement). Rows WITHOUT `to_address` (scanner rows predating
  this fact) fall back to attaching by beneficiary. Beneficiary-only
  attribution let a re-bound beneficiary (address migration) inherit the
  previous address's whole history — "uncollected" money at an address
  holding 0 on chain. Sweeps attach by the sweep's `from` address, which
  the sweeps ledger always carries.
  """
  @callback list_deposit_balances(limit :: pos_integer()) ::
              {:ok, [map()]} | {:error, term()}

  @doc "Complete authorization counts, independent of recent-table limits; live excludes consumed and expired rows."
  @callback issued_authorizations_summary(now_unix :: integer()) ::
              {:ok,
               %{issued: non_neg_integer(), live: non_neg_integer(), consumed: non_neg_integer()}}
              | {:error, term()}

  @doc "Complete count of unrecognised inflows, independent of the recent table."
  @callback unrecognised_inflows_summary() ::
              {:ok, %{count: non_neg_integer()}} | {:error, term()}

  @doc "Complete per-address ledger estimate using the same attribution as list_deposit_balances/1. Sum exact received minus swept amounts before rounding."
  @callback deposit_balances_summary() ::
              {:ok,
               %{
                 addresses: non_neg_integer(),
                 with_activity: non_neg_integer(),
                 unswept_usd: Decimal.t()
               }}
              | {:error, term()}

  @optional_callbacks put_address_binding: 1,
                      release_quarantined_payment: 2,
                      list_quarantined_payments: 3,
                      get_address_binding: 1,
                      list_address_bindings: 0,
                      payment_seen?: 1,
                      record_payment: 1,
                      get_last_scanned_block: 1,
                      put_last_scanned_block: 2,
                      list_payments: 1,
                      list_settlements_since: 2,
                      issuance_totals_since: 3,
                      record_issued_authorization: 1,
                      issued_authorization: 1,
                      live_authorization_nonces: 1,
                      mark_authorization_consumed: 1,
                      authorization_settled?: 1,
                      record_unrecognised_inflow: 1,
                      authorization_by_order_ref: 1,
                      authorization_by_settlement: 2,
                      list_issued_authorizations: 1,
                      list_unrecognised_inflows: 1,
                      list_deposit_balances: 1,
                      issued_authorizations_summary: 1,
                      unrecognised_inflows_summary: 0,
                      deposit_balances_summary: 0
end
