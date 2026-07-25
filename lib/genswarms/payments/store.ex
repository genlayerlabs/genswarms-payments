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

  @doc "Fetch a binding by beneficiary string; {:ok, nil} when unbound."
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
                      issuance_totals_since: 3
end
