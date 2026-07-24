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

  @doc "Persist a beneficiary↔address binding: %{beneficiary, index, address, namespace}."
  @callback put_address_binding(map()) :: :ok | {:error, term()}

  @doc "Fetch a binding by beneficiary string; {:ok, nil} when unbound."
  @callback get_address_binding(String.t()) :: {:ok, map() | nil} | {:error, term()}

  @doc "All bindings (boot: builds the watched address set + next index)."
  @callback list_address_bindings() :: {:ok, [map()]} | {:error, term()}

  @doc "Has idempotency_key already settled? Settlement dedup — MUST be durable in prod."
  @callback payment_seen?(String.t()) :: {:ok, boolean()} | {:error, term()}

  @doc """
  Record one creditable settlement with the existing settlement fields,
  `outbox_seq`, and the full method-supplied audit facts (`raw_amount`,
  `decimals`, `token_contract`, `chain`, `chain_id`, `block_number`,
  `log_index`, `tx_hash`, and `from_address` for USDC).

  A store may return `{:ok, seq}` with its positive monotone insertion
  sequence. Plain `:ok` remains valid for adapters that do not assign a
  sequence yet.
  """
  @callback record_payment(map()) :: :ok | {:ok, pos_integer()} | {:error, term()}

  @doc "Last fully-settled block for a chain name; {:ok, nil} when never scanned."
  @callback get_last_scanned_block(String.t()) ::
              {:ok, non_neg_integer() | nil} | {:error, term()}

  @doc "Advance the scan cursor for a chain (only after all its settlements recorded)."
  @callback put_last_scanned_block(String.t(), non_neg_integer()) :: :ok | {:error, term()}

  @doc "Settled payments for a beneficiary, newest first."
  @callback list_payments(String.t()) :: {:ok, [map()]} | {:error, term()}

  @doc """
  Return the outbox page whose rows satisfy:

      outbox_seq IS NOT NULL AND outbox_seq > after_seq

  Rows are ordered by `outbox_seq` ascending and limited to `limit`.
  `max_seq` is `COALESCE(MAX(outbox_seq), 0)` over the whole settlements
  table, not merely the returned page.
  """
  @callback list_settlements_since(after_seq :: non_neg_integer(), limit :: pos_integer()) ::
              {:ok, %{settlements: [map()], max_seq: non_neg_integer()}} | {:error, term()}

  @optional_callbacks put_address_binding: 1,
                      get_address_binding: 1,
                      list_address_bindings: 0,
                      payment_seen?: 1,
                      record_payment: 1,
                      get_last_scanned_block: 1,
                      put_last_scanned_block: 2,
                      list_payments: 1,
                      list_settlements_since: 2
end
