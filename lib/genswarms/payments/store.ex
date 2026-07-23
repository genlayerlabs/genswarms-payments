defmodule Genswarms.Payments.Store do
  @moduledoc """
  The OPTIONAL durable seam (host-owned schema). Every callback is optional —
  missing callbacks fall back to the in-memory mirror. BUT unlike budget reads
  in sibling packages, settlement WRITES fail CLOSED when a configured store
  errors: without durable dedup there is no safe way to guarantee a payment is
  credited exactly once. No store at all (dev) = memory fallback is fine.

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

  @doc "Record one settled payment: %{idempotency_key, beneficiary, amount_usd, method, ref, namespace, at}."
  @callback record_payment(map()) :: :ok | {:error, term()}

  @doc "Last fully-settled block for a chain name; {:ok, nil} when never scanned."
  @callback get_last_scanned_block(String.t()) :: {:ok, non_neg_integer() | nil} | {:error, term()}

  @doc "Advance the scan cursor for a chain (only after all its settlements recorded)."
  @callback put_last_scanned_block(String.t(), non_neg_integer()) :: :ok | {:error, term()}

  @doc "Settled payments for a beneficiary, newest first."
  @callback list_payments(String.t()) :: {:ok, [map()]} | {:error, term()}

  @optional_callbacks put_address_binding: 1,
                      get_address_binding: 1,
                      list_address_bindings: 0,
                      payment_seen?: 1,
                      record_payment: 1,
                      get_last_scanned_block: 1,
                      put_last_scanned_block: 2,
                      list_payments: 1
end
