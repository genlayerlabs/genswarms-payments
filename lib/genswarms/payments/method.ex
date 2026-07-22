defmodule Genswarms.Payments.Method do
  @moduledoc """
  A payment modality. Pull methods implement `poll/2` (invoked every tick with
  the core context); push methods implement `ingest_event/2` and MUST verify
  the payload's authenticity (webhook signature, facilitator sig) before
  returning settlements — the core trusts what a method returns. Settlements:
  %{beneficiary, amount_usd: Decimal, method, ref, idempotency_key, namespace}.
  """

  @type settlement :: map()
  @type core :: map()

  @typedoc """
  One chain's scan result for a poll round: the settlements found plus the
  block the range was safely scanned through (`safe_to`), or `nil` when the
  chain wasn't advanceable this round (RPC error, or nothing new yet) — the
  core only writes the cursor when `safe_to` is non-nil AND every settlement
  in that chain's list settled (recorded or already-seen); a settlement held
  back by a store failure keeps the cursor put so the next round re-presents.
  """
  @type chain_result :: {chain :: map(), [settlement()], safe_to :: non_neg_integer() | nil}

  @callback id() :: String.t()
  @callback capabilities() :: [:deposit_address | :intent]
  @callback poll(method_state :: map(), core()) :: {[chain_result()], map()}
  @callback ingest_event(payload :: map(), core()) :: {:ok, [settlement()]} | {:error, term()}

  @optional_callbacks poll: 2, ingest_event: 2
end
