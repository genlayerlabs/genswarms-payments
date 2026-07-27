Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

binding_address = "0x9858EfFD232B4033E47d90003D41EC34EcaEda94"
from_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

defmodule ReconcileStore do
  def reset(rows, binding) do
    :persistent_term.put({__MODULE__, :rows}, rows)
    :persistent_term.put({__MODULE__, :binding}, binding)
    :persistent_term.put({__MODULE__, :calls}, [])
    :persistent_term.put({__MODULE__, :max_seq}, nil)
  end

  def calls, do: :persistent_term.get({__MODULE__, :calls}, [])
  def max_seq!(max_seq), do: :persistent_term.put({__MODULE__, :max_seq}, max_seq)

  def list_settlements_since(after_seq, limit) do
    :persistent_term.put({__MODULE__, :calls}, calls() ++ [{after_seq, limit}])
    rows = :persistent_term.get({__MODULE__, :rows})

    {:ok,
     %{
       settlements:
         rows
         |> Enum.filter(&(&1.outbox_seq > after_seq))
         |> Enum.sort_by(& &1.outbox_seq)
         |> Enum.take(limit),
       max_seq:
         :persistent_term.get(
           {__MODULE__, :max_seq},
           Enum.reduce(rows, 0, &max(&1.outbox_seq, &2))
         ) || Enum.reduce(rows, 0, &max(&1.outbox_seq, &2))
     }}
  end

  def list_address_bindings, do: {:ok, [:persistent_term.get({__MODULE__, :binding})]}
  def put_address_binding(_binding), do: :ok
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_row), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
end

topic_address = fn "0x" <> hex ->
  "0x" <> String.duplicate("0", 24) <> String.downcase(hex)
end

row = %{
  beneficiary: "budget:reconcile",
  amount_usd: Decimal.new("2.5"),
  method: "usdc_base",
  ref: "0xGOOD:0",
  idempotency_key: "8453:0xGOOD:0",
  namespace: "llm_quota",
  at: ~U[2026-07-25 10:00:00Z],
  outbox_seq: 1,
  raw_amount: 2_500_000,
  decimals: 6,
  token_contract: "0xCONTRACT",
  chain: "base",
  chain_id: 8453,
  block_number: 150,
  log_index: 0,
  tx_hash: "0xGOOD",
  from_address: from_address
}

drift_row = %{
  row
  | idempotency_key: "8453:0xDRIFT:0",
    ref: "0xDRIFT:0",
    tx_hash: "0xDRIFT",
    from_address: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    outbox_seq: 2
}

unverifiable_row = %{
  row
  | idempotency_key: "8454:0xNOENDPOINT:0",
    ref: "0xNOENDPOINT:0",
    chain: "other",
    chain_id: 8454,
    tx_hash: "0xNOENDPOINT",
    outbox_seq: 3
}

legacy_row =
  row
  |> Map.drop([:raw_amount, :decimals, :token_contract, :chain_id, :from_address])
  |> Map.merge(%{
    idempotency_key: "base:legacy:0",
    ref: "legacy:0",
    tx_hash: "legacy",
    outbox_seq: 4
  })

incomplete_row =
  row
  |> Map.delete(:from_address)
  |> Map.merge(%{
    idempotency_key: "8453:0xINCOMPLETE:0",
    ref: "0xINCOMPLETE:0",
    tx_hash: "0xINCOMPLETE",
    outbox_seq: 5
  })

ReconcileStore.reset(
  [row, drift_row, unverifiable_row, legacy_row, incomplete_row],
  %{
    beneficiary: "budget:reconcile",
    index: 0,
    address: binding_address,
    namespace: "llm_quota"
  }
)

{:ok, events} = Agent.start_link(fn -> [] end)
{:ok, rpc_calls} = Agent.start_link(fn -> 0 end)

receipt = fn tx_hash, raw_amount ->
  %{
    "transactionHash" => tx_hash,
    "blockNumber" => "0x96",
    "logs" => [
      %{
        "address" => "0xcontract",
        "topics" => [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          topic_address.(from_address),
          topic_address.(binding_address)
        ],
        "data" => "0x" <> Integer.to_string(raw_amount, 16),
        "blockNumber" => "0x96",
        "logIndex" => "0x0",
        "transactionHash" => tx_hash
      }
    ]
  }
end

# C2: the finality leg asks the SAME independent endpoint for the `finalized`
# block tag. `finalized_head` is what that endpoint answers with; nil stands
# for a node that does not serve the tag (JSON-RPC null).
finalized_head = fn -> :persistent_term.get({__MODULE__, :finalized_head}, 200) end
set_finalized_head = fn head -> :persistent_term.put({__MODULE__, :finalized_head}, head) end

{:ok, finality_calls} = Agent.start_link(fn -> 0 end)

rpc_fn = fn chain, method, params ->
  true = chain.rpc_url == "https://independent.example/rpc"

  case {method, params} do
    {"eth_getTransactionReceipt", [tx_hash]} ->
      Agent.update(rpc_calls, &(&1 + 1))

      case tx_hash do
        "0xGOOD" -> {:ok, receipt.("0xGOOD", 2_500_000)}
        "0xDRIFT" -> {:ok, receipt.("0xDRIFT", 2_400_000)}
      end

    {"eth_getBlockByNumber", ["finalized", false]} ->
      Agent.update(finality_calls, &(&1 + 1))

      case finalized_head.() do
        nil -> {:ok, nil}
        head -> {:ok, %{"number" => "0x" <> Integer.to_string(head, 16)}}
      end

    {"eth_blockNumber", []} ->
      Agent.update(finality_calls, &(&1 + 1))
      {:ok, "0x" <> Integer.to_string(finalized_head.() + 20, 16)}
  end
end

state =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    allow_test_xpub: true,
    trusted_sources: ["ops"],
    targets: [],
    namespace: "llm_quota",
    store_mod: ReconcileStore,
    auto_tick: false,
    rpc_fn: rpc_fn,
    metrics_fn: fn event, meta -> Agent.update(events, &[{event, meta} | &1]) end,
    chains: [
      %{
        name: "base",
        chain_id: 8453,
        rpc_url: "https://primary.example/rpc",
        reconcile_rpc_url: "https://independent.example/rpc",
        usdc_contract: "0xCONTRACT"
      },
      %{
        name: "other",
        chain_id: 8454,
        rpc_url: "https://primary-other.example/rpc",
        usdc_contract: "0xCONTRACT"
      }
    ]
  })

untrusted = Payments.handle_message("stranger", Jason.encode!(%{action: "reconcile"}), state)
Check.check(f, "reconcile is trusted-source gated", match?({:noreply, ^state}, untrusted))

{:reply, json, _} =
  Payments.handle_message(
    "ops",
    Jason.encode!(%{action: "reconcile", limit: 50_000}),
    state
  )

reply = Jason.decode!(json)
metric_events = Agent.get(events, & &1)

Check.check(
  f,
  "reconcile detects drift without mutating credit state",
  Map.drop(reply, ["elapsed_ms"]) == %{
    "ok" => true,
    "checked" => 2,
    "drift" => ["8453:0xDRIFT:0"],
    "unverifiable" => 1,
    "legacy" => 1,
    "incomplete" => 1,
    "unfinalized" => 0,
    "finality_unverifiable" => 1
  } and is_integer(reply["elapsed_ms"]) and reply["elapsed_ms"] >= 0
)

# C2: the finality leg is informational and INDEPENDENT of the receipt leg —
# the base rows sit at block 150 under a finalized head of 200, so they are
# final; the chain without a second endpoint cannot be finality-checked at all
# and is reported as such rather than assumed finalized.
Check.check(
  f,
  "a chain with no reconcile endpoint reports finality_unverifiable, never finalized",
  Enum.any?(metric_events, fn {event, meta} ->
    event == "payments_reconcile_finality_unverifiable" and meta.chain == "other" and
      meta.reason == "reconcile_rpc_url_missing"
  end)
)

Check.check(
  f,
  "the finalized head is fetched ONCE per chain per run, not once per row",
  Agent.get(finality_calls, & &1) == 1 and
    Enum.count(metric_events, fn {event, meta} ->
      event == "payments_reconcile_finality_unverifiable" and meta.chain == "other"
    end) == 1
)

Check.check(
  f,
  "receipt sender is compared with stored from_address in addition to the live binding",
  Enum.any?(metric_events, fn {event, meta} ->
    event == "payments_reconcile_drift" and meta.idempotency_key == "8453:0xDRIFT:0" and
      "raw_amount" in meta.reasons and "from_address" in meta.reasons
  end)
)

Check.check(
  f,
  "missing second endpoint is counted and metered as unverifiable",
  Enum.any?(metric_events, fn {event, meta} ->
    event == "payments_reconcile_unverifiable" and
      meta.idempotency_key == "8454:0xNOENDPOINT:0" and
      meta.reason == :reconcile_rpc_url_missing
  end)
)

Check.check(
  f,
  "pre-0.2.0 row is counted as legacy and never silently skipped",
  Enum.any?(metric_events, fn {event, meta} ->
    event == "payments_reconcile_unverifiable" and
      meta.idempotency_key == "base:legacy:0" and meta.reason == "legacy_unverifiable"
  end)
)

Check.check(
  f,
  "0.2.0-era partial-fact row is counted and metered as incomplete",
  Enum.any?(metric_events, fn {event, meta} ->
    event == "payments_reconcile_incomplete" and
      meta.idempotency_key == "8453:0xINCOMPLETE:0" and
      meta.missing_facts == [:from_address]
  end)
)

Check.check(
  f,
  "reconcile limit clamps to [1, 200]",
  Enum.any?(ReconcileStore.calls(), &(&1 == {0, 200}))
)

ReconcileStore.reset(
  [row, drift_row],
  %{
    beneficiary: "budget:reconcile",
    index: 0,
    address: binding_address,
    namespace: "llm_quota"
  }
)

Agent.update(rpc_calls, fn _ -> 0 end)

{:reply, bounded_json, _} =
  Payments.handle_message(
    "ops",
    Jason.encode!(%{action: "reconcile", limit: 1}),
    state
  )

bounded_reply = Jason.decode!(bounded_json)

Check.check(
  f,
  "reconcile makes at most limit receipt RPCs and reports elapsed_ms",
  Agent.get(rpc_calls, & &1) == 1 and bounded_reply["ok"] == true and
    is_integer(bounded_reply["elapsed_ms"]) and bounded_reply["elapsed_ms"] >= 0
)

ReconcileStore.reset(
  [row],
  %{
    beneficiary: "budget:reconcile",
    index: 0,
    address: binding_address,
    namespace: "llm_quota"
  }
)

{:reply, missing_binding_json, _} =
  Payments.handle_message(
    "ops",
    Jason.encode!(%{action: "reconcile"}),
    %{state | bindings: %{}}
  )

missing_binding_reply = Jason.decode!(missing_binding_json)

Check.check(
  f,
  "missing live binding is unverifiable rather than false destination drift",
  missing_binding_reply["checked"] == 0 and missing_binding_reply["drift"] == [] and
    missing_binding_reply["unverifiable"] == 1
)

ReconcileStore.reset(
  [row],
  %{
    beneficiary: "budget:reconcile",
    index: 0,
    address: binding_address,
    namespace: "llm_quota"
  }
)

invalid_limit_replies =
  [1.0, "1", -1]
  |> Enum.map(fn limit ->
    {:reply, reply_json, _} =
      Payments.handle_message(
        "ops",
        Jason.encode!(%{action: "reconcile", limit: limit}),
        state
      )

    Jason.decode!(reply_json)
  end)

Check.check(
  f,
  "reconcile refuses non-integer and negative limits as bad_request",
  Enum.all?(invalid_limit_replies, &match?(%{"ok" => false, "error" => "bad_request"}, &1)) and
    ReconcileStore.calls() == []
)

lying_row = %{row | outbox_seq: 10}

ReconcileStore.reset(
  [lying_row],
  %{
    beneficiary: "budget:reconcile",
    index: 0,
    address: binding_address,
    namespace: "llm_quota"
  }
)

ReconcileStore.max_seq!(3)

{:reply, invalid_store_json, _} =
  Payments.handle_message(
    "ops",
    Jason.encode!(%{action: "reconcile"}),
    state
  )

Check.check(
  f,
  "reconcile refuses a row whose outbox_seq exceeds max_seq",
  match?(
    %{"ok" => false, "error" => "invalid_store_result"},
    Jason.decode!(invalid_store_json)
  )
)

poisoned_key_row = %{drift_row | idempotency_key: fn -> :poison end, outbox_seq: 1}

ReconcileStore.reset(
  [poisoned_key_row],
  %{
    beneficiary: "budget:reconcile",
    index: 0,
    address: binding_address,
    namespace: "llm_quota"
  }
)

poisoned_reconcile =
  try do
    {:reply, reply_json, _} =
      Payments.handle_message(
        "ops",
        Jason.encode!(%{action: "reconcile"}),
        state
      )

    {:ok, Jason.decode!(reply_json)}
  rescue
    error -> {:raised, error}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(
  f,
  "reconcile reply encoding failure refuses as encode_failed without raising",
  match?({:ok, %{"ok" => false, "error" => "encode_failed"}}, poisoned_reconcile)
)

# ── C2: finality legs ───────────────────────────────────────────────────────
# Crediting happens at fast_credit_depth (shallow, bounded by C1's caps).
# Reconciliation is where finality is QUERIED: a settled row above the
# `finalized` head is reported as unfinalized (informational — a credit is
# never reversed here), and an endpoint that cannot answer the tag is
# `finality_unverifiable`, NEVER silently treated as finalized.
reset_reconcile = fn ->
  ReconcileStore.reset(
    [row],
    %{
      beneficiary: "budget:reconcile",
      index: 0,
      address: binding_address,
      namespace: "llm_quota"
    }
  )

  Agent.update(events, fn _ -> [] end)
end

reconcile_now = fn st ->
  {:reply, json, _} = Payments.handle_message("ops", Jason.encode!(%{action: "reconcile"}), st)
  {Jason.decode!(json), Agent.get(events, & &1)}
end

reset_reconcile.()
set_finalized_head.(100)
{unfinalized_reply, unfinalized_events} = reconcile_now.(state)

Check.check(
  f,
  "a settled row above the finalized head is counted and metered as unfinalized",
  unfinalized_reply["unfinalized"] == 1 and unfinalized_reply["checked"] == 1 and
    unfinalized_reply["drift"] == [] and
    Enum.any?(unfinalized_events, fn {event, meta} ->
      event == "payments_reconcile_unfinalized" and meta.idempotency_key == "8453:0xGOOD:0" and
        meta.block_number == 150 and meta.finality_head == 100
    end)
)

reset_reconcile.()
set_finalized_head.(nil)
{null_finality_reply, null_finality_events} = reconcile_now.(state)

Check.check(
  f,
  "a null answer to the finalized tag is unverifiable, never treated as finalized",
  null_finality_reply["finality_unverifiable"] == 1 and
    null_finality_reply["unfinalized"] == 0 and
    Enum.any?(null_finality_events, fn {event, meta} ->
      event == "payments_reconcile_finality_unverifiable" and meta.chain == "base" and
        meta.reason == "finalized_tag_unsupported"
    end)
)

confirmations_state =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    allow_test_xpub: true,
    trusted_sources: ["ops"],
    targets: [],
    namespace: "llm_quota",
    store_mod: ReconcileStore,
    auto_tick: false,
    rpc_fn: rpc_fn,
    metrics_fn: fn event, meta -> Agent.update(events, &[{event, meta} | &1]) end,
    chains: [
      %{
        name: "base",
        chain_id: 8453,
        rpc_url: "https://primary.example/rpc",
        reconcile_rpc_url: "https://independent.example/rpc",
        usdc_contract: "0xCONTRACT",
        finality: {:confirmations, 5}
      }
    ]
  })

reset_reconcile.()
set_finalized_head.(100)
{confirmations_reply, _} = reconcile_now.(confirmations_state)

Check.check(
  f,
  "finality: {:confirmations, n} derives the head from eth_blockNumber instead of the tag",
  confirmations_reply["unfinalized"] == 1 and confirmations_reply["finality_unverifiable"] == 0
)

set_finalized_head.(200)

# M5: the head answered fine but the ROW's block_number is unreadable. That
# is not finalized and not unfinalized — it is unverifiable, and it used to be
# a number in the reply with nothing in telemetry behind it.
unparseable_block_row = %{
  row
  | idempotency_key: "8453:0xBADBLOCK:0",
    ref: "0xBADBLOCK:0",
    block_number: "not-a-block"
}

ReconcileStore.reset(
  [unparseable_block_row],
  %{
    beneficiary: "budget:reconcile",
    index: 0,
    address: binding_address,
    namespace: "llm_quota"
  }
)

Agent.update(events, fn _ -> [] end)
{bad_block_reply, bad_block_events} = reconcile_now.(state)

Check.check(
  f,
  "M5: a row with an unparseable block_number is counted AND metered as finality_unverifiable",
  bad_block_reply["finality_unverifiable"] == 1 and bad_block_reply["unfinalized"] == 0 and
    Enum.any?(bad_block_events, fn {event, meta} ->
      event == "payments_reconcile_finality_unverifiable" and
        meta.idempotency_key == "8453:0xBADBLOCK:0" and meta.chain == "base" and
        meta.reason == "block_number_unparseable"
    end)
)

# M6: a chain that declared `{:confirmations, n}` and never configured a
# second endpoint did not FAIL to answer — its operator opted out of the
# finality leg. The reply still refuses to call those rows finalized, but the
# alarm is reserved for endpoints that were asked and could not answer, so an
# upgrade does not hand legacy configs a metric that repeats forever.
opted_out_state =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    allow_test_xpub: true,
    trusted_sources: ["ops"],
    targets: [],
    namespace: "llm_quota",
    store_mod: ReconcileStore,
    auto_tick: false,
    rpc_fn: rpc_fn,
    metrics_fn: fn event, meta -> Agent.update(events, &[{event, meta} | &1]) end,
    chains: [
      %{
        name: "base",
        chain_id: 8453,
        rpc_url: "https://primary.example/rpc",
        usdc_contract: "0xCONTRACT",
        finality: {:confirmations, 5}
      }
    ]
  })

reset_reconcile.()
{opted_out_reply, opted_out_events} = reconcile_now.(opted_out_state)

Check.check(
  f,
  "M6: a {:confirmations, n} chain with no second endpoint still counts as finality_unverifiable",
  opted_out_reply["finality_unverifiable"] == 1 and opted_out_reply["unfinalized"] == 0
)

Check.check(
  f,
  "M6: ...but does NOT emit the per-run alarm — an opt-out is not a broken endpoint",
  not Enum.any?(opted_out_events, fn {event, _meta} ->
    event == "payments_reconcile_finality_unverifiable"
  end)
)

# The same chain WITHOUT the opt-out (default :finalized) keeps alarming.
not_opted_out_state = %{
  opted_out_state
  | chains: [
      %{
        name: "base",
        chain_id: 8453,
        rpc_url: "https://primary.example/rpc",
        usdc_contract: "0xCONTRACT"
      }
    ]
}

reset_reconcile.()
{_default_reply, default_events} = reconcile_now.(not_opted_out_state)

Check.check(
  f,
  "M6: the suppression is scoped to the opt-out — a default chain still alarms",
  Enum.any?(default_events, fn {event, meta} ->
    event == "payments_reconcile_finality_unverifiable" and meta.chain == "base" and
      meta.reason == "reconcile_rpc_url_missing"
  end)
)

invalid_finality =
  try do
    Payments.init!(%{
      xpub: xpub,
      allow_test_xpub: true,
      chains: [
        %{
          name: "base",
          chain_id: 8453,
          rpc_url: "https://primary.example/rpc",
          usdc_contract: "0xCONTRACT",
          finality: :probably
        }
      ]
    })
  rescue
    error -> {:raised, error}
  end

Check.check(
  f,
  "an unknown finality mode is refused at init",
  match?({:raised, %ArgumentError{}}, invalid_finality)
)

bad_reconcile_url =
  try do
    Payments.init!(%{
      xpub: xpub,
      allow_test_xpub: true,
      chains: [
        %{
          name: "base",
          chain_id: 8453,
          rpc_url: "https://primary.example/rpc",
          reconcile_rpc_url: "https://second.example/\"\noutput=/tmp/leak",
          usdc_contract: "0xCONTRACT"
        }
      ]
    })
  rescue
    error -> {:raised, error}
  end

Check.check(
  f,
  "reconcile_rpc_url receives the same curl-config injection validation as rpc_url",
  match?({:raised, %ArgumentError{}}, bad_reconcile_url)
)

Check.finish(f)
