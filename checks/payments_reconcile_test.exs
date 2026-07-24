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
  end

  def calls, do: :persistent_term.get({__MODULE__, :calls}, [])

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
       max_seq: Enum.reduce(rows, 0, &max(&1.outbox_seq, &2))
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

ReconcileStore.reset(
  [row, drift_row, unverifiable_row, legacy_row],
  %{
    beneficiary: "budget:reconcile",
    index: 0,
    address: binding_address,
    namespace: "llm_quota"
  }
)

{:ok, events} = Agent.start_link(fn -> [] end)

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

rpc_fn = fn chain, method, [tx_hash] ->
  true = chain.rpc_url == "https://independent.example/rpc"
  true = method == "eth_getTransactionReceipt"

  case tx_hash do
    "0xGOOD" -> {:ok, receipt.("0xGOOD", 2_500_000)}
    "0xDRIFT" -> {:ok, receipt.("0xDRIFT", 2_400_000)}
  end
end

state =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
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
  reply == %{
    "ok" => true,
    "checked" => 2,
    "drift" => ["8453:0xDRIFT:0"],
    "unverifiable" => 1,
    "legacy" => 1
  }
)

Check.check(
  f,
  "raw_amount mismatch emits the drift metric with idempotency key",
  Enum.any?(metric_events, fn {event, meta} ->
    event == "payments_reconcile_drift" and meta.idempotency_key == "8453:0xDRIFT:0" and
      "raw_amount" in meta.reasons
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
  "reconcile limit clamps to [1, 200]",
  Enum.any?(ReconcileStore.calls(), &(&1 == {0, 200}))
)

bad_reconcile_url =
  try do
    Payments.init!(%{
      xpub: xpub,
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
