Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

defmodule ScanStore do
  def reset do
    :persistent_term.put({__MODULE__, :d}, %{seen: MapSet.new(), rows: [], cursor: %{}, bindings: []})
  end
  defp d, do: :persistent_term.get({__MODULE__, :d})
  defp put(k, v), do: :persistent_term.put({__MODULE__, :d}, Map.put(d(), k, v))

  def seed_binding(b), do: put(:bindings, [b | d().bindings])
  def put_address_binding(b), do: put(:bindings, [b | d().bindings])
  def list_address_bindings, do: {:ok, d().bindings}
  def payment_seen?(k), do: {:ok, MapSet.member?(d().seen, k)}
  def record_payment(row) do
    put(:seen, MapSet.put(d().seen, row.idempotency_key))
    put(:rows, [row | d().rows])
    :ok
  end
  def rows, do: d().rows
  def get_last_scanned_block(chain), do: {:ok, Map.get(d().cursor, chain)}
  def put_last_scanned_block(chain, n), do: put(:cursor, Map.put(d().cursor, chain, n))
  def cursor(chain), do: Map.get(d().cursor, chain)
end

ScanStore.reset()

# Transfer(address,address,uint256) topic0
transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

pad_addr = fn "0x" <> hex -> "0x" <> String.duplicate("0", 24) <> String.downcase(hex) end
# 5 USDC with 6 decimals = 5_000_000 = 0x4c4b40
value_hex = "0x" <> String.pad_leading("4c4b40", 64, "0")

{:ok, rpc_log} = Agent.start_link(fn -> [] end)

state0 =
  Payments.init(%{
    name: :payments, xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt", trusted_sources: ["ingress"],
    targets: ["llm_proxy"], namespace: "llm_quota", store_mod: ScanStore,
    auto_tick: false, now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [%{name: "base", rpc_url: "injected", usdc_contract: "0xCONTRACT",
               confirmations: 10, decimals: 6, start_block: 100, max_block_range: 1000,
               address_chunk: 2}],
    rpc_fn: nil
  })

# bind a beneficiary so its address is watched
{:reply, j, state0} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:abc"}), state0)
addr = Jason.decode!(j)["address"]

canned = fn logs, latest ->
  fn _chain, method, params ->
    Agent.update(rpc_log, &[{method, params} | &1])
    case method do
      "eth_blockNumber" -> {:ok, "0x" <> Integer.to_string(latest, 16)}
      "eth_getLogs" -> {:ok, logs}
    end
  end
end

mk_log = fn to_addr, block, tx, idx ->
  %{"address" => "0xCONTRACT",
    "topics" => [transfer_sig, pad_addr.("0x" <> String.duplicate("a", 40)), pad_addr.(to_addr)],
    "data" => value_hex,
    "blockNumber" => "0x" <> Integer.to_string(block, 16),
    "transactionHash" => tx, "logIndex" => "0x" <> Integer.to_string(idx, 16)}
end

# latest=200, confirmations=10 ⇒ safe_to=190; one log at block 150 to OUR address,
# one to a stranger address (must be ignored), one UNCONFIRMED at 195 (must wait)
logs = [mk_log.(addr, 150, "0xT1", 0),
        mk_log.("0x" <> String.duplicate("b", 40), 151, "0xT2", 0),
        mk_log.(addr, 195, "0xT3", 0)]

state = %{state0 | rpc_fn: canned.(logs, 200)}
state = Payments.poll(state)

Check.check(f, "settled exactly the one confirmed payment to a bound address",
  length(ScanStore.rows()) == 1)

row = hd(ScanStore.rows())
Check.check(f, "amount converted at 6 decimals", Decimal.equal?(row.amount_usd, Decimal.new("5")))
Check.check(f, "beneficiary resolved from binding", row.beneficiary == "budget:abc")
Check.check(f, "method is usdc_<chain>", row.method == "usdc_base")
Check.check(f, "idempotency key is chain:tx:logIndex", row.idempotency_key == "base:0xT1:0")
Check.check(f, "cursor advanced to safe_to (190), NOT latest",
  ScanStore.cursor("base") == 190)

# unconfirmed log settles once the chain advances
state = %{state | rpc_fn: canned.(logs, 300)}
state = Payments.poll(state)
Check.check(f, "previously-unconfirmed log settles after confirmations",
  Enum.any?(ScanStore.rows(), &(&1.idempotency_key == "base:0xT3:0")))
Check.check(f, "no duplicate of the first payment",
  Enum.count(ScanStore.rows(), &(&1.idempotency_key == "base:0xT1:0")) == 1)

# getLogs range + address filter shape
calls = Agent.get(rpc_log, &Enum.reverse(&1))
get_logs = for {"eth_getLogs", [p]} <- calls, do: p
Check.check(f, "getLogs filters on the USDC contract + transfer topic",
  Enum.all?(get_logs, fn p ->
    p["address"] == "0xCONTRACT" and hd(p["topics"]) == transfer_sig
  end))

# a log from an unexpected contract address must never settle, even if
# otherwise valid (right transfer topic, right bound to-address, confirmed
# block) — a legit log in the same batch must still settle
ScanStore.reset()
fake_log = %{mk_log.(addr, 150, "0xFAKE", 0) | "address" => "0xEVILCONTRACT"}
legit_log = mk_log.(addr, 150, "0xLEGIT", 0)

state_evil = %{state0 | rpc_fn: canned.([fake_log, legit_log], 200)}
_state_evil = Payments.poll(state_evil)

Check.check(f, "log from an unexpected contract address is rejected while a legit log in the same batch settles",
  Enum.any?(ScanStore.rows(), &(&1.idempotency_key == "base:0xLEGIT:0")) and
    not Enum.any?(ScanStore.rows(), &(&1.ref == "0xFAKE:0")))

# RPC failure ⇒ cursor does not advance
ScanStore.reset()
ScanStore.seed_binding(%{beneficiary: "budget:abc", index: 0, address: addr, namespace: "llm_quota"})
state_err = %{state | rpc_fn: fn _, _, _ -> {:error, :timeout} end}
_state_err = Payments.poll(state_err)
Check.check(f, "RPC failure leaves cursor untouched (retry next tick)",
  ScanStore.cursor("base") == nil)

# address chunking: address_chunk 2, 3 bound addresses ⇒ 2 eth_getLogs calls per scanned range
ScanStore.reset()
ScanStore.seed_binding(%{beneficiary: "budget:abc", index: 0, address: addr, namespace: "llm_quota"})

state_chunk = Payments.init(%{
  name: :payments, xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt", trusted_sources: ["ingress"],
  targets: ["llm_proxy"], namespace: "llm_quota", store_mod: ScanStore,
  auto_tick: false, now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
  deliver_fn: fn _, _, _ -> :ok end,
  chains: [%{name: "base", rpc_url: "injected", usdc_contract: "0xCONTRACT",
             confirmations: 10, decimals: 6, start_block: 100, max_block_range: 1000,
             address_chunk: 2}],
  rpc_fn: nil
})

{:reply, jd, state_chunk} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:def"}), state_chunk)
_addr_def = Jason.decode!(jd)["address"]

{:reply, jg, state_chunk} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:ghi"}), state_chunk)
_addr_ghi = Jason.decode!(jg)["address"]

{:ok, rpc_log2} = Agent.start_link(fn -> [] end)
canned_empty = fn latest ->
  fn _chain, method, _params ->
    Agent.update(rpc_log2, &[method | &1])
    case method do
      "eth_blockNumber" -> {:ok, "0x" <> Integer.to_string(latest, 16)}
      "eth_getLogs" -> {:ok, []}
    end
  end
end

state_chunk = %{state_chunk | rpc_fn: canned_empty.(200)}
_state_chunk = Payments.poll(state_chunk)

get_logs_calls = Agent.get(rpc_log2, &Enum.count(&1, fn m -> m == "eth_getLogs" end))
Check.check(f, "address_chunk splits 3 bound addresses into 2 eth_getLogs calls",
  get_logs_calls == 2)

Check.finish(f)
