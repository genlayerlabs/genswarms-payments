# Standalone — NO Postgres, NO network.  mix run checks/payments_sweep_test.exs
#
# D4 — "measure the sweep, don't design it". The report exists so consolidation
# economics stop being a guess: how many derived addresses hold a balance, how
# much in total, and which is the largest.
#
# Three properties, in the order they can hurt:
#
#   1. READ-ONLY. This hub holds an xPUB; it CANNOT move a coin. The report
#      must never even ask a node to: every call is `eth_call` carrying the
#      ERC-20 `balanceOf(address)` selector against the configured token.
#   2. BOUNDED. One chain call per address, capped by `limit` — an operator
#      keystroke can never issue unbounded RPC work.
#   3. AN UNREADABLE BALANCE IS NEVER A ZERO. A node that fails on one address
#      is reported as `unreadable`; folding it into "zero" would understate the
#      exact number a sweep decision is made on.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

ns = "llm_quota"
token = "0x036CbD53842c5426634e7929541eC2318f3dCF7e"

bindings =
  for index <- 0..5 do
    {:ok, address} = Genswarms.Payments.HD.address(Genswarms.Payments.HD.parse_xpub(xpub) |> elem(1), index)
    %{beneficiary: "llmb_#{index}", index: index, address: address, namespace: ns}
  end

defmodule SweepStore do
  def put_address_binding(_binding), do: :ok
  def list_address_bindings, do: {:ok, :persistent_term.get({__MODULE__, :bindings})}
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
  def payment_seen?(_key), do: {:ok, false}
  # A sweep must never write. If the report ever reaches a write callback the
  # check fails loudly instead of quietly recording a settlement.
  def record_payment(_payment), do: raise("sweep_report must never record a payment")
  def list_settlements_since(_after, _limit), do: {:ok, %{settlements: [], max_seq: 0}}
end

:persistent_term.put({SweepStore, :bindings}, bindings)

calls = :ets.new(:payments_sweep_calls, [:public, :set])
:ets.insert(calls, {:log, []})

log_call = fn chain, method, params ->
  [{:log, log}] = :ets.lookup(calls, :log)
  :ets.insert(calls, {:log, log ++ [{Map.get(chain, :name), method, params}]})
end

logged = fn ->
  [{:log, log}] = :ets.lookup(calls, :log)
  log
end

hex_balance = fn units ->
  "0x" <> String.pad_leading(Integer.to_string(units, 16), 64, "0")
end

# address 0 holds 2.5 USDC, address 1 holds 10 USDC, address 3 is unreadable,
# every other address is empty.
balances = %{
  Enum.at(bindings, 0).address => {:ok, hex_balance.(2_500_000)},
  Enum.at(bindings, 1).address => {:ok, hex_balance.(10_000_000)},
  Enum.at(bindings, 3).address => {:error, {:rpc, -32_000, "node overloaded"}}
}

rpc_fn = fn chain, method, params ->
  log_call.(chain, method, params)

  case {method, params} do
    {"eth_call", [%{"data" => "0x70a08231" <> padded}, "latest"]} ->
      suffix = String.slice(padded, -40, 40)

      address =
        Enum.find(bindings, &(String.downcase(String.replace(&1.address, "0x", "")) == suffix))

      Map.get(balances, address && address.address, {:ok, hex_balance.(0)})

    _ ->
      {:error, :unexpected_call}
  end
end

state =
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    namespace: ns,
    trusted_sources: ["ops"],
    operator_sources: ["ops"],
    targets: [],
    store_mod: SweepStore,
    rpc_fn: rpc_fn,
    auto_tick: false,
    chains: [
      %{
        name: "base-sepolia",
        chain_id: 84_532,
        rpc_url: "https://sepolia.base.org",
        usdc_contract: token,
        confirmations: 5,
        decimals: 6
      }
    ]
  })

ask = fn state, from, msg ->
  {:reply, json, state} = Payments.handle_message(from, Jason.encode!(msg), state)
  {Jason.decode!(json), state}
end

Check.check(f, "sanity: the hub loaded all six bindings", map_size(state.bindings) == 6)

{report, state} = ask.(state, "ops", %{action: "sweep_report"})

Check.check(
  f,
  "sweep_report answers with the measured aggregate",
  report["ok"] == true and report["chain"] == "base-sepolia" and
    report["addresses_checked"] == 6 and report["bindings_total"] == 6 and
    report["complete"] == true
)

Check.check(
  f,
  "non-zero addresses, total and largest are reported as the sweep decision needs them",
  report["nonzero"] == 2 and report["total_usd"] == "12.5" and
    report["largest"]["balance_usd"] == "10" and
    report["largest"]["address"] == Enum.at(bindings, 1).address
)

Check.check(
  f,
  "an unreadable balance is counted as UNREADABLE, never folded into zero",
  report["unreadable"] == 1
)

Check.check(
  f,
  "only non-zero rows are enumerated, each with its beneficiary/index/address",
  Enum.map(report["addresses"], & &1["index"]) == [1, 0] and
    Enum.all?(report["addresses"], &(&1["beneficiary"] != nil and &1["address"] != nil))
)

Check.check(
  f,
  "READ-ONLY: every chain call is an eth_call carrying the balanceOf selector against the configured token",
  Enum.all?(logged.(), fn {_chain, method, params} ->
    method == "eth_call" and
      match?([%{"to" => ^token, "data" => "0x70a08231" <> _}, "latest"], params)
  end)
)

Check.check(
  f,
  "BOUNDED: exactly one call per checked address, no more",
  length(logged.()) == 6
)

:ets.insert(calls, {:log, []})

{limited, state} = ask.(state, "ops", %{action: "sweep_report", limit: 2})

Check.check(
  f,
  "the limit bounds the chain work and the report says the view is incomplete",
  limited["addresses_checked"] == 2 and limited["complete"] == false and
    length(logged.()) == 2
)

# ── R4-P4-I6: bounded in TIME, not only in count ───────────────────────────
# Every address is one sequential synchronous RPC inside this object's
# GenServer callback, so the count cap alone is a mailbox stall measured in
# minutes on an operator keystroke — taken, by construction, exactly when the
# RPC endpoint is degraded. The report must come back PARTIAL rather than hold
# the money hub.
:ets.insert(calls, {:log, []})

slow_state = %{
  state
  | sweep_budget_ms: 60,
    rpc_fn: fn chain, method, params ->
      Process.sleep(40)
      rpc_fn.(chain, method, params)
    end
}

{timed_out, _} = ask.(slow_state, "ops", %{action: "sweep_report"})

Check.check(
  f,
  "a slow endpoint spends the wall-clock budget and returns PARTIAL results, never a wedged hub",
  timed_out["ok"] == true and timed_out["budget_spent"] == true and
    timed_out["complete"] == false and timed_out["addresses_checked"] < 6 and
    timed_out["remaining"] == 6 - timed_out["addresses_checked"]
)

Check.check(
  f,
  "the truncated run stopped issuing chain calls the moment the budget was spent",
  length(logged.()) == timed_out["addresses_checked"]
)

:ets.insert(calls, {:log, []})
{capped, _} = ask.(state, "ops", %{action: "sweep_report", limit: 10_000})

Check.check(
  f,
  "an oversized limit is clamped by the hub's own hard cap, never honoured",
  capped["addresses_checked"] == 6 and capped["complete"] == true and
    capped["remaining"] == 0 and capped["budget_spent"] == false
)

:ets.insert(calls, {:log, []})
{partial, state} = ask.(state, "ops", %{action: "sweep_report", limit: 4})

Check.check(
  f,
  "a partial report says how much it did NOT measure, instead of implying a full sweep",
  partial["complete"] == false and partial["remaining"] == 2 and
    partial["addresses_checked"] == 4 and length(logged.()) == 4
)

{bad_chain, state} = ask.(state, "ops", %{action: "sweep_report", chain: "nope"})

Check.check(
  f,
  "an unknown chain refuses rather than silently measuring the wrong one",
  bad_chain["ok"] == false and bad_chain["error"] == "unknown_chain"
)

{bad_limit, _state} = ask.(state, "ops", %{action: "sweep_report", limit: "all"})

Check.check(
  f,
  "a non-integer limit is a bad_request, never an unbounded scan",
  bad_limit["ok"] == false and bad_limit["error"] == "bad_request"
)

# Two chains and no `chain` argument: refuse rather than guess which token the
# operator meant — a balance reported against the wrong contract is worse than
# no balance.
multi_state =
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    namespace: ns,
    trusted_sources: ["ops"],
    operator_sources: ["ops"],
    targets: [],
    store_mod: SweepStore,
    rpc_fn: rpc_fn,
    auto_tick: false,
    chains: [
      %{
        name: "base-sepolia",
        chain_id: 84_532,
        rpc_url: "https://sepolia.base.org",
        usdc_contract: token,
        confirmations: 5,
        decimals: 6
      },
      %{
        name: "base",
        chain_id: 8453,
        rpc_url: "https://mainnet.base.org",
        usdc_contract: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        confirmations: 5,
        decimals: 6
      }
    ]
  })

{ambiguous, _} = ask.(multi_state, "ops", %{action: "sweep_report"})

Check.check(
  f,
  "with several chains configured the report refuses to guess which one to measure",
  ambiguous["ok"] == false and ambiguous["error"] == "chain_required"
)

Check.finish(f)
