# Standalone — NO Postgres, NO network.  mix run checks/payments_chain_self_check_test.exs
#
# D4 — before a chain is ever scanned the endpoint must PROVE it is the chain
# the config claims: eth_chainId == chain_id, and the token contract's
# decimals() == the configured decimals. Both are silent money bugs otherwise
# (an endpoint pointed at the wrong network credits deposits that never
# arrived; a decimals mismatch mis-scales every amount by orders of
# magnitude). It runs at the first TICK, not at boot — an RPC may simply be
# down at boot and a refusal there would crash-loop the object over a
# transient outage. A failed or unverifiable check holds THAT chain only, and
# is retried next tick so a healed RPC recovers on its own.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
pad_addr = fn "0x" <> hex -> "0x" <> String.duplicate("0", 24) <> String.downcase(hex) end
value_hex = "0x" <> String.pad_leading("4c4b40", 64, "0")

defmodule SelfCheckStore do
  def reset,
    do:
      :persistent_term.put(
        {__MODULE__, :d},
        %{seen: MapSet.new(), rows: [], cursor: %{}, bindings: []}
      )

  defp d, do: :persistent_term.get({__MODULE__, :d})
  defp put(key, value), do: :persistent_term.put({__MODULE__, :d}, Map.put(d(), key, value))

  def put_address_binding(binding), do: put(:bindings, [binding | d().bindings])
  def list_address_bindings, do: {:ok, d().bindings}
  def payment_seen?(key), do: {:ok, MapSet.member?(d().seen, key)}

  def record_payment(row) do
    put(:seen, MapSet.put(d().seen, row.idempotency_key))
    put(:rows, [row | d().rows])
    :ok
  end

  def rows, do: d().rows
  def get_last_scanned_block(chain), do: {:ok, Map.get(d().cursor, chain)}
  def put_last_scanned_block(chain, block), do: put(:cursor, Map.put(d().cursor, chain, block))
  def cursor(chain), do: Map.get(d().cursor, chain)
end

mk_log = fn to_addr, block, tx, index ->
  %{
    "address" => "0xCONTRACT",
    "topics" => [transfer_sig, pad_addr.("0x" <> String.duplicate("a", 40)), pad_addr.(to_addr)],
    "data" => value_hex,
    "blockNumber" => "0x" <> Integer.to_string(block, 16),
    "transactionHash" => tx,
    "logIndex" => "0x" <> Integer.to_string(index, 16)
  }
end

chain = %{
  name: "base",
  chain_id: 8453,
  rpc_url: "injected",
  usdc_contract: "0xCONTRACT",
  confirmations: 10,
  decimals: 6,
  start_block: 100,
  max_block_range: 1000
}

boot = fn events, chains ->
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    allow_test_xpub: true,
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: SelfCheckStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-24 12:00:00Z] end,
    deliver_fn: fn _target, _from, _content -> :ok end,
    metrics_fn: fn event, meta -> Agent.update(events, &[{event, meta} | &1]) end,
    chains: chains,
    rpc_fn: nil
  })
end

# An endpoint answering everything correctly EXCEPT the fields under test.
endpoint = fn opts ->
  fn chain, method, params ->
    Agent.update(opts.log, &[{to_string(chain.name), method} | &1])

    case method do
      "eth_chainId" -> opts.chain_id.()
      "eth_call" -> opts.decimals.()
      "eth_blockNumber" -> {:ok, "0xc8"}
      "eth_getLogs" -> {:ok, opts.logs.(params)}
    end
  end
end

truthful_chain_id = fn -> {:ok, "0x2105"} end
truthful_decimals = fn -> {:ok, "0x" <> String.pad_leading("6", 64, "0")} end

# ── chain_id mismatch ───────────────────────────────────────────────────────
SelfCheckStore.reset()
{:ok, events} = Agent.start_link(fn -> [] end)
{:ok, log} = Agent.start_link(fn -> [] end)

state = boot.(events, [chain])

{:reply, address_json, state} =
  Payments.handle_message(
    "ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:d4"}),
    state
  )

address = Jason.decode!(address_json)["address"]
logs = [mk_log.(address, 150, "0xD4", 0)]

wrong_chain_id =
  endpoint.(%{
    log: log,
    chain_id: fn -> {:ok, "0x1"} end,
    decimals: truthful_decimals,
    logs: fn _params -> logs end
  })

state_wrong = Payments.poll(%{state | rpc_fn: wrong_chain_id})

Check.check(
  f,
  "an endpoint on the WRONG chain is held: no getLogs, no settlement, no cursor",
  not Enum.any?(Agent.get(log, & &1), fn {_chain, method} -> method == "eth_getLogs" end) and
    SelfCheckStore.rows() == [] and SelfCheckStore.cursor("base") == nil
)

Check.check(
  f,
  "the chain_id mismatch alarms with both the configured and observed values",
  Enum.any?(Agent.get(events, & &1), fn {event, meta} ->
    event == "payments_chain_self_check_failed" and meta.chain == "base" and
      meta.reason == "chain_id_mismatch" and meta.configured == 8453
  end)
)

Check.check(
  f,
  "a failed self-check is NOT cached — the chain is re-verified next tick",
  MapSet.size(state_wrong.chain_self_checks) == 0
)

# ── decimals mismatch ───────────────────────────────────────────────────────
SelfCheckStore.reset()
Agent.update(events, fn _ -> [] end)
Agent.update(log, fn _ -> [] end)

wrong_decimals =
  endpoint.(%{
    log: log,
    chain_id: truthful_chain_id,
    decimals: fn -> {:ok, "0x" <> String.pad_leading("12", 64, "0")} end,
    logs: fn _params -> logs end
  })

_state_decimals = Payments.poll(%{state | rpc_fn: wrong_decimals})

Check.check(
  f,
  "a token contract whose decimals disagree with the config holds the chain",
  not Enum.any?(Agent.get(log, & &1), fn {_chain, method} -> method == "eth_getLogs" end) and
    SelfCheckStore.rows() == [] and
    Enum.any?(Agent.get(events, & &1), fn {event, meta} ->
      event == "payments_chain_self_check_failed" and meta.reason == "decimals_mismatch" and
        meta.configured == 6 and meta.token_contract == "0xCONTRACT"
    end)
)

Check.check(
  f,
  "the decimals mismatch reports what the token actually answered",
  Enum.any?(Agent.get(events, & &1), fn {event, meta} ->
    event == "payments_chain_self_check_failed" and meta.reason == "decimals_mismatch" and
      meta.observed_decimals == 18
  end)
)

# M4: "the endpoint said nothing readable" is the SAME hold but a completely
# different repair from "the token says another number" — a proxy address
# with no code answers `0x`, which is a broken endpoint, not a wrong config.
SelfCheckStore.reset()
Agent.update(events, fn _ -> [] end)
Agent.update(log, fn _ -> [] end)

empty_decimals =
  endpoint.(%{
    log: log,
    chain_id: truthful_chain_id,
    decimals: fn -> {:ok, "0x"} end,
    logs: fn _params -> logs end
  })

state_empty_decimals = Payments.poll(%{state | rpc_fn: empty_decimals})

Check.check(
  f,
  "M4: an unparseable decimals() answer holds the chain as decimals_unverifiable, NOT a mismatch",
  not Enum.any?(Agent.get(log, & &1), fn {_chain, method} -> method == "eth_getLogs" end) and
    SelfCheckStore.rows() == [] and MapSet.size(state_empty_decimals.chain_self_checks) == 0 and
    Enum.any?(Agent.get(events, & &1), fn {event, meta} ->
      event == "payments_chain_self_check_failed" and meta.reason == "decimals_unverifiable" and
        meta.method == "eth_call" and meta.configured == 6 and
        meta.token_contract == "0xCONTRACT"
    end) and
    not Enum.any?(Agent.get(events, & &1), fn {_event, meta} ->
      Map.get(meta, :reason) == "decimals_mismatch"
    end)
)

# A nil body (a node answering JSON-RPC null) takes the same label.
SelfCheckStore.reset()
Agent.update(events, fn _ -> [] end)

nil_decimals =
  endpoint.(%{
    log: log,
    chain_id: truthful_chain_id,
    decimals: fn -> {:ok, nil} end,
    logs: fn _params -> logs end
  })

_state_nil_decimals = Payments.poll(%{state | rpc_fn: nil_decimals})

Check.check(
  f,
  "M4: a null decimals() answer is unverifiable too, never read as a mismatch",
  Enum.any?(Agent.get(events, & &1), fn {event, meta} ->
    event == "payments_chain_self_check_failed" and meta.reason == "decimals_unverifiable"
  end) and
    not Enum.any?(Agent.get(events, & &1), fn {_event, meta} ->
      Map.get(meta, :reason) == "decimals_mismatch"
    end)
)

# ── unverifiable ⇒ held, and a healed RPC recovers ──────────────────────────
SelfCheckStore.reset()
Agent.update(events, fn _ -> [] end)
Agent.update(log, fn _ -> [] end)

down =
  endpoint.(%{
    log: log,
    chain_id: fn -> {:error, :timeout} end,
    decimals: truthful_decimals,
    logs: fn _params -> logs end
  })

state_down = Payments.poll(%{state | rpc_fn: down})

Check.check(
  f,
  "an unverifiable self-check holds the chain — it is never assumed correct",
  SelfCheckStore.rows() == [] and SelfCheckStore.cursor("base") == nil and
    Enum.any?(Agent.get(events, & &1), fn {event, meta} ->
      event == "payments_chain_self_check_failed" and meta.reason == "unverifiable" and
        meta.method == "eth_chainId"
    end)
)

healthy =
  endpoint.(%{
    log: log,
    chain_id: truthful_chain_id,
    decimals: truthful_decimals,
    logs: fn _params -> logs end
  })

Agent.update(log, fn _ -> [] end)
state_healed = Payments.poll(%{state_down | rpc_fn: healthy})

Check.check(
  f,
  "a healed RPC recovers by itself on the next tick — the held payment settles",
  Enum.map(SelfCheckStore.rows(), & &1.idempotency_key) == ["8453:0xD4:0"] and
    SelfCheckStore.cursor("base") == 190
)

Check.check(
  f,
  "a passed self-check is cached per chain",
  MapSet.member?(state_healed.chain_self_checks, "base")
)

Agent.update(log, fn _ -> [] end)
_state_cached = Payments.poll(state_healed)

Check.check(
  f,
  "a verified chain is not re-verified every tick (no repeat eth_chainId/eth_call)",
  not Enum.any?(Agent.get(log, & &1), fn {_chain, method} ->
    method in ["eth_chainId", "eth_call"]
  end)
)

# ── isolation: one bad chain must not take the others down ──────────────────
SelfCheckStore.reset()
Agent.update(events, fn _ -> [] end)
Agent.update(log, fn _ -> [] end)

two_chain_state =
  boot.(events, [
    Map.merge(chain, %{name: "impostor", chain_id: 999, start_block: 100}),
    chain
  ])

{:reply, two_json, two_chain_state} =
  Payments.handle_message(
    "ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:two"}),
    two_chain_state
  )

two_addr = Jason.decode!(two_json)["address"]
two_logs = [mk_log.(two_addr, 150, "0xTWO", 0)]

mixed = fn chain, method, _params ->
  Agent.update(log, &[{to_string(chain.name), method} | &1])

  case {chain.name, method} do
    {"impostor", "eth_chainId"} -> {:ok, "0x1"}
    {_name, "eth_chainId"} -> {:ok, "0x" <> Integer.to_string(chain.chain_id, 16)}
    {_name, "eth_call"} -> truthful_decimals.()
    {_name, "eth_blockNumber"} -> {:ok, "0xc8"}
    {_name, "eth_getLogs"} -> {:ok, two_logs}
  end
end

_two_chain_state = Payments.poll(%{two_chain_state | rpc_fn: mixed})

Check.check(
  f,
  "a chain that fails its self-check is held while the honest chain still settles",
  Enum.map(SelfCheckStore.rows(), & &1.idempotency_key) == ["8453:0xTWO:0"] and
    SelfCheckStore.cursor("base") == 190 and SelfCheckStore.cursor("impostor") == nil and
    not Enum.any?(Agent.get(log, & &1), &(&1 == {"impostor", "eth_getLogs"}))
)

# An endpoint seam that RAISES (a nonconforming provider, a broken injected
# fn) is unverifiable too — never an uncaught crash of the whole tick.
SelfCheckStore.reset()
Agent.update(events, fn _ -> [] end)

raising_result =
  try do
    {:ok, Payments.poll(%{state | rpc_fn: fn _chain, _method, _params -> raise "boom" end})}
  rescue
    error -> {:raised, error}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(
  f,
  "a raising RPC seam is unverifiable, not a crashed tick",
  match?({:ok, _}, raising_result) and SelfCheckStore.rows() == [] and
    Enum.any?(Agent.get(events, & &1), fn {event, meta} ->
      event == "payments_chain_self_check_failed" and meta.reason == "unverifiable"
    end)
)

# ── a chain with no token contract is gated on chain_id alone ───────────────
SelfCheckStore.reset()
Agent.update(events, fn _ -> [] end)
Agent.update(log, fn _ -> [] end)

contractless_state = boot.(events, [Map.delete(chain, :usdc_contract)])

chain_id_only = fn chain, method, _params ->
  Agent.update(log, &[{to_string(chain.name), method} | &1])

  case method do
    "eth_chainId" -> truthful_chain_id.()
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, []}
  end
end

contractless_result =
  try do
    {:ok, Payments.poll(%{contractless_state | rpc_fn: chain_id_only})}
  rescue
    error -> {:raised, error}
  end

Check.check(
  f,
  "a chain with no token contract self-checks on chain_id alone (no decimals call)",
  match?({:ok, _}, contractless_result) and
    not Enum.any?(Agent.get(log, & &1), fn {_chain, method} -> method == "eth_call" end)
)

# ── the check is a poll-time gate, not a boot-time one ─────────────────────
Check.check(
  f,
  "init/1 never calls the RPC: a down endpoint at boot does not crash-loop the object",
  match?(
    {:ok, %{chain_self_checks: %MapSet{}}},
    Payments.init(%{
      xpub: xpub,
      allow_test_xpub: true,
      targets: [],
      chains: [chain],
      rpc_fn: fn _chain, _method, _params -> raise "endpoint down at boot" end
    })
  )
)

Check.finish(f)
