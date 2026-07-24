Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

# ── A1: a store whose list_address_bindings errors at boot must fail-flag,
# never fail-open — poll becomes a no-op, deposit_address is refused, and
# health reports the degraded state. Cursor callbacks on this same store
# are healthy, proving the flag is driven by list_address_bindings alone.
defmodule DegradedListStore do
  def reset, do: :persistent_term.put({__MODULE__, :d}, %{cursor: %{}, rows: [], seen: MapSet.new()})
  defp d, do: :persistent_term.get({__MODULE__, :d})
  defp put(k, v), do: :persistent_term.put({__MODULE__, :d}, Map.put(d(), k, v))

  def put_address_binding(_), do: {:error, :db_down}
  def list_address_bindings, do: {:error, :db_down}

  def payment_seen?(k), do: {:ok, MapSet.member?(d().seen, k)}

  def record_payment(row) do
    put(:seen, MapSet.put(d().seen, row.idempotency_key))
    put(:rows, [row | d().rows])
    :ok
  end

  def get_last_scanned_block(chain), do: {:ok, Map.get(d().cursor, chain)}
  def put_last_scanned_block(chain, n), do: (put(:cursor, Map.put(d().cursor, chain, n)); :ok)
  def rows, do: d().rows
  def cursor(chain), do: Map.get(d().cursor, chain)
end

DegradedListStore.reset()

transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
pad_addr = fn "0x" <> hex -> "0x" <> String.duplicate("0", 24) <> String.downcase(hex) end
value_hex = "0x" <> String.pad_leading("4c4b40", 64, "0")
some_addr = "0x" <> String.duplicate("c", 40)

{:ok, rpc_calls} = Agent.start_link(fn -> 0 end)

# rpc_fn that WOULD settle a payment if poll ever invoked it — a Transfer log
# to some_addr, confirmed. If poll is truly a no-op this is never called.
settling_rpc = fn _chain, method, _params ->
  Agent.update(rpc_calls, &(&1 + 1))

  case method do
    "eth_blockNumber" ->
      {:ok, "0xc8"}

    "eth_getLogs" ->
      {:ok,
       [
         %{
           "address" => "0xCONTRACT",
           "topics" => [transfer_sig, pad_addr.("0x" <> String.duplicate("a", 40)), pad_addr.(some_addr)],
           "data" => value_hex,
           "blockNumber" => "0x1",
           "transactionHash" => "0xT1",
           "logIndex" => "0x0"
         }
       ]}
  end
end

state =
  Payments.init!(%{
    name: :payments,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: DegradedListStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end,
    rpc_fn: settling_rpc,
    chains: [
      %{name: "base", rpc_url: "injected", usdc_contract: "0xCONTRACT", confirmations: 0, decimals: 6, start_block: 0}
    ]
  })

Check.check(f, "degraded_boot set true when a configured store's list_address_bindings errors",
  state.degraded_boot == true)

{:noreply, state} =
  Payments.handle_message("ingress", Jason.encode!(%{action: "tick"}), state)

Check.check(f, "poll is a no-op during degraded_boot: rpc_fn never invoked",
  Agent.get(rpc_calls, & &1) == 0)
Check.check(f, "poll is a no-op during degraded_boot: cursor never written",
  DegradedListStore.cursor("base") == nil)
Check.check(f, "poll is a no-op during degraded_boot: nothing settled",
  DegradedListStore.rows() == [])

{:reply, dep_json, state} =
  Payments.handle_message("ingress", Jason.encode!(%{action: "deposit_address", beneficiary: "budget:abc"}), state)
dep = Jason.decode!(dep_json)

Check.check(f, "deposit_address refused during degraded_boot",
  dep["ok"] == false and dep["error"] == "degraded_boot")

{:reply, health_json, _state} =
  Payments.handle_message("anyone", Jason.encode!(%{action: "health"}), state)
health = Jason.decode!(health_json)

Check.check(f, "health reports degraded_boot true", health["degraded_boot"] == true)

# ── A1 counterpart: a healthy store (list_address_bindings succeeds) boots
# normally — degraded_boot false, poll/deposit_address both work.
defmodule HealthyStore do
  def reset, do: :persistent_term.put({__MODULE__, :d}, %{bindings: [], cursor: %{}, rows: [], seen: MapSet.new()})
  defp d, do: :persistent_term.get({__MODULE__, :d})
  defp put(k, v), do: :persistent_term.put({__MODULE__, :d}, Map.put(d(), k, v))

  def put_address_binding(b), do: (put(:bindings, [b | d().bindings]); :ok)
  def list_address_bindings, do: {:ok, d().bindings}
  def payment_seen?(k), do: {:ok, MapSet.member?(d().seen, k)}

  def record_payment(row) do
    put(:seen, MapSet.put(d().seen, row.idempotency_key))
    put(:rows, [row | d().rows])
    :ok
  end

  def get_last_scanned_block(chain), do: {:ok, Map.get(d().cursor, chain)}
  def put_last_scanned_block(chain, n), do: (put(:cursor, Map.put(d().cursor, chain, n)); :ok)
end

HealthyStore.reset()

healthy_state =
  Payments.init!(%{
    name: :payments,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    store_mod: HealthyStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end
  })

Check.check(f, "healthy-store boot has degraded_boot false", healthy_state.degraded_boot == false)

{:reply, dep2_json, _} =
  Payments.handle_message("ingress", Jason.encode!(%{action: "deposit_address", beneficiary: "budget:abc"}), healthy_state)
Check.check(f, "healthy-store boot: deposit_address still works",
  Jason.decode!(dep2_json)["ok"] == true)

# ── A2: init-time store callback coherence — a store implementing only HALF
# of a safety group must raise, not boot silently misconfigured.
defmodule OnlyPutAddressStore do
  def put_address_binding(_), do: :ok
end

Check.check(f, "store exporting only put_address_binding (no list_address_bindings) raises at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init!(%{
         xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
         store_mod: OnlyPutAddressStore
       })
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

defmodule OnlySeenStore do
  def payment_seen?(_), do: {:ok, false}
end

Check.check(f, "store exporting only payment_seen? (no record_payment) raises at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init!(%{
         xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
         store_mod: OnlySeenStore
       })
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

Check.check(f, "store with both groups fully covered boots without raising",
  match?(%{}, Payments.init!(%{
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    store_mod: HealthyStore
  })))

# ── A3: in-memory cursor mirror — with NO store at all, a second poll over
# the same canned logs must not rescan the same window forever (the
# documented "missing callbacks fall back to memory" promise).
{:ok, rpc_log} = Agent.start_link(fn -> [] end)

canned = fn logs, latest ->
  fn _chain, method, params ->
    Agent.update(rpc_log, &[{method, params} | &1])

    case method do
      "eth_blockNumber" -> {:ok, "0x" <> Integer.to_string(latest, 16)}
      "eth_getLogs" -> {:ok, logs}
    end
  end
end

mirror_state =
  Payments.init!(%{
    name: :payments,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: nil,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "base", rpc_url: "injected", usdc_contract: "0xCONTRACT", confirmations: 0, decimals: 6, start_block: 0, max_block_range: 1000}
    ],
    rpc_fn: nil
  })

{:reply, mj, mirror_state} =
  Payments.handle_message("ingress", Jason.encode!(%{action: "deposit_address", beneficiary: "budget:mirror"}), mirror_state)
mirror_addr = Jason.decode!(mj)["address"]

mk_log = fn to_addr, block, tx, idx ->
  %{
    "address" => "0xCONTRACT",
    "topics" => [transfer_sig, pad_addr.("0x" <> String.duplicate("a", 40)), pad_addr.(to_addr)],
    "data" => value_hex,
    "blockNumber" => "0x" <> Integer.to_string(block, 16),
    "transactionHash" => tx,
    "logIndex" => "0x" <> Integer.to_string(idx, 16)
  }
end

logs = [mk_log.(mirror_addr, 5, "0xM1", 0)]

mirror_state = %{mirror_state | rpc_fn: canned.(logs, 100)}
mirror_state = Payments.poll(mirror_state)

first_from =
  Agent.get(rpc_log, & &1)
  |> Enum.reverse()
  |> Enum.find_value(fn
    {"eth_getLogs", [p]} -> p["fromBlock"]
    _ -> nil
  end)

Agent.update(rpc_log, fn _ -> [] end)
mirror_state = %{mirror_state | rpc_fn: canned.(logs, 200)}
_mirror_state = Payments.poll(mirror_state)

second_from =
  Agent.get(rpc_log, & &1)
  |> Enum.reverse()
  |> Enum.find_value(fn
    {"eth_getLogs", [p]} -> p["fromBlock"]
    _ -> nil
  end)

Check.check(
  f,
  "nil-store: second poll's fromBlock advances past the first (cursor mirror, no infinite rescan)",
  first_from != nil and second_from != nil and
    String.to_integer(String.trim_leading(second_from, "0x"), 16) >
      String.to_integer(String.trim_leading(first_from, "0x"), 16)
)

Check.finish(f)
