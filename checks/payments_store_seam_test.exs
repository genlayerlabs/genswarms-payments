# Standalone — NO Postgres, NO network.  mix run checks/payments_store_seam_test.exs
#
# Pins three store-seam hardening findings from the adversarial audit:
#
#   1a — EXIT-shaped store failures (GenServer.call timeout, dead Ecto pool)
#        must not crash init/1, settle/2, or record_payment — only `rescue`
#        was used, which never catches an EXIT.
#   1b — a coherence-legal bindings-group-only store (exports the bindings
#        group but NOT the settlement group) must fall back to memory dedup
#        for settlement, not freeze every settlement forever.
#   1c — a raising/exiting cursor-read store must yield {:error, _} so
#        scan_chain skips that chain this round, not silently rescan from
#        start_block forever.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub = "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
pad_addr = fn "0x" <> hex -> "0x" <> String.duplicate("0", 24) <> String.downcase(hex) end
value_hex = "0x" <> String.pad_leading("4c4b40", 64, "0")

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

# ── 1a(i): list_address_bindings EXITS at boot ──────────────────────────────
defmodule ExitingListStore do
  def put_address_binding(_), do: :ok
  def list_address_bindings, do: exit(:timeout)
end

state_a1 = Payments.init!(%{xpub: xpub, store_mod: ExitingListStore})

Check.check(f, "1a: list_address_bindings exiting at boot degrades (not crashes) init/1",
  state_a1.degraded_boot == true)

# ── 1a(ii): payment_seen? EXITS during settle ───────────────────────────────
defmodule ExitingSeenStore do
  def payment_seen?(_key), do: exit(:timeout)
  def record_payment(row), do: (:persistent_term.put({__MODULE__, :rows}, [row | rows()]); :ok)
  def rows, do: :persistent_term.get({__MODULE__, :rows}, [])
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_), do: :ok
  def get_last_scanned_block(_c), do: {:ok, nil}
  def put_last_scanned_block(_c, _n), do: :ok
end

:persistent_term.put({ExitingSeenStore, :rows}, [])

state_a2 =
  Payments.init!(%{
    xpub: xpub,
    trusted_sources: [],
    targets: ["t"],
    store_mod: ExitingSeenStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end
  })

s = %{
  beneficiary: "budget:abc",
  amount_usd: Decimal.new("5.00"),
  method: "usdc_base",
  ref: "0xTX:1",
  idempotency_key: "base:0xTX:1",
  namespace: "ns"
}

{n_a2, _state_a2} = Payments.settle([s], state_a2)

Check.check(f, "1a: payment_seen? exiting during settle holds the settlement (no crash)",
  n_a2 == 0 and ExitingSeenStore.rows() == [])

# ── 1a(iii): record_payment EXITS during settle (via a full poll round) ────
defmodule ExitingRecordStore do
  def payment_seen?(_k), do: {:ok, false}
  def record_payment(_row), do: exit(:timeout)
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(b), do: (:persistent_term.put({__MODULE__, :b}, [b | bindings()]); :ok)
  def bindings, do: :persistent_term.get({__MODULE__, :b}, [])
  def get_last_scanned_block(_c), do: {:ok, :persistent_term.get({__MODULE__, :cursor}, nil)}
  def put_last_scanned_block(_c, n), do: (:persistent_term.put({__MODULE__, :cursor}, n); :ok)
  def cursor, do: :persistent_term.get({__MODULE__, :cursor}, nil)
end

:persistent_term.put({ExitingRecordStore, :b}, [])
:persistent_term.erase({ExitingRecordStore, :cursor})

state_a3 =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    trusted_sources: ["ingress"],
    targets: ["t"],
    namespace: "ns",
    store_mod: ExitingRecordStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "base", chain_id: 8453, rpc_url: "injected", usdc_contract: "0xCONTRACT", confirmations: 0, decimals: 6, start_block: 0}
    ]
  })

{:reply, dj, state_a3} =
  Payments.handle_message("ingress", Jason.encode!(%{action: "deposit_address", beneficiary: "budget:abc"}), state_a3)
addr_a3 = Jason.decode!(dj)["address"]

logs_a3 = [mk_log.(addr_a3, 1, "0xT1", 0)]

settling_rpc = fn _chain, method, _params ->
  case method do
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, logs_a3}
  end
end

state_a3 = %{state_a3 | rpc_fn: settling_rpc}

result_a3 =
  try do
    Payments.poll(state_a3)
    :ok
  rescue
    e -> {:raised, e}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(f, "1a: record_payment exiting during poll does not crash the tick",
  result_a3 == :ok)
Check.check(f, "1a: record_payment exiting holds the settlement (binding exists, but never recorded)",
  result_a3 == :ok and ExitingRecordStore.bindings() != [])
Check.check(f, "1a: record_payment exiting leaves the cursor untouched",
  ExitingRecordStore.cursor() == nil)

# ── 1b: a coherence-legal bindings-group-only store must NOT freeze
# settlement — payment_seen?/record_payment are simply not exported, so
# settlement must fall back to memory dedup (same as a nil store), and the
# cursor must still advance.
defmodule BindingsOnlyStore do
  def put_address_binding(b), do: (:persistent_term.put({__MODULE__, :b}, [b | bindings()]); :ok)
  def bindings, do: :persistent_term.get({__MODULE__, :b}, [])
  def list_address_bindings, do: {:ok, bindings()}
end

:persistent_term.put({BindingsOnlyStore, :b}, [])

state_b1 =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    trusted_sources: ["ingress"],
    targets: ["t"],
    allow_ephemeral: true,
    namespace: "ns",
    store_mod: BindingsOnlyStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "base", chain_id: 8453, rpc_url: "injected", usdc_contract: "0xCONTRACT", confirmations: 0, decimals: 6, start_block: 0}
    ]
  })

Check.check(f, "1b: bindings-group-only store is coherence-legal (init doesn't raise)",
  match?(%{degraded_boot: false}, state_b1))

{:reply, dj_b1, state_b1} =
  Payments.handle_message("ingress", Jason.encode!(%{action: "deposit_address", beneficiary: "budget:xyz"}), state_b1)
addr_b1 = Jason.decode!(dj_b1)["address"]

logs_b1 = [mk_log.(addr_b1, 1, "0xB1", 0)]

rpc_b1 = fn _chain, method, _params ->
  case method do
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, logs_b1}
  end
end

state_b1 = %{state_b1 | rpc_fn: rpc_b1}
state_b1 = Payments.poll(state_b1)

Check.check(f, "1b: settlement settles via memory dedup despite non-nil, half-implemented store",
  MapSet.member?(state_b1.seen_keys, "8453:0xB1:0"))
Check.check(f, "1b: store without the settlement group uses the memory sequence counter",
  hd(state_b1.settlement_mirror).outbox_seq == 1 and state_b1.next_outbox_seq == 2)
Check.check(f, "1b: cursor advances (not frozen) once settled",
  Map.get(state_b1.cursor_mirror, "base") == 200 - 0)

# ── 1c: a raising cursor-read store must not trigger a silent
# rescan-from-start_block storm — scan_chain must skip the chain this round.
defmodule RaisingCursorStore do
  def get_last_scanned_block(_chain), do: raise("boom")
  def put_last_scanned_block(_c, _n), do: :ok
  def payment_seen?(_k), do: {:ok, false}
  def record_payment(_row), do: :ok
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(b), do: (:persistent_term.put({__MODULE__, :b}, [b | bindings()]); :ok)
  def bindings, do: :persistent_term.get({__MODULE__, :b}, [])
end

:persistent_term.put({RaisingCursorStore, :b}, [])

{:ok, calls_c1} = Agent.start_link(fn -> [] end)

state_c1 =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    trusted_sources: ["ingress"],
    targets: ["t"],
    namespace: "ns",
    store_mod: RaisingCursorStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "base", chain_id: 8453, rpc_url: "injected", usdc_contract: "0xCONTRACT", confirmations: 0, decimals: 6, start_block: 0}
    ]
  })

counting_rpc = fn _chain, method, _params ->
  Agent.update(calls_c1, &[method | &1])

  case method do
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, []}
  end
end

state_c1 = %{state_c1 | rpc_fn: counting_rpc}

result_c1 =
  try do
    Payments.poll(state_c1)
    :ok
  rescue
    e -> {:raised, e}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(f, "1c: a raising cursor-read store doesn't crash poll", result_c1 == :ok)
Check.check(f, "1c: a raising cursor-read store skips the chain — no eth_getLogs call issued",
  not Enum.member?(Agent.get(calls_c1, & &1), "eth_getLogs"))

# ── 1c(ii): same, but the store EXITS instead of raising ────────────────────
defmodule ExitingCursorStore do
  def get_last_scanned_block(_chain), do: exit(:timeout)
  def put_last_scanned_block(_c, _n), do: :ok
  def payment_seen?(_k), do: {:ok, false}
  def record_payment(_row), do: :ok
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_), do: :ok
end

{:ok, calls_c2} = Agent.start_link(fn -> [] end)

state_c2 =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    trusted_sources: ["ingress"],
    targets: ["t"],
    namespace: "ns",
    store_mod: ExitingCursorStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "base", chain_id: 8453, rpc_url: "injected", usdc_contract: "0xCONTRACT", confirmations: 0, decimals: 6, start_block: 0}
    ]
  })

counting_rpc2 = fn _chain, method, _params ->
  Agent.update(calls_c2, &[method | &1])

  case method do
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, []}
  end
end

state_c2 = %{state_c2 | rpc_fn: counting_rpc2}

result_c2 =
  try do
    Payments.poll(state_c2)
    :ok
  rescue
    e -> {:raised, e}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(f, "1c: an exiting cursor-read store doesn't crash poll", result_c2 == :ok)
Check.check(f, "1c: an exiting cursor-read store skips the chain — no eth_getLogs call issued",
  not Enum.member?(Agent.get(calls_c2, & &1), "eth_getLogs"))

# ── 1d: a payment_seen? returning {:ok, nil} (the realistic Repo.one-on-no-row
# adapter bug) must NOT escape as a CaseClauseError out of handle_message and
# crash-loop the object every tick. A bare {:ok, bool} match binds anything;
# with the is_boolean guard the non-boolean falls to the fail-closed clause:
# the tick completes, the payment is HELD (not settled, not lost), and the
# cursor does not advance past it — the next round re-presents it.
defmodule NilSeenStore do
  def payment_seen?(_k), do: {:ok, nil}
  def record_payment(row), do: (:persistent_term.put({__MODULE__, :rows}, [row | rows()]); :ok)
  def rows, do: :persistent_term.get({__MODULE__, :rows}, [])
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_), do: :ok
  def get_last_scanned_block(_c), do: {:ok, :persistent_term.get({__MODULE__, :cursor}, nil)}
  def put_last_scanned_block(_c, n), do: (:persistent_term.put({__MODULE__, :cursor}, n); :ok)
  def cursor, do: :persistent_term.get({__MODULE__, :cursor}, nil)
end

:persistent_term.put({NilSeenStore, :rows}, [])
:persistent_term.erase({NilSeenStore, :cursor})

{:ok, delivered_d1} = Agent.start_link(fn -> [] end)

state_d1 =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    trusted_sources: ["ingress"],
    targets: ["t"],
    namespace: "ns",
    store_mod: NilSeenStore,
    auto_tick: false,
    deliver_fn: fn t, from, _c -> (Agent.update(delivered_d1, &[{t, from} | &1]); :ok) end,
    chains: [
      %{name: "base", chain_id: 8453, rpc_url: "injected", usdc_contract: "0xCONTRACT", confirmations: 0, decimals: 6, start_block: 0}
    ]
  })

{:reply, dj_d1, state_d1} =
  Payments.handle_message("ingress", Jason.encode!(%{action: "deposit_address", beneficiary: "budget:nil"}), state_d1)
addr_d1 = Jason.decode!(dj_d1)["address"]

rpc_d1 = fn _chain, method, _params ->
  case method do
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, [mk_log.(addr_d1, 1, "0xNIL", 0)]}
  end
end

state_d1 = %{state_d1 | rpc_fn: rpc_d1}

result_d1 =
  try do
    {Payments.handle_message("ingress", Jason.encode!(%{action: "tick"}), state_d1), :ok}
  rescue
    e -> {:raised, e}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(f, "1d: payment_seen? returning {:ok, nil} does not raise out of the tick",
  match?({{:noreply, _}, :ok}, result_d1))
Check.check(f, "1d: the payment is held — not settled (no ledger row), not delivered",
  NilSeenStore.rows() == [] and Agent.get(delivered_d1, & &1) == [])
Check.check(f, "1d: the cursor does not advance past the held payment",
  NilSeenStore.cursor() == nil)

Check.finish(f)
