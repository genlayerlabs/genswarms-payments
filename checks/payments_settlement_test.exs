Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

defmodule LedgerStore do
  def reset do
    :persistent_term.put({__MODULE__, :seen}, MapSet.new())
    :persistent_term.put({__MODULE__, :rows}, [])
    :persistent_term.put({__MODULE__, :down}, false)
  end

  def down!(flag), do: :persistent_term.put({__MODULE__, :down}, flag)
  defp down?, do: :persistent_term.get({__MODULE__, :down}, false)
  def rows, do: :persistent_term.get({__MODULE__, :rows}, [])

  def payment_seen?(key) do
    if down?(),
      do: {:error, :db_down},
      else: {:ok, MapSet.member?(:persistent_term.get({__MODULE__, :seen}), key)}
  end

  def record_payment(row) do
    if down?() do
      {:error, :db_down}
    else
      :persistent_term.put(
        {__MODULE__, :seen},
        MapSet.put(:persistent_term.get({__MODULE__, :seen}), row.idempotency_key)
      )

      :persistent_term.put({__MODULE__, :rows}, [row | rows()])
      :ok
    end
  end

  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _n), do: :ok
end

defmodule SequencedLedgerStore do
  def reset, do: :persistent_term.put({__MODULE__, :rows}, [])
  def rows, do: :persistent_term.get({__MODULE__, :rows}, [])
  def payment_seen?(_key), do: {:ok, false}

  def record_payment(row) do
    :persistent_term.put({__MODULE__, :rows}, [row | rows()])
    {:ok, 41}
  end

  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _n), do: :ok
end

defmodule InvalidReturnLedgerStore do
  def set_return(value), do: :persistent_term.put({__MODULE__, :return}, value)
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_row), do: :persistent_term.get({__MODULE__, :return})
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _n), do: :ok
end

LedgerStore.reset()
{:ok, delivered} = Agent.start_link(fn -> [] end)

state =
  Payments.init!(%{
    name: :payments,
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    trusted_sources: [],
    targets: ["llm_proxy", "audit_log"],
    store_mod: LedgerStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
    deliver_fn: fn target, from, content ->
      Agent.update(delivered, &[{target, from, Jason.decode!(content)} | &1])
      :ok
    end
  })

# Old-format keys remain valid inputs and dedup by exact string equality;
# only newly built USDC settlements switch to chain_id-prefixed keys.
s = %{
  beneficiary: "budget:abc",
  amount_usd: Decimal.new("5.00"),
  method: "usdc_base",
  ref: "0xTX:3",
  idempotency_key: "base:0xTX:3",
  namespace: "llm_quota"
}

{n, state} = Payments.settle([s], state)
msgs = Agent.get(delivered, &Enum.reverse(&1))

Check.check(f, "one settlement settled", n == 1)

Check.check(
  f,
  "delivered to BOTH allowlisted targets, stamped with object name",
  Enum.map(msgs, fn {t, from, _} -> {t, from} end) ==
    [{"llm_proxy", :payments}, {"audit_log", :payments}]
)

{_, _, payload} = hd(msgs)

Check.check(
  f,
  "payload shape",
  payload["action"] == "payment_confirmed" and payload["beneficiary"] == "budget:abc" and
    payload["amount_usd"] == "5.00" and payload["method"] == "usdc_base" and
    payload["namespace"] == "llm_quota" and payload["at"] == "2026-07-22T12:00:00Z"
)

# idempotency: same key again ⇒ nothing
Agent.update(delivered, fn _ -> [] end)
{n2, state} = Payments.settle([s], state)

Check.check(
  f,
  "duplicate idempotency_key ⇒ zero settled, zero delivered",
  n2 == 0 and Agent.get(delivered, & &1) == []
)

Check.check(f, "ledger recorded exactly once", length(LedgerStore.rows()) == 1)

Check.check(
  f,
  "legacy :ok store remains valid and mirror row has no invented store sequence",
  hd(LedgerStore.rows()).outbox_seq == nil and
    hd(state.settlement_mirror).outbox_seq == nil
)

# FAIL CLOSED: store down ⇒ nothing settles, nothing delivered
LedgerStore.down!(true)
s2 = %{s | idempotency_key: "base:0xTX:4", ref: "0xTX:4"}
{n3, state} = Payments.settle([s2], state)

Check.check(
  f,
  "store down ⇒ fail closed (0 settled, 0 delivered)",
  n3 == 0 and Agent.get(delivered, & &1) == []
)

Check.check(
  f,
  "held settlement gets no mirror row or outbox sequence",
  length(state.settlement_mirror) == 1
)

# recovery: store back up ⇒ the SAME settlement goes through
LedgerStore.down!(false)
{n4, _state} = Payments.settle([s2], state)
Check.check(f, "after store recovery the held settlement settles", n4 == 1)

# no store (dev): memory dedup still works
ok_dev =
  Payments.init!(%{
    name: :p2,
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    targets: ["t"],
    allow_ephemeral: true,
    trusted_sources: [],
    store_mod: nil,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end
  })

{d1, ok_dev} = Payments.settle([s], ok_dev)
{d2, ok_dev} = Payments.settle([s], ok_dev)
s_mem2 = %{s | idempotency_key: "memory:2", ref: "memory:2"}
{d3, ok_dev} = Payments.settle([s_mem2], ok_dev)
Check.check(f, "dev mode (no store): settles once, memory-dedups the repeat", d1 == 1 and d2 == 0)

Check.check(
  f,
  "memory fallback mints monotone outbox sequences",
  d3 == 1 and Enum.map(ok_dev.settlement_mirror, & &1.outbox_seq) == [2, 1] and
    ok_dev.next_outbox_seq == 3
)

# A2: a new store may assign the sequence itself. The hub accepts the tuple
# without weakening any error path and keeps the assigned value on its mirror.
SequencedLedgerStore.reset()

seq_state =
  Payments.init!(%{
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    targets: ["t"],
    store_mod: SequencedLedgerStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end
  })

{seq_count, seq_state} =
  Payments.settle([%{s | idempotency_key: "sequenced:1", ref: "sequenced:1"}], seq_state)

Check.check(
  f,
  "record_payment {:ok, positive_seq} is a successful settlement",
  seq_count == 1 and hd(seq_state.settlement_mirror).outbox_seq == 41
)

Check.check(
  f,
  "store receives the full row before attaching its returned sequence",
  hd(SequencedLedgerStore.rows()).outbox_seq == nil
)

# The record-and-deliver dispatch must remain total over every result that
# record_payment_write/2 can produce. Invalid store returns are normalized to
# {:error, {:bad_return, _}} and must hold cleanly rather than reaching the
# success path or keep_settlement/3.
{:ok, invalid_return_deliveries} = Agent.start_link(fn -> [] end)

invalid_return_state =
  Payments.init!(%{
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    targets: ["t"],
    store_mod: InvalidReturnLedgerStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ ->
      Agent.update(invalid_return_deliveries, &[:delivered | &1])
      :ok
    end
  })

invalid_return_results =
  Enum.with_index([{:ok, 0}, {:ok, -1}, {:ok, "41"}, :unexpected, %{ok: true}], 1)
  |> Enum.map(fn {invalid_return, index} ->
    InvalidReturnLedgerStore.set_return(invalid_return)
    candidate = %{s | idempotency_key: "invalid-return:#{index}", ref: "invalid-return:#{index}"}

    try do
      Payments.settle([candidate], invalid_return_state)
    rescue
      error -> {:raised, error}
    catch
      kind, reason -> {:caught, kind, reason}
    end
  end)

Check.check(
  f,
  "invalid record_payment returns are total: all hold with no delivery or mirror row",
  Enum.all?(invalid_return_results, &match?({0, %{settlement_mirror: []}}, &1)) and
    Agent.get(invalid_return_deliveries, & &1) == []
)

# ── B1(i): multi-target delivery isolation — a raise on one target must not
# block delivery to the other (a disclosed gap the review called out).
{:ok, raise_flag} = Agent.start_link(fn -> true end)
{:ok, delivered2} = Agent.start_link(fn -> [] end)

isolating_deliver = fn target, from, _content ->
  if target == "flaky" and Agent.get(raise_flag, & &1) do
    raise "boom"
  else
    Agent.update(delivered2, &[{target, from} | &1])
    :ok
  end
end

state_iso =
  Payments.init!(%{
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    trusted_sources: [],
    targets: ["flaky", "reliable"],
    allow_ephemeral: true,
    store_mod: nil,
    auto_tick: false,
    deliver_fn: isolating_deliver
  })

s_iso = %{s | idempotency_key: "iso:1", ref: "iso:1"}
{n_iso, state_iso} = Payments.settle([s_iso], state_iso)

Check.check(
  f,
  "one target raising doesn't block delivery to the OTHER target",
  n_iso == 1 and Agent.get(delivered2, & &1) == [{"reliable", :payments}]
)

Check.check(
  f,
  "failed push is one-shot and its settlement remains in the outbox mirror",
  not Map.has_key?(state_iso, :undelivered) and
    Enum.any?(
      state_iso.settlement_mirror,
      &(&1.idempotency_key == "iso:1" and &1.outbox_seq == 1)
    )
)

# ── B1(ii): a GenServer-call-timeout-shaped EXIT must not crash the tick
# either. It is a one-shot push; the outbox, not poll, is the recovery path.
exiting_deliver = fn target, _from, _content ->
  if target == "flaky2", do: exit(:timeout), else: :ok
end

state_exit =
  Payments.init!(%{
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    trusted_sources: [],
    targets: ["flaky2"],
    allow_ephemeral: true,
    store_mod: nil,
    auto_tick: false,
    deliver_fn: exiting_deliver
  })

s_exit = %{s | idempotency_key: "exitk:1", ref: "exitk:1"}
{n_exit, state_exit} = Payments.settle([s_exit], state_exit)

Check.check(
  f,
  "a delivery EXIT is caught (no crash); settlement still counts and is sequenced",
  n_exit == 1 and hd(state_exit.settlement_mirror).outbox_seq == 1
)

{:ok, retried} = Agent.start_link(fn -> [] end)

working_deliver = fn target, from, _content ->
  Agent.update(retried, &[{target, from} | &1])
  :ok
end

state_exit = %{state_exit | deliver_fn: working_deliver}
state_exit = Payments.poll(state_exit)

Check.check(
  f,
  "poll does not redeliver a failed one-shot push",
  Agent.get(retried, & &1) == [] and not Map.has_key?(state_exit, :undelivered)
)

# ── 2a: an {:error, _} RETURN from deliver_fn is a failed one-shot push.
# The sequenced outbox row remains available for consumer recovery.
error_return_deliver = fn target, _from, _content ->
  if target == "down_by_return", do: {:error, :target_down}, else: :ok
end

state_2a =
  Payments.init!(%{
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    trusted_sources: [],
    targets: ["down_by_return"],
    allow_ephemeral: true,
    store_mod: nil,
    auto_tick: false,
    deliver_fn: error_return_deliver
  })

s_2a = %{s | idempotency_key: "returnfail:1", ref: "returnfail:1"}
{n_2a, state_2a} = Payments.settle([s_2a], state_2a)

Check.check(
  f,
  "2a: deliver_fn returning {:error,_} (not raising) is NOT counted as delivered",
  n_2a == 1 and hd(state_2a.settlement_mirror).idempotency_key == "returnfail:1"
)

{:ok, retried_2a} = Agent.start_link(fn -> [] end)

healed_deliver_2a = fn target, from, _content ->
  Agent.update(retried_2a, &[{target, from} | &1])
  :ok
end

state_2a = %{state_2a | deliver_fn: healed_deliver_2a}
state_2a = Payments.poll(state_2a)

Check.check(
  f,
  "2a: healing deliver_fn does not create tick redelivery",
  Agent.get(retried_2a, & &1) == [] and not Map.has_key?(state_2a, :undelivered)
)

# ── 2a(ii): multi-target, one fails by RETURN (not raise) — the other target
# still delivers normally in the same one-shot round.
{:ok, delivered_2a2} = Agent.start_link(fn -> [] end)

mixed_return_deliver = fn target, from, _content ->
  case target do
    "bad_return" ->
      {:error, :nope}

    _ ->
      Agent.update(delivered_2a2, &[{target, from} | &1])
      :ok
  end
end

state_2a2 =
  Payments.init!(%{
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    trusted_sources: [],
    targets: ["bad_return", "good_target"],
    allow_ephemeral: true,
    store_mod: nil,
    auto_tick: false,
    deliver_fn: mixed_return_deliver
  })

s_2a2 = %{s | idempotency_key: "returnfail:2", ref: "returnfail:2"}
{n_2a2, state_2a2} = Payments.settle([s_2a2], state_2a2)

Check.check(
  f,
  "2a: error-returning target is isolated; the succeeding target delivered",
  n_2a2 == 1 and
    Agent.get(delivered_2a2, & &1) == [{"good_target", :payments}] and
    not Map.has_key?(state_2a2, :undelivered)
)

# ── B3: record-write-only isolation (disclosed gap) — payment_seen? healthy,
# record_payment fails ⇒ held; heals ⇒ the SAME settlement settles.
defmodule WriteOnlyDownStore do
  def reset,
    do: :persistent_term.put({__MODULE__, :d}, %{seen: MapSet.new(), rows: [], down: true})

  defp d, do: :persistent_term.get({__MODULE__, :d})
  defp put(k, v), do: :persistent_term.put({__MODULE__, :d}, Map.put(d(), k, v))
  def down!(flag), do: put(:down, flag)

  def payment_seen?(key), do: {:ok, MapSet.member?(d().seen, key)}

  def record_payment(row) do
    if d().down do
      {:error, :db_down}
    else
      put(:seen, MapSet.put(d().seen, row.idempotency_key))
      put(:rows, [row | d().rows])
      :ok
    end
  end

  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_), do: :ok
  def get_last_scanned_block(_c), do: {:ok, nil}
  def put_last_scanned_block(_c, _n), do: :ok
  def rows, do: d().rows
end

WriteOnlyDownStore.reset()
{:ok, delivered3} = Agent.start_link(fn -> [] end)

state_wo =
  Payments.init!(%{
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    trusted_sources: [],
    targets: ["t"],
    store_mod: WriteOnlyDownStore,
    auto_tick: false,
    deliver_fn: fn t, from, _c ->
      Agent.update(delivered3, &[{t, from} | &1])
      :ok
    end
  })

s_wo = %{s | idempotency_key: "wo:1", ref: "wo:1"}
{n_wo, state_wo} = Payments.settle([s_wo], state_wo)

Check.check(
  f,
  "payment_seen? ok but record_payment fails ⇒ held (0 settled, 0 delivered)",
  n_wo == 0 and Agent.get(delivered3, & &1) == []
)

WriteOnlyDownStore.down!(false)
{n_wo2, _state_wo} = Payments.settle([s_wo], state_wo)

Check.check(
  f,
  "after record_payment heals, the SAME settlement settles",
  n_wo2 == 1 and length(WriteOnlyDownStore.rows()) == 1
)

# ── 2d: the shipped DEFAULT deliver_fn must honor the same contract the
# injected fns above are held to. It used to discard ObjectServer's return
# and hardcode :ok, so an error-shaped RETURN (e.g. {:error, :unknown_target})
# was silently counted as delivered — settlement recorded, dedup blocks
# re-presentation, delivery lost forever. The peer call's result now flows
# through map_peer_delivery_result/1; pin the mapping directly (ObjectServer
# itself is host-provided and not hermetically callable here).
Check.check(
  f,
  "2d: peer returning :ok maps to delivered",
  Payments.map_peer_delivery_result(:ok) == :ok
)

Check.check(
  f,
  "2d: peer returning {:ok, _} maps to delivered",
  Payments.map_peer_delivery_result({:ok, :queued}) == :ok
)

Check.check(
  f,
  "2d: peer returning {:error, _} passes through as the failure",
  Payments.map_peer_delivery_result({:error, :unknown_target}) == {:error, :unknown_target}
)

Check.check(
  f,
  "2d: any other peer return is a failure, not silently delivered",
  Payments.map_peer_delivery_result(:noop) == {:error, {:bad_return, :noop}} and
    Payments.map_peer_delivery_result(nil) == {:error, {:bad_return, nil}}
)

# The default fn itself (no deliver_fn injected): ObjectServer is absent in
# this hermetic run, so the apply raises UndefinedFunctionError — deliver_one
# must catch it and leave recovery to the outbox, never crash.
state_2d =
  Payments.init!(%{
    xpub:
      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    allow_test_xpub: true,
    trusted_sources: [],
    targets: ["peer_obj"],
    allow_ephemeral: true,
    store_mod: nil,
    auto_tick: false
  })

s_2d = %{s | idempotency_key: "default_fn:1", ref: "default_fn:1"}

result_2d =
  try do
    {:ok, Payments.settle([s_2d], state_2d)}
  rescue
    e -> {:raised, e}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(
  f,
  "2d: default deliver_fn with an absent ObjectServer doesn't crash; row remains readable",
  match?(
    {:ok, {1, %{settlement_mirror: [%{idempotency_key: "default_fn:1", outbox_seq: 1}]}}},
    result_2d
  )
)

Check.finish(f)
