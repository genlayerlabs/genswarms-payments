# Standalone — NO Postgres, NO network.  mix run checks/payments_operator_test.exs
#
# D3, the operator surface. Four properties, each one a way money gets lost or
# duplicated if it breaks:
#
#   1. AUTHORIZATION IS SEPARATE. Value-affecting actions need `operator_sources`,
#      which is NOT `trusted_sources`: the cron that ticks the watcher and the
#      consumer that reads the outbox are trusted, and neither may release money.
#   2. RELEASE MINTS A FRESH SEQUENCE. The released row must be visible to a
#      consumer whose cursor is ALREADY PAST the position the row would have had
#      when it was recorded — asserted directly here, because a record-time
#      sequence looks perfectly healthy in every other test and simply never
#      credits.
#   3. RELEASE IS IDEMPOTENT. A second release is a no-op success: no second
#      sequence, no second push, no second credit.
#   4. EVERY REFUSAL IS DISTINCT. Unknown key, not-quarantined, no store
#      callback, degraded boot — never a silent success, never a shared error.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

ns = "llm_quota"

defmodule OpStore do
  @rows :payments_op_rows
  @meta :payments_op_meta

  def reset do
    for t <- [@rows, @meta] do
      if :ets.whereis(t) != :undefined, do: :ets.delete(t)
      :ets.new(t, [:named_table, :public, :set])
    end

    :ets.insert(@meta, {:seq, 0})
    :ok
  end

  def put_row(row), do: :ets.insert(@rows, {row.idempotency_key, row})

  def all_rows, do: @rows |> :ets.tab2list() |> Enum.map(&elem(&1, 1))

  defp next_seq, do: :ets.update_counter(@meta, :seq, 1)

  # ── payments store callbacks ───────────────────────────────────────────────
  def put_address_binding(_binding), do: :ok
  def list_address_bindings, do: {:ok, []}
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok

  def payment_seen?(key), do: {:ok, :ets.member(@rows, key)}

  def record_payment(payment) do
    status = to_string(Map.get(payment, :status, "settled"))

    seq = if status == "settled", do: next_seq(), else: nil

    put_row(Map.put(payment, :outbox_seq, seq))

    if is_integer(seq), do: {:ok, seq}, else: :ok
  end

  def list_payments(beneficiary) do
    {:ok,
     all_rows()
     |> Enum.filter(&(&1.beneficiary == beneficiary and &1.status == "settled"))}
  end

  def list_settlements_since(after_seq, limit) do
    rows =
      all_rows()
      |> Enum.filter(&(is_integer(&1.outbox_seq) and &1.outbox_seq > after_seq))
      |> Enum.sort_by(& &1.outbox_seq)
      |> Enum.take(limit)

    max_seq =
      all_rows()
      |> Enum.map(&(&1.outbox_seq || 0))
      |> Enum.max(fn -> 0 end)

    {:ok, %{settlements: rows, max_seq: max_seq}}
  end

  def list_quarantined_payments(namespace, beneficiary, limit) do
    {:ok,
     all_rows()
     |> Enum.filter(fn row ->
       row.status == "quarantined" and row.namespace == namespace and
         (is_nil(beneficiary) or row.beneficiary == beneficiary)
     end)
     |> Enum.sort_by(& &1.idempotency_key)
     |> Enum.take(limit)}
  end

  # The pinned release: one atomic flip, a sequence minted AT RELEASE TIME from
  # the same generator every settled row is minted from, scoped by namespace.
  def release_quarantined_payment(namespace, key) do
    case :ets.lookup(@rows, key) do
      [{^key, %{namespace: ^namespace, status: "quarantined"} = row}] ->
        released = %{row | status: "settled", outbox_seq: next_seq()}
        put_row(released)
        {:ok, :released, released}

      [{^key, %{namespace: ^namespace, status: "settled"} = row}] ->
        {:ok, :already_settled, row}

      [{^key, %{namespace: ^namespace, status: other}}] ->
        {:error, {:not_releasable, other}}

      _ ->
        {:error, :not_found}
    end
  end
end

defmodule NoReleaseStore do
  def put_address_binding(_binding), do: :ok
  def list_address_bindings, do: {:ok, []}
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_payment), do: :ok
  def list_settlements_since(_after, _limit), do: {:ok, %{settlements: [], max_seq: 0}}
end

defmodule DegradedStore do
  def put_address_binding(_binding), do: :ok
  def list_address_bindings, do: {:error, :db_down}
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_payment), do: :ok
  def release_quarantined_payment(_ns, _key), do: raise("must never be reached")
end

OpStore.reset()

pushes = :ets.new(:payments_op_pushes, [:public, :set])
:ets.insert(pushes, {:log, []})

deliver_fn = fn target, _from, content ->
  [{:log, log}] = :ets.lookup(pushes, :log)
  :ets.insert(pushes, {:log, log ++ [{target, Jason.decode!(content)}]})
  :ok
end

pushed = fn ->
  [{:log, log}] = :ets.lookup(pushes, :log)
  log
end

pushed_actions = fn -> Enum.map(pushed.(), fn {_t, msg} -> msg["action"] end) end

confirms = fn -> pushed_actions.() |> Enum.count(&(&1 == "payment_confirmed")) end

state =
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    namespace: ns,
    trusted_sources: ["cron", "ops", "llm_proxy"],
    operator_sources: ["ops"],
    targets: ["llm_proxy"],
    store_mod: OpStore,
    deliver_fn: deliver_fn,
    max_payment_usd: "100",
    auto_tick: false
  })

ask = fn state, from, msg ->
  {:reply, json, state} = Payments.handle_message(from, Jason.encode!(msg), state)
  {Jason.decode!(json), state}
end

settlement = fn key, amount ->
  %{
    beneficiary: "llmb_alice",
    amount_usd: Decimal.new(amount),
    method: "usdc_base-sepolia",
    ref: "0x#{key}:0",
    idempotency_key: key,
    namespace: ns,
    chain: "base-sepolia",
    chain_id: 84_532,
    block_number: 1,
    log_index: 0,
    tx_hash: "0x#{key}",
    raw_amount: 1,
    decimals: 6,
    token_contract: "0xtoken",
    from_address: "0xsender"
  }
end

# ── 1. authorization is SEPARATE from ordinary bot glue ─────────────────────
{stranger, state} =
  ask.(state, "stranger", %{action: "release_payment", idempotency_key: "k1"})

Check.check(
  f,
  "an UNTRUSTED source cannot release: ok:false untrusted_source",
  stranger["ok"] == false and stranger["error"] == "untrusted_source"
)

{cron_try, state} =
  ask.(state, "cron", %{action: "release_payment", idempotency_key: "k1"})

Check.check(
  f,
  "a TRUSTED but non-operator source cannot release: ok:false not_an_operator",
  cron_try["ok"] == false and cron_try["error"] == "not_an_operator"
)

{cron_sweep, state} = ask.(state, "cron", %{action: "sweep_report"})

Check.check(
  f,
  "the operator gate covers sweep_report too (trusted cron refused)",
  cron_sweep["ok"] == false and cron_sweep["error"] == "not_an_operator"
)

{cron_held, state} = ask.(state, "cron", %{action: "quarantined"})

Check.check(
  f,
  "the operator gate covers the held queue too (trusted cron refused)",
  cron_held["ok"] == false and cron_held["error"] == "not_an_operator"
)

Check.check(
  f,
  "operator_sources does NOT default to trusted_sources",
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    trusted_sources: ["cron", "ops"],
    store_mod: OpStore,
    auto_tick: false
  }).operator_sources
  |> MapSet.size() == 0
)

# ── 2. quarantine, then a consumer cursor that runs PAST it ─────────────────
{_n, state} = Payments.settle([settlement.("held-1", "120")], state)

held_row = Enum.find(OpStore.all_rows(), &(&1.idempotency_key == "held-1"))

Check.check(
  f,
  "over-cap settlement is quarantined with a NULL sequence",
  held_row.status == "quarantined" and is_nil(held_row.outbox_seq)
)

Check.check(
  f,
  "the quarantine REASON is persisted with the row, not only metered",
  to_string(Map.get(held_row, :quarantine_reason)) == "max_payment"
)

Check.check(
  f,
  "quarantine announces payment_held, never payment_confirmed",
  pushed_actions.() == ["payment_held"]
)

{_n, state} =
  Payments.settle(
    [settlement.("ok-1", "5"), settlement.("ok-2", "5"), settlement.("ok-3", "5")],
    state
  )

{:ok, page} =
  Payments.settlements_since(%{store_mod: OpStore, namespace: ns}, 0, 100)

consumer_cursor = page.next_seq

Check.check(
  f,
  "the consumer cursor has advanced past every recorded row",
  consumer_cursor == 3 and length(page.settlements) == 3
)

# ── 3. release: fresh sequence AHEAD of that cursor ─────────────────────────
confirmed_before = confirms.()

{release, state} =
  ask.(state, "ops", %{action: "release_payment", idempotency_key: "held-1"})

Check.check(
  f,
  "release answers ok:true released:true with the fresh sequence",
  release["ok"] == true and release["released"] == true and
    release["idempotency_key"] == "held-1" and release["outbox_seq"] > consumer_cursor
)

Check.check(
  f,
  "release pushes the SAME payment_confirmed a normal settlement pushes",
  confirms.() == confirmed_before + 1 and List.last(pushed_actions.()) == "payment_confirmed"
)

{_target, confirmed} = List.last(pushed.())

Check.check(
  f,
  "the released payment_confirmed carries the credit-key fields (method, ref, namespace, amount)",
  confirmed["beneficiary"] == "llmb_alice" and confirmed["method"] == "usdc_base-sepolia" and
    confirmed["ref"] == "0xheld-1:0" and confirmed["namespace"] == ns and
    confirmed["amount_usd"] == "120"
)

{:ok, after_cursor} =
  Payments.settlements_since(%{store_mod: OpStore, namespace: ns}, consumer_cursor, 100)

Check.check(
  f,
  "THE PROPERTY: a consumer whose cursor is already past the row's original position still sees the released row",
  Enum.map(after_cursor.settlements, & &1.idempotency_key) == ["held-1"]
)

# ── 4. idempotent: no second sequence, no second push ───────────────────────
{again, state} =
  ask.(state, "ops", %{action: "release_payment", idempotency_key: "held-1"})

Check.check(
  f,
  "releasing an already-settled row is a no-op SUCCESS, not a refusal",
  again["ok"] == true and again["released"] == false and again["already"] == "settled"
)

Check.check(
  f,
  "the no-op release pushes nothing (a second credit is never attempted)",
  confirms.() == confirmed_before + 1
)

{:ok, after_replay} =
  Payments.settlements_since(%{store_mod: OpStore, namespace: ns}, consumer_cursor, 100)

Check.check(
  f,
  "the no-op release mints no second sequence — the outbox is unchanged",
  Enum.map(after_replay.settlements, & &1.outbox_seq) ==
    Enum.map(after_cursor.settlements, & &1.outbox_seq)
)

# ── 5. distinct refusals ───────────────────────────────────────────────────
{unknown, state} =
  ask.(state, "ops", %{action: "release_payment", idempotency_key: "nope"})

Check.check(
  f,
  "an unknown key refuses with unknown_key",
  unknown["ok"] == false and unknown["error"] == "unknown_key"
)

OpStore.put_row(%{
  idempotency_key: "refunded-1",
  beneficiary: "llmb_alice",
  namespace: ns,
  amount_usd: Decimal.new("5"),
  method: "usdc_base-sepolia",
  ref: "0xrefunded:0",
  status: "refunded",
  outbox_seq: nil,
  at: DateTime.utc_now()
})

{not_quarantined, state} =
  ask.(state, "ops", %{action: "release_payment", idempotency_key: "refunded-1"})

Check.check(
  f,
  "a row that is not quarantined refuses with not_quarantined + its status",
  not_quarantined["ok"] == false and not_quarantined["error"] == "not_quarantined" and
    not_quarantined["status"] == "refunded"
)

{foreign, state} =
  ask.(state, "ops", %{action: "release_payment", idempotency_key: ""})

Check.check(
  f,
  "a missing idempotency_key is a bad_request, never a release",
  foreign["ok"] == false and foreign["error"] == "bad_request"
)

no_release_state =
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    namespace: ns,
    trusted_sources: ["ops"],
    operator_sources: ["ops"],
    targets: ["llm_proxy"],
    store_mod: NoReleaseStore,
    deliver_fn: deliver_fn,
    auto_tick: false
  })

{no_store, _} =
  ask.(no_release_state, "ops", %{action: "release_payment", idempotency_key: "held-1"})

Check.check(
  f,
  "a store without the release callback refuses with no_release_store (never a fabricated success)",
  no_store["ok"] == false and no_store["error"] == "no_release_store"
)

degraded_state =
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    namespace: ns,
    trusted_sources: ["ops"],
    operator_sources: ["ops"],
    targets: ["llm_proxy"],
    store_mod: DegradedStore,
    deliver_fn: deliver_fn,
    auto_tick: false
  })

{degraded, _} =
  ask.(degraded_state, "ops", %{action: "release_payment", idempotency_key: "held-1"})

Check.check(
  f,
  "a degraded boot refuses the release (the store it would release against is unknown)",
  degraded["ok"] == false and degraded["error"] == "degraded_boot"
)

# ── 6. the held views ──────────────────────────────────────────────────────
{_n, state} = Payments.settle([settlement.("held-2", "150")], state)

{status, state} =
  ask.(state, "ops", %{action: "payment_status", beneficiary: "llmb_alice"})

Check.check(
  f,
  "payment_status reports SETTLED money and HELD money in one answer",
  status["ok"] == true and status["held_durable"] == true and
    length(status["held"]) == 1 and
    hd(status["held"])["idempotency_key"] == "held-2" and
    hd(status["held"])["amount_usd"] == "150" and
    hd(status["held"])["reason"] == "max_payment"
)

Check.check(
  f,
  "payment_status's settled leg excludes the held row",
  Enum.all?(status["payments"], &(&1["amount_usd"] != "150"))
)

{queue, state} = ask.(state, "ops", %{action: "quarantined"})

Check.check(
  f,
  "the held queue reports the namespace's held money with a total",
  queue["ok"] == true and queue["count"] == 1 and queue["total_usd"] == "150" and
    queue["namespace"] == ns
)

{capped, _state} = ask.(state, "ops", %{action: "quarantined", limit: 0})

Check.check(
  f,
  "the held queue clamps a nonsense limit rather than refusing or unbounding",
  capped["ok"] == true and length(capped["held"]) <= 1
)

{no_queue_store, _} =
  ask.(no_release_state, "ops", %{action: "quarantined"})

Check.check(
  f,
  "a store that cannot answer the held queue refuses — never an empty queue it cannot see",
  no_queue_store["ok"] == false and no_queue_store["error"] == "no_quarantine_store"
)

# ── 7. (R4-P4-M4) a released row that cannot be pushed is not creditable ────
# `pushed: false` has exactly one cause: the released row is missing the four
# fields the credit key is built from. The consumer's poll validates the SAME
# four fields, so it will classify the row permanently-bad and quarantine it
# consumer-side. Reporting "the poll will credit it" there is fabricated
# success generated by the very branch that detected the problem.
defmodule IncompleteReleaseStore do
  def put_address_binding(_binding), do: :ok
  def list_address_bindings, do: {:ok, []}
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_payment), do: :ok
  def list_settlements_since(_after, _limit), do: {:ok, %{settlements: [], max_seq: 0}}

  def release_quarantined_payment(namespace, key) do
    {:ok, :released,
     %{
       idempotency_key: key,
       beneficiary: "llmb_alice",
       # no method ⇒ no credit key ⇒ neither the push nor the poll can credit
       method: nil,
       ref: "0xincomplete:0",
       amount_usd: Decimal.new("7"),
       namespace: namespace,
       status: "settled",
       outbox_seq: 99
     }}
  end
end

incomplete_state =
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    namespace: ns,
    trusted_sources: ["ops"],
    operator_sources: ["ops"],
    targets: ["llm_proxy"],
    store_mod: IncompleteReleaseStore,
    deliver_fn: deliver_fn,
    auto_tick: false
  })

pushes_before = confirms.()

{incomplete, _} =
  ask.(incomplete_state, "ops", %{action: "release_payment", idempotency_key: "incomplete-1"})

Check.check(
  f,
  "a released row missing push fields is reported as NOT creditable, not as a push the poll will finish",
  incomplete["ok"] == true and incomplete["released"] == true and
    incomplete["pushed"] == false and incomplete["creditable"] == false
)

Check.check(
  f,
  "and nothing was pushed on the strength of it",
  confirms.() == pushes_before
)

Check.check(
  f,
  "a healthy release is still reported creditable",
  release["creditable"] == true
)

# ── 8. (R4-P4-M7) an unparseable amount is COUNTED, never summed as zero ────
OpStore.put_row(%{
  idempotency_key: "bad-amount-1",
  beneficiary: "llmb_alice",
  method: "usdc_base-sepolia",
  ref: "0xbad:0",
  amount_usd: "not a number",
  namespace: ns,
  status: "quarantined",
  outbox_seq: nil,
  quarantine_reason: "max_payment",
  at: DateTime.utc_now()
})

{defect_queue, _state} = ask.(state, "ops", %{action: "quarantined"})

Check.check(
  f,
  "an amount this hub cannot parse is counted, not folded into the total as $0",
  defect_queue["ok"] == true and defect_queue["amounts_unparsable"] == 1 and
    defect_queue["total_usd"] == "150"
)

Check.finish(f)
