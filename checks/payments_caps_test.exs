# Standalone — NO Postgres, NO network.  mix run checks/payments_caps_test.exs
#
# C1 — bound the blast radius. Every unbounded over-credit mode (a lying RPC
# oracle, a reorg, a decimals misconfiguration) becomes a bounded, LOUD one:
#
#   * a single settlement above max_payment_usd is QUARANTINED;
#   * settlements past max_issuance_per_window_usd in the trailing window are
#     QUARANTINED — except a beneficiary's own first small_topup_usd, so one
#     whale cannot deny everyone else's top-ups for the rest of the window;
#   * a quarantined row is durable, deduped, alarmed, notified to targets as
#     `payment_held`, and NEVER creditable: outbox_seq stays NULL (A3) so the
#     authoritative outbox read cannot see it. Release is phase 4.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

now = ~U[2026-07-24 12:00:00Z]

settlement = fn key, beneficiary, amount ->
  %{
    beneficiary: beneficiary,
    amount_usd: Decimal.new(amount),
    method: "usdc_base",
    ref: "#{key}:0",
    idempotency_key: key,
    namespace: "llm_quota",
    raw_amount: 1,
    decimals: 6,
    token_contract: "0xCONTRACT",
    chain: "base",
    chain_id: 8453,
    block_number: 10,
    log_index: 0,
    tx_hash: "0x#{key}",
    from_address: "0x" <> String.duplicate("a", 40)
  }
end

new_hub = fn overrides, deliveries, events ->
  Payments.init!(
    Map.merge(
      %{
        name: :payments,
        xpub: xpub,
        allow_test_xpub: true,
        trusted_sources: ["ops", "llm_proxy"],
        targets: ["llm_proxy"],
        allow_ephemeral: true,
        namespace: "llm_quota",
        store_mod: nil,
        auto_tick: false,
        now_fn: fn -> now end,
        deliver_fn: fn target, from, content ->
          Agent.update(deliveries, &[{target, from, Jason.decode!(content)} | &1])
          :ok
        end,
        metrics_fn: fn event, meta -> Agent.update(events, &[{event, meta} | &1]) end
      },
      overrides
    )
  )
end

# ── per-settlement cap ──────────────────────────────────────────────────────
{:ok, deliveries} = Agent.start_link(fn -> [] end)
{:ok, events} = Agent.start_link(fn -> [] end)

hub = new_hub.(%{max_payment_usd: "100"}, deliveries, events)

{settled, hub} = Payments.settle([settlement.("small", "budget:a", "99.99")], hub)
{over, hub} = Payments.settle([settlement.("whale", "budget:a", "100.01")], hub)

quarantined_row = Enum.find(hub.settlement_mirror, &(&1.idempotency_key == "whale"))

Check.check(
  f,
  "a settlement at or below max_payment_usd settles; one above it does NOT",
  settled == 1 and over == 0
)

Check.check(
  f,
  "the over-cap settlement is recorded quarantined with a NULL outbox sequence",
  quarantined_row.status == "quarantined" and quarantined_row.outbox_seq == nil
)

Check.check(
  f,
  "the settled row keeps status settled and a creditable sequence",
  Enum.find(hub.settlement_mirror, &(&1.idempotency_key == "small")).status == "settled" and
    Enum.find(hub.settlement_mirror, &(&1.idempotency_key == "small")).outbox_seq == 1
)

Check.check(
  f,
  "quarantine alarms through metrics_fn with the key, amount and reason",
  Enum.any?(Agent.get(events, & &1), fn {event, meta} ->
    event == "payments_quarantined" and meta.idempotency_key == "whale" and
      meta.amount_usd == "100.01" and meta.reason == "max_payment"
  end)
)

held_notice =
  Agent.get(deliveries, & &1)
  |> Enum.find(fn {_t, _from, payload} -> payload["action"] == "payment_held" end)

Check.check(
  f,
  "targets receive a one-shot payment_held notice (the user-visible hold hook)",
  match?(
    {"llm_proxy", :payments,
     %{
       "action" => "payment_held",
       "beneficiary" => "budget:a",
       "amount_usd" => "100.01",
       "reason" => "max_payment"
     }},
    held_notice
  )
)

Check.check(
  f,
  "a quarantined settlement never delivers payment_confirmed",
  Enum.count(Agent.get(deliveries, & &1), fn {_t, _from, p} ->
    p["action"] == "payment_confirmed"
  end) == 1
)

# Dedup: re-presenting the quarantined key must not re-quarantine, re-alarm,
# or re-notify — it is already recorded.
Agent.update(deliveries, fn _ -> [] end)
{repeat, hub} = Payments.settle([settlement.("whale", "budget:a", "100.01")], hub)

Check.check(
  f,
  "a quarantined key is deduped like a settled one (no second row, no second notice)",
  repeat == 0 and Agent.get(deliveries, & &1) == [] and
    Enum.count(hub.settlement_mirror, &(&1.idempotency_key == "whale")) == 1
)

# A3: the outbox read is the authoritative credit path. A quarantined row that
# appeared there would make the cheapest control in the system inert.
{:reply, page_json, hub} =
  Payments.handle_message(
    "llm_proxy",
    Jason.encode!(%{action: "settlements_since", after_seq: 0, limit: 100}),
    hub
  )

page = Jason.decode!(page_json)

Check.check(
  f,
  "quarantined rows are invisible to the settlement outbox read",
  page["ok"] == true and
    Enum.map(page["settlements"], & &1["idempotency_key"]) == ["small"]
)

# ── aggregate cap + per-beneficiary carve-out ───────────────────────────────
{:ok, deliveries2} = Agent.start_link(fn -> [] end)
{:ok, events2} = Agent.start_link(fn -> [] end)

capped =
  new_hub.(
    %{max_issuance_per_window_usd: "50", small_topup_usd: "5", issuance_window_hours: 24},
    deliveries2,
    events2
  )

{whale_n, capped} = Payments.settle([settlement.("agg:whale", "budget:whale", "49")], capped)
{over_n, capped} = Payments.settle([settlement.("agg:over", "budget:whale", "40")], capped)
{small_n, capped} = Payments.settle([settlement.("agg:small", "budget:small", "4")], capped)

{small_over_n, capped} =
  Payments.settle([settlement.("agg:small2", "budget:small", "4")], capped)

Check.check(
  f,
  "the window's first settlements settle until the aggregate cap is reached",
  whale_n == 1 and over_n == 0
)

Check.check(
  f,
  "the over-cap settlement is quarantined with reason aggregate",
  Enum.find(capped.settlement_mirror, &(&1.idempotency_key == "agg:over")).status ==
    "quarantined" and
    Enum.any?(Agent.get(events2, & &1), fn {event, meta} ->
      event == "payments_quarantined" and meta.idempotency_key == "agg:over" and
        meta.reason == "aggregate"
    end)
)

Check.check(
  f,
  "carve-out: a whale exhausting the window does NOT deny a small top-up",
  small_n == 1
)

Check.check(
  f,
  "carve-out is per beneficiary and bounded by small_topup_usd, not unlimited",
  small_over_n == 0 and
    Enum.find(capped.settlement_mirror, &(&1.idempotency_key == "agg:small2")).status ==
      "quarantined"
)

# The carve-out never overrides the per-settlement cap.
{:ok, deliveries3} = Agent.start_link(fn -> [] end)
{:ok, events3} = Agent.start_link(fn -> [] end)

both_caps =
  new_hub.(
    %{max_payment_usd: "3", max_issuance_per_window_usd: "50", small_topup_usd: "5"},
    deliveries3,
    events3
  )

{carve_over_n, both_caps} =
  Payments.settle([settlement.("carve:over", "budget:new", "4")], both_caps)

Check.check(
  f,
  "a carve-out-sized payment above max_payment_usd is still quarantined",
  carve_over_n == 0 and
    Enum.any?(Agent.get(events3, & &1), fn {event, meta} ->
      event == "payments_quarantined" and meta.idempotency_key == "carve:over" and
        meta.reason == "max_payment"
    end)
)

# A window that has rolled over frees the cap again.
rolled = %{both_caps | now_fn: fn -> DateTime.add(now, 25 * 3600, :second) end}

{rolled_n, _rolled} =
  Payments.settle([settlement.("carve:later", "budget:new", "2")], rolled)

Check.check(f, "settlements older than issuance_window_hours leave the window", rolled_n == 1)

# ── durable stores answer the window through issuance_totals_since/3 ─────────
defmodule WindowStore do
  def reset do
    :persistent_term.put({__MODULE__, :rows}, [])
    :persistent_term.put({__MODULE__, :mode}, :ok)
  end

  def mode!(mode), do: :persistent_term.put({__MODULE__, :mode}, mode)
  def rows, do: :persistent_term.get({__MODULE__, :rows}, [])
  def calls, do: :persistent_term.get({__MODULE__, :calls}, [])

  def payment_seen?(key), do: {:ok, Enum.any?(rows(), &(&1.idempotency_key == key))}

  def record_payment(row) do
    case :persistent_term.get({__MODULE__, :mode}) do
      :reject_status when row.status != "settled" ->
        {:error, :unsupported_status}

      :sequence_everything ->
        :persistent_term.put({__MODULE__, :rows}, [row | rows()])
        {:ok, length(rows())}

      _ ->
        :persistent_term.put({__MODULE__, :rows}, [row | rows()])
        if row.status == "settled", do: {:ok, length(rows())}, else: :ok
    end
  end

  def issuance_totals_since(namespace, beneficiary, since) do
    :persistent_term.put({__MODULE__, :calls}, [{namespace, beneficiary, since} | calls()])

    settled = Enum.filter(rows(), &(&1.status == "settled" and &1.namespace == namespace))

    {:ok,
     %{
       total_usd: Enum.reduce(settled, Decimal.new(0), &Decimal.add(&2, &1.amount_usd)),
       beneficiary_usd:
         settled
         |> Enum.filter(&(&1.beneficiary == beneficiary))
         |> Enum.reduce(Decimal.new(0), &Decimal.add(&2, &1.amount_usd))
     }}
  end

  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_binding), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
end

WindowStore.reset()
{:ok, deliveries4} = Agent.start_link(fn -> [] end)
{:ok, events4} = Agent.start_link(fn -> [] end)

durable =
  new_hub.(
    %{store_mod: WindowStore, max_issuance_per_window_usd: "10", small_topup_usd: "0"},
    deliveries4,
    events4
  )

{durable_n, durable} = Payments.settle([settlement.("dur:1", "budget:d", "9")], durable)
{durable_over_n, durable} = Payments.settle([settlement.("dur:2", "budget:d", "9")], durable)

Check.check(
  f,
  "a durable hub takes the trailing window from issuance_totals_since/3",
  durable_n == 1 and durable_over_n == 0 and
    Enum.any?(WindowStore.calls(), fn {namespace, beneficiary, since} ->
      namespace == "llm_quota" and beneficiary == "budget:d" and
        DateTime.compare(since, DateTime.add(now, -24 * 3600, :second)) == :eq
    end)
)

Check.check(
  f,
  "the quarantined row reaches the store carrying status quarantined",
  Enum.find(WindowStore.rows(), &(&1.idempotency_key == "dur:2")).status == "quarantined"
)

# ── version skew: a store that cannot record a quarantined row ───────────────
WindowStore.reset()
WindowStore.mode!(:reject_status)
{:ok, deliveries5} = Agent.start_link(fn -> [] end)
{:ok, events5} = Agent.start_link(fn -> [] end)

skewed =
  new_hub.(
    %{store_mod: WindowStore, max_payment_usd: "10"},
    deliveries5,
    events5
  )

{skew_n, skewed} = Payments.settle([settlement.("skew:1", "budget:s", "50")], skewed)

Check.check(
  f,
  "a store rejecting the quarantined status HOLDS the settlement (fail closed)",
  skew_n == 0 and skewed.settlement_mirror == [] and
    not MapSet.member?(skewed.seen_keys, "skew:1") and
    Agent.get(deliveries5, & &1) == []
)

Check.check(
  f,
  "the hub/store version skew is metered so an operator sees it",
  Enum.any?(Agent.get(events5, & &1), fn {event, meta} ->
    event == "payments_store_version_skew" and meta.idempotency_key == "skew:1" and
      meta.reason == "unsupported_status"
  end) and
    Enum.any?(Agent.get(events5, & &1), fn {event, meta} ->
      event == "payments_hold" and meta.idempotency_key == "skew:1"
    end)
)

# A held settlement must be re-presentable: once the store learns the status,
# the SAME settlement records.
WindowStore.mode!(:ok)
{healed_n, _skewed} = Payments.settle([settlement.("skew:1", "budget:s", "50")], skewed)

Check.check(
  f,
  "after the store is upgraded the held settlement records (quarantined, not credited)",
  healed_n == 0 and
    Enum.find(WindowStore.rows(), &(&1.idempotency_key == "skew:1")).status == "quarantined"
)

# A3 again, from the other side: a store that mints a sequence for a
# quarantined row would publish it to the outbox. Alarm and hold instead.
WindowStore.reset()
WindowStore.mode!(:sequence_everything)
{:ok, deliveries6} = Agent.start_link(fn -> [] end)
{:ok, events6} = Agent.start_link(fn -> [] end)

sequencing = new_hub.(%{store_mod: WindowStore, max_payment_usd: "10"}, deliveries6, events6)
{seq_n, sequencing} = Payments.settle([settlement.("seq:1", "budget:q", "50")], sequencing)

Check.check(
  f,
  "a store minting an outbox sequence for a quarantined row is refused, not credited",
  seq_n == 0 and sequencing.settlement_mirror == [] and Agent.get(deliveries6, & &1) == [] and
    Enum.any?(Agent.get(events6, & &1), fn {event, meta} ->
      event == "payments_store_version_skew" and meta.reason == "quarantine_sequence_assigned"
    end)
)

# ── cursor semantics: quarantine is a decision, a hold is not ───────────────
{:ok, deliveries7} = Agent.start_link(fn -> [] end)
{:ok, events7} = Agent.start_link(fn -> [] end)

cursor_hub = new_hub.(%{max_payment_usd: "1"}, deliveries7, events7)
over_cap = settlement.("cursor:1", "budget:c", "5")

{_n, cursor_hub} = Payments.settle([over_cap], cursor_hub)

Check.check(
  f,
  "a quarantined settlement does not hold its chain: the key is durably resolved",
  MapSet.member?(cursor_hub.seen_keys, "cursor:1")
)

# ── config validation for every new key ─────────────────────────────────────
invalid = fn overrides ->
  try do
    Payments.init!(
      Map.merge(%{xpub: xpub, allow_test_xpub: true, trusted_sources: [], targets: []}, overrides)
    )

    :did_not_raise
  rescue
    error -> {:raised, error}
  end
end

cap_config_cases = [
  {"max_payment_usd rejects a non-string", %{max_payment_usd: 10}},
  {"max_payment_usd rejects exponent notation", %{max_payment_usd: "1e6"}},
  {"max_payment_usd rejects a negative amount", %{max_payment_usd: "-5"}},
  {"max_payment_usd rejects zero", %{max_payment_usd: "0"}},
  {"max_payment_usd rejects a non-numeric string", %{max_payment_usd: "ten"}},
  {"max_issuance_per_window_usd rejects a non-string", %{max_issuance_per_window_usd: 10}},
  {"max_issuance_per_window_usd rejects exponent notation",
   %{max_issuance_per_window_usd: "1.0e3"}},
  {"max_issuance_per_window_usd rejects zero", %{max_issuance_per_window_usd: "0"}},
  {"small_topup_usd rejects a non-string", %{small_topup_usd: 5}},
  {"small_topup_usd rejects a negative amount", %{small_topup_usd: "-1"}},
  {"issuance_window_hours rejects zero", %{issuance_window_hours: 0}},
  {"issuance_window_hours rejects a non-integer", %{issuance_window_hours: 1.5}}
]

Check.check(
  f,
  "every new money/window config key is validated strictly at init",
  Enum.all?(cap_config_cases, fn {_label, overrides} ->
    match?({:raised, %ArgumentError{}}, invalid.(overrides))
  end)
)

Check.check(
  f,
  "each strict-config rejection names the offending key",
  Enum.all?(cap_config_cases, fn {_label, overrides} ->
    {:raised, error} = invalid.(overrides)
    key = overrides |> Map.keys() |> hd() |> Atom.to_string()
    Exception.message(error) =~ key
  end)
)

defaults =
  Payments.init!(%{xpub: xpub, allow_test_xpub: true, trusted_sources: [], targets: []})

Check.check(
  f,
  "defaults: max_payment_usd 10000, aggregate cap disabled, carve-out 5, window 24h",
  Decimal.equal?(defaults.max_payment_usd, Decimal.new("10000")) and
    defaults.max_issuance_per_window_usd == nil and
    Decimal.equal?(defaults.small_topup_usd, Decimal.new("5")) and
    defaults.issuance_window_hours == 24
)

Check.check(
  f,
  "small_topup_usd: \"0\" is a legal way to disable the carve-out",
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    small_topup_usd: "0"
  })
  |> Map.fetch!(:small_topup_usd)
  |> Decimal.equal?(Decimal.new(0))
)

# The aggregate cap over a durable store that cannot compute the window would
# hold EVERY settlement forever — that is a config error, refused at boot.
defmodule NoWindowStore do
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_row), do: :ok
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_binding), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
end

no_window_result =
  invalid.(%{
    store_mod: NoWindowStore,
    targets: ["llm_proxy"],
    max_issuance_per_window_usd: "100"
  })

Check.check(
  f,
  "an aggregate cap over a store without issuance_totals_since/3 is refused at init",
  match?({:raised, %ArgumentError{}}, no_window_result) and
    Exception.message(elem(no_window_result, 1)) =~ "issuance_totals_since/3"
)

Check.check(
  f,
  "the same store boots fine while the aggregate cap is disabled (the default)",
  match?(
    %{max_issuance_per_window_usd: nil},
    Payments.init!(%{
      xpub: xpub,
      allow_test_xpub: true,
      store_mod: NoWindowStore,
      targets: ["llm_proxy"]
    })
  )
)

Check.finish(f)
