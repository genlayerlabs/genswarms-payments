# Standalone — NO Postgres, NO network. mix run checks/payments_authorization_test.exs
#
# Task 5: the hub OWNS the issued-authorization registry (issuance action +
# five store callbacks) and credits a treasury Transfer ONLY when it carries
# a nonce the hub itself issued and can still resolve (spec §4.4's credit
# rule). Follows checks/payments_outbox_test.exs's shape: a fake in-memory
# store behind persistent_term, driven through the real public API
# (Payments.init!/1, Payments.handle_message/3, Payments.poll/1,
# Payments.settle/2 — never a private function).
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

defmodule AuthStore do
  # in-memory store via persistent_term, covering every callback group Task 5
  # needs coherent: bindings, settlement, issuance round-trip, nonce-filter
  # round-trip, plus the standalone unrecognised-inflow write.
  def reset do
    :persistent_term.put({__MODULE__, :d}, %{
      bindings: [],
      seen: MapSet.new(),
      rows: [],
      cursor: %{},
      issued_by_ref: %{},
      issued_by_nonce: %{},
      consumed: MapSet.new(),
      unrecognised: []
    })
  end

  defp d, do: :persistent_term.get({__MODULE__, :d})
  defp put(k, v), do: :persistent_term.put({__MODULE__, :d}, Map.put(d(), k, v))

  # ── bindings group ──
  def put_address_binding(b) do
    put(:bindings, [b | d().bindings])
    :ok
  end

  def list_address_bindings, do: {:ok, d().bindings}

  # ── settlement group ──
  def payment_seen?(k), do: {:ok, MapSet.member?(d().seen, k)}

  def record_payment(row) do
    put(:seen, MapSet.put(d().seen, row.idempotency_key))
    put(:rows, [row | d().rows])
    :ok
  end

  def get_last_scanned_block(chain), do: {:ok, Map.get(d().cursor, chain)}

  def put_last_scanned_block(chain, n) do
    put(:cursor, Map.put(d().cursor, chain, n))
    :ok
  end

  def rows, do: d().rows
  def cursor(chain), do: Map.get(d().cursor, chain)

  # ── entry A: issuance round trip ──
  # Idempotent by order_ref: a replay under the SAME order_ref changes
  # nothing and answers :duplicate — the original row is what a later
  # issued_authorization/1 lookup keeps returning.
  def record_issued_authorization(row) do
    if Map.has_key?(d().issued_by_ref, row.order_ref) do
      {:ok, :duplicate}
    else
      put(:issued_by_ref, Map.put(d().issued_by_ref, row.order_ref, row))
      put(:issued_by_nonce, Map.put(d().issued_by_nonce, row.nonce_hex, row))
      :ok
    end
  end

  def issued_authorization(nonce_hex), do: Map.get(d().issued_by_nonce, nonce_hex)

  # ── entry A: nonce-filter round trip ──
  # issued ∧ unconsumed ∧ unexpired — the reference implementation of the
  # bound the Store moduledoc promises.
  def live_authorization_nonces(now) do
    d().issued_by_nonce
    |> Enum.reject(fn {nonce, _row} -> MapSet.member?(d().consumed, nonce) end)
    |> Enum.filter(fn {_nonce, row} -> row.valid_before > now end)
    |> Enum.map(fn {nonce, _row} -> nonce end)
  end

  def mark_authorization_consumed(nonce_hex) do
    put(:consumed, MapSet.put(d().consumed, nonce_hex))
    :ok
  end

  def consumed?(nonce_hex), do: MapSet.member?(d().consumed, nonce_hex)

  def record_unrecognised_inflow(row) do
    put(:unrecognised, [row | d().unrecognised])
    :ok
  end

  def unrecognised, do: d().unrecognised
  def issued_count, do: map_size(d().issued_by_ref)
end

# A store implementing HALF of the issuance round trip — exactly the
# "worse than none" shape `validate_store_coherence!/1` exists to reject for
# every other callback group in this package.
defmodule HalfAuthStore do
  def record_issued_authorization(_row), do: :ok
end

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# The frozen literal from the plan (externally verified 2026-07-26), typed
# here by hand — deliberately NOT re-derived via Keccak, so this test and
# `checks/payments_keccak_test.exs`'s independent Keccak re-derivation only
# agree with usdc.ex's compile-time-computed constant if all three actually
# match. If usdc.ex's constant were wrong, the correlation below would
# silently fail and tests 5/9 would go red.
auth_sig = "0x98de503528ee59b575ef0c0a2576a82497bfc029a5685b209e9ec333479b10a5"

pad_addr = fn "0x" <> hex -> "0x" <> String.duplicate("0", 24) <> String.downcase(hex) end
pad_nonce = fn "0x" <> hex -> "0x" <> hex end

value_hex = fn units -> "0x" <> String.pad_leading(Integer.to_string(units, 16), 64, "0") end

mk_transfer_log = fn to_addr, from_addr, block, tx, idx, units ->
  %{
    "address" => "0xCONTRACT",
    "topics" => [transfer_sig, pad_addr.(from_addr), pad_addr.(to_addr)],
    "data" => value_hex.(units),
    "blockNumber" => "0x" <> Integer.to_string(block, 16),
    "transactionHash" => tx,
    "logIndex" => "0x" <> Integer.to_string(idx, 16)
  }
end

mk_auth_log = fn authorizer_addr, nonce_hex, block, tx, idx ->
  %{
    "address" => "0xCONTRACT",
    "topics" => [auth_sig, pad_addr.(authorizer_addr), pad_nonce.(nonce_hex)],
    "data" => "0x",
    "blockNumber" => "0x" <> Integer.to_string(block, 16),
    "transactionHash" => tx,
    "logIndex" => "0x" <> Integer.to_string(idx, 16)
  }
end

# Routes on the OUTGOING query's own topic0 — a canned two-leg RPC standing
# in for the watcher's two getLogs calls (Transfer over watched+treasury
# addresses, AuthorizationUsed over the live nonce filter).
mk_rpc = fn transfer_logs, auth_logs, latest ->
  fn chain, method, params ->
    case method do
      "eth_blockNumber" ->
        {:ok, "0x" <> Integer.to_string(latest, 16)}

      "eth_getLogs" ->
        [%{"topics" => topics}] = params

        case topics do
          [^transfer_sig | _] -> {:ok, transfer_logs}
          [^auth_sig | _] -> {:ok, auth_logs}
        end

      m ->
        Check.self_check_rpc(chain, m)
    end
  end
end

treasury = "0x" <> String.duplicate("9", 40)
stranger = "0x" <> String.duplicate("a", 40)

# ─────────────────────────────────────────────────────────────────────────
# Tests 1-4: issue_authorization, the hub action
# ─────────────────────────────────────────────────────────────────────────

AuthStore.reset()

issuance_config = %{
  xpub: xpub,
  allow_test_xpub: true,
  trusted_sources: ["ingress"],
  targets: [],
  namespace: "hub_ns",
  store_mod: AuthStore,
  auto_tick: false,
  chains: []
}

state = Payments.init!(issuance_config)

future_valid_before = DateTime.to_unix(DateTime.utc_now()) + 3600
nonce1 = "0x" <> String.duplicate("11", 32)

issue1 = %{
  "action" => "issue_authorization",
  "nonce" => nonce1,
  "order_ref" => "order-1",
  "beneficiary" => "user:alice",
  "amount_usd" => "25",
  "valid_before" => future_valid_before,
  # Attacker-controlled field: the reply and the stored row must carry the
  # HUB's namespace ("hub_ns"), never this one.
  "namespace" => "attacker_ns"
}

{:reply, json1, state} = Payments.handle_message("ingress", Jason.encode!(issue1), state)
reply1 = Jason.decode!(json1)

Check.check(f, "1. trusted issue_authorization succeeds", reply1["ok"] == true)

Check.check(
  f,
  "1. the reply carries the HUB's namespace, not the caller's",
  reply1["namespace"] == "hub_ns"
)

Check.check(
  f,
  "1. the stored row is sealed under the HUB's namespace too",
  AuthStore.issued_authorization(nonce1).namespace == "hub_ns"
)

# 2. an untrusted source's issuance is refused; nothing is written.
untrusted_nonce = "0x" <> String.duplicate("ee", 32)

untrusted_issue = %{
  "action" => "issue_authorization",
  "nonce" => untrusted_nonce,
  "order_ref" => "order-untrusted",
  "beneficiary" => "user:mallory",
  "amount_usd" => "25",
  "valid_before" => future_valid_before
}

untrusted_result =
  Payments.handle_message("stranger", Jason.encode!(untrusted_issue), state)

Check.check(
  f,
  "2. an untrusted source gets no reply (same stance as deposit_address)",
  match?({:noreply, _}, untrusted_result)
)

Check.check(
  f,
  "2. nothing was written for the untrusted attempt",
  AuthStore.issued_authorization(untrusted_nonce) == nil
)

# 3. repeating the SAME order_ref is idempotent: one row, the FIRST content
# wins even if a replay's payload differs (proving the store's row survives
# unchanged rather than being silently overwritten).
before_count = AuthStore.issued_count()

replay = %{
  issue1
  | "nonce" => "0x" <> String.duplicate("22", 32),
    "amount_usd" => "999"
}

{:reply, json3, state} = Payments.handle_message("ingress", Jason.encode!(replay), state)
reply3 = Jason.decode!(json3)

Check.check(f, "3. the replayed issuance still answers ok:true", reply3["ok"] == true)

Check.check(
  f,
  "3. repeating order_ref writes no second row",
  AuthStore.issued_count() == before_count
)

Check.check(
  f,
  "3. the original nonce/amount are what's still on record (first write wins)",
  AuthStore.issued_authorization(nonce1).amount_usd == Decimal.new("25")
)

# 4. malformed shape ⇒ typed refusal, nothing written.
bad_nonce_msg = %{issue1 | "nonce" => "not-a-nonce", "order_ref" => "order-bad-nonce"}
{:reply, json4a, state} = Payments.handle_message("ingress", Jason.encode!(bad_nonce_msg), state)
reply4a = Jason.decode!(json4a)

Check.check(
  f,
  "4. a malformed nonce is refused with a typed error",
  reply4a["ok"] == false and reply4a["error"] == "bad_nonce"
)

Check.check(
  f,
  "4. nothing was written for the bad-nonce attempt",
  AuthStore.issued_count() == before_count
)

bad_amount_msg = %{
  issue1
  | "nonce" => "0x" <> String.duplicate("33", 32),
    "order_ref" => "order-bad-amount",
    "amount_usd" => "0"
}

{:reply, json4b, _state} =
  Payments.handle_message("ingress", Jason.encode!(bad_amount_msg), state)

reply4b = Jason.decode!(json4b)

Check.check(
  f,
  "4. a non-positive amount is refused with a typed error",
  reply4b["ok"] == false and reply4b["error"] == "bad_amount"
)

Check.check(
  f,
  "4. nothing was written for the bad-amount attempt either",
  AuthStore.issued_count() == before_count
)

# ─────────────────────────────────────────────────────────────────────────
# Tests 5-10: the credit rule (spec §4.4)
# ─────────────────────────────────────────────────────────────────────────

AuthStore.reset()

now = ~U[2026-07-26 12:00:00Z]
now_unix = DateTime.to_unix(now)

scan_config = %{
  xpub: xpub,
  allow_test_xpub: true,
  trusted_sources: ["ingress"],
  targets: [],
  namespace: "hub_ns",
  store_mod: AuthStore,
  auto_tick: false,
  now_fn: fn -> now end,
  deliver_fn: fn _, _, _ -> :ok end,
  chains: [
    %{
      name: "base",
      chain_id: 8453,
      rpc_url: "injected",
      usdc_contract: "0xCONTRACT",
      treasury_address: treasury,
      confirmations: 0,
      fast_credit_depth: 0,
      decimals: 6,
      start_block: 0,
      max_block_range: 1000,
      address_chunk: 200
    }
  ],
  rpc_fn: nil
}

scan_state0 = Payments.init!(scan_config)

# Issue an authorization THIS hub will correlate against.
issue5 = %{
  "action" => "issue_authorization",
  "nonce" => "0x" <> String.duplicate("aa", 32),
  "order_ref" => "order-5",
  "beneficiary" => "user:issued5",
  "amount_usd" => "40",
  "valid_before" => now_unix + 3600
}

{:reply, _json, scan_state0} =
  Payments.handle_message("ingress", Jason.encode!(issue5), scan_state0)

nonce5 = "0x" <> String.duplicate("aa", 32)

# 5. an inflow whose nonce we issued SETTLES, and the nonce is marked
# consumed (it drops out of live_authorization_nonces).
transfer5 = mk_transfer_log.(treasury, stranger, 10, "0xTX5", 0, 30_000_000)
auth5 = mk_auth_log.(stranger, nonce5, 10, "0xTX5", 1)

scan_state5 =
  %{scan_state0 | rpc_fn: mk_rpc.([transfer5], [auth5], 100)}
  |> Payments.poll()

Check.check(f, "5. the treasury inflow settled", length(AuthStore.rows()) == 1)

row5 = hd(AuthStore.rows())

Check.check(f, "5. it settled under the issued beneficiary", row5.beneficiary == "user:issued5")

Check.check(f, "5. the settled amount is what actually MOVED", Decimal.equal?(row5.amount_usd, Decimal.new("30")))

Check.check(f, "5. the method is usdc_authorization", row5.method == "usdc_authorization")

Check.check(
  f,
  "5. the settlement is stamped under the HUB's namespace",
  row5.namespace == "hub_ns"
)

Check.check(
  f,
  "5. the nonce is now marked consumed",
  AuthStore.consumed?(nonce5)
)

Check.check(
  f,
  "5. and it no longer appears in live_authorization_nonces",
  nonce5 not in AuthStore.live_authorization_nonces(now_unix)
)

# 6. a Transfer to the treasury with NO correlated AuthorizationUsed log at
# all does NOT settle and is recorded as unrecognised. The cursor still
# advances (proven by a later poll never re-processing block 20).
transfer6 = mk_transfer_log.(treasury, stranger, 150, "0xTX6", 0, 15_000_000)

scan_state6 =
  %{scan_state5 | rpc_fn: mk_rpc.([transfer6], [], 200)}
  |> Payments.poll()

Check.check(
  f,
  "6. the unrecognised inflow did NOT settle",
  length(AuthStore.rows()) == 1
)

Check.check(
  f,
  "6. it WAS recorded as an unrecognised inflow",
  Enum.any?(AuthStore.unrecognised(), &(&1.tx_hash == "0xTX6"))
)

Check.check(
  f,
  "6. the chain's cursor still advanced past it (not held)",
  AuthStore.cursor("base") == 200
)

# 7. THE spec §4.4 test: a nonce that correlates to a DIFFERENT domain (e.g.
# a future deposit-sweep collection landing in this same treasury wallet)
# must NOT settle even though a real on-chain correlation exists — the
# registry (`issued_authorization/1`), not the mere presence of a nonce, is
# what the credit rule trusts. Exercised directly against `Payments.settle/2`
# (the public entry point), because the watcher's OWN getLogs filter already
# only ever asks the chain about nonces THIS hub issued — this test pins the
# credit rule's OWN independent refusal as defense in depth, regardless of
# whether the scan-level filter would also have prevented the correlation.
foreign_nonce = "0x" <> String.duplicate("bb", 32)

sweep_settlement = %{
  beneficiary: nil,
  namespace: nil,
  method: "usdc_authorization",
  nonce_hex: foreign_nonce,
  amount_usd: Decimal.new("500"),
  ref: "0xSWEEP:0",
  idempotency_key: "8453:0xSWEEP:0",
  raw_amount: 500_000_000,
  decimals: 6,
  token_contract: "0xCONTRACT",
  chain: "base",
  chain_id: 8453,
  block_number: 30,
  log_index: 0,
  tx_hash: "0xSWEEP",
  from_address: stranger
}

{settled_count7, scan_state7} = Payments.settle([sweep_settlement], scan_state6)

Check.check(f, "7. the collection-domain inflow settled ZERO payments", settled_count7 == 0)

Check.check(
  f,
  "7. it never appears in the settlement ledger",
  not Enum.any?(AuthStore.rows(), &(&1.idempotency_key == "8453:0xSWEEP:0"))
)

Check.check(
  f,
  "7. it WAS recorded as unrecognised instead",
  Enum.any?(AuthStore.unrecognised(), &(&1.tx_hash == "0xSWEEP"))
)

Check.check(
  f,
  "7. the foreign nonce was never marked consumed by us (we never issued it)",
  not AuthStore.consumed?(foreign_nonce)
)

# 8. a deposit-address inflow keeps settling exactly as before — entry B is
# unaffected by the treasury lane sharing the same watcher.
{:reply, dep_json, scan_state8} =
  Payments.handle_message(
    "ingress",
    Jason.encode!(%{"action" => "deposit_address", "beneficiary" => "user:depositor"}),
    scan_state7
  )

depositor_addr = Jason.decode!(dep_json)["address"]
transfer8 = mk_transfer_log.(depositor_addr, stranger, 250, "0xTX8", 0, 5_000_000)

scan_state8 =
  %{scan_state8 | rpc_fn: mk_rpc.([transfer8], [], 300)}
  |> Payments.poll()

row8 = Enum.find(AuthStore.rows(), &(&1.idempotency_key == "8453:0xTX8:0"))

Check.check(f, "8. the deposit-address inflow still settled", row8 != nil)
Check.check(f, "8. under its own bound beneficiary", row8 && row8.beneficiary == "user:depositor")
Check.check(f, "8. tagged usdc_<chain>, not usdc_authorization", row8 && row8.method == "usdc_base")

# 9. entry A's idempotency key has the SAME shape as entry B's
# ("<chain_id>:<tx_hash>:<log_index>"), and processing the same log twice
# settles it exactly once.
Check.check(
  f,
  "9. entry A's idempotency key has the same shape as entry B's",
  row5.idempotency_key == "8453:0xTX5:0" and row8.idempotency_key == "8453:0xTX8:0"
)

{replay_count9, _state9} = Payments.settle([sweep_settlement | []], scan_state8)
# re-settling the ALREADY-processed authorization settlement (row5's own
# shape) a second time must not double-credit it.
already_settled = %{
  beneficiary: "user:issued5",
  namespace: "hub_ns",
  method: "usdc_authorization",
  nonce_hex: nonce5,
  amount_usd: Decimal.new("30"),
  ref: "0xTX5:0",
  idempotency_key: "8453:0xTX5:0",
  raw_amount: 30_000_000,
  decimals: 6,
  token_contract: "0xCONTRACT",
  chain: "base",
  chain_id: 8453,
  block_number: 10,
  log_index: 0,
  tx_hash: "0xTX5",
  from_address: stranger
}

{replay5_count, _state9b} = Payments.settle([already_settled], scan_state8)

Check.check(f, "9. re-settling zero-new inflow (foreign) still settles nothing", replay_count9 == 0)

Check.check(
  f,
  "9. re-processing the SAME already-settled log settles it a second time: zero",
  replay5_count == 0
)

Check.check(
  f,
  "9. and the ledger still has exactly one row for that key",
  Enum.count(AuthStore.rows(), &(&1.idempotency_key == "8453:0xTX5:0")) == 1
)

# 10. an expired, unconsumed authorization does NOT appear in
# live_authorization_nonces — the bound is real, not just documented.
AuthStore.reset()

expired_nonce = "0x" <> String.duplicate("cc", 32)
live_nonce = "0x" <> String.duplicate("dd", 32)

:ok =
  AuthStore.record_issued_authorization(%{
    nonce_hex: expired_nonce,
    order_ref: "order-expired",
    beneficiary: "user:expired",
    amount_usd: Decimal.new("10"),
    namespace: "hub_ns",
    valid_before: now_unix - 10,
    issued_at: now
  })

:ok =
  AuthStore.record_issued_authorization(%{
    nonce_hex: live_nonce,
    order_ref: "order-live",
    beneficiary: "user:live",
    amount_usd: Decimal.new("10"),
    namespace: "hub_ns",
    valid_before: now_unix + 10,
    issued_at: now
  })

live_set = AuthStore.live_authorization_nonces(now_unix)

Check.check(f, "10. an expired, unconsumed nonce is NOT in the live set", expired_nonce not in live_set)
Check.check(f, "10. an unexpired nonce IS in the live set", live_nonce in live_set)

# ─────────────────────────────────────────────────────────────────────────
# Bonus coverage (cheap, directly called for by the plan's narrative)
# ─────────────────────────────────────────────────────────────────────────

# Defense in depth (plan step 3): an AuthorizationUsed-shaped log (3 topics,
# topic0 == @authorization_used_topic) routed through the TRANSFER query's
# results — e.g. a misbehaving RPC ignoring the topic filter — must never be
# mistaken for a Transfer. It should be silently skipped, and the genuine
# Transfer log in the same batch must still settle.
AuthStore.reset()

mixed_state0 =
  Payments.init!(%{scan_config | store_mod: AuthStore})

{:reply, mixed_dep_json, mixed_state0} =
  Payments.handle_message(
    "ingress",
    Jason.encode!(%{"action" => "deposit_address", "beneficiary" => "user:mixed"}),
    mixed_state0
  )

mixed_addr = Jason.decode!(mixed_dep_json)["address"]

# A genuine settlement to a WATCHED deposit address (not the treasury), so
# "it settled" is an unambiguous signal distinct from the treasury's
# unrecognised-by-default path exercised in tests 6/7.
legit_transfer = mk_transfer_log.(mixed_addr, stranger, 5, "0xLEGIT", 0, 1_000_000)
foreign_shaped_log = mk_auth_log.(stranger, "0x" <> String.duplicate("ff", 32), 5, "0xNOISE", 0)

mixed_state =
  %{
    mixed_state0
    | rpc_fn: fn chain, method, params ->
        case method do
          "eth_blockNumber" ->
            {:ok, "0x64"}

          "eth_getLogs" ->
            [%{"topics" => topics}] = params

            case topics do
              [^transfer_sig | _] -> {:ok, [foreign_shaped_log, legit_transfer]}
              [^auth_sig | _] -> {:ok, []}
            end

          m ->
            Check.self_check_rpc(chain, m)
        end
      end
  }
  |> Payments.poll()

Check.check(
  f,
  "bonus: an AuthorizationUsed-shaped log through the Transfer path never crashes the scan",
  true
)

Check.check(
  f,
  "bonus: only the genuine Transfer log settled (the foreign-shaped one was skipped, not settled)",
  length(AuthStore.rows()) == 1 and hd(AuthStore.rows()).tx_hash == "0xLEGIT"
)

_ = mixed_state

# Coherence (init-time): implementing HALF of the issuance round trip is
# refused at init, exactly like every other callback group in this package.
Check.check(
  f,
  "bonus: a store exporting only record_issued_authorization/1 raises at init",
  match?({:error, _}, Payments.init(%{scan_config | store_mod: HalfAuthStore}))
)

Check.finish(f)
