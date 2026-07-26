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
      settled_nonces: MapSet.new(),
      unrecognised: [],
      fail_record: false,
      fail_lookup: false,
      fail_consume: false,
      fail_settled_lookup: false
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

  # A store blip is switchable so the FAIL-CLOSED hold path can be driven
  # through a real poll/1 round (C1) instead of being reasoned about.
  def fail_record(flag), do: put(:fail_record, flag)

  def record_payment(row) do
    if d().fail_record do
      {:error, :db_down}
    else
      put(:seen, MapSet.put(d().seen, row.idempotency_key))
      put(:rows, [row | d().rows])

      # N1: track SETTLED nonces independently of `consumed` — this is what
      # lets the fake reproduce mark_authorization_consumed failing on a
      # settled row (nonce stays live+unconsumed) while still knowing the
      # nonce was, in fact, already credited once.
      if row.status == "settled" and is_binary(Map.get(row, :nonce_hex)) do
        put(:settled_nonces, MapSet.put(d().settled_nonces, row.nonce_hex))
      end

      :ok
    end
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
  # nothing and answers `{:ok, :duplicate, stored_row}` — the ROW ON RECORD,
  # which is what the hub must echo back instead of the request that lost the
  # race (a request-echo acks a nonce that was never registered).
  def record_issued_authorization(row) do
    if Map.has_key?(d().issued_by_ref, row.order_ref) do
      {:ok, :duplicate, Map.fetch!(d().issued_by_ref, row.order_ref)}
    else
      put(:issued_by_ref, Map.put(d().issued_by_ref, row.order_ref, row))
      put(:issued_by_nonce, Map.put(d().issued_by_nonce, row.nonce_hex, row))
      :ok
    end
  end

  # A lookup blip is switchable too (F2), same reason as fail_record: driven
  # through a real poll/1 round rather than reasoned about via a separate fake
  # module.
  def fail_lookup(flag), do: put(:fail_lookup, flag)

  def issued_authorization(nonce_hex) do
    if d().fail_lookup do
      raise "issuance lookup is down"
    else
      Map.get(d().issued_by_nonce, nonce_hex)
    end
  end

  # ── entry A: nonce-filter round trip ──
  # issued ∧ unconsumed ∧ unexpired — the reference implementation of the
  # bound the Store moduledoc promises.
  def live_authorization_nonces(now) do
    d().issued_by_nonce
    |> Enum.reject(fn {nonce, _row} -> MapSet.member?(d().consumed, nonce) end)
    |> Enum.filter(fn {_nonce, row} -> row.valid_before > now end)
    |> Enum.map(fn {nonce, _row} -> nonce end)
  end

  # N1: switchable so the documented best-effort/logged failure path can be
  # driven through a real poll/1 round instead of being reasoned about.
  def fail_consume(flag), do: put(:fail_consume, flag)

  def mark_authorization_consumed(nonce_hex) do
    if d().fail_consume do
      {:error, :db_down}
    else
      put(:consumed, MapSet.put(d().consumed, nonce_hex))
      :ok
    end
  end

  def consumed?(nonce_hex), do: MapSet.member?(d().consumed, nonce_hex)

  # N1: has a SETTLED settlement already been recorded for this nonce? This
  # is the nonce-level guard the credit rule checks BEFORE crediting — the
  # only wall standing between a genuinely later on-chain reuse of the same
  # nonce (different tx_hash, so a different idempotency_key that ordinary
  # settlement dedup never catches) and a second credit.
  def fail_settled_lookup(flag), do: put(:fail_settled_lookup, flag)

  def authorization_settled?(nonce_hex) do
    if d().fail_settled_lookup do
      raise "settled lookup is down"
    else
      {:ok, MapSet.member?(d().settled_nonces, nonce_hex)}
    end
  end

  def settled?(nonce_hex), do: MapSet.member?(d().settled_nonces, nonce_hex)

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

# (I1) The damning shape: a fully production-looking DURABLE settlement store
# that knows nothing about authorizations. It satisfies every pre-existing
# gate, and every payment it acknowledges would land as an unrecognised
# inflow.
defmodule NoAuthStore do
  def put_address_binding(_b), do: :ok
  def list_address_bindings, do: {:ok, []}
  def payment_seen?(_k), do: {:ok, false}
  def record_payment(_row), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _n), do: :ok
end

# (C3) A store that detects the duplicate but cannot hand back the row it
# deduped against. The hub has no order_ref lookup, so it CANNOT repair this
# — it must refuse rather than echo the request's unregistered nonce.
defmodule RowlessDupStore do
  def record_issued_authorization(_row), do: {:ok, :duplicate}
  def issued_authorization(_nonce), do: nil
  def live_authorization_nonces(_now), do: []
  def mark_authorization_consumed(_nonce), do: :ok
  def record_unrecognised_inflow(_row), do: :ok
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

# (C3) The reply itself — not only the store — must be the STORED row. A
# caller that timed out on the first reply retries the order with a freshly
# minted nonce; echoing THAT nonce back with ok:true hands the user a nonce
# nobody registered, which never enters the getLogs filter and buries their
# payment as an unrecognised inflow.
Check.check(
  f,
  "3. (C3) the replay's reply carries the STORED nonce, not the request's",
  reply3["nonce"] == nonce1
)

Check.check(
  f,
  "3. (C3) the replay's reply carries the STORED amount, not the request's",
  reply3["amount_usd"] == "25"
)

Check.check(
  f,
  "3. (C3) the nonce the replay acked is genuinely registered",
  AuthStore.issued_authorization(reply3["nonce"]) != nil
)

Check.check(
  f,
  "3. (C3) and the request's own fresh nonce was never registered",
  AuthStore.issued_authorization("0x" <> String.duplicate("22", 32)) == nil
)

# (C3) A store that answers :duplicate without the row cannot be honored —
# the hub has no order_ref lookup to read it back with. Refuse, never echo.
rowless_state =
  Payments.init!(%{issuance_config | store_mod: RowlessDupStore})

{:reply, rowless_json, _} =
  Payments.handle_message("ingress", Jason.encode!(issue1), rowless_state)

rowless_reply = Jason.decode!(rowless_json)

Check.check(
  f,
  "3. (C3) a row-less :duplicate is refused, not answered ok:true",
  rowless_reply["ok"] == false and rowless_reply["error"] == "store_unavailable"
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

# (I4) The window is BOUNDED AT ISSUE TIME — the "bounded by construction"
# property the whole live-nonce-filter design rests on was previously
# delegated entirely to the caller.
now_at_issue = DateTime.to_unix(DateTime.utc_now())

refuse_valid_before = fn label, value ->
  msg = %{
    issue1
    | "nonce" => "0x" <> String.duplicate("44", 32),
      "order_ref" => "order-#{label}",
      "valid_before" => value
  }

  {:reply, json, _} = Payments.handle_message("ingress", Jason.encode!(msg), state)
  Jason.decode!(json)
end

expired_reply = refuse_valid_before.("expired", now_at_issue - 1)

Check.check(
  f,
  "4. (I4) an already-expired valid_before is refused, not acked ok:true",
  expired_reply["ok"] == false and expired_reply["error"] == "expired_valid_before"
)

far_reply = refuse_valid_before.("far", now_at_issue + 100 * 365 * 24 * 3600)

Check.check(
  f,
  "4. (I4) a valid_before beyond the configured window is refused",
  far_reply["ok"] == false and far_reply["error"] == "valid_before_too_far"
)

absent_reply = refuse_valid_before.("absent", nil)

Check.check(
  f,
  "4. (I4) an absent/non-integer valid_before is refused",
  absent_reply["ok"] == false and absent_reply["error"] == "bad_valid_before"
)

Check.check(
  f,
  "4. (I4) none of the three unbounded-window attempts wrote a row",
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

# ─────────────────────────────────────────────────────────────────────────
# Fix wave — every case below was PROVEN exploitable against the first cut
# of this lane, through the same public API (init!/1, handle_message/3,
# poll/1, settle/2). Each one is red without its fix.
# ─────────────────────────────────────────────────────────────────────────

alice = "0x" <> String.duplicate("a1", 20)
mallory = "0x" <> String.duplicate("b2", 20)
carol = "0x" <> String.duplicate("c3", 20)
sweeper = "0x" <> String.duplicate("d4", 20)
eve = "0x" <> String.duplicate("e5", 20)

issue_for = fn state, nonce, order_ref, beneficiary, amount, valid_before ->
  {:reply, _json, state} =
    Payments.handle_message(
      "ingress",
      Jason.encode!(%{
        "action" => "issue_authorization",
        "nonce" => nonce,
        "order_ref" => order_ref,
        "beneficiary" => beneficiary,
        "amount_usd" => amount,
        "valid_before" => valid_before
      }),
      state
    )

  state
end

row_for = fn key -> Enum.find(AuthStore.rows(), &(&1.idempotency_key == key)) end

# ── C2: correlation is {tx_hash, authorizer}, never tx_hash alone ─────────
#
# Two AuthorizationUsed events in ONE tx (a batched multicall — EIP-3009
# authorizations are submittable by anyone, so their order inside a tx is
# attacker-controlled). Keying tx_hash => nonce with last-write-wins attached
# ONE arbitrary nonce to BOTH treasury Transfers: $100 of Alice's payment was
# credited to Mallory, who had spent $1.
AuthStore.reset()
c2a_state = Payments.init!(scan_config)

nonce_alice = "0x" <> String.duplicate("a1", 32)
nonce_mallory = "0x" <> String.duplicate("b2", 32)

c2a_state = issue_for.(c2a_state, nonce_alice, "order-alice", "user:alice", "100", now_unix + 3600)

c2a_state =
  issue_for.(c2a_state, nonce_mallory, "order-mallory", "user:mallory", "1", now_unix + 3600)

c2a_state =
  %{
    c2a_state
    | rpc_fn:
        mk_rpc.(
          [
            mk_transfer_log.(treasury, alice, 10, "0xTXB", 0, 100_000_000),
            mk_transfer_log.(treasury, mallory, 10, "0xTXB", 2, 1_000_000)
          ],
          [
            mk_auth_log.(alice, nonce_alice, 10, "0xTXB", 1),
            mk_auth_log.(mallory, nonce_mallory, 10, "0xTXB", 3)
          ],
          100
        )
  }
  |> Payments.poll()

alice_row = row_for.("8453:0xTXB:0")
mallory_row = row_for.("8453:0xTXB:2")

Check.check(
  f,
  "C2. two authorizations in one tx: Alice's Transfer credits ALICE",
  alice_row != nil and alice_row.beneficiary == "user:alice" and
    Decimal.equal?(alice_row.amount_usd, Decimal.new("100"))
)

Check.check(
  f,
  "C2. and Mallory's Transfer credits MALLORY, for what Mallory actually moved",
  mallory_row != nil and mallory_row.beneficiary == "user:mallory" and
    Decimal.equal?(mallory_row.amount_usd, Decimal.new("1"))
)

Check.check(
  f,
  "C2. no crossed credit exists in the ledger at all",
  Enum.count(AuthStore.rows()) == 2
)

_ = c2a_state

# Two Transfers, ONE AuthorizationUsed (the Task-6 sweep shape: a collection
# Transfer riding the same tx as a user's authorization). Only the Transfer
# whose `from` IS the authorizer may settle; the other is unrecognised.
AuthStore.reset()
c2b_state = Payments.init!(scan_config)
nonce_carol = "0x" <> String.duplicate("c3", 32)
c2b_state = issue_for.(c2b_state, nonce_carol, "order-carol", "user:carol", "10", now_unix + 3600)

c2b_state =
  %{
    c2b_state
    | rpc_fn:
        mk_rpc.(
          [
            mk_transfer_log.(treasury, carol, 10, "0xTXC", 0, 10_000_000),
            mk_transfer_log.(treasury, sweeper, 10, "0xTXC", 2, 900_000_000)
          ],
          [mk_auth_log.(carol, nonce_carol, 10, "0xTXC", 1)],
          100
        )
  }
  |> Payments.poll()

credited_total =
  Enum.reduce(AuthStore.rows(), Decimal.new(0), &Decimal.add(&2, &1.amount_usd))

Check.check(
  f,
  "C2. two Transfers + one authorization: only the authorizer's own Transfer settles",
  Enum.count(AuthStore.rows()) == 1 and row_for.("8453:0xTXC:0") != nil
)

Check.check(
  f,
  "C2. the unmatched Transfer is unrecognised, never credited",
  Decimal.equal?(credited_total, Decimal.new("10")) and
    Enum.any?(AuthStore.unrecognised(), &(&1.idempotency_key == "8453:0xTXC:2"))
)

_ = c2b_state

# authorizer ≠ from: a real, issued, live authorization in the tx — but the
# Transfer came from somebody else, which EIP-3009 says cannot be its
# settlement. Never credited.
AuthStore.reset()
c2c_state = Payments.init!(scan_config)
nonce_dave = "0x" <> String.duplicate("d4", 32)
c2c_state = issue_for.(c2c_state, nonce_dave, "order-dave", "user:dave", "50", now_unix + 3600)

c2c_state =
  %{
    c2c_state
    | rpc_fn:
        mk_rpc.(
          [mk_transfer_log.(treasury, stranger, 10, "0xTXD", 0, 50_000_000)],
          [mk_auth_log.(alice, nonce_dave, 10, "0xTXD", 1)],
          100
        )
  }
  |> Payments.poll()

Check.check(
  f,
  "C2. authorizer≠from settles NOTHING",
  AuthStore.rows() == []
)

Check.check(
  f,
  "C2. authorizer≠from is recorded unrecognised with a reason, and the nonce stays live",
  Enum.any?(AuthStore.unrecognised(), &(&1.tx_hash == "0xTXD" and &1.reason == "not_issued")) and
    not AuthStore.consumed?(nonce_dave)
)

_ = c2c_state

# Ambiguity fails CLOSED: two treasury Transfers in one tx sharing a `from`
# with a single authorization. Nothing distinguishes which Transfer the
# authorization settles, and log order is attacker-controlled — so the hub
# refuses BOTH rather than guess (and never pays one authorization twice).
AuthStore.reset()
c2d_state = Payments.init!(scan_config)
nonce_eve = "0x" <> String.duplicate("e5", 32)
c2d_state = issue_for.(c2d_state, nonce_eve, "order-eve", "user:eve", "10", now_unix + 3600)

c2d_state =
  %{
    c2d_state
    | rpc_fn:
        mk_rpc.(
          [
            mk_transfer_log.(treasury, eve, 10, "0xTXE", 0, 10_000_000),
            mk_transfer_log.(treasury, eve, 10, "0xTXE", 2, 10_000_000)
          ],
          [mk_auth_log.(eve, nonce_eve, 10, "0xTXE", 1)],
          100
        )
  }
  |> Payments.poll()

Check.check(
  f,
  "C2. an undecidable correlation credits NOTHING (never one authorization paid twice)",
  AuthStore.rows() == []
)

Check.check(
  f,
  "C2. both ambiguous Transfers are recorded with reason ambiguous_correlation",
  Enum.count(AuthStore.unrecognised(), &(&1.reason == "ambiguous_correlation")) == 2
)

_ = c2d_state

# The other undecidable shape: ONE Transfer, but TWO issued authorizations in
# the tx whose authorizer is that Transfer's sender. Pathological, and the
# hub still must not pick one.
AuthStore.reset()
c2e_state = Payments.init!(scan_config)
nonce_e1 = "0x" <> String.duplicate("e1", 32)
nonce_e2 = "0x" <> String.duplicate("e2", 32)
c2e_state = issue_for.(c2e_state, nonce_e1, "order-e1", "user:e1", "10", now_unix + 3600)
c2e_state = issue_for.(c2e_state, nonce_e2, "order-e2", "user:e2", "20", now_unix + 3600)

c2e_state =
  %{
    c2e_state
    | rpc_fn:
        mk_rpc.(
          [mk_transfer_log.(treasury, eve, 10, "0xTXF", 0, 10_000_000)],
          [
            mk_auth_log.(eve, nonce_e1, 10, "0xTXF", 1),
            mk_auth_log.(eve, nonce_e2, 10, "0xTXF", 2)
          ],
          100
        )
  }
  |> Payments.poll()

Check.check(
  f,
  "C2. one Transfer matching TWO issued authorizations credits neither",
  AuthStore.rows() == [] and
    Enum.any?(AuthStore.unrecognised(), &(&1.reason == "ambiguous_correlation"))
)

Check.check(
  f,
  "C2. and the refused row records BOTH candidates it declined to choose between",
  (fn ->
     row = Enum.find(AuthStore.unrecognised(), &(&1.tx_hash == "0xTXF"))
     row != nil and Enum.sort(row.nonce_candidates) == Enum.sort([nonce_e1, nonce_e2])
   end).()
)

_ = c2e_state

# ── C1: a HELD settlement must leave the nonce live, and the retry credits ──
AuthStore.reset()
c1_state = Payments.init!(scan_config)
nonce_c1 = "0x" <> String.duplicate("1c", 32)
c1_state = issue_for.(c1_state, nonce_c1, "order-c1", "user:c1", "40", now_unix + 3600)

c1_logs = {
  [mk_transfer_log.(treasury, alice, 10, "0xTXH", 0, 30_000_000)],
  [mk_auth_log.(alice, nonce_c1, 10, "0xTXH", 1)]
}

AuthStore.fail_record(true)

c1_state =
  %{c1_state | rpc_fn: mk_rpc.(elem(c1_logs, 0), elem(c1_logs, 1), 100)}
  |> Payments.poll()

Check.check(
  f,
  "C1. a store blip HOLDS the settlement (no row, cursor unmoved)",
  AuthStore.rows() == [] and AuthStore.cursor("base") == nil
)

Check.check(
  f,
  "C1. the held settlement leaves the nonce LIVE (re-presentation stays lossless)",
  not AuthStore.consumed?(nonce_c1) and
    nonce_c1 in AuthStore.live_authorization_nonces(now_unix)
)

AuthStore.fail_record(false)

c1_state =
  %{c1_state | rpc_fn: mk_rpc.(elem(c1_logs, 0), elem(c1_logs, 1), 100)}
  |> Payments.poll()

c1_row = row_for.("8453:0xTXH:0")

Check.check(
  f,
  "C1. the retry CREDITS it (money is not lost to a transient store failure)",
  c1_row != nil and c1_row.beneficiary == "user:c1" and
    Decimal.equal?(c1_row.amount_usd, Decimal.new("30"))
)

Check.check(
  f,
  "C1. and only now is the nonce consumed",
  AuthStore.consumed?(nonce_c1)
)

_ = c1_state

# ── I2: the credit is capped by what was AUTHORIZED ───────────────────────
AuthStore.reset()
i2_state = Payments.init!(scan_config)
nonce_i2 = "0x" <> String.duplicate("21", 32)
i2_state = issue_for.(i2_state, nonce_i2, "order-i2", "user:i2", "1", now_unix + 3600)

i2_state =
  %{
    i2_state
    | rpc_fn:
        mk_rpc.(
          [mk_transfer_log.(treasury, alice, 10, "0xTXI", 0, 5_000_000_000)],
          [mk_auth_log.(alice, nonce_i2, 10, "0xTXI", 1)],
          100
        )
  }
  |> Payments.poll()

i2_row = row_for.("8453:0xTXI:0")

Check.check(
  f,
  "I2. a Transfer moving MORE than it authorized credits only the authorized amount",
  i2_row != nil and Decimal.equal?(i2_row.amount_usd, Decimal.new("1"))
)

Check.check(
  f,
  "I2. and the anomaly is on the row (the moved amount is not silently discarded)",
  i2_row != nil and i2_row.credit_capped == true and
    Decimal.equal?(i2_row.moved_amount_usd, Decimal.new("5000"))
)

_ = i2_state

# ── I4: the bound is real end to end — an expired authorization drops out ──
AuthStore.reset()
i4_state = Payments.init!(scan_config)
nonce_i4 = "0x" <> String.duplicate("41", 32)
i4_state = issue_for.(i4_state, nonce_i4, "order-i4", "user:i4", "10", now_unix + 60)

later = DateTime.add(now, 3600, :second)

i4_state =
  %{
    i4_state
    | now_fn: fn -> later end,
      rpc_fn:
        mk_rpc.(
          [mk_transfer_log.(treasury, alice, 10, "0xTXJ", 0, 10_000_000)],
          [mk_auth_log.(alice, nonce_i4, 10, "0xTXJ", 1)],
          100
        )
  }
  |> Payments.poll()

Check.check(
  f,
  "I4. an expired authorization has left the live set (the getLogs filter is bounded)",
  AuthStore.live_authorization_nonces(DateTime.to_unix(later)) == []
)

Check.check(
  f,
  "I4. so its late Transfer settles nothing and is held as unrecognised",
  AuthStore.rows() == [] and Enum.any?(AuthStore.unrecognised(), &(&1.tx_hash == "0xTXJ"))
)

_ = i4_state

# ── M3: never trust the provider echoed only what was asked for ───────────
#
# The AuthorizationUsed query is filtered to the LIVE nonces, but a
# misbehaving or compromised RPC can ignore `topics[2]`. If the hub takes
# whatever comes back, an ALREADY-CONSUMED nonce re-correlates to a brand new
# Transfer — a different idempotency_key, so settlement dedup does not save
# it, and the same authorization is credited twice.
AuthStore.reset()
m3_state = Payments.init!(scan_config)
nonce_z = "0x" <> String.duplicate("2f", 32)
nonce_w = "0x" <> String.duplicate("3f", 32)
m3_state = issue_for.(m3_state, nonce_z, "order-z", "user:z", "50", now_unix + 3600)

m3_state =
  %{
    m3_state
    | rpc_fn:
        mk_rpc.(
          [mk_transfer_log.(treasury, alice, 10, "0xTXM1", 0, 50_000_000)],
          [mk_auth_log.(alice, nonce_z, 10, "0xTXM1", 1)],
          100
        )
  }
  |> Payments.poll()

# A second live nonce keeps the filter non-empty, so the query IS made and
# the provider gets its chance to answer with something else.
m3_state = issue_for.(m3_state, nonce_w, "order-w", "user:w", "50", now_unix + 3600)

m3_state =
  %{
    m3_state
    | rpc_fn:
        mk_rpc.(
          [mk_transfer_log.(treasury, alice, 210, "0xTXM2", 0, 50_000_000)],
          # NOT what was asked for: nonce_z was consumed in the round above.
          [mk_auth_log.(alice, nonce_z, 210, "0xTXM2", 1)],
          300
        )
  }
  |> Payments.poll()

Check.check(
  f,
  "M3. a provider echoing a CONSUMED nonce cannot re-credit its authorization",
  Enum.count(AuthStore.rows()) == 1 and row_for.("8453:0xTXM2:0") == nil
)

Check.check(
  f,
  "M3. the off-filter correlation is dropped and the inflow held as unrecognised",
  Enum.any?(AuthStore.unrecognised(), &(&1.tx_hash == "0xTXM2" and &1.reason == "not_issued"))
)

_ = m3_state

# ── I1: an inert authorization lane must not boot ─────────────────────────
AuthStore.reset()

Check.check(
  f,
  "I1. a durable settlement store with NO authorization callbacks refuses to boot",
  match?({:error, _}, Payments.init(%{scan_config | store_mod: NoAuthStore}))
)

Check.check(
  f,
  "I1. a nil store with a treasury chain refuses to boot too",
  match?({:error, _}, Payments.init(%{scan_config | store_mod: nil}))
)

Check.check(
  f,
  "I1. the explicit allow_ephemeral opt-out still boots (same precedent as the ledger gate)",
  match?(
    {:ok, _},
    Payments.init(Map.merge(scan_config, %{store_mod: NoAuthStore, allow_ephemeral: true}))
  )
)

Check.check(
  f,
  "I1. and a full authorization store boots normally",
  match?({:ok, _}, Payments.init(scan_config))
)

Check.check(
  f,
  "I1. a hub with no registry refuses issue_authorization instead of acking ok:true",
  (fn ->
     inert = Payments.init!(%{issuance_config | store_mod: nil})
     {:reply, json, _} = Payments.handle_message("ingress", Jason.encode!(issue1), inert)
     reply = Jason.decode!(json)
     reply["ok"] == false
   end).()
)

# ── I3: the treasury address can never be a watched binding ───────────────
AuthStore.reset()

:ok =
  AuthStore.put_address_binding(%{
    beneficiary: "user:evil",
    index: 0,
    address: treasury,
    namespace: "hub_ns"
  })

Check.check(
  f,
  "I3. a binding on the treasury address refuses to boot",
  match?({:error, _}, Payments.init(scan_config))
)

# Scan-time precedence, for a collision written AFTER boot (a peer hub, an
# operator repairing a row by hand): the treasury branch resolves FIRST, so
# the credit rule still runs and the inflow is never settled to the colliding
# binding's beneficiary.
AuthStore.reset()
i3_state = Payments.init!(scan_config)

i3_state = %{
  i3_state
  | bindings:
      Map.put(i3_state.bindings, "user:evil", %{
        index: 0,
        address: treasury,
        namespace: "hub_ns"
      })
}

i3_state =
  %{
    i3_state
    | rpc_fn:
        mk_rpc.([mk_transfer_log.(treasury, stranger, 10, "0xTXK", 0, 777_000_000)], [], 100)
  }
  |> Payments.poll()

Check.check(
  f,
  "I3. a post-boot collision still cannot bypass the credit rule",
  AuthStore.rows() == [] and
    Enum.any?(AuthStore.unrecognised(), &(&1.tx_hash == "0xTXK"))
)

_ = i3_state

# ─────────────────────────────────────────────────────────────────────────
# A second review pass surfaced two more real defects, neither covered by
# the fix wave above.
# ─────────────────────────────────────────────────────────────────────────

# ── F2: a store FAULT on the issuance lookup must HOLD, not fail open ─────
#
# `issued_authorization_lookup/2` used to hand `nil` to `store_result/4` as
# BOTH the "not exported" default AND the catch-all for a raise/exit from an
# exported callback — so a store that RAISES on `issued_authorization/1`
# looked identical to "this hub never issued this nonce": the Transfer was
# recorded unrecognised and the chain's cursor advanced past a payment the
# hub actually has a row for. Fail CLOSED instead, mirroring the settlement
# ledger's own dedup-read stance (payment_seen_lookup/2, C1's
# record_payment blip): hold the settlement, leave the nonce live, let the
# next tick ask the store again.
AuthStore.reset()
f2_state = Payments.init!(scan_config)
nonce_f2 = "0x" <> String.duplicate("7a", 32)
f2_state = issue_for.(f2_state, nonce_f2, "order-f2", "user:f2", "30", now_unix + 3600)

f2_logs = {
  [mk_transfer_log.(treasury, alice, 10, "0xTXF2", 0, 30_000_000)],
  [mk_auth_log.(alice, nonce_f2, 10, "0xTXF2", 1)]
}

AuthStore.fail_lookup(true)

f2_state =
  %{f2_state | rpc_fn: mk_rpc.(elem(f2_logs, 0), elem(f2_logs, 1), 100)}
  |> Payments.poll()

Check.check(
  f,
  "F2. a store that RAISES on the lookup HOLDS the inflow (nothing settled)",
  AuthStore.rows() == []
)

Check.check(
  f,
  "F2. and does NOT record it as unrecognised — unknown is not 'not ours'",
  AuthStore.unrecognised() == []
)

Check.check(f, "F2. the chain's cursor did not advance past it", AuthStore.cursor("base") == nil)

Check.check(
  f,
  "F2. the nonce stays live so the retry can still correlate",
  not AuthStore.consumed?(nonce_f2) and nonce_f2 in AuthStore.live_authorization_nonces(now_unix)
)

AuthStore.fail_lookup(false)

f2_state =
  %{f2_state | rpc_fn: mk_rpc.(elem(f2_logs, 0), elem(f2_logs, 1), 100)}
  |> Payments.poll()

f2_row = row_for.("8453:0xTXF2:0")

Check.check(
  f,
  "F2. once the store recovers, the SAME money credits on the next round",
  f2_row != nil and f2_row.beneficiary == "user:f2" and
    Decimal.equal?(f2_row.amount_usd, Decimal.new("30"))
)

Check.check(f, "F2. and only now is the nonce consumed", AuthStore.consumed?(nonce_f2))

_ = f2_state

# ── M1: a store with no registry callback is refused, in ANY chain config ─
#
# The I1 boot gate (`validate_authorization_store!/3`) only fires when SOME
# chain configures `treasury_address` — a hub with no treasury chain at all
# sails past it with no registry whatsoever, and previously that meant
# `issue_authorization` could still answer ok:true via the `allow_ephemeral`
# memory fallback: a nonce nothing could ever look up, acked anyway. Fixed:
# `issue_authorization` refuses `no_authorization_store` per-action whenever
# `record_issued_authorization/1` isn't exported, with NO ephemeral escape —
# mirrors `release_payment`'s `no_release_store`, which never had one either.
storeless_no_treasury_config = %{
  xpub: xpub,
  allow_test_xpub: true,
  trusted_sources: ["ingress"],
  targets: [],
  namespace: "hub_ns",
  auto_tick: false,
  chains: [],
  # The exact gap: allow_ephemeral was set for entirely unrelated reasons (a
  # dev swarm with no durable settlement store either) and, pre-fix, that
  # SAME flag silently let this lane through too.
  allow_ephemeral: true
}

storeless_state = Payments.init!(storeless_no_treasury_config)

{:reply, json_m1, _state} =
  Payments.handle_message(
    "ingress",
    Jason.encode!(%{
      "action" => "issue_authorization",
      "nonce" => "0x" <> String.duplicate("6f", 32),
      "order_ref" => "order-m1",
      "beneficiary" => "user:m1",
      "amount_usd" => "20",
      "valid_before" => now_unix + 3600
    }),
    storeless_state
  )

reply_m1 = Jason.decode!(json_m1)

Check.check(
  f,
  "M1. no chain has a treasury AND allow_ephemeral is set — issuance is still refused, not acked",
  reply_m1["ok"] == false and reply_m1["error"] == "no_authorization_store"
)

{:reply, json_m1_again, _state} =
  Payments.handle_message(
    "ingress",
    Jason.encode!(%{
      "action" => "issue_authorization",
      "nonce" => "0x" <> String.duplicate("8b", 32),
      "order_ref" => "order-m1",
      "beneficiary" => "user:m1",
      "amount_usd" => "20",
      "valid_before" => now_unix + 3600
    }),
    storeless_state
  )

Check.check(
  f,
  "M1. nothing was written for the refused attempt (a repeat with the SAME order_ref is refused identically, not treated as a duplicate)",
  Jason.decode!(json_m1_again) == reply_m1
)

# ─────────────────────────────────────────────────────────────────────────
# Fix wave — N1 (re-review): the one-credit-per-nonce guarantee must be a
# HUB-STATE fact, not something borrowed from mark_authorization_consumed/1
# never failing. `idempotency_key` dedup only ever catches re-observing the
# SAME on-chain log twice; it does nothing for a genuinely SECOND on-chain
# use of the same nonce arriving in a DIFFERENT transaction (a different
# tx_hash ⇒ a different idempotency_key). `authorization_settled?/1` is the
# nonce-level guard that closes that gap independent of whether consumption
# was ever marked.
# ─────────────────────────────────────────────────────────────────────────

# ── N1: double-use of a settled nonce is refused, not double-credited ────
AuthStore.reset()
n1_state = Payments.init!(scan_config)
nonce_n1 = "0x" <> String.duplicate("99", 32)
n1_state = issue_for.(n1_state, nonce_n1, "order-n1", "user:n1", "30", now_unix + 3600)

# mark_authorization_consumed's documented best-effort path: it fails on the
# very Transfer that settles, so the nonce stays live and unconsumed even
# though it was, in fact, already credited once.
AuthStore.fail_consume(true)

n1_state =
  %{
    n1_state
    | rpc_fn:
        mk_rpc.(
          [mk_transfer_log.(treasury, alice, 10, "0xTXN1", 0, 30_000_000)],
          [mk_auth_log.(alice, nonce_n1, 10, "0xTXN1", 1)],
          100
        )
  }
  |> Payments.poll()

Check.check(
  f,
  "N1. the first Transfer settles despite the consume-marking failure",
  length(AuthStore.rows()) == 1
)

Check.check(
  f,
  "N1. the consume failure leaves the nonce LIVE and unconsumed (the documented best-effort path)",
  not AuthStore.consumed?(nonce_n1) and nonce_n1 in AuthStore.live_authorization_nonces(now_unix)
)

# A SECOND, genuinely different on-chain use of the SAME nonce, in a
# DIFFERENT transaction. Its idempotency_key ("8453:0xTXN2:0") is unrelated
# to the first settlement's ("8453:0xTXN1:0"), so ordinary settlement dedup
# does not see it, and issued_authorization/1 still returns the row — only
# the nonce-level SETTLED check can refuse this.
n1_state =
  %{
    n1_state
    | rpc_fn:
        mk_rpc.(
          [mk_transfer_log.(treasury, alice, 210, "0xTXN2", 0, 30_000_000)],
          [mk_auth_log.(alice, nonce_n1, 210, "0xTXN2", 1)],
          300
        )
  }
  |> Payments.poll()

Check.check(
  f,
  "N1. the second Transfer on the SAME nonce (different tx_hash) is NOT credited",
  row_for.("8453:0xTXN2:0") == nil
)

Check.check(
  f,
  "N1. it is recorded unrecognised with reason authorization_already_settled",
  Enum.any?(
    AuthStore.unrecognised(),
    &(&1.tx_hash == "0xTXN2" and &1.reason == "authorization_already_settled")
  )
)

Check.check(
  f,
  "N1. total credited across BOTH Transfers is the FIRST amount only (30, not 60)",
  AuthStore.rows()
  |> Enum.reduce(Decimal.new(0), &Decimal.add(&2, &1.amount_usd))
  |> Decimal.equal?(Decimal.new("30"))
)

AuthStore.fail_consume(false)
_ = n1_state

# ── N1 store-fault: authorization_settled? raising HOLDS, never guesses ───
#
# Neither "already settled" nor "never settled" is a safe answer to
# fabricate when the store cannot actually say — the former risks refusing a
# nonce that was never credited, the latter risks the exact double-credit
# this callback exists to prevent.
AuthStore.reset()
n1f_state = Payments.init!(scan_config)
nonce_n1f = "0x" <> String.duplicate("88", 32)
n1f_state = issue_for.(n1f_state, nonce_n1f, "order-n1f", "user:n1f", "20", now_unix + 3600)

n1f_logs = {
  [mk_transfer_log.(treasury, alice, 10, "0xTXN1F", 0, 20_000_000)],
  [mk_auth_log.(alice, nonce_n1f, 10, "0xTXN1F", 1)]
}

AuthStore.fail_settled_lookup(true)

n1f_state =
  %{n1f_state | rpc_fn: mk_rpc.(elem(n1f_logs, 0), elem(n1f_logs, 1), 100)}
  |> Payments.poll()

Check.check(
  f,
  "N1. a RAISING authorization_settled? HOLDS (nothing settled, nothing buried)",
  AuthStore.rows() == [] and AuthStore.unrecognised() == []
)

Check.check(f, "N1. the chain's cursor did not advance past it", AuthStore.cursor("base") == nil)

Check.check(
  f,
  "N1. the nonce stays live so the retry can still correlate",
  not AuthStore.consumed?(nonce_n1f) and
    nonce_n1f in AuthStore.live_authorization_nonces(now_unix)
)

AuthStore.fail_settled_lookup(false)

n1f_state =
  %{n1f_state | rpc_fn: mk_rpc.(elem(n1f_logs, 0), elem(n1f_logs, 1), 100)}
  |> Payments.poll()

n1f_row = row_for.("8453:0xTXN1F:0")

Check.check(
  f,
  "N1. once the store recovers, the SAME money credits on the next round",
  n1f_row != nil and n1f_row.beneficiary == "user:n1f" and
    Decimal.equal?(n1f_row.amount_usd, Decimal.new("20"))
)

_ = n1f_state

Check.finish(f)
