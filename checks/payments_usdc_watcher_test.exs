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

# ── B2: one malformed RPC field must not crash the whole tick.

# (i) a real-world provider quirk: eth_blockNumber returns {:ok, nil}. This
# used to blow up hex_int/1 (no matching clause) and crash the poll round;
# now the chain's scan is caught and skipped, cursor left untouched.
ScanStore.reset()
ScanStore.seed_binding(%{beneficiary: "budget:abc", index: 0, address: addr, namespace: "llm_quota"})

null_block_rpc = fn _chain, method, _params ->
  case method do
    "eth_blockNumber" -> {:ok, nil}
    "eth_getLogs" -> {:ok, []}
  end
end

state_null = %{state0 | rpc_fn: null_block_rpc}

result =
  try do
    Payments.poll(state_null)
    :ok
  rescue
    e -> {:raised, e}
  end

Check.check(f, "eth_blockNumber returning {:ok, nil} doesn't crash poll", result == :ok)
Check.check(f, "cursor untouched after a bad-shape RPC response",
  ScanStore.cursor("base") == nil)

# (ii) two chains: chain 1's RPC returns garbage (crashes hex_int), chain 2's
# RPC works fine ⇒ chain 2's payment still settles this round.
ScanStore.reset()

state_two_chain =
  Payments.init(%{
    name: :payments,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: ScanStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "bad_chain", rpc_url: "injected", usdc_contract: "0xCONTRACT",
        confirmations: 10, decimals: 6, start_block: 100, max_block_range: 1000},
      %{name: "good_chain", rpc_url: "injected", usdc_contract: "0xCONTRACT",
        confirmations: 10, decimals: 6, start_block: 100, max_block_range: 1000}
    ],
    rpc_fn: nil
  })

{:reply, jt, state_two_chain} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:twochain"}), state_two_chain)
addr_two = Jason.decode!(jt)["address"]

good_log = mk_log.(addr_two, 150, "0xGOOD", 0)

mixed_rpc = fn chain, method, _params ->
  case {chain.name, method} do
    {"bad_chain", "eth_blockNumber"} -> {:ok, :not_a_hex_string}
    {"bad_chain", "eth_getLogs"} -> {:ok, []}
    {"good_chain", "eth_blockNumber"} -> {:ok, "0xc8"}
    {"good_chain", "eth_getLogs"} -> {:ok, [good_log]}
  end
end

state_two_chain = %{state_two_chain | rpc_fn: mixed_rpc}
_state_two_chain = Payments.poll(state_two_chain)

Check.check(f, "a chain with a bad RPC shape is skipped while the other chain still settles",
  Enum.any?(ScanStore.rows(), &(&1.idempotency_key == "good_chain:0xGOOD:0")) and
    ScanStore.cursor("bad_chain") == nil)

# ── 2b: pin the cursor fail-closed invariant (coverage-only — the adversarial
# audit found this unpinned: a hand-applied mutation removing the `held?`
# guard in apply_chain_result/2 passes the pre-existing suite). Also pins
# the dedup-counts-as-settled leg of the same invariant.
defmodule CursorInvariantStore do
  def reset,
    do:
      :persistent_term.put(
        {__MODULE__, :d},
        %{seen: MapSet.new(), rows: [], cursor: %{}, bindings: [], down: false}
      )

  defp d, do: :persistent_term.get({__MODULE__, :d})
  defp put(k, v), do: :persistent_term.put({__MODULE__, :d}, Map.put(d(), k, v))
  def down!(flag), do: put(:down, flag)
  def seed_seen(key), do: put(:seen, MapSet.put(d().seen, key))

  def put_address_binding(b), do: put(:bindings, [b | d().bindings])
  def list_address_bindings, do: {:ok, d().bindings}
  def payment_seen?(k), do: {:ok, MapSet.member?(d().seen, k)}

  def record_payment(row) do
    if d().down do
      {:error, :db_down}
    else
      put(:seen, MapSet.put(d().seen, row.idempotency_key))
      put(:rows, [row | d().rows])
      :ok
    end
  end

  def get_last_scanned_block(chain), do: {:ok, Map.get(d().cursor, chain)}
  def put_last_scanned_block(chain, n), do: put(:cursor, Map.put(d().cursor, chain, n))
  def cursor(chain), do: Map.get(d().cursor, chain)
  def rows, do: d().rows
end

CursorInvariantStore.reset()

state_ci =
  Payments.init(%{
    name: :payments,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: CursorInvariantStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "cursorinv", rpc_url: "injected", usdc_contract: "0xCONTRACT",
        confirmations: 0, decimals: 6, start_block: 0, max_block_range: 1000}
    ],
    rpc_fn: nil
  })

{:reply, jci, state_ci} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:ci"}), state_ci)
addr_ci = Jason.decode!(jci)["address"]

log_ci = mk_log.(addr_ci, 5, "0xCI1", 0)

rpc_ci = fn _chain, method, _params ->
  case method do
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, [log_ci]}
  end
end

CursorInvariantStore.down!(true)
state_ci = %{state_ci | rpc_fn: rpc_ci}
state_ci = Payments.poll(state_ci)

Check.check(f, "2b: record_payment erroring holds the settlement — cursor stays nil, zero rows",
  CursorInvariantStore.cursor("cursorinv") == nil and CursorInvariantStore.rows() == [])

CursorInvariantStore.down!(false)
state_ci = Payments.poll(state_ci)

Check.check(f, "2b: after the store heals, the SAME payment settles and the cursor advances to safe_to",
  length(CursorInvariantStore.rows()) == 1 and CursorInvariantStore.cursor("cursorinv") == 200)

# dedup-counts-as-settled leg: a log already durably seen must still advance
# the cursor even though record_payment is never invoked for it this round.
CursorInvariantStore.reset()

state_dedup =
  Payments.init(%{
    name: :payments,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: CursorInvariantStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "dedupchain", rpc_url: "injected", usdc_contract: "0xCONTRACT",
        confirmations: 0, decimals: 6, start_block: 0, max_block_range: 1000}
    ],
    rpc_fn: nil
  })

{:reply, jd, state_dedup} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:dedup"}), state_dedup)
addr_dedup = Jason.decode!(jd)["address"]

log_dedup = mk_log.(addr_dedup, 5, "0xDEDUP", 0)
CursorInvariantStore.seed_seen("dedupchain:0xDEDUP:0")

rpc_dedup = fn _chain, method, _params ->
  case method do
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, [log_dedup]}
  end
end

state_dedup = %{state_dedup | rpc_fn: rpc_dedup}
state_dedup = Payments.poll(state_dedup)

Check.check(f, "2b: a log already durably seen (dedup) still counts as settled — cursor advances",
  CursorInvariantStore.cursor("dedupchain") == 200 and CursorInvariantStore.rows() == [])

# ── 2c: pin the cursor-read fallback default. A store that EXPORTS
# get_last_scanned_block/1 but RAISES on every call (other settlement
# callbacks work fine) must fail closed: scan_from/2 sees {:error, _} from
# the cursor read (NOT {:ok, nil}), so the chain's round is skipped
# entirely — no eth_getLogs at all this tick, not even a rescan from
# start_block. The wrong default ({:ok, nil}) would instead treat the
# raising store as "never scanned" and rescan from start_block, calling
# eth_getLogs with fromBlock = start_block — the rescan-storm bug.
defmodule RaisingCursorStore do
  def reset,
    do: :persistent_term.put({__MODULE__, :d}, %{seen: MapSet.new(), rows: [], bindings: []})

  defp d, do: :persistent_term.get({__MODULE__, :d})
  defp put(k, v), do: :persistent_term.put({__MODULE__, :d}, Map.put(d(), k, v))

  def put_address_binding(b), do: put(:bindings, [b | d().bindings])
  def list_address_bindings, do: {:ok, d().bindings}
  def payment_seen?(k), do: {:ok, MapSet.member?(d().seen, k)}

  def record_payment(row) do
    put(:seen, MapSet.put(d().seen, row.idempotency_key))
    put(:rows, [row | d().rows])
    :ok
  end

  def rows, do: d().rows

  # EXPORTED but RAISING on every call — the fallback default matters here.
  def get_last_scanned_block(_chain), do: raise("cursor store unavailable")
  def put_last_scanned_block(_chain, _n), do: :ok
end

RaisingCursorStore.reset()

state_raising0 =
  Payments.init(%{
    name: :payments,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: RaisingCursorStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
    deliver_fn: fn _, _, _ -> :ok end,
    chains: [
      %{name: "base", rpc_url: "injected", usdc_contract: "0xCONTRACT",
        confirmations: 10, decimals: 6, start_block: 100, max_block_range: 1000,
        address_chunk: 2}
    ],
    rpc_fn: nil
  })

# bind a beneficiary so the chain has something to (not) scan for
{:reply, jraise, state_raising0} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "deposit_address", beneficiary: "budget:raising"}), state_raising0)
addr_raising = Jason.decode!(jraise)["address"]

# canned logs are available (would settle if fetched) but must never be reached
log_raising = mk_log.(addr_raising, 150, "0xRAISING", 0)

{:ok, rpc_log_raising} = Agent.start_link(fn -> [] end)

rpc_raising = fn _chain, method, _params ->
  Agent.update(rpc_log_raising, &[method | &1])
  case method do
    "eth_blockNumber" -> {:ok, "0xc8"}
    "eth_getLogs" -> {:ok, [log_raising]}
  end
end

state_raising = %{state_raising0 | rpc_fn: rpc_raising}
_state_raising = Payments.poll(state_raising)

raising_get_logs_calls =
  Agent.get(rpc_log_raising, &Enum.count(&1, fn m -> m == "eth_getLogs" end))

Check.check(f,
  "2c: get_last_scanned_block raising skips the round — zero eth_getLogs, no rescan-from-start_block",
  raising_get_logs_calls == 0)

Check.check(f, "2c: no settlement recorded and cursor stays untouched while the cursor store raises",
  RaisingCursorStore.rows() == [])

# ── 2e: zero-value Transfer dust filter. `transfer(victim, 0)` emits a real
# log anyone can produce for only gas; settling it grows the ledger, the
# seen-set, and the delivery fan-out for free. A zero-amount log must produce
# NO settlement and NO delivery — but it is not held either: the cursor still
# advances normally, and a legit non-zero log in the same batch settles.
ScanStore.reset()
ScanStore.seed_binding(%{beneficiary: "budget:abc", index: 0, address: addr, namespace: "llm_quota"})

{:ok, delivered_dust} = Agent.start_link(fn -> [] end)

zero_value_hex = "0x" <> String.duplicate("0", 64)
zero_log = %{mk_log.(addr, 150, "0xDUST", 0) | "data" => zero_value_hex}
paid_log = mk_log.(addr, 151, "0xPAID", 0)

state_dust = %{
  state0
  | rpc_fn: canned.([zero_log, paid_log], 200),
    deliver_fn: fn t, from, _c -> (Agent.update(delivered_dust, &[{t, from} | &1]); :ok) end
}

_state_dust = Payments.poll(state_dust)

Check.check(f, "2e: a zero-value Transfer log never settles (no ledger row)",
  not Enum.any?(ScanStore.rows(), &(&1.idempotency_key == "base:0xDUST:0")))
Check.check(f, "2e: a zero-value Transfer log never delivers payment_confirmed",
  Agent.get(delivered_dust, & &1) == [{"llm_proxy", :payments}])
Check.check(f, "2e: the legit non-zero log in the same batch still settles",
  Enum.any?(ScanStore.rows(), &(&1.idempotency_key == "base:0xPAID:0")))
Check.check(f, "2e: the cursor still advances normally past the skipped dust log",
  ScanStore.cursor("base") == 190)

# ── R3-I1: a log MISSING transactionHash must fail CLOSED like every other
# malformed field — chain held, cursor unmoved, NO settlement and NO ledger
# write. It used to settle under the nil-interpolated garbage key
# "base::0"; with a durable store that key was then seen FOREVER, silently
# swallowing any future hash-less log at logIndex 0 while the cursor
# advanced past it.
ScanStore.reset()
ScanStore.seed_binding(%{beneficiary: "budget:abc", index: 0, address: addr, namespace: "llm_quota"})

hashless_log = Map.delete(mk_log.(addr, 150, "0xIGNORED", 0), "transactionHash")

state_hashless = %{state0 | rpc_fn: canned.([hashless_log], 200)}

hashless_result =
  try do
    Payments.poll(state_hashless)
    :ok
  rescue
    e -> {:raised, e}
  end

Check.check(f, "R3-I1: a log without transactionHash never settles (no ledger row, no garbage key)",
  ScanStore.rows() == [])
Check.check(f, "R3-I1: the failure is contained (poll survives, chain merely held)",
  hashless_result == :ok)
Check.check(f, "R3-I1: cursor NOT advanced past the hash-less log",
  ScanStore.cursor("base") == nil)

# a later well-formed log at the same (block, logIndex) coordinates still
# settles under its correct chain:tx:logIndex key — nothing was poisoned
state_healed = %{state0 | rpc_fn: canned.([mk_log.(addr, 150, "0xHEALED", 0)], 200)}
_state_healed = Payments.poll(state_healed)

Check.check(f, "R3-I1: a later well-formed log settles under its correct key",
  Enum.map(ScanStore.rows(), & &1.idempotency_key) == ["base:0xHEALED:0"] and
    ScanStore.cursor("base") == 190)

# ── R3-M1: hex comparisons are case-INSENSITIVE. A nonstandard node emitting
# uppercase hex (topic0, to-address topic, contract address, data) used to be
# silently missed on the topic0 exact-match while the cursor advanced — a
# lost payment, never re-presented.
ScanStore.reset()
ScanStore.seed_binding(%{beneficiary: "budget:abc", index: 0, address: addr, namespace: "llm_quota"})

upcase_hex = fn "0x" <> h -> "0x" <> String.upcase(h) end

upper_log = %{
  "address" => "0XCONTRACT",
  "topics" => [
    upcase_hex.(transfer_sig),
    upcase_hex.(pad_addr.("0x" <> String.duplicate("a", 40))),
    upcase_hex.(pad_addr.(addr))
  ],
  "data" => upcase_hex.(value_hex),
  "blockNumber" => "0x96",
  "transactionHash" => "0xUPPER",
  "logIndex" => "0x0"
}

state_upper = %{state0 | rpc_fn: canned.([upper_log], 200)}
_state_upper = Payments.poll(state_upper)

Check.check(f, "R3-M1: an uppercase-hex log (topic0/topics/address/data) settles normally",
  Enum.any?(ScanStore.rows(), &(&1.idempotency_key == "base:0xUPPER:0")) and
    ScanStore.cursor("base") == 190)

Check.finish(f)
