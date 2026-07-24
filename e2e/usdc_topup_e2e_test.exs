# Cross-package END-TO-END: the REAL genswarms-payments settlement hub and the
# REAL genswarms-llm-proxy, wired together in one BEAM, driving the full
# USDC -> credit -> spend user story. Until this harness each package was only
# ever tested against fakes of the other; every seam here is the live one:
#
#   - the hub's deliver_fn invokes the REAL Proxy.handle_message/3 on the live
#     proxy state (exactly what a host's ObjectServer.deliver_message does),
#     returning :ok only when the proxy's reply is ok:true — the D2
#     serial-per-ref delivery/retry contract, end to end;
#   - the proxy runs its REAL Bandit listener and a REAL local fake upstream
#     with known token costs, so budget math is exact;
#   - the proxy's topup_hint_fun asks the LIVE hub for the deposit address
#     (the host wiring), so the block notice carries the real ADDR0;
#   - the chain is canned JSON-RPC (no network, no Postgres — hermetic).
#
# Run: sh e2e/run.sh   (or: cd e2e && mix run usdc_topup_e2e_test.exs)
Code.require_file(Path.join([__DIR__, "..", "checks", "support.exs"]))
f = Check.start()

alias Genswarms.Payments
alias Genswarms.LlmProxy, as: Proxy
alias Genswarms.LlmProxy.Curl

Application.ensure_all_started(:bandit)
Application.ensure_all_started(:plug)

# The genswarms engine is a peer/runtime dep of both packages and absent here.
# The proxy's plug defaults deliver_fn (block notices, metric bumps) to
# Genswarms.Objects.ObjectServer.deliver_message/4 — stand in for the host's
# object server and RECORD every delivery so the block-notice content (with
# the hub-provided top-up hint) is assertable.
defmodule Genswarms.Objects.ObjectServer do
  @moduledoc false
  def start, do: Agent.start_link(fn -> [] end, name: __MODULE__.Log)

  def deliver_message(swarm_name, to, from, content) do
    Agent.update(__MODULE__.Log, &[{swarm_name, to, from, content} | &1])
    :ok
  end

  # Block notices arrive as {"action":"slot_reply", "content": notice} to the
  # sender object; metric bumps etc. are other shapes — filter them out.
  def notices do
    Agent.get(__MODULE__.Log, &Enum.reverse/1)
    |> Enum.flat_map(fn {_sw, _to, _from, content} ->
      case Jason.decode(content) do
        {:ok, %{"action" => "slot_reply", "content" => notice}} -> [notice]
        _ -> []
      end
    end)
  end
end

# ── Fake upstream with KNOWN token costs: 1e6 prompt / 5e5 completion at
# 0.25/0.75 per Mtok -> exactly $0.625 per chat call.
defmodule E2E.FakeUpstream do
  use Plug.Router

  plug(:match)
  plug(Plug.Parsers, parsers: [:json], pass: ["application/json"], json_decoder: Jason)
  plug(:dispatch)

  post "/v1/chat/completions" do
    resp = %{
      "id" => "chatcmpl-e2e-fake",
      "object" => "chat.completion",
      "created" => System.system_time(:second),
      "model" => "fake-model",
      "choices" => [
        %{
          "index" => 0,
          "message" => %{"role" => "assistant", "content" => "pong-e2e"},
          "finish_reason" => "stop"
        }
      ],
      "usage" => %{
        "prompt_tokens" => 1_000_000,
        "completion_tokens" => 500_000,
        "total_tokens" => 1_500_000
      }
    }

    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(200, Jason.encode!(resp))
  end

  match _ do
    Plug.Conn.send_resp(conn, 404, "")
  end
end

# ── Proxy-side durable store: real store_mod contract — budget accounting
# (llm_budget_status/record_llm_call) PLUS the credit-ledger extension
# (llm_credit_balance/1 + record_llm_credit_entry/1 with the GLOBAL
# idempotency_key uniqueness the Store behaviour demands). credit_down!
# simulates a credit-write outage (scenario 8): record_llm_credit_entry
# errors while everything else stays healthy.
defmodule E2E.ProxyStore do
  def start_link,
    do: Agent.start_link(fn -> %{budget: %{}, ledger: [], credit_down: false} end, name: __MODULE__)

  def credit_down!(flag), do: Agent.update(__MODULE__, &%{&1 | credit_down: flag})

  def llm_budget_status(identity, day, session_id, default_limit) do
    Agent.get_and_update(__MODULE__, fn st ->
      key = {identity, day}
      row = Map.get(st.budget, key) || new_row(identity, day, session_id, default_limit)
      row = %{row | session_id: session_id}
      {row, %{st | budget: Map.put(st.budget, key, row)}}
    end)
  end

  def record_llm_call(identity, day, session_id, attrs, default_limit) do
    Agent.get_and_update(__MODULE__, fn st ->
      key = {identity, day}
      cost = dec(attrs[:cost_usd] || 0)
      row = Map.get(st.budget, key) || new_row(identity, day, session_id, default_limit)
      status = to_string(attrs[:status] || "ok")

      row = %{
        row
        | session_id: session_id,
          spent_usd: Decimal.add(row.spent_usd, cost),
          requests: row.requests + if(status == "ok", do: 1, else: 0)
      }

      {row, %{st | budget: Map.put(st.budget, key, row)}}
    end)
  end

  def llm_usage_today(_day), do: %{spent_usd: Decimal.new("0")}

  def llm_credit_balance(identity) do
    {:ok,
     Agent.get(__MODULE__, fn st ->
       st.ledger
       |> Enum.filter(&(&1.budget_identity == identity))
       |> Enum.reduce(Decimal.new("0"), &Decimal.add(&2, &1.amount_usd))
     end)}
  end

  def record_llm_credit_entry(entry) do
    Agent.get_and_update(__MODULE__, fn st ->
      cond do
        st.credit_down ->
          {{:error, :db_down}, st}

        Enum.any?(st.ledger, &(&1.idempotency_key == entry.idempotency_key)) ->
          {{:error, :duplicate}, st}

        true ->
          {:ok, %{st | ledger: [entry | st.ledger]}}
      end
    end)
  end

  def credit_entries, do: Agent.get(__MODULE__, &Enum.reverse(&1.ledger))

  defp new_row(identity, day, session_id, default_limit) do
    %{
      budget_identity: identity,
      day: day,
      session_id: session_id,
      spent_usd: Decimal.new("0"),
      limit_usd: dec(default_limit),
      requests: 0
    }
  end

  defp dec(%Decimal{} = d), do: d
  defp dec(v), do: Decimal.new(to_string(v))
end

# ── Hub-side durable store (same contract as checks/ ScanStore) + a cursor
# override for the re-present-same-range leg of the idempotency scenario.
defmodule E2E.HubStore do
  def reset,
    do: :persistent_term.put({__MODULE__, :d}, %{seen: MapSet.new(), rows: [], cursor: %{}, bindings: []})

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

  def rows, do: Enum.reverse(d().rows)
  def get_last_scanned_block(chain), do: {:ok, Map.get(d().cursor, chain)}
  def put_last_scanned_block(chain, n), do: put(:cursor, Map.put(d().cursor, chain, n))
  def cursor(chain), do: Map.get(d().cursor, chain)
  def set_cursor(chain, n), do: put(:cursor, Map.put(d().cursor, chain, n))
end

# ── Canned chain: eth_blockNumber/eth_getLogs honoring the requested block
# range, so cursor-driven scans behave like a real node.
defmodule E2E.Chain do
  @transfer_sig "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

  def reset, do: :persistent_term.put({__MODULE__, :d}, %{logs: [], latest: 200})
  defp d, do: :persistent_term.get({__MODULE__, :d})
  def latest!(n), do: :persistent_term.put({__MODULE__, :d}, %{d() | latest: n})

  # A confirmed USDC Transfer of `raw` base units to `to_addr` at `block`.
  def pay!(to_addr, block, tx, idx, raw) do
    log = %{
      "address" => "0xCONTRACT",
      "topics" => [@transfer_sig, pad_addr("0x" <> String.duplicate("a", 40)), pad_addr(to_addr)],
      "data" => "0x" <> String.pad_leading(Integer.to_string(raw, 16), 64, "0"),
      "blockNumber" => hex(block),
      "transactionHash" => tx,
      "logIndex" => hex(idx)
    }

    :persistent_term.put({__MODULE__, :d}, %{d() | logs: d().logs ++ [log]})
  end

  def rpc(_chain, "eth_blockNumber", _params), do: {:ok, hex(d().latest)}

  def rpc(_chain, "eth_getLogs", [params]) do
    from = hex_int(params["fromBlock"])
    to = hex_int(params["toBlock"])

    {:ok,
     Enum.filter(d().logs, fn log ->
       b = hex_int(log["blockNumber"])
       b >= from and b <= to
     end)}
  end

  defp pad_addr("0x" <> hex), do: "0x" <> String.duplicate("0", 24) <> String.downcase(hex)
  defp hex(n), do: "0x" <> Integer.to_string(n, 16)
  defp hex_int("0x" <> h), do: String.to_integer(h, 16)
end

# ── Harness plumbing: the ack-drop flag (scenario 6) and the cross-seam
# delivery/reply log.
defmodule E2E.Flags do
  def reset, do: :persistent_term.put({__MODULE__, :drop_ack}, false)
  def drop_ack!(flag), do: :persistent_term.put({__MODULE__, :drop_ack}, flag)
  def drop_ack?, do: :persistent_term.get({__MODULE__, :drop_ack}, false)
end

defmodule E2E.DeliveryLog do
  def start, do: Agent.start_link(fn -> [] end, name: __MODULE__)
  def record(event), do: Agent.update(__MODULE__, &[event | &1])
  def all, do: Agent.get(__MODULE__, &Enum.reverse/1)
  def deliveries, do: for({:delivery, d} <- all(), do: d)
  def replies, do: for({:reply, r} <- all(), do: r)
end

# ── The hub lives behind an Agent so ticks, deposit_address requests, and the
# proxy's topup_hint_fun all act on ONE evolving state — as in a real host.
defmodule E2E.Hub do
  def start(state), do: Agent.start_link(fn -> state end, name: __MODULE__)

  def call(from, msg) do
    Agent.get_and_update(
      __MODULE__,
      fn st ->
        case Payments.handle_message(from, Jason.encode!(msg), st) do
          {:reply, json, st2} -> {Jason.decode!(json), st2}
          {:noreply, st2} -> {nil, st2}
        end
      end,
      30_000
    )
  end

  def tick, do: call("cron", %{action: "tick"})
  def undelivered, do: Agent.get(__MODULE__, & &1.undelivered)
end

# ═══════════════════════════════════════════════════════════════════════════
# 1. Boot both packages for real.
# ═══════════════════════════════════════════════════════════════════════════

addr0 = "0x9858EfFD232B4033E47d90003D41EC34EcaEda94"
xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

upstream_port = 42_901
proxy_port = 42_902

{:ok, _} = Genswarms.Objects.ObjectServer.start()
{:ok, _} = E2E.ProxyStore.start_link()
{:ok, _} = E2E.DeliveryLog.start()
E2E.HubStore.reset()
E2E.Chain.reset()
E2E.Flags.reset()

{:ok, _fake_upstream} =
  Bandit.start_link(plug: E2E.FakeUpstream, scheme: :http, ip: {127, 0, 0, 1}, port: upstream_port)

# Proxy: tiny daily limit that exactly two $0.625 chat calls exhaust; credits
# wired to the hub's object name + namespace; topup_hint_fun asks the LIVE hub
# for the deposit address (host wiring).
{:ok, proxy} =
  Proxy.init(%{
    port: proxy_port,
    upstream_endpoint: "http://127.0.0.1:#{upstream_port}/v1/chat/completions",
    upstream_api_key: "fake-upstream-key",
    provider: "openai-compatible",
    prices: %{prompt_per_mtok: 0.25, completion_per_mtok: 0.75},
    store_mod: E2E.ProxyStore,
    default_daily_limit: "1.25",
    swarm_name: "e2e",
    connect_timeout_s: 2,
    upstream_timeout_s: 5,
    # re-notify on every distinct block (default 4h dedup would hide the
    # second block notice this story asserts on)
    notice_repeat_ms: 1,
    payments_source: "payments",
    credit_namespace: "llm_quota",
    credit_per_usd: "1.0",
    topup_hint_fun: fn identity ->
      case E2E.Hub.call("ingress", %{action: "deposit_address", beneficiary: identity}) do
        %{"ok" => true, "address" => address} -> "💳 Top up: send USDC (base) to #{address}"
        _ -> nil
      end
    end
  })

Check.check(f, "proxy boots with credits_enabled (payments_source configured)",
  proxy.credits_enabled == true)

{:ok, token} =
  Proxy.register_session(proxy.state_pid, %{
    conversation_id: "tg:e2e:0",
    slot: :agent_e2e,
    kind: :dm,
    workspace_key: "default"
  })

session = Proxy.lookup_session(proxy.state_pid, token)
beneficiary = session.budget_identity

# The hub's deliver_fn: a delivery to target "llm_proxy" invokes the REAL
# proxy ingress on the live state, and counts as delivered ONLY when the
# proxy's reply is ok:true — exactly the host's mapping. E2E.Flags.drop_ack?
# simulates the delivered-but-ack-lost window (the proxy processed the
# message; the hub never learned) that at-least-once delivery must survive.
deliver_fn = fn target, from, content ->
  case target do
    "llm_proxy" ->
      E2E.DeliveryLog.record({:delivery, %{target: target, from: from, content: Jason.decode!(content)}})

      case Proxy.handle_message(from, content, proxy) do
        {:reply, json, _st} ->
          reply = Jason.decode!(json)
          E2E.DeliveryLog.record({:reply, reply})

          cond do
            E2E.Flags.drop_ack?() -> {:error, :simulated_ack_timeout}
            reply["ok"] == true -> :ok
            true -> {:error, {:proxy_nack, reply}}
          end

        {:noreply, _st} ->
          {:error, :proxy_ignored}
      end

    other ->
      {:error, {:unknown_target, other}}
  end
end

hub_state =
  Payments.init!(%{
    name: :payments,
    swarm_name: "e2e",
    xpub: xpub,
    trusted_sources: ["ingress", "cron"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: E2E.HubStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
    deliver_fn: deliver_fn,
    rpc_fn: &E2E.Chain.rpc/3,
    chains: [
      %{name: "base", rpc_url: "injected", usdc_contract: "0xCONTRACT",
        confirmations: 10, decimals: 6, start_block: 100, max_block_range: 1000}
    ]
  })

{:ok, _} = E2E.Hub.start(hub_state)

# Helpers over the live seams.
chat_body =
  Jason.encode!(%{"model" => "gpt-e2e", "messages" => [%{"role" => "user", "content" => "hi"}]})

chat = fn ->
  {:ok, status, body} =
    Curl.post(proxy.endpoint,
      body: chat_body,
      headers: [{"authorization", "Bearer #{token}"}, {"content-type", "application/json"}],
      timeout: 5
    )

  {status, Jason.decode!(body)}
end

balance = fn -> Proxy.credit_balance(proxy.state_pid, E2E.ProxyStore, beneficiary) end

quota_status = fn ->
  {:reply, json, _} =
    Proxy.handle_message(
      "dashboard",
      Jason.encode!(%{
        action: "quota_status",
        conversation_id: "tg:e2e:0",
        kind: "dm",
        workspace_key: "default"
      }),
      proxy
    )

  Jason.decode!(json)
end

# ═══════════════════════════════════════════════════════════════════════════
# 2. Deposit address: trusted source asks the hub for the proxy's budget
#    identity -> ADDR0, stable on re-request.
# ═══════════════════════════════════════════════════════════════════════════

dep1 = E2E.Hub.call("ingress", %{action: "deposit_address", beneficiary: beneficiary})
dep2 = E2E.Hub.call("ingress", %{action: "deposit_address", beneficiary: beneficiary})

Check.check(f, "deposit_address for the proxy's budget identity is ADDR0",
  dep1["ok"] == true and dep1["address"] == addr0 and dep1["namespace"] == "llm_quota")
Check.check(f, "deposit address is stable on re-request", dep2["address"] == addr0)

# ═══════════════════════════════════════════════════════════════════════════
# 3. Exhaust the free budget with REAL HTTP chat calls; the third call blocks
#    and the Telegram notice carries the hub-provided top-up hint.
# ═══════════════════════════════════════════════════════════════════════════

{s1, r1} = chat.()
{s2, r2} = chat.()

Check.check(f, "two real chat calls at $0.625 each pass through the $1.25 budget",
  s1 == 200 and s2 == 200 and
    get_in(r1, ["choices", Access.at(0), "message", "content"]) == "pong-e2e" and
    get_in(r2, ["choices", Access.at(0), "message", "content"]) == "pong-e2e")

{s3, r3} = chat.()

Check.check(f, "third call is budget-blocked (spent == limit, zero credit)",
  s3 == 200 and r3["model"] == "llm-proxy-budget" and
    get_in(r3, ["x_router", "budget_exhausted"]) == true)

notices = Genswarms.Objects.ObjectServer.notices()

Check.check(f, "block notice carries the hub-provided top-up hint with ADDR0",
  match?([_ | _], notices) and
    String.contains?(List.last(notices), "daily LLM limit") and
    String.contains?(List.last(notices), "💳 Top up: send USDC (base) to #{addr0}"))

# ═══════════════════════════════════════════════════════════════════════════
# 4. Pay: a confirmed 2.5 USDC Transfer to ADDR0; tick the hub; the proxy's
#    balance shows exactly the converted amount over the strings-only wire.
# ═══════════════════════════════════════════════════════════════════════════

E2E.Chain.pay!(addr0, 150, "0xT1", 0, 2_500_000)
E2E.Chain.latest!(200)
E2E.Hub.tick()

Check.check(f, "hub settled the payment durably (ledger row, method usdc_base, ref 0xT1:0)",
  Enum.map(E2E.HubStore.rows(), & &1.idempotency_key) == ["base:0xT1:0"])

[first_delivery | _] = E2E.DeliveryLog.deliveries()

Check.check(f, "delivery happened over the live seam, stamped payment_confirmed from :payments",
  first_delivery.from == :payments and
    first_delivery.content["action"] == "payment_confirmed" and
    first_delivery.content["beneficiary"] == beneficiary and
    first_delivery.content["namespace"] == "llm_quota")

wire_amount = first_delivery.content["amount_usd"]

Check.check(f, "wire amount_usd is the plain decimal STRING \"2.5\" (strings-only contract)",
  wire_amount == "2.5" and is_binary(wire_amount) and Regex.match?(~r/^\d+(\.\d+)?$/, wire_amount))

[first_reply | _] = E2E.DeliveryLog.replies()

Check.check(f, "proxy acked the credit: ok:true, credited 2.50, balance 2.50",
  first_reply["ok"] == true and first_reply["credited_usd"] == "2.50" and
    first_reply["balance_usd"] == "2.50")

Check.check(f, "proxy durable credit balance is exactly 2.5", Decimal.equal?(balance.(), Decimal.new("2.5")))
Check.check(f, "quota_status shows credit balance \"2.50\"",
  get_in(quota_status.(), ["credit", "balance_usd"]) == "2.50")
Check.check(f, "hub has nothing queued undelivered", E2E.Hub.undelivered() == %{})

# ═══════════════════════════════════════════════════════════════════════════
# 5. Spend credit: the next real HTTP call succeeds; the whole $0.625 cost is
#    credit-funded (spent already at the limit) -> balance 1.875 exactly.
# ═══════════════════════════════════════════════════════════════════════════

{s4, r4} = chat.()

Check.check(f, "with credit, the blocked identity's next real call reaches the upstream",
  s4 == 200 and get_in(r4, ["choices", Access.at(0), "message", "content"]) == "pong-e2e" and
    r4["model"] != "llm-proxy-budget")

Check.check(f, "balance debited by the upstream's exact cost (2.5 - 0.625 = 1.875)",
  Decimal.equal?(balance.(), Decimal.new("1.875")))

Check.check(f, "the debit is a durable ledger entry (kind debit, -0.625, debit:<request_id> key)",
  Enum.any?(E2E.ProxyStore.credit_entries(), fn e ->
    e.kind == "debit" and Decimal.equal?(e.amount_usd, Decimal.new("-0.625")) and
      String.starts_with?(e.idempotency_key, "debit:")
  end))

Check.check(f, "quota_status stays consistent (balance \"1.88\" at 2dp, spend past limit)",
  get_in(quota_status.(), ["credit", "balance_usd"]) == "1.88")

# ═══════════════════════════════════════════════════════════════════════════
# 6. Idempotency across the seam, both directions:
#    (a) delivered-but-ack-lost: the proxy credits, the hub never learns and
#        redelivers the SAME confirmation -> proxy answers duplicate:true,
#        balance unchanged, exactly one ledger entry;
#    (b) hub-side re-present: cursor rolled back re-scans the same log range
#        -> the hub's durable dedup settles nothing, delivers nothing.
# ═══════════════════════════════════════════════════════════════════════════

E2E.Chain.pay!(addr0, 195, "0xT2", 0, 1_000_000)
E2E.Chain.latest!(300)
E2E.Flags.drop_ack!(true)
E2E.Hub.tick()

Check.check(f, "6a: ack lost — proxy already credited (+1.00) but hub queued the delivery for retry",
  Decimal.equal?(balance.(), Decimal.new("2.875")) and
    Map.has_key?(E2E.Hub.undelivered(), "base:0xT2:0"))

E2E.Flags.drop_ack!(false)
replies_before = length(E2E.DeliveryLog.replies())
E2E.Hub.tick()

Check.check(f, "6a: redelivery of the SAME confirmation is answered duplicate:true",
  match?(%{"ok" => true, "duplicate" => true}, List.last(E2E.DeliveryLog.replies())) and
    length(E2E.DeliveryLog.replies()) == replies_before + 1)

Check.check(f, "6a: balance unchanged and undelivered queue cleared",
  Decimal.equal?(balance.(), Decimal.new("2.875")) and E2E.Hub.undelivered() == %{})

Check.check(f, "6a: exactly ONE durable credit entry for usdc_base:0xT2:0",
  Enum.count(E2E.ProxyStore.credit_entries(), &(&1.idempotency_key == "usdc_base:0xT2:0")) == 1)

deliveries_before = length(E2E.DeliveryLog.deliveries())
E2E.HubStore.set_cursor("base", 100)
E2E.Hub.tick()

Check.check(f, "6b: re-scanning the same log range settles nothing new and delivers nothing (hub dedup)",
  length(E2E.DeliveryLog.deliveries()) == deliveries_before and
    length(E2E.HubStore.rows()) == 2 and
    Decimal.equal?(balance.(), Decimal.new("2.875")) and
    E2E.HubStore.cursor("base") == 290)

# ═══════════════════════════════════════════════════════════════════════════
# 7. Drain to block: spend until credits go <= 0 -> blocked again, hint again.
# ═══════════════════════════════════════════════════════════════════════════

drained = for _ <- 1..5, do: chat.()

Check.check(f, "five more $0.625 calls all succeed while balance stays > 0",
  Enum.all?(drained, fn {s, r} -> s == 200 and r["model"] != "llm-proxy-budget" end))

Check.check(f, "balance is now negative (0.375 - 0.625 = -0.25): overdraft on the last straddle",
  Decimal.equal?(balance.(), Decimal.new("-0.25")))

{s_blocked, r_blocked} = chat.()
notices2 = Genswarms.Objects.ObjectServer.notices()

Check.check(f, "credits <= 0 -> blocked again",
  s_blocked == 200 and r_blocked["model"] == "llm-proxy-budget" and
    get_in(r_blocked, ["x_router", "budget_exhausted"]) == true)

Check.check(f, "the fresh block notice carries the top-up hint again",
  length(notices2) == 2 and String.contains?(List.last(notices2), addr0))

# ═══════════════════════════════════════════════════════════════════════════
# 8. Outage retry across the seam (D2): the proxy's credit store is down when
#    a NEW payment's delivery arrives -> proxy NACKs retryable -> the hub
#    keeps it undelivered and redelivers next tick after the store heals ->
#    credited exactly once.
# ═══════════════════════════════════════════════════════════════════════════

E2E.ProxyStore.credit_down!(true)
E2E.Chain.pay!(addr0, 295, "0xT4", 0, 3_000_000)
E2E.Chain.latest!(400)
E2E.Hub.tick()

outage_reply = List.last(E2E.DeliveryLog.replies())

Check.check(f, "store down: proxy answers ok:false retryable (fail closed, key released)",
  outage_reply["ok"] == false and outage_reply["retryable"] == true and
    outage_reply["error"] == "store_unavailable")

Check.check(f, "hub recorded the settlement but holds the delivery undelivered",
  Enum.any?(E2E.HubStore.rows(), &(&1.idempotency_key == "base:0xT4:0")) and
    Map.has_key?(E2E.Hub.undelivered(), "base:0xT4:0") and
    Decimal.equal?(balance.(), Decimal.new("-0.25")))

E2E.ProxyStore.credit_down!(false)
E2E.Hub.tick()

Check.check(f, "after the store heals, the hub's retry credits EXACTLY once (-0.25 + 3.00 = 2.75)",
  Decimal.equal?(balance.(), Decimal.new("2.75")) and
    E2E.Hub.undelivered() == %{} and
    Enum.count(E2E.ProxyStore.credit_entries(), &(&1.idempotency_key == "usdc_base:0xT4:0")) == 1)

{s5, r5} = chat.()

Check.check(f, "the healed, re-credited identity spends again (2.75 - 0.625 = 2.125)",
  s5 == 200 and get_in(r5, ["choices", Access.at(0), "message", "content"]) == "pong-e2e" and
    Decimal.equal?(balance.(), Decimal.new("2.125")))

Check.check(f, "quota_status renders the final balance \"2.13\" (2dp)",
  get_in(quota_status.(), ["credit", "balance_usd"]) == "2.13")

# ═══════════════════════════════════════════════════════════════════════════
# 9. Cross-contract sanity: every hub-emitted confirmation's method has no
#    colon (the proxy rejects ":" in method), every ref DOES carry the
#    tx:logIndex colon, and every confirmation was ultimately accepted.
# ═══════════════════════════════════════════════════════════════════════════

confirmations =
  E2E.DeliveryLog.deliveries()
  |> Enum.map(& &1.content)
  |> Enum.filter(&(&1["action"] == "payment_confirmed"))

Check.check(f, "every emitted method is colon-free \"usdc_base\"; every ref is tx:logIndex",
  confirmations != [] and
    Enum.all?(confirmations, fn c ->
      c["method"] == "usdc_base" and not String.contains?(c["method"], ":") and
        Regex.match?(~r/^0x[^:]+:\d+$/, c["ref"])
    end))

Check.check(f, "no confirmation was ever rejected as malformed (colon-refs accepted by the proxy)",
  not Enum.any?(E2E.DeliveryLog.replies(), &(&1["error"] == "bad_payment_confirmed")))

Proxy.terminate(:normal, proxy)
Check.finish(f)
IO.puts("USDC_TOPUP_E2E: ALL PASS")
