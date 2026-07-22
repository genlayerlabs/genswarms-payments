Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

defmodule FakeStore do
  # in-memory store via persistent_term so module-fun callbacks reach it
  def reset, do: :persistent_term.put({__MODULE__, :bindings}, %{})
  def all, do: :persistent_term.get({__MODULE__, :bindings}, %{})

  def put_address_binding(b) do
    :persistent_term.put({__MODULE__, :bindings}, Map.put(all(), b.beneficiary, b))
    :ok
  end

  def get_address_binding(ben), do: {:ok, Map.get(all(), ben)}
  def list_address_bindings, do: {:ok, Map.values(all())}
end

defmodule DownStore do
  def put_address_binding(_), do: {:error, :db_down}
  def get_address_binding(_), do: {:error, :db_down}
  def list_address_bindings, do: {:ok, []}
end

FakeStore.reset()

config = %{
  name: :payments,
  xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
  trusted_sources: ["telegram_ingress"],
  targets: ["llm_proxy"],
  namespace: "llm_quota",
  store_mod: FakeStore,
  auto_tick: false
}

state = Payments.init(config)

req = Jason.encode!(%{action: "deposit_address", beneficiary: "budget:abc"})

# untrusted source ⇒ silence (fail-closed)
Check.check(f, "untrusted source gets no reply",
  match?({:noreply, _}, Payments.handle_message("randomagent", req, state)))

{:reply, json, state2} = Payments.handle_message("telegram_ingress", req, state)
reply = Jason.decode!(json)

Check.check(f, "trusted source gets an EIP-55 address",
  reply["ok"] == true and String.starts_with?(reply["address"], "0x") and
    byte_size(reply["address"]) == 42)

{:reply, json2, _} = Payments.handle_message("telegram_ingress", req, state2)
Check.check(f, "same beneficiary ⇒ same address (stable for life)",
  Jason.decode!(json2)["address"] == reply["address"])

other = Jason.encode!(%{action: "deposit_address", beneficiary: "budget:def"})
{:reply, json3, _} = Payments.handle_message("telegram_ingress", other, state2)
Check.check(f, "different beneficiary ⇒ different address",
  Jason.decode!(json3)["address"] != reply["address"])

Check.check(f, "binding persisted durably (index 0 for first beneficiary)",
  match?({:ok, %{index: 0}}, FakeStore.get_address_binding("budget:abc")))

# reboot: bindings + next_index rebuilt from store
state_reboot = Payments.init(config)
{:reply, json4, _} = Payments.handle_message("telegram_ingress", req, state_reboot)
Check.check(f, "address survives restart via store",
  Jason.decode!(json4)["address"] == reply["address"])

# fail-closed allocation: store down ⇒ error reply, no address minted
down = Payments.init(%{config | store_mod: DownStore})
{:reply, json5, _} = Payments.handle_message("telegram_ingress", req, down)
Check.check(f, "store down ⇒ allocation refused (fail closed)",
  Jason.decode!(json5)["ok"] == false)

# health + bad JSON
{:reply, h, _} = Payments.handle_message("anyone", Jason.encode!(%{action: "health"}), state)
Check.check(f, "health is unauthenticated and ok", Jason.decode!(h)["ok"] == true)
Check.check(f, "malformed JSON ⇒ noreply",
  match?({:noreply, _}, Payments.handle_message("telegram_ingress", "{nope", state)))

# ── 2c: untrusted "tick" is fully gated (coverage-only — the adversarial
# audit found this unpinned). An untrusted source's tick must be a no-op:
# {:noreply, _} AND poll must never actually run (rpc_fn never invoked).
{:ok, rpc_calls_2c} = Agent.start_link(fn -> 0 end)

counting_rpc_2c = fn _chain, _method, _params ->
  Agent.update(rpc_calls_2c, &(&1 + 1))
  {:ok, "0x1"}
end

state_2c =
  Payments.init(%{
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["telegram_ingress"],
    targets: ["llm_proxy"],
    store_mod: nil,
    auto_tick: false,
    rpc_fn: counting_rpc_2c,
    chains: [%{name: "base", rpc_url: "injected", usdc_contract: "0x0"}]
  })

tick_req = Jason.encode!(%{action: "tick"})

Check.check(f, "2c: untrusted tick gets {:noreply, _}",
  match?({:noreply, _}, Payments.handle_message("stranger", tick_req, state_2c)))

Payments.handle_message("stranger", tick_req, state_2c)

Check.check(f, "2c: untrusted tick never actually polls — rpc_fn is never invoked",
  Agent.get(rpc_calls_2c, & &1) == 0)

# C2: rpc_url validation — a curl --config tempfile is built from this value
# as `url = "#{rpc_url}"`; a quote lets an attacker close that value early
# and inject config directives, a backslash/control char is just as
# unsanitary. Reject at init, don't wait for curl to choke on it.
bad_rpc_config = Map.put(config, :chains, [%{name: "base", rpc_url: ~s(https://evil.example/"), usdc_contract: "0x0"}])

Check.check(f, "rpc_url containing a quote raises ArgumentError at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init(bad_rpc_config)
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

backslash_rpc_config = Map.put(config, :chains, [%{name: "base", rpc_url: "https://evil.example/\\injected", usdc_contract: "0x0"}])

Check.check(f, "rpc_url containing a backslash raises ArgumentError at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init(backslash_rpc_config)
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

control_char_rpc_config = Map.put(config, :chains, [%{name: "base", rpc_url: "https://evil.example/\ninjected", usdc_contract: "0x0"}])

Check.check(f, "rpc_url containing a control character raises ArgumentError at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init(control_char_rpc_config)
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

clean_rpc_config = Map.put(config, :chains, [%{name: "base", rpc_url: "https://mainnet.base.org/v2/KEY", usdc_contract: "0x0"}])
Check.check(f, "a clean rpc_url boots without raising",
  match?(%{}, Payments.init(clean_rpc_config)))

Check.finish(f)
