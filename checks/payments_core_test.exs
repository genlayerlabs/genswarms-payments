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
  allow_ephemeral: true,
  namespace: "llm_quota",
  store_mod: FakeStore,
  auto_tick: false
}

state = Payments.init!(config)

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
state_reboot = Payments.init!(config)
{:reply, json4, _} = Payments.handle_message("telegram_ingress", req, state_reboot)
Check.check(f, "address survives restart via store",
  Jason.decode!(json4)["address"] == reply["address"])

# fail-closed allocation: store down ⇒ error reply, no address minted
down = Payments.init!(%{config | store_mod: DownStore})
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
  Payments.init!(%{
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["telegram_ingress"],
    targets: ["llm_proxy"],
    allow_ephemeral: true,
    store_mod: nil,
    auto_tick: false,
    rpc_fn: counting_rpc_2c,
    chains: [%{name: "base", chain_id: 8453, rpc_url: "injected", usdc_contract: "0x0"}]
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
bad_rpc_config = Map.put(config, :chains, [%{name: "base", chain_id: 8453, rpc_url: ~s(https://evil.example/"), usdc_contract: "0x0"}])

Check.check(f, "rpc_url containing a quote raises ArgumentError at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init!(bad_rpc_config)
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

backslash_rpc_config = Map.put(config, :chains, [%{name: "base", chain_id: 8453, rpc_url: "https://evil.example/\\injected", usdc_contract: "0x0"}])

Check.check(f, "rpc_url containing a backslash raises ArgumentError at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init!(backslash_rpc_config)
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

control_char_rpc_config = Map.put(config, :chains, [%{name: "base", chain_id: 8453, rpc_url: "https://evil.example/\ninjected", usdc_contract: "0x0"}])

Check.check(f, "rpc_url containing a control character raises ArgumentError at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init!(control_char_rpc_config)
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

clean_rpc_config = Map.put(config, :chains, [%{name: "base", chain_id: 8453, rpc_url: "https://mainnet.base.org/v2/KEY", usdc_contract: "0x0"}])
Check.check(f, "a clean rpc_url boots without raising",
  match?(%{}, Payments.init!(clean_rpc_config)))

# 3b: a chain missing rpc_url entirely must raise at init (ArgumentError),
# not silently boot and blow up later at runtime with a KeyError the first
# time Rpc.call tries chain.rpc_url.
missing_rpc_config = Map.put(config, :chains, [%{name: "base", chain_id: 8453, usdc_contract: "0x0"}])

Check.check(f, "a chain missing rpc_url entirely raises ArgumentError at init",
  match?(
    {:error, %ArgumentError{}},
    (try do
       Payments.init!(missing_rpc_config)
       {:ok, :did_not_raise}
     rescue
       e -> {:error, e}
     end)
  ))

# chain_id is an immutable on-chain identity used in settlement dedup keys,
# so every configured chain must supply it as an integer.
missing_chain_id_config =
  Map.put(config, :chains, [%{name: "base", rpc_url: "injected", usdc_contract: "0x0"}])

missing_chain_id_result =
  try do
    Payments.init!(missing_chain_id_config)
    {:ok, :did_not_raise}
  rescue
    e -> {:error, e}
  end

Check.check(f, "a chain missing required chain_id raises clearly at init",
  match?({:error, %ArgumentError{}}, missing_chain_id_result) and
    Exception.message(elem(missing_chain_id_result, 1)) =~ "missing required chain_id")

non_integer_chain_id_config =
  Map.put(config, :chains, [
    %{name: "base", chain_id: "8453", rpc_url: "injected", usdc_contract: "0x0"}
  ])

non_integer_chain_id_result =
  try do
    Payments.init!(non_integer_chain_id_config)
    {:ok, :did_not_raise}
  rescue
    e -> {:error, e}
  end

Check.check(f, "a non-integer chain_id raises clearly at init",
  match?({:error, %ArgumentError{}}, non_integer_chain_id_result) and
    Exception.message(elem(non_integer_chain_id_result, 1)) =~ "non-integer required chain_id")

# chain_id identifies real on-chain state and must never admit sentinel-like
# zero/negative values into permanent idempotency keys.
for invalid_chain_id <- [0, -1] do
  invalid_chain_id_config =
    Map.put(config, :chains, [
      %{name: "base", chain_id: invalid_chain_id, rpc_url: "injected", usdc_contract: "0x0"}
    ])

  invalid_chain_id_result =
    try do
      Payments.init!(invalid_chain_id_config)
      {:ok, :did_not_raise}
    rescue
      e -> {:error, e}
    end

  Check.check(f, "chain_id #{invalid_chain_id} is rejected as non-positive",
    match?({:error, %ArgumentError{}}, invalid_chain_id_result) and
      Exception.message(elem(invalid_chain_id_result, 1)) =~
        "non-positive required chain_id: #{invalid_chain_id}")
end

# Scan cursors are keyed by chain.name, while chain_id is the immutable
# on-chain identity in settlement keys. Both fields must be unique across the
# complete chain list.
duplicate_name_config =
  Map.put(config, :chains, [
    %{name: "base", chain_id: 8453, rpc_url: "injected-a", usdc_contract: "0x0"},
    %{name: "base", chain_id: 84532, rpc_url: "injected-b", usdc_contract: "0x1"}
  ])

duplicate_name_result =
  try do
    Payments.init!(duplicate_name_config)
    {:ok, :did_not_raise}
  rescue
    e -> {:error, e}
  end

Check.check(f, "duplicate chain names are rejected clearly at init",
  match?({:error, %ArgumentError{}}, duplicate_name_result) and
    Exception.message(elem(duplicate_name_result, 1)) =~
      ~s(duplicate chain name "base"))

duplicate_chain_id_config =
  Map.put(config, :chains, [
    %{name: "base", chain_id: 8453, rpc_url: "injected-a", usdc_contract: "0x0"},
    %{name: "base_archive", chain_id: 8453, rpc_url: "injected-b", usdc_contract: "0x1"}
  ])

duplicate_chain_id_result =
  try do
    Payments.init!(duplicate_chain_id_config)
    {:ok, :did_not_raise}
  rescue
    e -> {:error, e}
  end

Check.check(f, "duplicate chain_ids are rejected clearly at init",
  match?({:error, %ArgumentError{}}, duplicate_chain_id_result) and
    Exception.message(elem(duplicate_chain_id_result, 1)) =~
      "duplicate chain_id 8453")

# C3: a hub that credits targets must not silently use restart-volatile
# address allocation and settlement dedup.
ephemeral_config = %{
  xpub: config.xpub,
  trusted_sources: ["ingress"],
  targets: ["llm_proxy"],
  store_mod: nil
}

ephemeral_result = Payments.init(ephemeral_config)

Check.check(f, "non-empty targets without durable settlement dedup are refused",
  match?({:error, %ArgumentError{}}, ephemeral_result) and
    elem(ephemeral_result, 1).message =~ "memory mode re-mints addresses and re-credits history")

bindings_only_result = Payments.init(%{ephemeral_config | store_mod: FakeStore})

Check.check(f, "a store without the durable settlement pair is also refused",
  match?({:error, %ArgumentError{}}, bindings_only_result))

Check.check(f, "allow_ephemeral:true is the explicit opt-out",
  match?({:ok, %{}}, Payments.init(Map.put(ephemeral_config, :allow_ephemeral, true))))

# ── Engine contract pin: ObjectServer matches init/1 against {:ok, state} —
# v0.1.0 returned the bare state map and crash-looped at real swarm boot
# (caught 2026-07-24 on the first live engine boot, missed by every direct-call
# check and the cross-package e2e).
{:ok, engine_state} =
  Genswarms.Payments.init(%{
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: ["ingress"],
    targets: ["llm_proxy"],
    allow_ephemeral: true
  })

Check.check(f, "engine contract: init/1 returns {:ok, state} (ObjectServer shape)",
  is_map(engine_state) and Map.has_key?(engine_state, :bindings))

Check.finish(f)
