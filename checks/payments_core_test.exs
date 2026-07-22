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

Check.finish(f)
