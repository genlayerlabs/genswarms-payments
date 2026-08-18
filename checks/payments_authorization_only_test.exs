# Standalone — no Postgres, no network.
# Proves an authorization-only hub can boot without an xpub while every
# derived-address surface stays explicitly fail-closed.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

defmodule AuthorizationOnlyStore do
  def reset do
    :persistent_term.put({__MODULE__, :issued}, %{})
  end

  # The complete bindings callback group is present, but this read must never
  # happen when derived addresses are disabled. It simulates a stale bindings
  # table that should not be allowed back into the live watcher set.
  def put_address_binding(_row), do: :ok
  def list_address_bindings, do: raise("authorization-only mode loaded HD bindings")

  def record_issued_authorization(row) do
    rows = :persistent_term.get({__MODULE__, :issued})
    :persistent_term.put({__MODULE__, :issued}, Map.put(rows, row.nonce_hex, row))
    :ok
  end

  def issued_authorization(nonce), do: Map.get(:persistent_term.get({__MODULE__, :issued}), nonce)
  def live_authorization_nonces(_now), do: Map.keys(:persistent_term.get({__MODULE__, :issued}))
  def mark_authorization_consumed(_nonce), do: :ok
  def authorization_settled?(_nonce), do: {:ok, false}
  def record_unrecognised_inflow(_row), do: :ok
end

AuthorizationOnlyStore.reset()

config = %{
  deposit_addresses_enabled: false,
  trusted_sources: ["commands", "ops"],
  operator_sources: ["ops"],
  targets: [],
  namespace: "authorization_only",
  store_mod: AuthorizationOnlyStore,
  auto_tick: false,
  chains: [
    %{
      name: "base",
      chain_id: 8453,
      rpc_url: "injected",
      usdc_contract: "0xUSDC",
      treasury_address: "0xTREASURY",
      confirmations: 0,
      fast_credit_depth: 0,
      decimals: 6,
      start_block: 0
    }
  ]
}

state = Payments.init!(config)

Check.check(f, "authorization-only init does not require an xpub", state.xpub == nil)

Check.check(
  f,
  "authorization-only init exposes the disabled custody lane",
  state.deposit_addresses_enabled == false and state.bindings == %{} and
    state.degraded_boot == false
)

{:reply, health_json, state} =
  Payments.handle_message("anyone", Jason.encode!(%{"action" => "health"}), state)

health = Jason.decode!(health_json)

Check.check(
  f,
  "health reports authorization-only mode",
  health["ok"] == true and health["deposit_addresses_enabled"] == false and
    health["bindings"] == 0
)

{:reply, deposit_json, state} =
  Payments.handle_message(
    "commands",
    Jason.encode!(%{"action" => "deposit_address", "beneficiary" => "user:1"}),
    state
  )

deposit = Jason.decode!(deposit_json)

Check.check(
  f,
  "deposit address allocation fails closed",
  deposit == %{"ok" => false, "error" => "deposit_addresses_disabled"}
)

{:reply, sweep_json, state} =
  Payments.handle_message(
    "ops",
    Jason.encode!(%{"action" => "sweep_report", "chain" => "base"}),
    state
  )

sweep = Jason.decode!(sweep_json)

Check.check(
  f,
  "derived-address sweep reporting fails closed",
  sweep["action"] == "sweep_report" and sweep["ok"] == false and
    sweep["error"] == "deposit_addresses_disabled"
)

nonce = "0x" <> String.duplicate("ab", 32)

{:reply, issue_json, _state} =
  Payments.handle_message(
    "commands",
    Jason.encode!(%{
      "action" => "issue_authorization",
      "nonce" => nonce,
      "order_ref" => "authorization-only-1",
      "beneficiary" => "user:1",
      "amount_usd" => "5",
      "valid_before" => DateTime.to_unix(DateTime.utc_now()) + 600
    }),
    state
  )

issue = Jason.decode!(issue_json)

Check.check(
  f,
  "EIP-3009 issuance remains active without an xpub",
  issue["ok"] == true and issue["nonce"] == nonce and
    AuthorizationOnlyStore.issued_authorization(nonce).beneficiary == "user:1"
)

invalid_flag =
  try do
    Payments.init!(%{deposit_addresses_enabled: "false"})
    :accepted
  rescue
    error -> error
  end

Check.check(
  f,
  "authorization-only opt-in requires a real boolean",
  match?(%ArgumentError{}, invalid_flag)
)

default_still_requires_xpub =
  try do
    Payments.init!(%{})
    :accepted
  rescue
    error -> error
  end

Check.check(
  f,
  "the default deposit-address mode still requires an xpub",
  match?(%KeyError{}, default_still_requires_xpub)
)

Check.finish(f)
