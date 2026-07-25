# Standalone — NO Postgres, NO network.  mix run checks/payments_boot_gates_test.exs
#
# D7 — a publicly known test xpub is a PUBLIC key whose xprv is in every BIP32
# tutorial. Watching it means crediting deposits anyone on earth can sweep.
# Booting one raises unless allow_test_xpub: true says the operator meant it
# (a local rig only — never mainnet).
#
# D2 — a binding loaded at boot under another namespace is watched but its
# settlements are HELD. Crediting it would silently re-namespace someone
# else's money; dropping it would lose a real deposit.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

test_xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

# A structurally valid xpub that is NOT on the denylist, minted here by
# re-chain-coding the denylisted one (same on-curve pubkey, different chain
# code ⇒ a different extended key, different serialization, different
# checksum). Minted rather than pasted so this check never has to assert the
# provenance of some other published key.
base58_alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

base58_decode = fn string ->
  string
  |> String.graphemes()
  |> Enum.reduce(0, fn char, acc -> acc * 58 + elem(:binary.match(base58_alphabet, char), 0) end)
  |> :binary.encode_unsigned()
end

base58check_encode = fn payload ->
  full = payload <> binary_part(:crypto.hash(:sha256, :crypto.hash(:sha256, payload)), 0, 4)
  alphabet = String.graphemes(base58_alphabet)

  Stream.unfold(:binary.decode_unsigned(full), fn
    0 -> nil
    n -> {Enum.at(alphabet, rem(n, 58)), div(n, 58)}
  end)
  |> Enum.reverse()
  |> Enum.join()
end

raw = base58_decode.(test_xpub)
payload = binary_part(raw, 0, byte_size(raw) - 4)
<<header::binary-13, _chain_code::binary-32, pubkey::binary-33>> = payload

other_xpub = base58check_encode.(header <> :binary.copy(<<7>>, 32) <> pubkey)

init = fn config ->
  try do
    {:ok, Payments.init!(config)}
  rescue
    error -> {:raised, error}
  end
end

denied = init.(%{xpub: test_xpub, trusted_sources: [], targets: []})

Check.check(
  f,
  "D7: booting a publicly known test xpub raises at init",
  match?({:raised, %ArgumentError{}}, denied) and
    Exception.message(elem(denied, 1)) =~ "publicly known test xpub"
)

Check.check(
  f,
  "D7: allow_test_xpub: true is the explicit, documented opt-out",
  match?({:ok, %{}}, init.(%{xpub: test_xpub, allow_test_xpub: true}))
)

Check.check(
  f,
  "D7: allow_test_xpub: false is still a refusal (no truthy-ish escape)",
  match?({:raised, %ArgumentError{}}, init.(%{xpub: test_xpub, allow_test_xpub: false}))
)

Check.check(
  f,
  "D7: a non-boolean allow_test_xpub is rejected, not treated as truthy",
  Enum.all?(["true", 1, :yes], fn value ->
    match?({:raised, %ArgumentError{}}, init.(%{xpub: test_xpub, allow_test_xpub: value}))
  end)
)

Check.check(
  f,
  "D7: an xpub outside the denylist needs no opt-out",
  match?({:ok, %{}}, init.(%{xpub: other_xpub}))
)

Check.check(
  f,
  "D7: init/1 surfaces the refusal as {:error, _} for the engine, never a crash",
  match?({:error, %ArgumentError{}}, Payments.init(%{xpub: test_xpub}))
)

# The engine contract for the OTHER boolean gate must be just as strict.
Check.check(
  f,
  "a non-boolean allow_ephemeral is rejected rather than read as truthy",
  match?(
    {:raised, %ArgumentError{}},
    init.(%{xpub: other_xpub, targets: ["t"], allow_ephemeral: "true"})
  )
)

# ── D2: namespace coherence at boot ─────────────────────────────────────────
defmodule MixedNamespaceStore do
  def reset do
    :persistent_term.put({__MODULE__, :rows}, [])
    :persistent_term.put({__MODULE__, :seen}, MapSet.new())
  end

  def rows, do: :persistent_term.get({__MODULE__, :rows}, [])

  def list_address_bindings do
    {:ok,
     [
       %{beneficiary: "budget:home", index: 0, address: "0xHOME", namespace: "llm_quota"},
       %{beneficiary: "budget:foreign", index: 1, address: "0xFOREIGN", namespace: "other_hub"}
     ]}
  end

  def put_address_binding(_binding), do: :ok

  def payment_seen?(key),
    do: {:ok, MapSet.member?(:persistent_term.get({__MODULE__, :seen}), key)}

  def record_payment(row) do
    :persistent_term.put(
      {__MODULE__, :seen},
      MapSet.put(:persistent_term.get({__MODULE__, :seen}), row.idempotency_key)
    )

    :persistent_term.put({__MODULE__, :rows}, [row | rows()])
    :ok
  end

  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
end

MixedNamespaceStore.reset()
{:ok, boot_events} = Agent.start_link(fn -> [] end)
{:ok, deliveries} = Agent.start_link(fn -> [] end)

hub =
  Payments.init!(%{
    name: :payments,
    xpub: test_xpub,
    allow_test_xpub: true,
    trusted_sources: ["ops"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: MixedNamespaceStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-24 12:00:00Z] end,
    deliver_fn: fn target, from, content ->
      Agent.update(deliveries, &[{target, from, Jason.decode!(content)} | &1])
      :ok
    end,
    metrics_fn: fn event, meta -> Agent.update(boot_events, &[{event, meta} | &1]) end
  })

Check.check(
  f,
  "D2: a foreign-namespace binding alarms at boot with both namespaces",
  Enum.any?(Agent.get(boot_events, & &1), fn {event, meta} ->
    event == "payments_namespace_mismatch" and meta.stage == "binding_load" and
      meta.beneficiary == "budget:foreign" and meta.binding_namespace == "other_hub" and
      meta.hub_namespace == "llm_quota"
  end)
)

Check.check(
  f,
  "D2: the foreign binding is still WATCHED (its address is not forgotten)",
  Map.has_key?(hub.bindings, "budget:foreign") and
    hub.bindings["budget:foreign"].address == "0xFOREIGN"
)

Check.check(
  f,
  "D2: no alarm for a binding whose namespace matches the hub",
  not Enum.any?(Agent.get(boot_events, & &1), fn {_event, meta} ->
    Map.get(meta, :beneficiary) == "budget:home"
  end)
)

settlement = fn beneficiary, namespace, key ->
  %{
    beneficiary: beneficiary,
    amount_usd: Decimal.new("5"),
    method: "usdc_base",
    ref: "#{key}:0",
    idempotency_key: key,
    namespace: namespace
  }
end

Agent.update(boot_events, fn _ -> [] end)

{settled_foreign, hub} =
  Payments.settle([settlement.("budget:foreign", "other_hub", "ns:foreign")], hub)

Check.check(
  f,
  "D2: a foreign-namespace binding's settlement is HELD — never re-namespaced",
  settled_foreign == 0 and MixedNamespaceStore.rows() == [] and
    Agent.get(deliveries, & &1) == []
)

Check.check(
  f,
  "D2: the hold is metered every round so the operator sees it, and it is NOT a quarantine",
  Enum.any?(Agent.get(boot_events, & &1), fn {event, meta} ->
    event == "payments_namespace_mismatch" and meta.stage == "settle" and
      meta.idempotency_key == "ns:foreign"
  end) and
    not Enum.any?(Agent.get(boot_events, & &1), fn {event, _meta} ->
      event == "payments_quarantined"
    end)
)

Check.check(
  f,
  "D2: a held settlement keeps its chain's cursor back (the key is not resolved)",
  not MapSet.member?(hub.seen_keys, "ns:foreign")
)

{settled_home, _hub} =
  Payments.settle([settlement.("budget:home", "llm_quota", "ns:home")], hub)

Check.check(
  f,
  "D2: same-namespace bindings settle normally alongside a held foreign one",
  settled_home == 1 and
    Enum.map(MixedNamespaceStore.rows(), & &1.idempotency_key) == ["ns:home"]
)

# A binding minted by this hub always carries the hub's namespace, so a clean
# store produces no held settlements at all.
defmodule CleanStore do
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_binding), do: :ok
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_row), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
end

clean =
  Payments.init!(%{
    xpub: test_xpub,
    allow_test_xpub: true,
    trusted_sources: ["ops"],
    targets: ["llm_proxy"],
    namespace: "llm_quota",
    store_mod: CleanStore,
    auto_tick: false,
    deliver_fn: fn _target, _from, _content -> :ok end
  })

Check.check(
  f,
  "a store with no foreign bindings tracks none",
  MapSet.size(clean.foreign_namespace_bindings) == 0
)

# ── chain config validation for the new C2 keys ─────────────────────────────
chain_init = fn chain ->
  try do
    {:ok,
     Payments.init!(%{
       xpub: test_xpub,
       allow_test_xpub: true,
       chains: [
         Map.merge(
           %{name: "base", chain_id: 8453, rpc_url: "https://rpc.example", usdc_contract: "0xC"},
           chain
         )
       ]
     })}
  rescue
    error -> {:raised, error}
  end
end

invalid_chain_cases = [
  {"finality must be :finalized or {:confirmations, n}", %{finality: "finalized"}},
  {"finality rejects a negative confirmation depth", %{finality: {:confirmations, -1}}},
  {"fast_credit_depth must be a non-negative integer", %{fast_credit_depth: -1}},
  {"fast_credit_depth rejects a non-integer", %{fast_credit_depth: "3"}},
  {"confirmations must be a non-negative integer", %{confirmations: -2}},
  {"decimals must be a non-negative integer", %{decimals: "6"}},
  {"a chain must be a map", %{}}
]

Check.check(
  f,
  "every new chain-level key is validated at init",
  Enum.all?(Enum.drop(invalid_chain_cases, -1), fn {_label, chain} ->
    match?({:raised, %ArgumentError{}}, chain_init.(chain))
  end)
)

Check.check(
  f,
  "a non-map chain entry is refused instead of crashing later",
  match?(
    {:raised, %ArgumentError{}},
    try do
      {:ok, Payments.init!(%{xpub: test_xpub, allow_test_xpub: true, chains: [:base]})}
    rescue
      error -> {:raised, error}
    end
  )
)

Check.check(
  f,
  "a non-list chains value is refused instead of crashing later",
  match?(
    {:raised, %ArgumentError{}},
    try do
      {:ok, Payments.init!(%{xpub: test_xpub, allow_test_xpub: true, chains: %{name: "base"}})}
    rescue
      error -> {:raised, error}
    end
  )
)

Check.check(
  f,
  "valid finality modes boot: :finalized (default) and {:confirmations, n}",
  match?({:ok, %{}}, chain_init.(%{})) and
    match?({:ok, %{}}, chain_init.(%{finality: :finalized})) and
    match?({:ok, %{}}, chain_init.(%{finality: {:confirmations, 30}})) and
    match?({:ok, %{}}, chain_init.(%{fast_credit_depth: 0}))
)

Check.finish(f)
