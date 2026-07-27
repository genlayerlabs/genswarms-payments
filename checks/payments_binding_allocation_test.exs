# Standalone — NO Postgres, NO network.
#   mix run checks/payments_binding_allocation_test.exs
#
# `next_index` is an in-memory allocator over a SHARED durable table, so two
# orchestrators (any rolling restart) can hold the same value. Before this fix,
# the loser of that race returned the error with `next_index` UNCHANGED and then
# re-offered the same permanently-taken index to every subsequent new user: not
# a race, a LIVELOCK — `/topup` stayed broken on that instance long after the
# restart finished, and the failure was indistinguishable from "the DB is down".
#
# Pinned here:
#   * `:index_taken` ⇒ advance and retry (bounded), so the loser converges;
#   * `:binding_conflict` ⇒ refuse immediately. THIS beneficiary is already
#     bound to a different address, and rebinding would strand money already
#     sent to the first one;
#   * retries are BOUNDED, and exhaustion refuses rather than spinning;
#   * a legacy store that cannot tell the two apart keeps the old behaviour.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

ns = "llm_quota"

defmodule AllocStore do
  # `taken` is the set of HD indices another orchestrator already owns.
  def configure(taken, mode \\ :index_taken) do
    :persistent_term.put({__MODULE__, :taken}, MapSet.new(taken))
    :persistent_term.put({__MODULE__, :mode}, mode)
    :persistent_term.put({__MODULE__, :attempts}, [])
  end

  def attempts, do: :persistent_term.get({__MODULE__, :attempts}, []) |> Enum.reverse()

  def put_address_binding(binding) do
    :persistent_term.put(
      {__MODULE__, :attempts},
      [binding.index | :persistent_term.get({__MODULE__, :attempts}, [])]
    )

    if MapSet.member?(:persistent_term.get({__MODULE__, :taken}), binding.index) do
      {:error, :persistent_term.get({__MODULE__, :mode})}
    else
      :ok
    end
  end

  def list_address_bindings do
    {:ok,
     [
       %{
         beneficiary: "llmb_existing",
         index: 4,
         address: "0x4444444444444444444444444444444444444444",
         namespace: "llm_quota"
       }
     ]}
  end

  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_payment), do: :ok
  def list_settlements_since(_after, _limit), do: {:ok, %{settlements: [], max_seq: 0}}
end

boot = fn ->
  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    namespace: ns,
    trusted_sources: ["commands"],
    targets: ["llm_proxy"],
    store_mod: AllocStore,
    auto_tick: false
  })
end

ask = fn state, beneficiary ->
  {:reply, json, state} =
    Payments.handle_message(
      "commands",
      Jason.encode!(%{action: "deposit_address", beneficiary: beneficiary}),
      state
    )

  {Jason.decode!(json), state}
end

# ── the livelock scenario: another instance already took 5 and 6 ────────────
AllocStore.configure([5, 6])
state = boot.()

Check.check(f, "sanity: this hub would allocate index 5 next", state.next_index == 5)

{first, state} = ask.(state, "llmb_new_a")

Check.check(
  f,
  "the loser of a concurrent allocation still gets an address (it steps over the taken indices)",
  first["ok"] == true and is_binary(first["address"])
)

Check.check(
  f,
  "it tried 5, 6 and then 7 — advancing on each :index_taken",
  AllocStore.attempts() == [5, 6, 7]
)

Check.check(
  f,
  "and the advance PERSISTS: the next user starts above the taken indices",
  state.next_index == 8
)

{second, state} = ask.(state, "llmb_new_b")

Check.check(
  f,
  "the next new user is served without re-walking the taken indices",
  second["ok"] == true and Enum.drop(AllocStore.attempts(), 3) == [8]
)

Check.check(
  f,
  "two users never share an address",
  first["address"] != second["address"]
)

# ── a rebind of an EXISTING beneficiary is still refused, with no retry ─────
AllocStore.configure([5], :binding_conflict)
conflict_state = boot.()

{conflict, conflict_state} = ask.(conflict_state, "llmb_rebind")

Check.check(
  f,
  "phase-0 invariant kept: a binding_conflict refuses the allocation",
  conflict["ok"] == false and conflict["error"] == "store_unavailable"
)

Check.check(
  f,
  "a binding_conflict is NEVER retried at another index (a rebind must not be worked around)",
  AllocStore.attempts() == [5] and conflict_state.next_index == 5
)

Check.check(
  f,
  "a store with no get_address_binding/1 keeps the old refusal exactly (no read to adopt from)",
  conflict["address"] == nil
)

# ── R4-P4-I4: a conflict against a store that CAN say what the beneficiary is
# bound to is ADOPTED, not refused. The old refusal was permanent — this
# process's `state.bindings` was never repaired, so every later /topup by that
# user on this instance failed identically, reported as "the DB is down" while
# the store held the exact answer.
defmodule AdoptStore do
  @bound %{
    beneficiary: "llmb_peer_written",
    index: 11,
    address: "0x1111111111111111111111111111111111111111",
    namespace: "llm_quota"
  }

  def configure(mode), do: :persistent_term.put({__MODULE__, :mode}, mode)
  def bound, do: @bound

  def put_address_binding(binding) do
    :persistent_term.put(
      {__MODULE__, :attempts},
      [binding.index | :persistent_term.get({__MODULE__, :attempts}, [])]
    )

    {:error, :binding_conflict}
  end

  def attempts, do: :persistent_term.get({__MODULE__, :attempts}, []) |> Enum.reverse()

  def get_address_binding(_beneficiary) do
    case :persistent_term.get({__MODULE__, :mode}) do
      :ok -> {:ok, @bound}
      :foreign -> {:ok, %{@bound | namespace: "someone_else"}}
      :missing -> {:ok, nil}
      :down -> {:error, :db_down}
    end
  end

  def list_address_bindings, do: {:ok, []}
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_payment), do: :ok
  def list_settlements_since(_after, _limit), do: {:ok, %{settlements: [], max_seq: 0}}
end

adopt_boot = fn ->
  :persistent_term.put({AdoptStore, :attempts}, [])

  Payments.init!(%{
    xpub: xpub,
    allow_test_xpub: true,
    namespace: ns,
    trusted_sources: ["commands"],
    targets: ["llm_proxy"],
    store_mod: AdoptStore,
    auto_tick: false
  })
end

AdoptStore.configure(:ok)
adopt_state = adopt_boot.()
{adopted, adopt_state} = ask.(adopt_state, "llmb_peer_written")

Check.check(
  f,
  "a binding_conflict serves the STORED address instead of refusing",
  adopted["ok"] == true and adopted["address"] == AdoptStore.bound().address
)

Check.check(
  f,
  "the adopted address is the store's, never a fresh derivation at this hub's index",
  AdoptStore.attempts() == [0] and adopted["address"] != nil
)

Check.check(
  f,
  "the in-memory map is REPAIRED, so the next /topup is a plain hit with no second write",
  adopt_state.bindings["llmb_peer_written"].address == AdoptStore.bound().address and
    adopt_state.next_index == 12
)

{again, _adopt_state} = ask.(adopt_state, "llmb_peer_written")

Check.check(
  f,
  "the SAME beneficiary can never end up with two addresses",
  again["address"] == adopted["address"] and AdoptStore.attempts() == [0]
)

# A binding under a FOREIGN namespace must not be served: settlements to it
# would be HELD, so handing it out invites a deposit into a black hole.
AdoptStore.configure(:foreign)
foreign_state = adopt_boot.()
{foreign, foreign_state} = ask.(foreign_state, "llmb_peer_written")

Check.check(
  f,
  "a binding under a FOREIGN namespace is refused, never adopted",
  foreign["ok"] == false and foreign["error"] == "namespace_mismatch" and
    MapSet.member?(foreign_state.foreign_namespace_bindings, "llmb_peer_written")
)

# "You are bound" followed by "I cannot say to what" is a store defect, not an
# answer — and it must never become a second derivation.
AdoptStore.configure(:missing)
{missing, missing_state} = ask.(adopt_boot.(), "llmb_peer_written")

Check.check(
  f,
  "a conflict whose read-back finds NO row refuses (never derives a second address)",
  missing["ok"] == false and missing["error"] == "store_unavailable" and
    missing_state.bindings["llmb_peer_written"] == nil
)

AdoptStore.configure(:down)
{read_down, _} = ask.(adopt_boot.(), "llmb_peer_written")

Check.check(
  f,
  "a conflict whose read-back ERRORS refuses",
  read_down["ok"] == false and read_down["error"] == "store_unavailable"
)

# ── retries are bounded ────────────────────────────────────────────────────
AllocStore.configure(Enum.to_list(0..500))
exhausted_state = boot.()

{exhausted, _exhausted_state} = ask.(exhausted_state, "llmb_never")

Check.check(
  f,
  "an endless run of taken indices refuses instead of spinning",
  exhausted["ok"] == false and exhausted["error"] == "store_unavailable"
)

Check.check(
  f,
  "the retry is bounded (25 attempts, not a walk of the whole keyspace)",
  length(AllocStore.attempts()) == 25
)

Check.finish(f)
