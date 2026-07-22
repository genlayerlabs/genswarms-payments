Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

defmodule LedgerStore do
  def reset do
    :persistent_term.put({__MODULE__, :seen}, MapSet.new())
    :persistent_term.put({__MODULE__, :rows}, [])
    :persistent_term.put({__MODULE__, :down}, false)
  end

  def down!(flag), do: :persistent_term.put({__MODULE__, :down}, flag)
  defp down?, do: :persistent_term.get({__MODULE__, :down}, false)
  def rows, do: :persistent_term.get({__MODULE__, :rows}, [])

  def payment_seen?(key) do
    if down?(), do: {:error, :db_down},
      else: {:ok, MapSet.member?(:persistent_term.get({__MODULE__, :seen}), key)}
  end

  def record_payment(row) do
    if down?() do
      {:error, :db_down}
    else
      :persistent_term.put({__MODULE__, :seen},
        MapSet.put(:persistent_term.get({__MODULE__, :seen}), row.idempotency_key))
      :persistent_term.put({__MODULE__, :rows}, [row | rows()])
      :ok
    end
  end

  def list_address_bindings, do: {:ok, []}
end

LedgerStore.reset()
{:ok, delivered} = Agent.start_link(fn -> [] end)

state =
  Payments.init(%{
    name: :payments,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    trusted_sources: [],
    targets: ["llm_proxy", "audit_log"],
    store_mod: LedgerStore,
    auto_tick: false,
    now_fn: fn -> ~U[2026-07-22 12:00:00Z] end,
    deliver_fn: fn target, from, content ->
      Agent.update(delivered, &[{target, from, Jason.decode!(content)} | &1])
      :ok
    end
  })

s = %{
  beneficiary: "budget:abc",
  amount_usd: Decimal.new("5.00"),
  method: "usdc_base",
  ref: "0xTX:3",
  idempotency_key: "base:0xTX:3",
  namespace: "llm_quota"
}

{n, state} = Payments.settle([s], state)
msgs = Agent.get(delivered, &Enum.reverse(&1))

Check.check(f, "one settlement settled", n == 1)
Check.check(f, "delivered to BOTH allowlisted targets, stamped with object name",
  Enum.map(msgs, fn {t, from, _} -> {t, from} end) ==
    [{"llm_proxy", :payments}, {"audit_log", :payments}])

{_, _, payload} = hd(msgs)
Check.check(f, "payload shape",
  payload["action"] == "payment_confirmed" and payload["beneficiary"] == "budget:abc" and
    payload["amount_usd"] == "5.00" and payload["method"] == "usdc_base" and
    payload["namespace"] == "llm_quota" and payload["at"] == "2026-07-22T12:00:00Z")

# idempotency: same key again ⇒ nothing
Agent.update(delivered, fn _ -> [] end)
{n2, state} = Payments.settle([s], state)
Check.check(f, "duplicate idempotency_key ⇒ zero settled, zero delivered",
  n2 == 0 and Agent.get(delivered, & &1) == [])
Check.check(f, "ledger recorded exactly once", length(LedgerStore.rows()) == 1)

# FAIL CLOSED: store down ⇒ nothing settles, nothing delivered
LedgerStore.down!(true)
s2 = %{s | idempotency_key: "base:0xTX:4", ref: "0xTX:4"}
{n3, state} = Payments.settle([s2], state)
Check.check(f, "store down ⇒ fail closed (0 settled, 0 delivered)",
  n3 == 0 and Agent.get(delivered, & &1) == [])

# recovery: store back up ⇒ the SAME settlement goes through
LedgerStore.down!(false)
{n4, _state} = Payments.settle([s2], state)
Check.check(f, "after store recovery the held settlement settles", n4 == 1)

# no store (dev): memory dedup still works
ok_dev =
  Payments.init(%{
    name: :p2,
    xpub: "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt",
    targets: ["t"],
    trusted_sources: [],
    store_mod: nil,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end
  })

{d1, ok_dev} = Payments.settle([s], ok_dev)
{d2, _} = Payments.settle([s], ok_dev)
Check.check(f, "dev mode (no store): settles once, memory-dedups the repeat",
  d1 == 1 and d2 == 0)

Check.finish(f)
