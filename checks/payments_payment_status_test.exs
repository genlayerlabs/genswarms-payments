# Standalone — NO Postgres, NO network.  mix run checks/payments_payment_status_test.exs
#
# payment_status is README's reconciliation path but used to fail OPEN:
# during degraded_boot it answered ok:true/address:null/payments:[], and a
# list_payments error/raise/exit collapsed to ok:true/[] — indistinguishable
# from "this beneficiary genuinely has zero payments". Pins the fix:
#   - degraded_boot ⇒ {ok:false, error:"degraded_boot"}
#   - list_payments exported-but-errors/raises/exits ⇒
#     {ok:false, error:"store_unavailable"}
#   - not-exported/nil store (memory mode, truthfully has no rows) ⇒ current
#     shape, ok:true, plus a `durable: false` field
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub = "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

# ── degraded_boot ⇒ refuse, don't answer ok:true with a fabricated empty list
defmodule DegradedStatusStore do
  def put_address_binding(_), do: :ok
  def list_address_bindings, do: {:error, :db_down}
  def payment_seen?(_), do: {:ok, false}
  def record_payment(_row), do: :ok
  def get_last_scanned_block(_c), do: {:ok, nil}
  def put_last_scanned_block(_c, _n), do: :ok
  def list_payments(_ben), do: {:ok, [%{amount_usd: Decimal.new("1"), method: "x", ref: "r", at: ~U[2026-01-01 00:00:00Z]}]}
end

state_degraded =
  Payments.init!(%{
    xpub: xpub,
    trusted_sources: ["ingress"],
    targets: ["t"],
    store_mod: DegradedStatusStore,
    auto_tick: false
  })

Check.check(f, "sanity: this store really does degrade boot", state_degraded.degraded_boot == true)

{:reply, dj, _} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "payment_status", beneficiary: "budget:abc"}), state_degraded)
degraded_reply = Jason.decode!(dj)

Check.check(f, "payment_status during degraded_boot refuses truthfully: ok:false, error:degraded_boot",
  degraded_reply["ok"] == false and degraded_reply["error"] == "degraded_boot")

# ── list_payments EXPORTED but errors/raises/exits ⇒ store_unavailable, not
# a fabricated ok:true/[]
defmodule ErroringListPaymentsStore do
  def put_address_binding(_), do: :ok
  def list_address_bindings, do: {:ok, []}
  def payment_seen?(_), do: {:ok, false}
  def record_payment(_row), do: :ok
  def get_last_scanned_block(_c), do: {:ok, nil}
  def put_last_scanned_block(_c, _n), do: :ok
  def list_payments(_ben), do: :persistent_term.get({__MODULE__, :behavior}, {:error, :db_down})
end

healthy_status_config = %{
  xpub: xpub,
  trusted_sources: ["ingress"],
  targets: ["t"],
  auto_tick: false,
  store_mod: nil
}

:persistent_term.put({ErroringListPaymentsStore, :behavior}, {:error, :db_down})

state_err =
  Payments.init!(%{healthy_status_config | store_mod: ErroringListPaymentsStore})

{:reply, ej, _} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "payment_status", beneficiary: "budget:abc"}), state_err)
err_reply = Jason.decode!(ej)

Check.check(f, "list_payments returning {:error,_} ⇒ ok:false, error:store_unavailable (not ok:true/[])",
  err_reply["ok"] == false and err_reply["error"] == "store_unavailable")

:persistent_term.put({ErroringListPaymentsStore, :behavior}, fn -> raise "boom" end)

defmodule RaisingListPaymentsStore do
  def put_address_binding(_), do: :ok
  def list_address_bindings, do: {:ok, []}
  def payment_seen?(_), do: {:ok, false}
  def record_payment(_row), do: :ok
  def get_last_scanned_block(_c), do: {:ok, nil}
  def put_last_scanned_block(_c, _n), do: :ok
  def list_payments(_ben), do: raise("boom")
end

state_raise = Payments.init!(%{healthy_status_config | store_mod: RaisingListPaymentsStore})

{:reply, rj, _} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "payment_status", beneficiary: "budget:abc"}), state_raise)
raise_reply = Jason.decode!(rj)

Check.check(f, "list_payments raising ⇒ ok:false, error:store_unavailable (not a crash, not ok:true/[])",
  raise_reply["ok"] == false and raise_reply["error"] == "store_unavailable")

defmodule ExitingListPaymentsStore do
  def put_address_binding(_), do: :ok
  def list_address_bindings, do: {:ok, []}
  def payment_seen?(_), do: {:ok, false}
  def record_payment(_row), do: :ok
  def get_last_scanned_block(_c), do: {:ok, nil}
  def put_last_scanned_block(_c, _n), do: :ok
  def list_payments(_ben), do: exit(:timeout)
end

state_exit = Payments.init!(%{healthy_status_config | store_mod: ExitingListPaymentsStore})

{:reply, xj, _} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "payment_status", beneficiary: "budget:abc"}), state_exit)
exit_reply = Jason.decode!(xj)

Check.check(f, "list_payments exiting ⇒ ok:false, error:store_unavailable (no crash)",
  exit_reply["ok"] == false and exit_reply["error"] == "store_unavailable")

# ── not-exported / nil store (memory mode) ⇒ current shape stays, PLUS a
# truthful durable:false field (this beneficiary genuinely has no rows —
# memory mode never had any to lose)
state_mem =
  Payments.init!(%{
    xpub: xpub,
    trusted_sources: ["ingress"],
    targets: ["t"],
    store_mod: nil,
    auto_tick: false
  })

{:reply, mj, _} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "payment_status", beneficiary: "budget:abc"}), state_mem)
mem_reply = Jason.decode!(mj)

Check.check(f, "nil store: payment_status still answers ok:true with empty payments",
  mem_reply["ok"] == true and mem_reply["payments"] == [])
Check.check(f, "nil store: payment_status is truthfully marked durable:false",
  mem_reply["durable"] == false)

# ── a store that answers successfully ⇒ ok:true, durable:true, rows present
defmodule HealthyListPaymentsStore do
  def put_address_binding(_), do: :ok
  def list_address_bindings, do: {:ok, []}
  def payment_seen?(_), do: {:ok, false}
  def record_payment(_row), do: :ok
  def get_last_scanned_block(_c), do: {:ok, nil}
  def put_last_scanned_block(_c, _n), do: :ok

  def list_payments(_ben),
    do: {:ok, [%{amount_usd: Decimal.new("5"), method: "usdc_base", ref: "0xT:0", at: ~U[2026-07-22 12:00:00Z]}]}
end

state_ok = Payments.init!(%{healthy_status_config | store_mod: HealthyListPaymentsStore})

{:reply, oj, _} =
  Payments.handle_message("ingress",
    Jason.encode!(%{action: "payment_status", beneficiary: "budget:abc"}), state_ok)
ok_reply = Jason.decode!(oj)

Check.check(f, "healthy store: payment_status ok:true, durable:true, payments present",
  ok_reply["ok"] == true and ok_reply["durable"] == true and length(ok_reply["payments"]) == 1)

Check.finish(f)
