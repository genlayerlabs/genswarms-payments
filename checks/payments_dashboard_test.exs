# Standalone — NO Postgres, NO network.  mix run checks/payments_dashboard_test.exs
#
# Genswarms.Payments.Dashboard — the package's schema-1 page, read entirely
# through the host's Store adapter. Pins:
#   1. a store exporting both list reads yields ONE page with the three
#      sections, rows projected/sanitized (refs shortened, money 2-dp);
#   2. the inflows table is PRESENT when empty — "none seen" is an answer;
#   3. status derivation: consumed > expired > live;
#   4. a store exporting neither read (or erroring) yields %{} — no page,
#      never a zeroed lie;
#   5. a raising store yields %{}, never a crash into the dashboard feed.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
check = fn label, ok -> Check.check(f, label, ok) end

alias Genswarms.Payments.Dashboard

now = System.os_time(:second)

defmodule DashStore do
  def seed(topups, inflows) do
    Process.put(:dash_topups, topups)
    Process.put(:dash_inflows, inflows)
  end

  def list_issued_authorizations(_limit), do: {:ok, Process.get(:dash_topups, [])}
  def list_unrecognised_inflows(_limit), do: {:ok, Process.get(:dash_inflows, [])}
end

defmodule DashStoreBare do
  # exports NEITHER list read
  def unrelated, do: :ok
end

defmodule DashStoreRaising do
  def list_issued_authorizations(_), do: raise("db down")
  def list_unrecognised_inflows(_), do: raise("db down")
end

DashStore.seed(
  [
    %{
      order_ref: String.duplicate("a", 64),
      beneficiary: "user:42",
      amount_usd: "1.0",
      valid_before: now + 900,
      created_at: DateTime.utc_now(),
      consumed_at: nil,
      tx_ref: "0x" <> String.duplicate("b", 64)
    },
    %{
      order_ref: "consumed-ref",
      beneficiary: "user:42",
      amount_usd: "2.0",
      valid_before: now + 900,
      created_at: DateTime.utc_now(),
      consumed_at: DateTime.utc_now()
    },
    %{
      order_ref: "expired-ref",
      beneficiary: "user:43",
      amount_usd: "3.0",
      valid_before: now - 60,
      created_at: DateTime.utc_now(),
      consumed_at: nil
    }
  ],
  []
)

metric = fn section, label ->
  section["items"] |> Enum.find(%{}, &(&1["label"] == label)) |> Map.get("value")
end

ext = Dashboard.dashboard_extension(store_mod: DashStore)

case ext do
  %{"dashboard_pages" => [page]} ->
    check.("one page under the schema-1 contract", page["schema"] == 1 and page["id"] == "topups")
    [metrics, topups_table, inflows_table] = page["sections"]

    check.(
      "metrics count live/consumed",
      metric.(metrics, "live") == 1 and metric.(metrics, "consumed") == 1
    )

    [row1 | _] = topups_table["rows"]

    check.(
      "refs and tx are shortened, money is 2-dp",
      String.ends_with?(row1["order_ref"], "…") and String.ends_with?(row1["tx_ref"], "…") and
        row1["amount_usd"] == "1.00"
    )

    check.(
      "each row shows the signature's own deadline (valid_before as ISO time)",
      is_binary(row1["expires"]) and String.contains?(row1["expires"], "T") and
        Enum.any?(topups_table["columns"], &(&1["key"] == "expires"))
    )

    check.(
      "status derivation: live / consumed / expired",
      Enum.map(topups_table["rows"], & &1["status"]) == ["live", "consumed", "expired"]
    )

    check.(
      "the empty inflows table is PRESENT — 'none seen' is an answer",
      inflows_table["rows"] == [] and is_binary(inflows_table["meta"])
    )

  other ->
    check.("one page under the schema-1 contract (got #{inspect(other)})", false)
end

check.(
  "a store without the list reads yields no page",
  Dashboard.dashboard_extension(store_mod: DashStoreBare) == %{}
)

check.(
  "a raising store yields no page, never a crash",
  Dashboard.dashboard_extension(store_mod: DashStoreRaising) == %{}
)

Check.finish(f)
IO.puts("PAYMENTS_DASHBOARD: ALL PASS")
