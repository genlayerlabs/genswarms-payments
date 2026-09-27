ExUnit.start()

defmodule ReliabilityPaymentStore do
  def topups do
    List.duplicate(%{valid_before: 1, consumed_at: nil, amount_usd: "1"}, 25) ++
      [%{valid_before: System.os_time(:second) + 3600, consumed_at: nil, amount_usd: "1"}]
  end

  def deposits, do: List.duplicate(%{received_usd: "1", swept_usd: "0.25", last_at: nil}, 51)
  def list_issued_authorizations(n), do: read(:topups, {:ok, Enum.take(topups(), n)})
  def list_unrecognised_inflows(_), do: read(:inflows, {:ok, []})
  def list_deposit_balances(n), do: read(:deposits, {:ok, Enum.take(deposits(), n)})

  def issued_authorizations_summary(now) do
    read(
      :topup_summary,
      {:ok,
       %{
         issued: length(topups()),
         consumed: 0,
         live: Enum.count(topups(), &(&1.valid_before > now))
       }}
    )
  end

  def unrecognised_inflows_summary, do: read(:inflow_summary, {:ok, %{count: 26}})

  def deposit_balances_summary do
    amount =
      Enum.reduce(deposits(), Decimal.new(0), fn r, sum ->
        Decimal.add(sum, Decimal.sub(Decimal.new(r.received_usd), Decimal.new(r.swept_usd)))
      end)

    read(:deposit_summary, {:ok, %{addresses: 51, with_activity: 0, unswept_usd: amount}})
  end

  defp read(key, default) do
    case Process.get(key, default) do
      :raise -> raise "database unavailable"
      result -> result
    end
  end
end

defmodule ReliabilityPaymentLegacyStore do
  defdelegate list_issued_authorizations(n), to: ReliabilityPaymentStore
  defdelegate list_unrecognised_inflows(n), to: ReliabilityPaymentStore
  defdelegate list_deposit_balances(n), to: ReliabilityPaymentStore
end

defmodule PaymentReliabilityTest do
  use ExUnit.Case, async: true

  defp pages(store \\ ReliabilityPaymentStore),
    do: Genswarms.Payments.Dashboard.dashboard_extension(store_mod: store)["dashboard_pages"]

  defp page(pages, id), do: Enum.find(pages, &(&1["id"] == id))

  defp metric(pages, id, label) do
    page(pages, id)["sections"]
    |> Enum.flat_map(&Map.get(&1, "items", []))
    |> Enum.find(
      &(&1["label"] == label or (label == "issued" and &1["label"] == "issued (recent)"))
    )
    |> Map.fetch!("value")
  end

  test "complete money headlines exceed bounded table slices" do
    p = pages()
    assert metric(p, "topups", "issued") == 26
    assert metric(p, "topups", "live") == 1
    assert metric(p, "topups", "unrecognised inflows") == 26
    assert metric(p, "deposits", "addresses") == 51
    assert metric(p, "deposits", "unswept (est. USDC)") == "38.25"
    assert length(Enum.at(page(p, "topups")["sections"], 1)["rows"]) == 25
    assert length(Enum.at(page(p, "deposits")["sections"], 1)["rows"]) == 50
  end

  test "summary failure and malformed results preserve healthy sections" do
    for bad <- [{:error, :unavailable}, {:ok, %{}}, {:ok, %{issued: nil}}, :raise] do
      Process.put(:topup_summary, bad)
      p = pages()
      assert metric(p, "topups", "issued") == "unavailable"
      assert metric(p, "topups", "unrecognised inflows") == 26
      assert metric(p, "deposits", "unswept (est. USDC)") == "38.25"
    end
  end

  test "failed or malformed detail reads show unavailable and retain aggregate evidence" do
    for bad <- [
          {:error, :unavailable},
          :raise,
          {:ok, [nil]},
          {:ok, [%{}]},
          {:ok, [%{valid_before: 1, amount_usd: "bad"}]}
        ] do
      Process.put(:topups, bad)
      p = pages()
      assert metric(p, "topups", "live") == 1
      table = Enum.at(page(p, "topups")["sections"], 1)
      assert table["meta"] =~ "unavailable"
      assert table["rows"] == []
      assert metric(p, "topups", "unrecognised inflows") == 26
    end
  end

  test "old stores keep details but never fabricate complete totals" do
    p = pages(ReliabilityPaymentLegacyStore)
    assert metric(p, "topups", "issued") == "unavailable"
    assert metric(p, "deposits", "unswept (est. USDC)") == "unavailable"
  end

  test "older live authorization remains counted after 25 expired rows" do
    assert metric(pages(), "topups", "live") == 1
  end

  test "51 deposits sum exact amounts before formatting" do
    assert metric(pages(), "deposits", "unswept (est. USDC)") == "38.25"
  end
end
