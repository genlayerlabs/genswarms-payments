defmodule Genswarms.Payments.Dashboard do
  @moduledoc """
  The package's dashboard page (schema-1 extension contract): the
  authorization lane's top-ups and the §4.4 unrecognised-inflow audit trail,
  read entirely through the host's `Genswarms.Payments.Store` adapter.

  A host opts in with one line, same as every other package page:

      probed_extension(Genswarms.Payments.Dashboard, store_mod: MyHost.Store)

  Design points, learned from the sibling hosts' pages:

    * NO compile dependency in either direction — the host probes
      `function_exported?/3`; a host without the payments lane never knows
      this module exists.
    * The unrecognised-inflows table is ALWAYS present when the page is:
      an empty audit trail is the answer "none seen", never a hidden
      section. Unauthorized money arriving at the treasury is exactly the
      thing an operator must be able to see is NOT happening.
    * The page is absent (`%{}`) when the store exports neither list read or
      both answer errors — no durable data must mean no page, never a
      zeroed lie.
  """

  @doc "Schema-1 dashboard extension. Opts: `store_mod:` (required)."
  def dashboard_extension(opts) do
    store = Keyword.fetch!(opts, :store_mod)

    topups = list(store, :list_issued_authorizations, 25)
    inflows = list(store, :list_unrecognised_inflows, 25)

    if topups == :absent and inflows == :absent do
      %{}
    else
      %{"dashboard_pages" => [page(rows_or_empty(topups), rows_or_empty(inflows))]}
    end
  rescue
    _ -> %{}
  catch
    _, _ -> %{}
  end

  defp list(store, fun, limit) do
    if Code.ensure_loaded?(store) and function_exported?(store, fun, 1) do
      case apply(store, fun, [limit]) do
        {:ok, rows} when is_list(rows) -> rows
        _ -> :absent
      end
    else
      :absent
    end
  end

  defp rows_or_empty(:absent), do: []
  defp rows_or_empty(rows), do: rows

  defp page(topups, inflows) do
    now = System.os_time(:second)

    %{
      "schema" => 1,
      "id" => "topups",
      "label" => "Top-ups",
      "icon" => "hero-credit-card",
      "sections" => [
        %{
          "type" => "metrics",
          "title" => "Authorization lane",
          "columns" => 4,
          "items" => [
            %{"label" => "issued (recent)", "value" => length(topups)},
            %{"label" => "live", "value" => Enum.count(topups, &(status(&1, now) == "live"))},
            %{
              "label" => "consumed",
              "value" => Enum.count(topups, &(status(&1, now) == "consumed"))
            },
            %{
              "label" => "unrecognised inflows",
              "value" => length(inflows)
            }
          ]
        },
        %{
          "type" => "table",
          "title" => "Recent top-ups",
          "columns" => [
            %{"key" => "order_ref", "label" => "Order"},
            %{"key" => "beneficiary", "label" => "Beneficiary"},
            %{"key" => "amount_usd", "label" => "USDC", "align" => "right"},
            %{"key" => "status", "label" => "Status"},
            %{"key" => "tx_ref", "label" => "Tx"},
            %{"key" => "created_at", "label" => "Issued"}
          ],
          "rows" => Enum.map(topups, &topup_row(&1, now))
        },
        %{
          "type" => "table",
          "title" => "Unrecognised treasury inflows (§4.4 — never credited)",
          "meta" => "an empty table means none seen — the healthy state",
          "columns" => [
            %{"key" => "chain", "label" => "Chain"},
            %{"key" => "tx_hash", "label" => "Tx"},
            %{"key" => "from_addr", "label" => "From"},
            %{"key" => "amount_usd", "label" => "USDC", "align" => "right"},
            %{"key" => "reason", "label" => "Reason"},
            %{"key" => "seen_at", "label" => "Seen"}
          ],
          "rows" => Enum.map(inflows, &inflow_row/1)
        }
      ]
    }
  end

  defp topup_row(row, now) do
    %{
      "order_ref" => row |> field(:order_ref) |> shorten(12),
      "beneficiary" => field(row, :beneficiary),
      "amount_usd" => money(field(row, :amount_usd)),
      "status" => status(row, now),
      "tx_ref" => row |> field(:tx_ref) |> shorten(14),
      "created_at" => stamp(field(row, :created_at))
    }
  end

  defp inflow_row(row) do
    %{
      "chain" => field(row, :chain),
      "tx_hash" => row |> field(:tx_hash) |> shorten(14),
      "from_addr" => row |> field(:from_addr) |> shorten(12),
      "amount_usd" => money(field(row, :amount_usd)),
      "reason" => field(row, :reason),
      "seen_at" => stamp(field(row, :seen_at))
    }
  end

  # consumed > expired > live: consumption is a fact; expiry is the clock.
  defp status(row, now) do
    cond do
      not is_nil(field(row, :consumed_at)) -> "consumed"
      is_integer(field(row, :valid_before)) and field(row, :valid_before) <= now -> "expired"
      true -> "live"
    end
  end

  defp field(row, key), do: Map.get(row, key) || Map.get(row, to_string(key))

  defp shorten(nil, _n), do: nil
  defp shorten(value, n) when is_binary(value) and byte_size(value) > n,
    do: String.slice(value, 0, n) <> "…"

  defp shorten(value, _n), do: value

  defp money(nil), do: nil

  defp money(value) do
    value |> Decimal.new() |> Decimal.round(2) |> Decimal.to_string(:normal)
  rescue
    _ -> to_string(value)
  end

  defp stamp(%DateTime{} = dt), do: dt |> DateTime.truncate(:second) |> DateTime.to_iso8601()
  defp stamp(other), do: other && to_string(other)
end
