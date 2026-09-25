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
    * Headline summaries cover all records independently of bounded detail
      tables. Failed/unsupported reads are unavailable, never fabricated zeros.
      The page is absent only when the store exports none of its callbacks.
  """

  @doc "Schema-1 dashboard extension. Opts: `store_mod:` (required)."
  def dashboard_extension(opts) do
    store = Keyword.fetch!(opts, :store_mod)

    now = System.os_time(:second)
    topups = list(store, :list_issued_authorizations, 25, &topup_row(&1, now))
    inflows = list(store, :list_unrecognised_inflows, 25, &inflow_row/1)
    deposits = list(store, :list_deposit_balances, 50, &deposit_row/1)

    authorizations =
      summary(store, :issued_authorizations_summary, [now], [:issued, :live, :consumed])

    inflow_summary = summary(store, :unrecognised_inflows_summary, [], [:count])

    deposit_summary =
      summary(store, :deposit_balances_summary, [], [:addresses, :with_activity], [:unswept_usd])

    pages =
      [
        if(
          topups != :absent or inflows != :absent or authorizations != :absent or
            inflow_summary != :absent,
          do: page(topups, inflows, authorizations, inflow_summary)
        ),
        if(deposits != :absent or deposit_summary != :absent,
          do: deposits_page(deposits, deposit_summary)
        )
      ]
      |> Enum.reject(&is_nil/1)

    if pages == [], do: %{}, else: %{"dashboard_pages" => pages}
  end

  defp read(store, fun, args) do
    if Code.ensure_loaded?(store) and function_exported?(store, fun, length(args)),
      do: apply(store, fun, args),
      else: :absent
  rescue
    _ -> :unavailable
  catch
    _, _ -> :unavailable
  end

  defp list(store, fun, limit, project) do
    case read(store, fun, [limit]) do
      {:ok, rows} when is_list(rows) -> Enum.map(Enum.take(rows, limit), project)
      :absent -> :absent
      _ -> :unavailable
    end
  rescue
    _ -> :unavailable
  catch
    _, _ -> :unavailable
  end

  defp summary(store, fun, args, counts, amounts \\ []) do
    case read(store, fun, args) do
      {:ok, row} when is_map(row) ->
        if Enum.all?(counts, &(is_integer(row[&1]) and row[&1] >= 0)) and
             Enum.all?(amounts, &match?(%Decimal{coef: c} when is_integer(c), row[&1])),
           do: row,
           else: :unavailable

      :absent ->
        :absent

      _ ->
        :unavailable
    end
  end

  defp value(row, key) when is_map(row), do: Map.fetch!(row, key)
  defp value(_, _), do: "unavailable"
  defp rows_or_empty(rows) when is_list(rows), do: rows
  defp rows_or_empty(_), do: []

  defp detail_meta(rows, limit) when is_list(rows),
    do: "up to #{limit} recent rows; totals cover all records"

  defp detail_meta(_, _), do: "unavailable — detail read failed or unsupported"

  defp page(topups, inflows, authorizations, inflow_summary) do
    %{
      "schema" => 1,
      "id" => "topups",
      "label" => "Top-ups",
      "icon" => "hero-credit-card",
      # Sidebar section (dashboard ≥ sidebar-groups): the page's natural
      # home, declared by the producer so a new host needs no stamping.
      # This declaration WINS — host stamps are fill-nil-only, for pages
      # whose packages don't declare yet.
      "group" => "Money",
      "sections" => [
        %{
          "type" => "metrics",
          "title" => "Authorization lane",
          "columns" => 4,
          "items" => [
            %{"label" => "issued", "value" => value(authorizations, :issued)},
            %{"label" => "live", "value" => value(authorizations, :live)},
            %{
              "label" => "consumed",
              "value" => value(authorizations, :consumed)
            },
            %{
              "label" => "unrecognised inflows",
              "value" => value(inflow_summary, :count)
            }
          ]
        },
        %{
          "type" => "table",
          "title" => "Recent top-ups",
          "meta" => detail_meta(topups, 25),
          "columns" => [
            %{"key" => "order_ref", "label" => "Order"},
            %{"key" => "beneficiary", "label" => "Beneficiary"},
            %{"key" => "amount_usd", "label" => "USDC", "align" => "right"},
            %{"key" => "status", "label" => "Status"},
            %{"key" => "tx_ref", "label" => "Tx"},
            %{"key" => "created_at", "label" => "Issued"},
            # The signature's own on-chain deadline (valid_before). Without
            # it, a "live" row whose 15-min LINK died reads as a mystery —
            # the row is live because the SIGNATURE window (1h) still runs
            # (Albert, first live read of the page, 2026-07-27).
            %{"key" => "expires", "label" => "Sig. valid until"}
          ],
          "rows" => rows_or_empty(topups)
        },
        %{
          "type" => "table",
          "title" => "Unrecognised treasury inflows (§4.4 — never credited)",
          "meta" => detail_meta(inflows, 25),
          "columns" => [
            %{"key" => "chain", "label" => "Chain"},
            %{"key" => "tx_hash", "label" => "Tx"},
            %{"key" => "from_addr", "label" => "From"},
            %{"key" => "amount_usd", "label" => "USDC", "align" => "right"},
            %{"key" => "reason", "label" => "Reason"},
            %{"key" => "seen_at", "label" => "Seen"}
          ],
          "rows" => rows_or_empty(inflows)
        }
      ]
    }
  end

  # The collection view (plan 3, C1): what sits on deposit addresses,
  # UNCOLLECTED, per the store's own ledger. Honest labeling is the design:
  # this is settled-minus-swept from the database — the chain's balance is
  # read by the sweep executor at signing time, never by a page that
  # refreshes every second.
  defp deposits_page(rows, summary) do
    unswept = if is_map(summary), do: money(summary.unswept_usd), else: "unavailable"

    %{
      "schema" => 1,
      "id" => "deposits",
      "label" => "Deposits",
      "icon" => "hero-wallet",
      "group" => "Money",
      "meta" => "ledger estimate — the sweep executor reads chain truth",
      "sections" => [
        %{
          "type" => "metrics",
          "title" => "Entry-B deposit addresses",
          "columns" => 4,
          "items" => [
            %{"label" => "addresses", "value" => value(summary, :addresses)},
            %{"label" => "unswept (est. USDC)", "value" => unswept},
            %{
              "label" => "with activity",
              "value" => value(summary, :with_activity)
            },
            %{"label" => "sweep lane", "value" => "manual — operator /payments sweep"}
          ]
        },
        %{
          "type" => "table",
          "title" => "Per address (received − swept = est. uncollected)",
          "meta" => detail_meta(rows, 50),
          "columns" => [
            %{"key" => "beneficiary", "label" => "Beneficiary"},
            %{"key" => "address", "label" => "Address"},
            %{"key" => "received_usd", "label" => "Received", "align" => "right"},
            %{"key" => "swept_usd", "label" => "Swept", "align" => "right"},
            %{"key" => "unswept_usd", "label" => "Uncollected (est.)", "align" => "right"},
            %{"key" => "last_at", "label" => "Last activity"}
          ],
          "rows" => rows_or_empty(rows)
        }
      ]
    }
  end

  defp deposit_row(row) do
    received = field(row, :received_usd)
    swept = field(row, :swept_usd)

    %{
      "beneficiary" => field(row, :beneficiary),
      "address" => row |> field(:address) |> shorten(14),
      "received_usd" => money(received),
      "swept_usd" => money(swept || 0),
      "unswept_usd" => money(field(row, :unswept_usd) || money_sub(received, swept)),
      "last_at" => stamp(field(row, :last_at))
    }
  end

  defp money_sub(nil, _), do: nil
  defp money_sub(a, nil), do: dec(a)
  defp money_sub(a, b), do: Decimal.sub(dec(a), dec(b))

  defp dec(%Decimal{} = d), do: d

  defp dec(other), do: Decimal.new(other)

  defp topup_row(row, now) do
    true = is_integer(field(row, :valid_before))

    %{
      "order_ref" => row |> field(:order_ref) |> shorten(12),
      "beneficiary" => field(row, :beneficiary),
      "amount_usd" => money(field(row, :amount_usd)),
      "status" => status(row, now),
      "tx_ref" => row |> field(:tx_ref) |> shorten(14),
      "created_at" => stamp(field(row, :created_at)),
      "expires" => unix_stamp(field(row, :valid_before))
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

  defp money(value) do
    %Decimal{coef: coefficient} = amount = dec(value)
    true = is_integer(coefficient)
    amount |> Decimal.round(2) |> Decimal.to_string(:normal)
  end

  defp stamp(%DateTime{} = dt), do: dt |> DateTime.truncate(:second) |> DateTime.to_iso8601()
  defp stamp(other), do: other && to_string(other)

  defp unix_stamp(seconds) when is_integer(seconds) do
    case DateTime.from_unix(seconds) do
      {:ok, dt} -> DateTime.to_iso8601(dt)
      _ -> nil
    end
  end

  defp unix_stamp(_), do: nil
end
