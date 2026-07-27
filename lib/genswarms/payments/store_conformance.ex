defmodule Genswarms.Payments.StoreConformance do
  @moduledoc """
  Executable conformance suite for a host's `Genswarms.Payments.Store`
  implementation — run it against the REAL store, connected to a REAL
  (throwaway) database, as part of the host's own gates.

  ## Why this exists

  The Store contract is ~18 functions whose SEMANTICS carry the money
  guarantees; the typespecs alone cannot. Both real defects this lane has
  shipped were semantic, not shape:

    * a test double that answered the same row for ANY argument let a
      completely broken acknowledgement path go green — so this suite's
      lookups always probe the MISS case next to the hit;
    * `record_issued_authorization/1` echoing the RETRY's row instead of the
      row ON RECORD would ack a nonce that was never registered and bury the
      user's payment as an unrecognised inflow — so the duplicate case pins
      which nonce comes back.

  A host passing this suite has proven the behaviors `Genswarms.Payments`
  (the hub) and `Genswarms.Payments.TopupAck` (the presenter) actually rely
  on. It does NOT replace the host's own store tests: the host owns its
  schema, its outbox sequencing, and everything else the contract leaves
  open.

  ## What it needs

  A store module whose contract functions are exported and pointed at a
  database that may be freely written to. Rows are minted under unique keys,
  so a dirty database does not confuse it — but run it against a throwaway
  instance anyway; it writes.

  ## Usage (host gate)

      Genswarms.Payments.StoreConformance.run!(MyHost.Store)

  Raises on the first violation with a labeled message; prints one `ok` line
  per property otherwise. Sections whose optional callbacks the store does
  not export are reported as `skip` — a host adopting the authorization lane
  or the TopupAck presenter must not see any skips in those sections.
  """

  @doc "Run every applicable section against `store`. Raises on first violation."
  def run!(store) do
    u = System.unique_integer([:positive])
    now = System.os_time(:second)

    authorization_section(store, u, now)
    settlement_section(store, u, now)
    cursor_section(store, u)
    inflow_section(store, u)
    :ok
  end

  # ── issued authorizations ───────────────────────────────────────────────────

  defp authorization_section(store, u, now) do
    if exported?(store, :record_issued_authorization, 1) do
      nonce = nonce_hex()
      order_ref = "conformance-ref-#{u}"
      beneficiary = "conformance-user-#{u}"

      row = %{
        nonce_hex: nonce,
        order_ref: order_ref,
        beneficiary: beneficiary,
        amount_usd: Decimal.new("1.50"),
        namespace: "llm_quota",
        valid_before: now + 900,
        issued_at: now
      }

      assert!(store.record_issued_authorization(row) == :ok, "a fresh issuance records as :ok")

      # The duplicate MUST echo the row ON RECORD — a retry arrives with a
      # freshly minted nonce, and echoing the retry acks a nonce the watcher
      # will never match (see the callback doc: this is a money bug, not a
      # style point).
      retry = %{row | nonce_hex: nonce_hex()}

      case store.record_issued_authorization(retry) do
        {:ok, :duplicate, stored} ->
          assert!(
            stored_field(stored, :nonce_hex) == nonce,
            "the duplicate echoes the nonce ON RECORD, never the retry's " <>
              "(got #{inspect(stored_field(stored, :nonce_hex))})"
          )

        other ->
          assert!(false, "a replayed order_ref answers {:ok, :duplicate, row}, got #{inspect(other)}")
      end

      ok("record_issued_authorization: idempotent by order_ref, echoing the row of record")

      if exported?(store, :issued_authorization, 1) do
        assert!(
          match?(%{}, store.issued_authorization(nonce)),
          "issued_authorization answers the exact nonce"
        )

        assert!(
          store.issued_authorization(nonce_hex()) == nil,
          "issued_authorization answers nil for a nonce never issued — " <>
            "a store that answers ANY argument is how a broken ack goes green"
        )

        ok("issued_authorization: exact hit + honest miss")
      else
        skip("issued_authorization/1 not exported")
      end

      if exported?(store, :authorization_by_order_ref, 1) do
        found = store.authorization_by_order_ref(order_ref)

        assert!(
          is_map(found) and stored_field(found, :beneficiary) == beneficiary and
            not is_nil(stored_field(found, :amount_usd)),
          "authorization_by_order_ref returns the row with beneficiary + amount_usd"
        )

        assert!(
          store.authorization_by_order_ref("conformance-miss-#{u}") == nil,
          "authorization_by_order_ref answers nil for an unknown ref"
        )

        assert!(
          store.authorization_by_order_ref("") == nil,
          "authorization_by_order_ref never raises on junk input (\"\" -> nil)"
        )

        ok("authorization_by_order_ref: exact hit + honest miss + never raises")
      else
        skip("authorization_by_order_ref/1 not exported (REQUIRED for TopupAck)")
      end

      live_and_consume(store, nonce, u, now)

      if exported?(store, :list_issued_authorizations, 1) do
        {:ok, listed} = store.list_issued_authorizations(100)

        assert!(
          Enum.any?(listed, &(stored_field(&1, :order_ref) == order_ref)),
          "list_issued_authorizations includes a just-issued row"
        )

        ok("list_issued_authorizations: the dashboard projection sees issued rows")
      else
        skip("list_issued_authorizations/1 not exported (dashboard page absent)")
      end
    else
      skip("record_issued_authorization/1 not exported — authorization lane not adopted")
    end
  end

  defp live_and_consume(store, nonce, u, now) do
    if exported?(store, :live_authorization_nonces, 1) and
         exported?(store, :mark_authorization_consumed, 1) do
      live = store.live_authorization_nonces(now)
      assert!(is_list(live) and nonce in live, "a fresh unconsumed nonce is live")

      # An issuance already past its window must never be watched: the token
      # contract will reject it on chain, so watching it is pure noise.
      expired_nonce = nonce_hex()

      _ =
        store.record_issued_authorization(%{
          nonce_hex: expired_nonce,
          order_ref: "conformance-expired-#{u}",
          beneficiary: "conformance-user-#{u}",
          amount_usd: Decimal.new("1.00"),
          namespace: "llm_quota",
          valid_before: now - 60,
          issued_at: now - 900
        })

      assert!(
        expired_nonce not in store.live_authorization_nonces(now),
        "a nonce past valid_before is not live"
      )

      assert!(store.mark_authorization_consumed(nonce) == :ok, "consume answers :ok")

      assert!(
        nonce not in store.live_authorization_nonces(now),
        "a consumed nonce is not live"
      )

      assert!(
        store.mark_authorization_consumed(nonce) == :ok,
        "consume is idempotent (:ok on the second call)"
      )

      ok("live_authorization_nonces + mark_authorization_consumed: window, consume, idempotence")
    else
      skip("live/consume callbacks not exported")
    end
  end

  # ── settlements + the joins the presenter relies on ─────────────────────────

  defp settlement_section(store, u, now) do
    if exported?(store, :record_payment, 1) and exported?(store, :payment_seen?, 1) do
      nonce = nonce_hex()
      order_ref = "conformance-settle-ref-#{u}"
      beneficiary = "conformance-settle-user-#{u}"
      method = "usdc_base_sepolia"
      ref = "0xconformancetx#{u}:1"
      idem = "#{method}:#{ref}"

      _ =
        store.record_issued_authorization(%{
          nonce_hex: nonce,
          order_ref: order_ref,
          beneficiary: beneficiary,
          amount_usd: Decimal.new("2.00"),
          namespace: "llm_quota",
          valid_before: now + 900,
          issued_at: now
        })

      if exported?(store, :authorization_settled?, 1) do
        assert!(
          store.authorization_settled?(nonce) == {:ok, false},
          "authorization_settled? is false before any settlement"
        )
      end

      assert!(
        store.payment_seen?(idem) == {:ok, false},
        "payment_seen? is false for a never-settled key"
      )

      result =
        store.record_payment(%{
          idempotency_key: idem,
          beneficiary: beneficiary,
          namespace: "llm_quota",
          amount_usd: Decimal.new("2.00"),
          method: method,
          ref: ref,
          status: "settled",
          at: DateTime.utc_now(),
          facts: %{"nonce_hex" => nonce, "tx_hash" => "0xconformancetx#{u}"}
        })

      assert!(
        result == :ok or match?({:ok, seq} when is_integer(seq) and seq > 0, result),
        "a settled settlement records (:ok or {:ok, seq}), got #{inspect(result)}"
      )

      assert!(store.payment_seen?(idem) == {:ok, true}, "payment_seen? flips true after settling")

      if exported?(store, :authorization_settled?, 1) do
        assert!(
          store.authorization_settled?(nonce) == {:ok, true},
          "authorization_settled? sees the settlement through its facts nonce"
        )

        ok("record_payment + payment_seen? + authorization_settled?: the dedup walls hold")
      else
        ok("record_payment + payment_seen?: dedup holds (authorization_settled? not exported)")
      end

      if exported?(store, :authorization_by_settlement, 2) do
        found = store.authorization_by_settlement(method, ref)

        assert!(
          is_map(found) and stored_field(found, :beneficiary) == beneficiary,
          "authorization_by_settlement joins settlement -> issued row (exact facts nonce)"
        )

        assert!(
          store.authorization_by_settlement(method, "0xconformance-other-#{u}") == nil,
          "authorization_by_settlement answers nil for an unknown ref — " <>
            "a fuzzy match here could edit a stranger's card"
        )

        ok("authorization_by_settlement: exact join + honest miss")
      else
        skip("authorization_by_settlement/2 not exported (REQUIRED for TopupAck credit state)")
      end

      totals_section(store, beneficiary, u)
      deposit_view_section(store, beneficiary, u)
    else
      skip("record_payment/payment_seen? not exported")
    end
  end

  # The C1 collection view: a bound deposit address shows its settled
  # entry-B receipts; authorization-lane settlements never count (they pay
  # the treasury directly). Requires the binding write to stage the row.
  defp deposit_view_section(store, beneficiary, u) do
    if exported?(store, :put_address_binding, 1) and
         exported?(store, :list_deposit_balances, 1) do
      address = "0x" <> (Integer.to_string(u, 16) |> String.downcase() |> String.pad_leading(40, "c"))

      :ok =
        store.put_address_binding(%{
          beneficiary: beneficiary,
          # the contract's key is :index (the HD derivation index)
          index: 1_000_000 + rem(u, 1_000_000),
          address: address,
          namespace: "llm_quota"
        })

      {:ok, rows} = store.list_deposit_balances(200)
      mine = Enum.find(rows, &(stored_field(&1, :beneficiary) == beneficiary))

      assert!(is_map(mine), "a bound deposit address appears in the collection view")

      # settlement_section settled 2.00 for this beneficiary via method
      # "usdc_base_sepolia" — an entry-B-shaped method — so it must count
      received = stored_field(mine, :received_usd)

      assert!(
        not is_nil(received) and Decimal.compare(Decimal.new(received), Decimal.new("2.00")) != :lt,
        "the view counts settled entry-B receipts (got #{inspect(received)})"
      )

      ok("list_deposit_balances: bound addresses with their settled receipts")

      # Address attribution (2026-07-27, found live): a receipt whose facts
      # name a DIFFERENT receiving address must NOT count toward this
      # binding. Attribution by beneficiary alone let a re-bound beneficiary
      # (address migration) inherit the previous address's entire history —
      # the Deposits page claimed uncollected money at an address holding 0
      # on chain. Rows WITHOUT `to_address` (scanner rows predating
      # 2026-07-27) still attach by beneficiary — the assert above pins that
      # fallback.
      elsewhere_result =
        store.record_payment(%{
          idempotency_key: "conformance-elsewhere-#{u}",
          beneficiary: beneficiary,
          namespace: "llm_quota",
          amount_usd: Decimal.new("7.00"),
          method: "usdc_base_sepolia",
          ref: "0xconformance-elsewhere-#{u}:1",
          status: "settled",
          at: DateTime.utc_now(),
          facts: %{"to_address" => "0x" <> String.duplicate("e", 40)}
        })

      assert!(
        elsewhere_result == :ok or
          match?({:ok, seq} when is_integer(seq) and seq > 0, elsewhere_result),
        "the elsewhere-addressed settlement records, got #{inspect(elsewhere_result)}"
      )

      {:ok, rows_after} = store.list_deposit_balances(200)
      mine_after = Enum.find(rows_after, &(stored_field(&1, :beneficiary) == beneficiary))
      received_after = stored_field(mine_after, :received_usd)

      assert!(
        Decimal.equal?(Decimal.new(received_after), Decimal.new(received)),
        "a receipt addressed to a DIFFERENT to_address never counts toward this binding " <>
          "(got #{inspect(received_after)}, expected unchanged #{inspect(received)})"
      )

      ok("list_deposit_balances: receipts attribute by ADDRESS when the fact exists")
    else
      skip("deposit collection view not exported (Deposits page absent)")
    end
  end

  defp totals_section(store, beneficiary, u) do
    if exported?(store, :issuance_totals_since, 3) do
      since = DateTime.add(DateTime.utc_now(), -3600, :second)

      case store.issuance_totals_since("llm_quota", beneficiary, since) do
        {:ok, %{total_usd: total, beneficiary_usd: mine}} ->
          assert!(
            Decimal.compare(mine, Decimal.new("2.00")) != :lt,
            "issuance_totals_since counts this beneficiary's settled amount"
          )

          assert!(
            Decimal.compare(total, mine) != :lt,
            "the namespace total is never below the beneficiary's"
          )

          {:ok, %{beneficiary_usd: other}} =
            store.issuance_totals_since("llm_quota", "conformance-nobody-#{u}", since)

          assert!(
            Decimal.equal?(other, Decimal.new(0)),
            "a beneficiary with no settlements sums to zero"
          )

          ok("issuance_totals_since: the C1 cap reads real sums")

        other ->
          assert!(false, "issuance_totals_since answered #{inspect(other)}")
      end
    else
      skip("issuance_totals_since/3 not exported")
    end
  end

  # ── scan cursor ─────────────────────────────────────────────────────────────

  defp cursor_section(store, u) do
    if exported?(store, :get_last_scanned_block, 1) and
         exported?(store, :put_last_scanned_block, 2) do
      chain_a = "conformance-chain-a-#{u}"
      chain_b = "conformance-chain-b-#{u}"

      assert!(
        store.get_last_scanned_block(chain_a) == {:ok, nil},
        "a never-scanned chain answers {:ok, nil}"
      )

      assert!(store.put_last_scanned_block(chain_a, 123) == :ok, "cursor writes")
      assert!(store.get_last_scanned_block(chain_a) == {:ok, 123}, "cursor roundtrips")
      assert!(store.put_last_scanned_block(chain_a, 456) == :ok, "cursor advances")
      assert!(store.get_last_scanned_block(chain_a) == {:ok, 456}, "the advance sticks")

      assert!(
        store.get_last_scanned_block(chain_b) == {:ok, nil},
        "chains have independent cursors"
      )

      ok("scan cursor: roundtrip, advance, per-chain isolation")
    else
      skip("scan cursor callbacks not exported")
    end
  end

  # ── unrecognised treasury inflows ───────────────────────────────────────────

  defp inflow_section(store, u) do
    if exported?(store, :record_unrecognised_inflow, 1) do
      row = %{
        chain: "conformance-chain-#{u}",
        tx_hash: "0xconformanceinflow#{u}",
        log_index: 1,
        from_addr: "0x" <> String.duplicate("a", 40),
        amount_usd: Decimal.new("0.50"),
        nonce_hex: nonce_hex(),
        reason: "not_issued"
      }

      assert!(store.record_unrecognised_inflow(row) == :ok, "an unrecognised inflow records")

      assert!(
        store.record_unrecognised_inflow(row) == :ok,
        "a replayed (chain, tx_hash, log_index) dedupes as :ok — a rescan must not error the scan"
      )

      ok("record_unrecognised_inflow: audit row + rescan dedupe")

      if exported?(store, :list_unrecognised_inflows, 1) do
        {:ok, listed} = store.list_unrecognised_inflows(100)

        assert!(
          Enum.any?(listed, &(stored_field(&1, :tx_hash) == row.tx_hash)),
          "list_unrecognised_inflows includes the recorded inflow"
        )

        ok("list_unrecognised_inflows: the audit trail is visible")
      else
        skip("list_unrecognised_inflows/1 not exported (dashboard page absent)")
      end
    else
      skip("record_unrecognised_inflow/1 not exported")
    end
  end

  # ── plumbing ────────────────────────────────────────────────────────────────

  defp exported?(store, fun, arity) do
    Code.ensure_loaded?(store) and function_exported?(store, fun, arity)
  end

  # Hosts may return rows with atom or string keys; both are conformant.
  defp stored_field(row, key), do: Map.get(row, key) || Map.get(row, to_string(key))

  defp nonce_hex do
    "0x" <> Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)
  end

  defp assert!(true, _label), do: :ok

  defp assert!(false, label) do
    raise("STORE CONFORMANCE FAILED — #{label}")
  end

  defp assert!(other, label) do
    raise("STORE CONFORMANCE FAILED — #{label} (non-boolean: #{inspect(other)})")
  end

  defp ok(label), do: IO.puts("  ✓ conformance: #{label}")
  defp skip(label), do: IO.puts("  – conformance skip: #{label}")
end
