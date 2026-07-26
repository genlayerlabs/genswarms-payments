defmodule Genswarms.Payments.Usdc do
  @moduledoc """
  Pull modality: watches ERC-20 USDC `Transfer` logs to bound deposit
  addresses across configured EVM chains. Reorg-safe (settles only logs at
  least `confirmations` blocks deep), cursor-driven (`last_scanned_block` per
  chain, read via the core; the CORE — not this module — decides when to
  advance it, only after the round's settlements were all recorded: the
  fail-closed rule), chunked address filters (`address_chunk`, default 200)
  so user growth never exceeds RPC filter limits, and range-capped
  (`max_block_range`, default 2000) so a cold start never issues an
  unbounded getLogs.
  """

  @behaviour Genswarms.Payments.Method
  require Logger
  alias Genswarms.Payments.Keccak

  @transfer_topic "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

  # AuthorizationUsed(address,bytes32) — EIP-3009's consumption event, indexed
  # on BOTH the authorizer and the nonce. Computed at compile time (not typed
  # by hand as hex) so the only residual bug surface is the signature STRING,
  # not a transcription of the hash — and `checks/payments_keccak_test.exs`
  # independently re-derives the same hash from the same string against a
  # frozen literal copied from the verified spec value, so the two only agree
  # if both the signature string AND the Keccak implementation are right.
  @authorization_used_topic "0x" <>
                              (Keccak.hash_256("AuthorizationUsed(address,bytes32)")
                               |> Base.encode16(case: :lower))

  @impl true
  def id, do: "usdc"

  @impl true
  def capabilities, do: [:deposit_address]

  @impl true
  def poll(method_state, core) do
    watched = watched_addresses(core)

    results =
      Enum.map(core.chains, fn chain ->
        case safe_scan_chain(chain, watched, core) do
          {:ok, settlements, safe_to} ->
            {chain, settlements, safe_to}

          {:error, why} ->
            Logger.warning(
              "payments/usdc: scan of #{chain.name} failed: #{inspect(why)} — retrying next tick"
            )

            core.emit_metric.("payments_hold", %{
              stage: "chain_scan",
              chain: to_string(chain.name),
              reason: inspect(why)
            })

            {chain, [], nil}
        end
      end)

    {results, method_state}
  end

  # A misbehaving/nonconforming RPC provider can hand back a field this
  # module's hex parsing can't make sense of (e.g. a real-world provider
  # returning {:ok, nil} for eth_blockNumber instead of a hex string). One
  # chain's malformed response must not crash the whole tick and take every
  # other configured chain's round down with it — catch it here so only THIS
  # chain's scan is skipped (cursor untouched, retried next tick).
  defp safe_scan_chain(chain, watched, core) do
    try do
      scan_chain(chain, watched, core)
    rescue
      e -> {:error, {:bad_rpc_shape, Exception.message(e)}}
    end
  end

  # Returns {:ok, settlements, safe_to_block} | {:error, why}. The CORE
  # advances the cursor only after settle/2 recorded everything (fail closed).
  defp scan_chain(chain, watched, core) do
    case core.rpc_fn.(chain, "eth_blockNumber", []) do
      {:ok, latest_hex} ->
        latest = hex_int(latest_hex)
        safe_to = latest - fast_credit_depth(chain)

        case scan_from(chain, core) do
          {:ok, from} when from > safe_to ->
            # nothing new is deep enough to be safe yet — skip this round
            {:ok, [], nil}

          {:ok, from} ->
            to = min(safe_to, from + Map.get(chain, :max_block_range, 2000) - 1)
            fetch_and_convert(chain, core, watched, from, to)

          {:error, why} ->
            {:error, why}
        end

      {:error, why} ->
        {:error, why}
    end
  end

  # The CREDIT leg's depth, explicitly labelled (C2). This is the fast,
  # shallow path a user waits on — NOT finality, which the hub queries from
  # the chain on the reconciliation leg (`finality: :finalized`). Defaults to
  # the chain's `confirmations` so existing configs keep their current depth.
  defp fast_credit_depth(chain) do
    Map.get(chain, :fast_credit_depth, Map.get(chain, :confirmations, 12))
  end

  defp scan_from(chain, core) do
    case core.get_last_scanned_block.(chain.name) do
      {:ok, nil} -> {:ok, Map.get(chain, :start_block, 0)}
      {:ok, n} -> {:ok, n + 1}
      {:error, why} -> {:error, {:cursor_read, why}}
    end
  end

  defp fetch_and_convert(chain, core, watched, from, to) do
    chunk_size = Map.get(chain, :address_chunk, 200)

    with {:ok, transfer_logs} <- fetch_transfer_logs(chain, core, watched, from, to, chunk_size),
         {:ok, auths_by_tx} <- fetch_nonce_correlations(chain, core, from, to, chunk_size) do
      # Defense in depth: don't trust the RPC provider to have honored the
      # toBlock bound — a log at or beyond the reorg-risk edge must wait
      # for a later, deeper round even if it comes back in this response.
      # Likewise don't trust the address filter — a misbehaving/compromised
      # RPC could hand back a Transfer-shaped log from an unrelated
      # contract; only settle logs actually emitted by the configured USDC
      # contract.
      # `removed: true` marks a log the provider retracted after a reorg —
      # it is officially NOT part of the chain, so it must never settle or
      # deliver. Skipping (not holding) is correct: the log is gone, there
      # is nothing to re-present, and the canonical replacement (if any)
      # arrives as its own normal log. In practice a ≥confirmations-deep
      # getLogs range should never contain one; defense in depth.
      settlements =
        transfer_logs
        |> Enum.reject(&(&1["removed"] == true))
        |> Enum.filter(&(hex_int(&1["blockNumber"]) <= to))
        |> Enum.filter(&same_contract?(&1, chain))
        |> Enum.flat_map(&to_settlement(&1, chain, watched, auths_by_tx))
        |> mark_correlation_conflicts()

      {:ok, settlements, to}
    else
      {:error, why} -> {:error, why}
    end
  end

  # ── entry A leg 1: Transfer logs, watched deposit addresses PLUS the
  # treasury address (if configured) in the SAME filter — the treasury has no
  # single beneficiary (see transfer_settlement/6) so it is deliberately never
  # added to `watched`, only to this getLogs address list.
  defp fetch_transfer_logs(chain, core, watched, from, to, chunk_size) do
    case transfer_filter_addresses(chain, watched) do
      [] ->
        {:ok, []}

      addrs ->
        addrs
        |> Enum.chunk_every(chunk_size)
        |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, acc} ->
          params = %{
            "address" => chain.usdc_contract,
            "fromBlock" => hex(from),
            "toBlock" => hex(to),
            "topics" => [@transfer_topic, nil, Enum.map(chunk, &pad_topic_address/1)]
          }

          case core.rpc_fn.(chain, "eth_getLogs", [params]) do
            {:ok, logs} -> {:cont, {:ok, acc ++ logs}}
            {:error, why} -> {:halt, {:error, why}}
          end
        end)
    end
  end

  defp transfer_filter_addresses(chain, watched) do
    (Map.keys(watched) ++ List.wrap(treasury_address(chain))) |> Enum.uniq()
  end

  defp treasury_address(chain) do
    case Map.get(chain, :treasury_address) do
      nil -> nil
      addr -> String.downcase(addr)
    end
  end

  # ── entry A leg 2: AuthorizationUsed logs, filtered to the nonces THIS hub
  # issued and still considers live (issued ∧ unconsumed ∧ unexpired — see
  # Store.live_authorization_nonces/1). No treasury_address configured on
  # this chain ⇒ the lane is off, skip the query entirely (same "nothing to
  # ask" shortcut fetch_transfer_logs takes for an empty address set).
  defp fetch_nonce_correlations(chain, core, from, to, chunk_size) do
    case treasury_address(chain) do
      nil ->
        {:ok, %{}}

      _treasury ->
        case core.live_authorization_nonces.() do
          {:ok, []} -> {:ok, %{}}
          {:ok, nonces} -> fetch_authorization_logs(chain, core, nonces, from, to, chunk_size)
          {:error, why} -> {:error, why}
        end
    end
  end

  # Normalized exactly like the topic the correlation later matches on
  # (`pad_topic_bytes32/1` downcases its query side too), so the membership
  # re-check below is a genuine string equality and not a case trap.
  defp live_nonce_set(nonces) do
    nonces
    |> Enum.filter(&is_binary/1)
    |> MapSet.new(&String.downcase/1)
  end

  # Same 200-per-chunk mechanism as the Transfer query, over the LIVE NONCE
  # SET rather than addresses — bounded by construction (every authorization
  # expires), never by user or transaction count.
  defp fetch_authorization_logs(chain, core, nonces, from, to, chunk_size) do
    result =
      nonces
      |> Enum.chunk_every(chunk_size)
      |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, acc} ->
        params = %{
          "address" => chain.usdc_contract,
          "fromBlock" => hex(from),
          "toBlock" => hex(to),
          "topics" => [@authorization_used_topic, nil, Enum.map(chunk, &pad_topic_bytes32/1)]
        }

        case core.rpc_fn.(chain, "eth_getLogs", [params]) do
          {:ok, logs} -> {:cont, {:ok, acc ++ logs}}
          {:error, why} -> {:halt, {:error, why}}
        end
      end)

    case result do
      {:ok, logs} -> {:ok, authorizations_by_tx(logs, chain, to, live_nonce_set(nonces))}
      {:error, why} -> {:error, why}
    end
  end

  # tx_hash => [%{nonce_hex, authorizer}], built from the SAME reorg/contract
  # defenses the Transfer leg applies (removed / toBlock / same_contract?)
  # before the correlation in treasury_inflow/4 ever consults it.
  #
  # A LIST, not a single nonce, and it keeps the `authorizer` — one tx can
  # legitimately carry many AuthorizationUsed events (a batched multicall, and
  # EIP-3009 authorizations are submittable by ANYONE, so their order inside a
  # tx is attacker-controlled). Keying `tx_hash => nonce` and last-write-wins
  # attached one arbitrary nonce to every treasury Transfer in the tx: an
  # attacker who spent $1 alongside a victim's $100 payment could steer the
  # victim's credit to themselves. Correlation is `authorizer == Transfer.from`
  # (that is exactly what EIP-3009's `receiveWithAuthorization` guarantees),
  # never log position.
  #
  # The `live` re-check is the same stance already applied to topic0 and to
  # `address`: the getLogs call asked only for nonces this hub issued and still
  # considers live, but a provider that ignores `topics[2]` must not be able to
  # inject a correlation for a nonce we never asked about.
  defp authorizations_by_tx(logs, chain, to, live) do
    logs
    |> Enum.reject(&(&1["removed"] == true))
    |> Enum.filter(&(hex_int(&1["blockNumber"]) <= to))
    |> Enum.filter(&same_contract?(&1, chain))
    |> Enum.reduce(%{}, fn log, acc ->
      case authorization_used(log) do
        {:ok, tx_hash, authorizer, nonce_hex} ->
          if MapSet.member?(live, nonce_hex) do
            add_authorization(acc, tx_hash, %{nonce_hex: nonce_hex, authorizer: authorizer})
          else
            acc
          end

        :skip ->
          acc
      end
    end)
  end

  # Deduplicated on the nonce: a provider echoing the same log twice must not
  # manufacture the "two authorizations for one Transfer" ambiguity that the
  # hub deliberately refuses to guess through.
  defp add_authorization(acc, tx_hash, entry) do
    Map.update(acc, tx_hash, [entry], fn list ->
      if Enum.any?(list, &(&1.nonce_hex == entry.nonce_hex)), do: list, else: [entry | list]
    end)
  end

  # AuthorizationUsed(address indexed authorizer, bytes32 indexed nonce) — the
  # authorizer is topics[1] and the nonce is topics[2]. Same fail-closed shape
  # check as to_settlement/4: a topic0 mismatch is a normal defense-in-depth
  # skip (this getLogs call is already filtered to @authorization_used_topic,
  # but never trust the provider echoed only what was asked for), while a
  # genuinely malformed 3-topic shape raises — rescued by safe_scan_chain/3,
  # chain held, cursor unmoved — rather than silently losing a correlation for
  # a payment that may be real.
  defp authorization_used(log) do
    case log["topics"] do
      [topic0, authorizer_topic, nonce_topic]
      when is_binary(topic0) and is_binary(authorizer_topic) and is_binary(nonce_topic) ->
        if String.downcase(topic0) == @authorization_used_topic do
          {:ok, tx_hash!(log), topic_address(authorizer_topic), String.downcase(nonce_topic)}
        else
          :skip
        end

      other ->
        raise ArgumentError, "log with malformed topics: #{inspect(other)}"
    end
  end

  defp same_contract?(log, chain) do
    String.downcase(log["address"] || "") == String.downcase(chain.usdc_contract)
  end

  defp to_settlement(log, chain, watched, auths_by_tx) do
    case log["topics"] do
      # Topic0 matching is case-INSENSITIVE, like same_contract?/2 and
      # topic_address/1 — the module already decided hex case can vary, and
      # an uppercase-hex provider must not silently miss payments while the
      # cursor advances (credit lost, never re-presented).
      [topic0, from_topic, to_topic]
      when is_binary(topic0) and is_binary(from_topic) and is_binary(to_topic) ->
        if String.downcase(topic0) == @transfer_topic do
          transfer_settlement(log, chain, watched, from_topic, to_topic, auths_by_tx)
        else
          # Defense in depth (Task 5 plan step 3): an AuthorizationUsed log
          # has three topics too, but its topic0 is @authorization_used_topic,
          # never @transfer_topic, so it falls here and is skipped — never
          # mistaken for a Transfer even if a misbehaving RPC handed one back
          # on this query.
          []
        end

      # The getLogs filter pinned topic0 to @transfer_topic and position 2 to
      # the watched-address set, so any other shape (nil to-topic, wrong
      # arity, non-list) is provider garbage that may be a real payment with
      # mangled topics. Fail CLOSED like tx_hash!/1 — raise, rescued by
      # safe_scan_chain/3, chain held with the cursor unmoved — instead of
      # skipping and advancing the cursor past a payment we could not read.
      other ->
        raise ArgumentError, "log with malformed topics: #{inspect(other)}"
    end
  end

  # The TREASURY branch is resolved FIRST, before the watched-address lookup.
  # `init/1` already refuses to boot when a binding's address equals a chain's
  # treasury_address, so this ordering is belt-and-braces for a collision that
  # appears after boot (a peer hub writing a binding, an operator repairing a
  # row by hand): resolving `watched` first would hand the treasury a single
  # beneficiary and skip the §4.4 credit rule entirely, crediting every user's
  # authorization payment to whoever happened to own that binding.
  defp transfer_settlement(log, chain, watched, from_topic, to_topic, auths_by_tx) do
    to_addr = topic_address(to_topic)

    cond do
      to_addr == treasury_address(chain) ->
        treasury_inflow(log, chain, from_topic, auths_by_tx)

      true ->
        case Map.fetch(watched, to_addr) do
          {:ok, %{beneficiary: beneficiary, namespace: namespace}} ->
            build_settlement(log, chain, from_topic, %{
              beneficiary: beneficiary,
              namespace: namespace,
              method: "usdc_#{chain.name}"
            })

          :error ->
            []
        end
    end
  end

  # The treasury has no single beneficiary — unlike every other watched
  # address, `watched` cannot resolve who to credit here (see the module
  # doc). This branch produces the raw inflow candidate only: beneficiary and
  # namespace stay unresolved (nil) and `method` is tagged distinctly
  # (`"usdc_authorization"`) so the hub's settle path knows to run the §4.4
  # credit rule — nonce correlation, not a watched-address lookup — before
  # this can ever become creditable.
  #
  # `nonce_candidates` are the nonces this Transfer could possibly belong to:
  # AuthorizationUsed events in the SAME tx whose `authorizer` IS this
  # Transfer's `from`. EIP-3009's `receiveWithAuthorization(from, to, value,
  # ...)` moves value FROM the authorizer, so authorizer≠from is proof the two
  # logs are unrelated no matter how adjacent they sit. Usually the list is
  # empty (a plain unsolicited transfer into the treasury) or has exactly one
  # entry; the hub refuses to guess for anything else. `nonce_hex` mirrors the
  # single-candidate case only, and exists for the audit/metric surface.
  defp treasury_inflow(log, chain, from_topic, auths_by_tx) do
    from_addr = topic_address(from_topic)

    candidates =
      auths_by_tx
      |> Map.get(tx_hash!(log), [])
      |> Enum.filter(&(&1.authorizer == from_addr))
      |> Enum.map(& &1.nonce_hex)

    build_settlement(log, chain, from_topic, %{
      beneficiary: nil,
      namespace: nil,
      method: "usdc_authorization",
      nonce_candidates: candidates,
      nonce_hex: single_candidate(candidates)
    })
  end

  defp single_candidate([nonce]), do: nonce
  defp single_candidate(_), do: nil

  # The one ambiguity `authorizer == from` cannot resolve by itself: TWO
  # treasury Transfers in one tx sharing the same `from`. Each would match the
  # same single authorization, and crediting both would pay one authorization
  # twice. There is no non-guessing way to say which Transfer the
  # authorization belongs to (log ordering is attacker-controlled), so BOTH
  # are flagged and the hub holds them as unrecognised — under-credit an
  # operator can repair, never a double-credit.
  defp mark_correlation_conflicts(settlements) do
    counts =
      settlements
      |> Enum.filter(&(Map.get(&1, :method) == "usdc_authorization"))
      |> Enum.frequencies_by(&{Map.get(&1, :tx_hash), Map.get(&1, :from_address)})

    Enum.map(settlements, fn s ->
      if Map.get(counts, {Map.get(s, :tx_hash), Map.get(s, :from_address)}, 0) > 1 do
        Map.put(s, :authorization_conflict, true)
      else
        s
      end
    end)
  end

  defp build_settlement(log, chain, from_topic, extra) do
    decimals = Map.get(chain, :decimals, 6)
    raw = hex_int(log["data"])

    # Zero-value Transfer events are real logs anyone can emit for
    # only gas (`transfer(victim, 0)`), and "any amount becomes
    # credit" means USDC actually ARRIVING — 0 is not an arrival.
    # Settling them would let an attacker grow the ledger, seen-set,
    # and delivery fan-out for free; skip with no ledger write (the
    # cursor still advances normally — nothing is held).
    if raw == 0 do
      []
    else
      amount = Decimal.div(Decimal.new(raw), Decimal.new(Integer.pow(10, decimals)))
      block_number = hex_int(log["blockNumber"])
      log_index = hex_int(log["logIndex"])
      tx_hash = tx_hash!(log)

      [
        Map.merge(
          %{
            amount_usd: amount,
            ref: "#{tx_hash}:#{log_index}",
            idempotency_key: "#{chain.chain_id}:#{tx_hash}:#{log_index}",
            raw_amount: raw,
            decimals: decimals,
            token_contract: chain.usdc_contract,
            chain: chain.name,
            chain_id: chain.chain_id,
            block_number: block_number,
            log_index: log_index,
            tx_hash: tx_hash,
            from_address: topic_address(from_topic)
          },
          extra
        )
      ]
    end
  end

  # A log with no usable transactionHash must fail CLOSED like every other
  # malformed field: missing logIndex/data/blockNumber crash hex_int/1, are
  # rescued by safe_scan_chain/3, and hold the whole chain (cursor unmoved,
  # retried next tick). Interpolating nil instead minted the garbage durable
  # dedup key "<chain>::<logIndex>" — once a durable store had seen it, every
  # future hash-less log at that logIndex was silently swallowed while the
  # cursor advanced: credit lost, no log line, no held settlement, and a ref
  # (":0") reconciliation can't tie to a tx.
  defp tx_hash!(log) do
    case log["transactionHash"] do
      tx when is_binary(tx) and tx != "" -> tx
      other -> raise ArgumentError, "log missing transactionHash: #{inspect(other)}"
    end
  end

  # watched: %{lowercase_address => %{beneficiary, namespace}}
  defp watched_addresses(core) do
    Map.new(core.bindings, fn {ben, b} ->
      {String.downcase(b.address), %{beneficiary: ben, namespace: b.namespace}}
    end)
  end

  # Case-insensitive (downcase BEFORE matching the prefix, so even an "0X"
  # prefix from a nonstandard node normalizes instead of crashing the scan).
  defp topic_address(topic) when is_binary(topic) do
    "0x" <> hex = String.downcase(topic)
    "0x" <> String.slice(hex, -40, 40)
  end

  defp pad_topic_address("0x" <> hex),
    do: "0x" <> String.duplicate("0", 24) <> String.downcase(hex)

  # Nonces are already 32 bytes (64 hex chars) — pad_leading is a defensive
  # no-op for the well-formed case, matching pad_topic_address/1's stance of
  # never trusting incoming case/width even when it "should" already be right.
  defp pad_topic_bytes32("0x" <> hex),
    do: "0x" <> String.pad_leading(String.downcase(hex), 64, "0")

  defp hex(n) when is_integer(n), do: "0x" <> Integer.to_string(n, 16)
  defp hex_int("0x" <> h), do: String.to_integer(h, 16)
  defp hex_int(n) when is_integer(n), do: n
end
