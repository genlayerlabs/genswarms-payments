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

  @transfer_topic "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

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

    case fetch_logs(chain, core, watched, from, to, chunk_size) do
      {:ok, logs} ->
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
          logs
          |> Enum.reject(&(&1["removed"] == true))
          |> Enum.filter(&(hex_int(&1["blockNumber"]) <= to))
          |> Enum.filter(&same_contract?(&1, chain))
          |> Enum.flat_map(&to_settlement(&1, chain, watched))

        {:ok, settlements, to}

      {:error, why} ->
        {:error, why}
    end
  end

  defp fetch_logs(_chain, _core, watched, _from, _to, _chunk_size) when map_size(watched) == 0 do
    {:ok, []}
  end

  defp fetch_logs(chain, core, watched, from, to, chunk_size) do
    watched
    |> Map.keys()
    |> Enum.chunk_every(chunk_size)
    |> Enum.reduce_while({:ok, []}, fn addrs, {:ok, acc} ->
      params = %{
        "address" => chain.usdc_contract,
        "fromBlock" => hex(from),
        "toBlock" => hex(to),
        "topics" => [@transfer_topic, nil, Enum.map(addrs, &pad_topic_address/1)]
      }

      case core.rpc_fn.(chain, "eth_getLogs", [params]) do
        {:ok, logs} -> {:cont, {:ok, acc ++ logs}}
        {:error, why} -> {:halt, {:error, why}}
      end
    end)
  end

  defp same_contract?(log, chain) do
    String.downcase(log["address"] || "") == String.downcase(chain.usdc_contract)
  end

  defp to_settlement(log, chain, watched) do
    case log["topics"] do
      # Topic0 matching is case-INSENSITIVE, like same_contract?/2 and
      # topic_address/1 — the module already decided hex case can vary, and
      # an uppercase-hex provider must not silently miss payments while the
      # cursor advances (credit lost, never re-presented).
      [topic0, from_topic, to_topic]
      when is_binary(topic0) and is_binary(from_topic) and is_binary(to_topic) ->
        if String.downcase(topic0) == @transfer_topic do
          transfer_settlement(log, chain, watched, from_topic, to_topic)
        else
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

  defp transfer_settlement(log, chain, watched, from_topic, to_topic) do
    case Map.fetch(watched, topic_address(to_topic)) do
      {:ok, %{beneficiary: beneficiary, namespace: namespace}} ->
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
            %{
              beneficiary: beneficiary,
              amount_usd: amount,
              method: "usdc_#{chain.name}",
              ref: "#{tx_hash}:#{log_index}",
              idempotency_key: "#{chain.chain_id}:#{tx_hash}:#{log_index}",
              namespace: namespace,
              raw_amount: raw,
              decimals: decimals,
              token_contract: chain.usdc_contract,
              chain: chain.name,
              chain_id: chain.chain_id,
              block_number: block_number,
              log_index: log_index,
              tx_hash: tx_hash,
              from_address: topic_address(from_topic)
            }
          ]
        end

      :error ->
        []
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

  defp hex(n) when is_integer(n), do: "0x" <> Integer.to_string(n, 16)
  defp hex_int("0x" <> h), do: String.to_integer(h, 16)
  defp hex_int(n) when is_integer(n), do: n
end
