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
        confirmations = Map.get(chain, :confirmations, 12)
        safe_to = latest - confirmations

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
        settlements =
          logs
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
      [@transfer_topic, _from_topic, to_topic] ->
        to_addr = topic_address(to_topic)

        case Map.fetch(watched, to_addr) do
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
              log_index = hex_int(log["logIndex"])
              tx_hash = tx_hash!(log)

              [
                %{
                  beneficiary: beneficiary,
                  amount_usd: amount,
                  method: "usdc_#{chain.name}",
                  ref: "#{tx_hash}:#{log_index}",
                  idempotency_key: "#{chain.name}:#{tx_hash}:#{log_index}",
                  namespace: namespace
                }
              ]
            end

          :error ->
            []
        end

      _other ->
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

  defp topic_address("0x" <> hex), do: "0x" <> String.downcase(String.slice(hex, -40, 40))

  defp pad_topic_address("0x" <> hex),
    do: "0x" <> String.duplicate("0", 24) <> String.downcase(hex)

  defp hex(n) when is_integer(n), do: "0x" <> Integer.to_string(n, 16)
  defp hex_int("0x" <> h), do: String.to_integer(h, 16)
  defp hex_int(n) when is_integer(n), do: n
end
