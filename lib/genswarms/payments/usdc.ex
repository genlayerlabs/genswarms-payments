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
        case scan_chain(chain, watched, core) do
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
        settlements =
          logs
          |> Enum.filter(&(hex_int(&1["blockNumber"]) <= to))
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

  defp to_settlement(log, chain, watched) do
    case log["topics"] do
      [@transfer_topic, _from_topic, to_topic] ->
        to_addr = topic_address(to_topic)

        case Map.fetch(watched, to_addr) do
          {:ok, %{beneficiary: beneficiary, namespace: namespace}} ->
            decimals = Map.get(chain, :decimals, 6)
            raw = hex_int(log["data"])
            amount = Decimal.div(Decimal.new(raw), Decimal.new(Integer.pow(10, decimals)))
            log_index = hex_int(log["logIndex"])

            [
              %{
                beneficiary: beneficiary,
                amount_usd: amount,
                method: "usdc_#{chain.name}",
                ref: "#{log["transactionHash"]}:#{log_index}",
                idempotency_key: "#{chain.name}:#{log["transactionHash"]}:#{log_index}",
                namespace: namespace
              }
            ]

          :error ->
            []
        end

      _other ->
        []
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
