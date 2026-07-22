defmodule Genswarms.Payments do
  @moduledoc """
  Payment settlement hub object. Owns beneficiary identity (stable HD deposit
  address per beneficiary), the idempotent settlement ledger, and stamped
  `payment_confirmed` delivery to allowlisted targets. Modalities implement
  `Genswarms.Payments.Method`; v1 ships USDC in-tree. Trust is fail-closed:
  no trusted_sources ⇒ nobody can act; no targets ⇒ nobody is credited.
  """

  require Logger
  alias Genswarms.Payments.HD

  def init(config) do
    xpub =
      case HD.parse_xpub(Map.fetch!(config, :xpub) |> to_string()) do
        {:ok, parsed} -> parsed
        {:error, why} -> raise ArgumentError, "payments: invalid xpub (#{why})"
      end

    store_mod = Map.get(config, :store_mod)

    bindings =
      store_result(store_mod, :list_address_bindings, [], {:ok, []})
      |> case do
        {:ok, rows} -> Map.new(rows, fn b -> {b.beneficiary, Map.delete(b, :beneficiary)} end)
        _ -> %{}
      end

    next_index =
      bindings |> Map.values() |> Enum.map(& &1.index) |> Enum.max(fn -> -1 end) |> Kernel.+(1)

    %{
      name: Map.get(config, :name, :payments),
      swarm_name: Map.get(config, :swarm_name, "swarm"),
      xpub: xpub,
      trusted_sources: MapSet.new(Map.get(config, :trusted_sources, []) |> Enum.map(&to_string/1)),
      targets: Map.get(config, :targets, []) |> Enum.map(&to_string/1),
      namespace: Map.get(config, :namespace, "default") |> to_string(),
      store_mod: store_mod,
      deliver_fn: Map.get(config, :deliver_fn, default_deliver_fn(Map.get(config, :swarm_name, "swarm"))),
      now_fn: Map.get(config, :now_fn, &DateTime.utc_now/0),
      rpc_fn: Map.get(config, :rpc_fn),
      chains: Map.get(config, :chains, []),
      methods: Map.get(config, :methods, [Genswarms.Payments.Usdc]),
      method_states: %{},
      auto_tick: Map.get(config, :auto_tick, true),
      poll_interval_ms: Map.get(config, :poll_interval_ms, 60_000),
      bindings: bindings,
      next_index: next_index,
      seen_keys: MapSet.new()
    }
  end

  def handle_message(from, content, state) do
    case Jason.decode(content) do
      {:ok, %{"action" => "health"}} ->
        {:reply, Jason.encode!(%{ok: true, bindings: map_size(state.bindings)}), state}

      {:ok, %{"action" => action} = msg} when action in ~w(deposit_address payment_status ingest_event) ->
        if trusted?(from, state) do
          handle_action(action, msg, state)
        else
          Logger.warning("payments: untrusted source #{inspect(from)} sent #{action}")
          {:noreply, state}
        end

      {:ok, _} ->
        {:noreply, state}

      {:error, _} ->
        Logger.warning("payments: undecodable message from #{inspect(from)}")
        {:noreply, state}
    end
  end

  @doc """
  Settle confirmed payments: durable-dedup each, record it, then deliver the
  stamped `payment_confirmed` to every allowlisted target. FAIL CLOSED: if the
  configured store errors on the dedup read OR the record write, the
  settlement is skipped this round (the watcher will re-present it — the scan
  cursor only advances on full success). Returns {settled_count, state}.
  """
  def settle(settlements, state) when is_list(settlements) do
    Enum.reduce(settlements, {0, state}, fn s, {n, st} ->
      case settle_one(s, st) do
        {:settled, st} -> {n + 1, st}
        {:skipped, st} -> {n, st}
      end
    end)
  end

  defp settle_one(%{idempotency_key: key} = s, state) do
    seen_memory? = MapSet.member?(state.seen_keys, key)

    case {seen_memory?, store_result(state.store_mod, :payment_seen?, [key], nil)} do
      {true, _} ->
        {:skipped, state}

      {_, {:ok, true}} ->
        {:skipped, %{state | seen_keys: MapSet.put(state.seen_keys, key)}}

      {false, {:ok, false}} ->
        record_and_deliver(s, state)

      {false, nil} when is_nil(state.store_mod) ->
        record_and_deliver(s, state)

      {false, _error} ->
        Logger.error("payments: dedup read failed for #{key} — FAIL CLOSED, holding settlement")
        {:skipped, state}
    end
  end

  defp record_and_deliver(%{idempotency_key: key} = s, state) do
    row = %{
      idempotency_key: key,
      beneficiary: s.beneficiary,
      amount_usd: s.amount_usd,
      method: s.method,
      ref: s.ref,
      namespace: s.namespace,
      at: state.now_fn.()
    }

    case store_write(state.store_mod, :record_payment, [row]) do
      :ok ->
        content =
          Jason.encode!(%{
            action: "payment_confirmed",
            beneficiary: s.beneficiary,
            amount_usd: Decimal.to_string(s.amount_usd),
            method: s.method,
            ref: s.ref,
            namespace: s.namespace,
            at: DateTime.to_iso8601(row.at)
          })

        Enum.each(state.targets, fn target ->
          try do
            state.deliver_fn.(target, state.name, content)
          rescue
            e -> Logger.error("payments: delivery to #{target} raised: #{Exception.message(e)}")
          end
        end)

        {:settled, %{state | seen_keys: MapSet.put(state.seen_keys, key)}}

      {:error, why} ->
        Logger.error("payments: record_payment failed (#{inspect(why)}) — FAIL CLOSED, holding #{key}")
        {:skipped, state}
    end
  end

  defp trusted?(from, state), do: MapSet.member?(state.trusted_sources, to_string(from))

  defp handle_action("deposit_address", %{"beneficiary" => ben}, state) when is_binary(ben) and ben != "" do
    case ensure_binding(ben, state) do
      {:ok, binding, state} ->
        {:reply, Jason.encode!(%{ok: true, beneficiary: ben, address: binding.address, namespace: binding.namespace}), state}

      {:error, why, state} ->
        {:reply, Jason.encode!(%{ok: false, error: to_string(why)}), state}
    end
  end

  defp handle_action("payment_status", %{"beneficiary" => ben}, state) when is_binary(ben) do
    payments =
      case store_result(state.store_mod, :list_payments, [ben], {:ok, []}) do
        {:ok, rows} -> Enum.map(rows, &Map.take(&1, [:amount_usd, :method, :ref, :at]) |> stringify())
        _ -> []
      end

    binding = Map.get(state.bindings, ben)

    {:reply,
     Jason.encode!(%{ok: true, beneficiary: ben, address: binding && binding.address, payments: payments}),
     state}
  end

  defp handle_action("ingest_event", _msg, state) do
    # Push modalities land in Task 8 (method verifies signature before settle).
    {:reply, Jason.encode!(%{ok: false, error: "no_push_methods"}), state}
  end

  defp handle_action(_, _msg, state),
    do: {:reply, Jason.encode!(%{ok: false, error: "bad_request"}), state}

  defp ensure_binding(ben, state) do
    case Map.fetch(state.bindings, ben) do
      {:ok, binding} ->
        {:ok, binding, state}

      :error ->
        index = state.next_index

        case HD.address(state.xpub, index) do
          {:ok, address} ->
            binding = %{index: index, address: address, namespace: state.namespace}
            row = Map.put(binding, :beneficiary, ben)

            # Fail CLOSED: with a configured store erroring, never hand out an
            # address whose binding isn't durable — the watcher would credit
            # nobody for money sent to it.
            case store_write(state.store_mod, :put_address_binding, [row]) do
              :ok ->
                {:ok, binding,
                 %{state | bindings: Map.put(state.bindings, ben, binding), next_index: index + 1}}

              {:error, why} ->
                Logger.error("payments: binding persist failed (#{inspect(why)}) — refusing allocation")
                {:error, :store_unavailable, state}
            end

          {:error, why} ->
            {:error, why, state}
        end
    end
  end

  # ── store seam helpers ──────────────────────────────────────────────────────
  # Reads fall back to the default; writes fail closed only when a store IS
  # configured and errors (nil store = dev memory mode = treat write as ok).

  defp store_result(nil, _fun, _args, default), do: default

  defp store_result(mod, fun, args, default) do
    if function_exported?(mod, fun, length(args)) do
      try do
        apply(mod, fun, args)
      rescue
        e ->
          Logger.error("payments: store #{fun} raised: #{Exception.message(e)}")
          default
      end
    else
      default
    end
  end

  defp store_write(nil, _fun, _args), do: :ok

  defp store_write(mod, fun, args) do
    if function_exported?(mod, fun, length(args)) do
      try do
        case apply(mod, fun, args) do
          :ok -> :ok
          {:error, why} -> {:error, why}
          other -> {:error, {:bad_return, other}}
        end
      rescue
        e -> {:error, {:raised, Exception.message(e)}}
      end
    else
      :ok
    end
  end

  defp stringify(map), do: Map.new(map, fn {k, v} -> {to_string(k), to_string(v)} end)

  defp default_deliver_fn(swarm_name) do
    # Genswarms.Objects.ObjectServer is a host-provided peer module, not a
    # compile-time dep of this package — dispatch via apply/3 so this module
    # compiles standalone (a direct remote call would warn/fail under
    # --warnings-as-errors since the module isn't available at compile time).
    fn target, from, content ->
      apply(Genswarms.Objects.ObjectServer, :deliver_message, [swarm_name, target, from, content])
      :ok
    end
  end
end
