defmodule Genswarms.Payments do
  @moduledoc """
  Payment settlement hub object. Owns beneficiary identity (stable HD deposit
  address per beneficiary), the idempotent settlement ledger, and stamped
  `payment_confirmed` delivery to allowlisted targets. Modalities implement
  `Genswarms.Payments.Method`; v1 ships USDC in-tree. Trust is fail-closed:
  no trusted_sources ⇒ nobody can act; no targets ⇒ nobody is credited.

  Boot is fail-FLAGGED, not fail-crashed: if a configured store's
  `list_address_bindings/0` errors or raises, `init/1` cannot know the true
  watched-address set or the next free HD index, so it sets `degraded_boot:
  true` rather than guessing. While degraded, `poll/1` is a no-op and
  `deposit_address` is refused — it self-heals only via a restart (a
  transient DB blip at pod boot shouldn't crash-loop the object, but it also
  must never scan an empty watched set or hand out a reused address). See
  `init_bindings/1`.
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
    validate_store_coherence!(store_mod)

    chains = Map.get(config, :chains, [])
    Enum.each(chains, &validate_rpc_url!/1)

    {bindings, degraded_boot?} = init_bindings(store_mod)

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
      rpc_fn: Map.get(config, :rpc_fn, &Genswarms.Payments.Rpc.call/3),
      chains: chains,
      methods: Map.get(config, :methods, [Genswarms.Payments.Usdc]),
      method_states: %{},
      auto_tick: Map.get(config, :auto_tick, true),
      poll_interval_ms: Map.get(config, :poll_interval_ms, 60_000),
      bindings: bindings,
      next_index: next_index,
      seen_keys: MapSet.new(),
      undelivered: %{},
      cursor_mirror: %{},
      degraded_boot: degraded_boot?
    }
  end

  # Boot-time binding load. FAIL-FLAGGED (not raised): when a CONFIGURED store's
  # list_address_bindings errors or raises, we don't know the true watched set
  # or the true next HD index — polling over an empty set would silently drop
  # every in-flight deposit, and minting from index 0 would reuse an address
  # already handed out. We refuse to guess: bindings come back empty and
  # degraded_boot is set, which fail-closes poll/1 and deposit_address until a
  # restart re-attempts boot against a (hopefully recovered) store. We chose
  # flag-over-raise so a transient DB blip at pod boot doesn't crash-loop the
  # object — see Store's moduledoc.
  defp init_bindings(nil), do: {%{}, false}

  defp init_bindings(mod) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :list_address_bindings, 0) do
      try do
        case apply(mod, :list_address_bindings, []) do
          {:ok, rows} ->
            {Map.new(rows, fn b -> {b.beneficiary, Map.delete(b, :beneficiary)} end), false}

          {:error, why} ->
            Logger.error(
              "payments: list_address_bindings failed at boot (#{inspect(why)}) — DEGRADED BOOT: polling and allocation refused until restart"
            )

            {%{}, true}
        end
      rescue
        e ->
          Logger.error(
            "payments: list_address_bindings raised at boot (#{Exception.message(e)}) — DEGRADED BOOT: polling and allocation refused until restart"
          )

          {%{}, true}
      end
    else
      {%{}, false}
    end
  end

  # Init-time coherence gate: a store that implements only HALF of a callback
  # group is worse than one that implements none of it — with only
  # put_address_binding (no list_address_bindings) every restart forgets the
  # watched set and reuses HD indices already handed out; with only
  # payment_seen? (no record_payment) every settlement dedup-checks against a
  # ledger nothing ever writes to, i.e. it always looks unseen ⇒ double-credit.
  # list_payments is read-only reporting, not part of either safety group.
  defp validate_store_coherence!(nil), do: :ok

  defp validate_store_coherence!(mod) do
    Code.ensure_loaded(mod)
    validate_group!(mod, [{:put_address_binding, 1}, {:list_address_bindings, 0}])

    validate_group!(mod, [
      {:payment_seen?, 1},
      {:record_payment, 1},
      {:get_last_scanned_block, 1},
      {:put_last_scanned_block, 2}
    ])

    :ok
  end

  defp validate_group!(mod, funs) do
    exported? = fn {f, a} -> function_exported?(mod, f, a) end

    case funs |> Enum.map(exported?) |> Enum.uniq() do
      [_all_same] ->
        :ok

      _mixed ->
        names = Enum.map_join(funs, ", ", fn {f, a} -> "#{f}/#{a}" end)

        raise ArgumentError,
              "payments: store #{inspect(mod)} implements only part of the callback group [#{names}] — implement all of them or none (partial coverage silently causes address reuse or double-credit)"
    end
  end

  # Genswarms.Payments.Rpc writes rpc_url verbatim into a curl --config
  # tempfile as `url = "#{rpc_url}"` — a quote lets it close that value
  # early and inject arbitrary curl config directives; a backslash or other
  # control character is equally unsanitary in that file format. Reject at
  # init rather than let it reach curl.
  defp validate_rpc_url!(%{rpc_url: url}) do
    url = to_string(url)

    if String.contains?(url, ["\"", "\\"]) or String.match?(url, ~r/[\x00-\x1f\x7f]/) do
      raise ArgumentError,
            "payments: rpc_url contains a quote, backslash, or control character — refusing (curl --config injection guard)"
    end

    :ok
  end

  defp validate_rpc_url!(_chain), do: :ok

  def handle_message(from, content, state) do
    case Jason.decode(content) do
      {:ok, %{"action" => "health"}} ->
        {:reply,
         Jason.encode!(%{
           ok: true,
           bindings: map_size(state.bindings),
           degraded_boot: state.degraded_boot
         }), state}

      {:ok, %{"action" => "tick"}} ->
        if trusted?(from, state), do: {:noreply, poll(state)}, else: {:noreply, state}

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

        failed_targets =
          Enum.filter(state.targets, fn target ->
            deliver_one(state.deliver_fn, target, state.name, content) == :error
          end)

        state = %{state | seen_keys: MapSet.put(state.seen_keys, key)}

        state =
          if failed_targets == [] do
            state
          else
            Logger.error(
              "payments: #{key} undelivered to #{inspect(failed_targets)} — queued for retry next tick"
            )

            %{
              state
              | undelivered: Map.put(state.undelivered, key, %{targets: failed_targets, content: content})
            }
          end

        {:settled, state}

      {:error, why} ->
        Logger.error("payments: record_payment failed (#{inspect(why)}) — FAIL CLOSED, holding #{key}")
        {:skipped, state}
    end
  end

  # Recorded (settled = durable) is never undone by a delivery failure — this
  # is at-least-once delivery for transient per-target failures. A per-target
  # `catch kind, reason` (not `rescue`) is what actually stops these from
  # escaping record_and_deliver: a `raise` surfaces as :error, but a
  # GenServer-call-timeout-shaped failure surfaces as an EXIT, which `rescue`
  # never catches — it would have escaped the object entirely, dropping the
  # delivery forever (recorded but never delivered; dedup blocks
  # re-presentation). The failed target is queued and retried at the start of
  # every subsequent poll until it succeeds. This is NOT at-least-once across
  # a process crash inside this window — if the process dies between
  # record_payment and reaching this point the queued retry itself is lost
  # with it (see README's delivery section for the honest guarantee).
  defp deliver_one(deliver_fn, target, from, content) do
    try do
      deliver_fn.(target, from, content)
      :ok
    catch
      kind, reason ->
        Logger.error(
          "payments: delivery to #{target} failed (#{kind}: #{inspect(reason)}) — will retry next tick"
        )

        :error
    end
  end

  # Runs at the start of every poll round (never during degraded_boot — see
  # poll/1) so a delivery queued by a previous round's failure gets another
  # shot before this round's own settlements are attempted.
  defp retry_undelivered(state) do
    Enum.reduce(state.undelivered, state, fn {key, %{targets: targets, content: content}}, st ->
      still_failed =
        Enum.filter(targets, fn target ->
          deliver_one(st.deliver_fn, target, st.name, content) == :error
        end)

      undelivered =
        if still_failed == [] do
          Map.delete(st.undelivered, key)
        else
          Map.put(st.undelivered, key, %{targets: still_failed, content: content})
        end

      %{st | undelivered: undelivered}
    end)
  end

  @doc """
  One watch round: every pull method scans, settlements settle, and each
  chain's cursor advances ONLY if all of that chain's settlements settled
  (skipped-by-dedup counts as settled; skipped-by-store-failure does not —
  the fail-closed rule keeps the cursor back so the next round re-presents).
  A no-op during `degraded_boot` (init couldn't establish the true watched
  set — see `init_bindings/1` — so scanning would silently miss deposits and
  advance nobody's cursor; only a restart against a recovered store clears
  it). Otherwise starts by retrying any previously-undelivered targets.
  """
  def poll(%{degraded_boot: true} = state) do
    Logger.error(
      "payments: poll skipped — degraded_boot from a store failure at init; restart once the store recovers"
    )

    state
  end

  def poll(state) do
    state = retry_undelivered(state)

    core = %{
      chains: state.chains,
      rpc_fn: state.rpc_fn,
      bindings: state.bindings,
      get_last_scanned_block: fn chain_name ->
        if state.store_mod && function_exported?(state.store_mod, :get_last_scanned_block, 1) do
          store_result(state.store_mod, :get_last_scanned_block, [chain_name], {:ok, nil})
        else
          {:ok, Map.get(state.cursor_mirror, chain_name)}
        end
      end
    }

    Enum.reduce(state.methods, state, fn method, st ->
      Code.ensure_loaded(method)

      if function_exported?(method, :poll, 2) do
        method_state = Map.get(st.method_states, method, %{})
        {per_chain, new_method_state} = method.poll(method_state, core)

        st = %{st | method_states: Map.put(st.method_states, method, new_method_state)}
        Enum.reduce(per_chain, st, &apply_chain_result/2)
      else
        st
      end
    end)
  end

  # Advances the cursor for one chain's scan result — but only when there was
  # something to advance to (safe_to != nil) AND settle/2 got every one of
  # this chain's settlements durably recorded (or deduped). A settlement
  # STILL held after settle/2 (its idempotency_key missing from seen_keys)
  # means the store failed on it — fail closed, leave the cursor put. The
  # in-memory mirror is updated on this same success path UNCONDITIONALLY —
  # it stands in for durable storage whenever the store is absent or doesn't
  # implement the cursor callbacks, so dev mode doesn't rescan the same
  # window forever.
  defp apply_chain_result({chain, settlements, safe_to}, state) do
    {_n, new_state} = settle(settlements, state)

    held? = Enum.any?(settlements, &(not MapSet.member?(new_state.seen_keys, &1.idempotency_key)))

    if safe_to != nil and not held? do
      new_state = %{new_state | cursor_mirror: Map.put(new_state.cursor_mirror, chain.name, safe_to)}

      case store_write(new_state.store_mod, :put_last_scanned_block, [chain.name, safe_to]) do
        :ok ->
          new_state

        {:error, why} ->
          Logger.error("payments: cursor write failed for #{chain.name}: #{inspect(why)}")
          new_state
      end
    else
      new_state
    end
  end

  defp trusted?(from, state), do: MapSet.member?(state.trusted_sources, to_string(from))

  defp handle_action("deposit_address", %{"beneficiary" => _ben}, %{degraded_boot: true} = state) do
    {:reply, Jason.encode!(%{ok: false, error: "degraded_boot"}), state}
  end

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
