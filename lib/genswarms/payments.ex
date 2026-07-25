defmodule Genswarms.Payments do
  @moduledoc """
  Payment settlement hub object. Owns beneficiary identity (stable HD deposit
  address per beneficiary), the idempotent settlement ledger, and stamped
  `payment_confirmed` delivery to allowlisted targets. Modalities implement
  `Genswarms.Payments.Method`; v1 ships USDC in-tree. Trust is fail-closed:
  no trusted_sources ⇒ nobody can act; no targets ⇒ nobody is credited.

  `deliver_fn` contract: `fn target, from, content -> :ok | {:error, term()}`.
  Only a literal `:ok` return counts as delivered — anything else (an
  `{:error, _}` tuple, a raise, or an EXIT such as a GenServer call timeout)
  is treated as a failure and instrumented. Push is one-shot best-effort;
  the sequenced settlement outbox is the authoritative recovery path.

  Boot is fail-FLAGGED, not fail-crashed: if a configured store's
  `list_address_bindings/0` errors or raises, `init/1` cannot know the true
  watched-address set or the next free HD index, so it sets `degraded_boot:
  true` rather than guessing. While degraded, `poll/1` is a no-op and
  `deposit_address` is refused — it self-heals only via a restart (a
  transient DB blip at pod boot shouldn't crash-loop the object, but it also
  must never scan an empty watched set or hand out a reused address). See
  `init_bindings/1`.

  A hub with non-empty `targets` also refuses to boot unless its store
  exports durable settlement dedup, or `allow_ephemeral: true` explicitly
  accepts restart-volatile address allocation and dedup for development.
  """

  require Logger
  alias Genswarms.Payments.HD

  # Engine contract (Genswarms.Objects.ObjectHandler): init/1 MUST return
  # {:ok, state} — ObjectServer matches on the tuple and a bare map crash-loops
  # the object at swarm boot. init!/1 returns the bare state for tests and
  # embedders that manage state themselves.
  def init(config) do
    try do
      {:ok, init!(config)}
    rescue
      error -> {:error, error}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  @doc false
  def init!(config) do
    xpub =
      case HD.parse_xpub(Map.fetch!(config, :xpub) |> to_string()) do
        {:ok, parsed} -> parsed
        {:error, why} -> raise ArgumentError, "payments: invalid xpub (#{why})"
      end

    store_mod = Map.get(config, :store_mod)
    validate_store_coherence!(store_mod)

    chains = Map.get(config, :chains, [])
    Enum.each(chains, &validate_chain!/1)
    validate_unique_chain_field!(chains, :name)
    validate_unique_chain_field!(chains, :chain_id)

    targets = Map.get(config, :targets, []) |> Enum.map(&to_string/1)
    validate_durable_settlement_store!(store_mod, targets, Map.get(config, :allow_ephemeral))

    {bindings, degraded_boot?} = init_bindings(store_mod)

    next_index =
      bindings |> Map.values() |> Enum.map(& &1.index) |> Enum.max(fn -> -1 end) |> Kernel.+(1)

    %{
      name: Map.get(config, :name, :payments),
      swarm_name: Map.get(config, :swarm_name, "swarm"),
      xpub: xpub,
      trusted_sources:
        MapSet.new(Map.get(config, :trusted_sources, []) |> Enum.map(&to_string/1)),
      targets: targets,
      namespace: Map.get(config, :namespace, "default") |> to_string(),
      store_mod: store_mod,
      deliver_fn:
        Map.get(config, :deliver_fn, default_deliver_fn(Map.get(config, :swarm_name, "swarm"))),
      now_fn: Map.get(config, :now_fn, &DateTime.utc_now/0),
      rpc_fn: Map.get(config, :rpc_fn, &Genswarms.Payments.Rpc.call/3),
      metrics_fn: Map.get(config, :metrics_fn, &default_metrics_fn/2),
      chains: chains,
      methods: Map.get(config, :methods, [Genswarms.Payments.Usdc]),
      method_states: %{},
      auto_tick: Map.get(config, :auto_tick, true),
      poll_interval_ms: Map.get(config, :poll_interval_ms, 60_000),
      bindings: bindings,
      next_index: next_index,
      seen_keys: MapSet.new(),
      settlement_mirror: [],
      next_outbox_seq: 1,
      cursor_mirror: %{},
      degraded_boot: degraded_boot?,
      ephemeral_outbox:
        Map.get(config, :allow_ephemeral) == true and
          not durable_settlement_store?(store_mod)
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
      catch
        kind, reason ->
          Logger.error(
            "payments: list_address_bindings #{kind}-ed at boot (#{inspect(reason)}) — DEGRADED BOOT: polling and allocation refused until restart"
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

  defp validate_durable_settlement_store!(_store_mod, [], _allow_ephemeral), do: :ok

  defp validate_durable_settlement_store!(store_mod, _targets, allow_ephemeral) do
    durable? = durable_settlement_store?(store_mod)

    if not durable? and allow_ephemeral != true do
      raise ArgumentError,
            "payments: non-empty targets require durable settlement dedup; memory mode re-mints addresses and re-credits history on restart — configure a store exporting payment_seen?/1 and record_payment/1, or set allow_ephemeral: true explicitly"
    end

    :ok
  end

  # Genswarms.Payments.Rpc writes rpc_url verbatim into a curl --config
  # tempfile as `url = "#{rpc_url}"` — a quote lets it close that value
  # early and inject arbitrary curl config directives; a backslash or other
  # control character is equally unsanitary in that file format. Reject at
  # init rather than let it reach curl. rpc_url is REQUIRED on every chain —
  # a chain missing the key entirely used to silently pass validation and
  # only blow up later at runtime with a KeyError the first time Rpc.call
  # tried chain.rpc_url; that's now an ArgumentError at init instead.
  defp validate_chain!(chain) do
    case Map.fetch(chain, :chain_id) do
      {:ok, chain_id} when is_integer(chain_id) and chain_id > 0 ->
        :ok

      {:ok, chain_id} when is_integer(chain_id) ->
        raise ArgumentError,
              "payments: chain #{inspect(Map.get(chain, :name, chain))} has non-positive required chain_id: #{inspect(chain_id)}"

      {:ok, chain_id} ->
        raise ArgumentError,
              "payments: chain #{inspect(Map.get(chain, :name, chain))} has non-integer required chain_id: #{inspect(chain_id)}"

      :error ->
        raise ArgumentError,
              "payments: chain #{inspect(Map.get(chain, :name, chain))} is missing required chain_id"
    end

    validate_rpc_url!(chain, :rpc_url, true)
    validate_rpc_url!(chain, :reconcile_rpc_url, false)
  end

  defp validate_unique_chain_field!(chains, field) do
    duplicate =
      Enum.reduce_while(chains, MapSet.new(), fn chain, seen ->
        # Compared in string form: `name: :base` and `name: "base"` interpolate
        # to the same method/cursor text key, so they must count as the same
        # name here. The ORIGINAL value is what the error reports.
        original = Map.get(chain, field)
        value = to_string(original)

        if MapSet.member?(seen, value) do
          {:halt, {:duplicate, original}}
        else
          {:cont, MapSet.put(seen, value)}
        end
      end)

    case duplicate do
      {:duplicate, value} ->
        label = if field == :name, do: "chain name", else: "chain_id"

        raise ArgumentError,
              "payments: duplicate #{label} #{inspect(value)}; every configured chain must have a unique #{field}"

      %MapSet{} ->
        :ok
    end
  end

  defp validate_rpc_url!(chain, field, required?) do
    case Map.fetch(chain, field) do
      {:ok, url} ->
        url = to_string(url)

        if String.contains?(url, ["\"", "\\"]) or String.match?(url, ~r/[\x00-\x1f\x7f]/) do
          raise ArgumentError,
                "payments: #{field} contains a quote, backslash, or control character — refusing (curl --config injection guard)"
        end

        :ok

      :error when required? ->
        raise ArgumentError,
              "payments: chain #{inspect(Map.get(chain, :name, chain))} is missing required #{field}"

      :error ->
        :ok
    end
  end

  @doc """
  Read a durable settlement outbox page without ObjectServer state.

  The store callback is optional. A missing callback returns
  `{:error, :no_outbox_store}`; store errors are returned unchanged. Rows are
  filtered to the configured namespace after the store read, while `next_seq`
  and `complete` describe the unfiltered store page so a foreign-namespace-only
  page still advances the consumer.
  """
  def settlements_since(
        %{store_mod: store_mod, namespace: namespace} = config,
        after_seq,
        limit
      )
      when is_integer(after_seq) and after_seq >= 0 and is_integer(limit) and limit > 0 do
    result =
      if exported?(store_mod, :list_settlements_since, 2) do
        outbox_store_read(store_mod, after_seq, limit)
      else
        {:error, :no_outbox_store}
      end

    case result do
      {:ok, %{settlements: rows, max_seq: max_seq}} ->
        next_seq = highest_seq(rows, after_seq)

        {:ok,
         %{
           settlements: Enum.filter(rows, &namespace_match?(&1, namespace)),
           max_seq: max_seq,
           next_seq: next_seq,
           complete: rows == [] or length(rows) < limit or next_seq >= max_seq
         }}

      {:error, why} = error ->
        emit_metric(
          Map.get(config, :metrics_fn, &default_metrics_fn/2),
          "payments_read_refused",
          %{reason: why, action: "settlements_since"}
        )

        error
    end
  end

  def settlements_since(config, _after_seq, _limit) when is_map(config) do
    emit_metric(
      Map.get(config, :metrics_fn, &default_metrics_fn/2),
      "payments_read_refused",
      %{reason: :bad_request, action: "settlements_since"}
    )

    {:error, :bad_request}
  end

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

      {:ok, %{"action" => action} = msg}
      when action in ~w(deposit_address payment_status ingest_event settlements_since reconcile) ->
        if trusted?(from, state) do
          handle_action(action, msg, state, from)
        else
          Logger.warning("payments: untrusted source #{inspect(from)} sent #{action}")

          if action in ~w(settlements_since reconcile) do
            emit_metric(state, "payments_read_refused", %{
              action: action,
              reason: "untrusted_source"
            })
          end

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

    case {seen_memory?, payment_seen_lookup(state.store_mod, key)} do
      {true, _} ->
        {:skipped, state}

      {_, {:ok, true}} ->
        {:skipped, %{state | seen_keys: MapSet.put(state.seen_keys, key)}}

      {false, {:ok, false}} ->
        record_and_deliver(s, state)

      {false, :no_store} ->
        record_and_deliver(s, state)

      {false, {:error, _why}} ->
        Logger.error("payments: dedup read failed for #{key} — FAIL CLOSED, holding settlement")
        emit_metric(state, "payments_hold", %{idempotency_key: key, stage: "dedup_read"})
        {:skipped, state}
    end
  end

  # NOT-EXPORTED (nil store, or a configured store that simply doesn't
  # implement payment_seen?/1 — legal per validate_store_coherence!/1 when
  # the WHOLE settlement group is absent) means "no durable dedup available",
  # which is exactly the nil-store memory-fallback situation — settle via
  # memory dedup. EXPORTED-BUT-ERRORED (raised, exited, returned {:error, _},
  # or returned any non-boolean — a store answering {:ok, nil} the way
  # Repo.one does on no row cannot answer "seen?" truthfully) is the only
  # case that fails closed. The is_boolean guard matters: a bare {:ok, bool}
  # match binds ANYTHING, so {:ok, nil} would escape settle_one's case as a
  # CaseClauseError and crash-loop the object every tick.
  defp payment_seen_lookup(mod, key) do
    if exported?(mod, :payment_seen?, 1) do
      case store_result(mod, :payment_seen?, [key], {:error, :store_failed}) do
        {:ok, bool} when is_boolean(bool) -> {:ok, bool}
        {:error, why} -> {:error, why}
        _other -> {:error, :store_failed}
      end
    else
      :no_store
    end
  end

  # Same NOT-EXPORTED (memory mode — truthfully has no rows) vs
  # EXPORTED-BUT-ERRORED (refuse rather than fabricate an empty list)
  # distinction as payment_seen_lookup/2, for payment_status's list_payments.
  defp list_payments_lookup(mod, ben) do
    if exported?(mod, :list_payments, 1) do
      case store_result(mod, :list_payments, [ben], {:error, :store_failed}) do
        {:ok, rows} -> {:ok, rows}
        {:error, why} -> {:error, why}
        _other -> {:error, :store_failed}
      end
    else
      :no_store
    end
  end

  defp record_and_deliver(%{idempotency_key: key} = s, state) do
    row =
      s
      |> Map.put(:at, state.now_fn.())
      |> Map.put(:outbox_seq, nil)

    case record_payment_write(state.store_mod, row) do
      result when result in [:memory, :ok] ->
        finish_recorded_settlement(row, s, key, result, state)

      {:ok, seq} = result when is_integer(seq) and seq > 0 ->
        finish_recorded_settlement(row, s, key, result, state)

      {:error, why} ->
        Logger.error(
          "payments: record_payment failed (#{inspect(why)}) — FAIL CLOSED, holding #{key}"
        )

        emit_metric(state, "payments_hold", %{idempotency_key: key, stage: "record_payment"})
        {:skipped, state}
    end
  end

  defp finish_recorded_settlement(row, s, key, result, state) do
    {row, state} = keep_settlement(row, result, state)

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

    state = %{
      state
      | seen_keys: MapSet.put(state.seen_keys, key),
        settlement_mirror: [row | state.settlement_mirror]
    }

    emit_metric(state, "payments_settled", %{idempotency_key: key, outbox_seq: row.outbox_seq})

    Enum.each(state.targets, fn target ->
      deliver_one(state, target, state.name, content, key)
    end)

    {:settled, state}
  end

  defp keep_settlement(row, {:ok, seq}, state) when is_integer(seq) and seq > 0 do
    {Map.put(row, :outbox_seq, seq), state}
  end

  defp keep_settlement(row, :ok, state), do: {row, state}

  defp keep_settlement(row, :memory, state) do
    {Map.put(row, :outbox_seq, state.next_outbox_seq),
     %{state | next_outbox_seq: state.next_outbox_seq + 1}}
  end

  # Push is a one-shot latency optimisation. A failure is isolated per target,
  # logged, and metered; the durable outbox read is the recovery mechanism.
  # `catch` is deliberate: a GenServer timeout is an EXIT, not an exception.
  defp deliver_one(state, target, from, content, idempotency_key) do
    try do
      case state.deliver_fn.(target, from, content) do
        :ok ->
          :ok

        other ->
          Logger.error(
            "payments: one-shot delivery to #{target} returned #{inspect(other)} (not :ok); recover via settlements_since"
          )

          emit_metric(state, "payments_push_failed", %{
            idempotency_key: idempotency_key,
            target: target,
            reason: inspect(other)
          })

          :error
      end
    catch
      kind, reason ->
        Logger.error(
          "payments: one-shot delivery to #{target} failed (#{kind}: #{inspect(reason)}); recover via settlements_since"
        )

        emit_metric(state, "payments_push_failed", %{
          idempotency_key: idempotency_key,
          target: target,
          reason: "#{kind}: #{inspect(reason)}"
        })

        :error
    end
  end

  @doc """
  One watch round: every pull method scans, settlements settle, and each
  chain's cursor advances ONLY if all of that chain's settlements settled
  (skipped-by-dedup counts as settled; skipped-by-store-failure does not —
  the fail-closed rule keeps the cursor back so the next round re-presents).
  A no-op during `degraded_boot` (init couldn't establish the true watched
  set — see `init_bindings/1` — so scanning would silently miss deposits and
  advance nobody's cursor; only a restart against a recovered store clears
  it).
  """
  def poll(%{degraded_boot: true} = state) do
    Logger.error(
      "payments: poll skipped — degraded_boot from a store failure at init; restart once the store recovers"
    )

    emit_metric(state, "payments_hold", %{stage: "degraded_boot"})
    state
  end

  def poll(state) do
    core = %{
      chains: state.chains,
      rpc_fn: state.rpc_fn,
      bindings: state.bindings,
      emit_metric: fn event, meta -> emit_metric(state, event, meta) end,
      get_last_scanned_block: fn chain_name ->
        if exported?(state.store_mod, :get_last_scanned_block, 1) do
          # Default is {:error, _}, NOT {:ok, nil} — a raising/exiting store
          # must never be mistaken for "never scanned", which would silently
          # rescan from start_block every tick (a getLogs storm). {:ok, nil}
          # is reserved for a store that genuinely, successfully, reports no
          # prior cursor.
          store_result(
            state.store_mod,
            :get_last_scanned_block,
            [chain_name],
            {:error, :store_failed}
          )
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
      new_state = %{
        new_state
        | cursor_mirror: Map.put(new_state.cursor_mirror, chain.name, safe_to)
      }

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

  defp handle_action(
         "deposit_address",
         %{"beneficiary" => _ben},
         %{degraded_boot: true} = state,
         _from
       ) do
    emit_metric(state, "payments_hold", %{stage: "degraded_boot", action: "deposit_address"})
    {:reply, Jason.encode!(%{ok: false, error: "degraded_boot"}), state}
  end

  defp handle_action("deposit_address", %{"beneficiary" => ben}, state, _from)
       when is_binary(ben) and ben != "" do
    case ensure_binding(ben, state) do
      {:ok, binding, state} ->
        {:reply,
         Jason.encode!(%{
           ok: true,
           beneficiary: ben,
           address: binding.address,
           namespace: binding.namespace
         }), state}

      {:error, why, state} ->
        {:reply, Jason.encode!(%{ok: false, error: to_string(why)}), state}
    end
  end

  # payment_status is README's reconciliation path — it must refuse rather
  # than fail open. During degraded_boot init couldn't establish the true
  # watched set, so an "ok:true, payments:[]" answer here would be
  # indistinguishable from "genuinely zero payments" when it's really "we
  # don't know". Checked ahead of the generic clause below.
  defp handle_action(
         "payment_status",
         %{"beneficiary" => _ben},
         %{degraded_boot: true} = state,
         _from
       ) do
    {:reply, Jason.encode!(%{ok: false, error: "degraded_boot"}), state}
  end

  defp handle_action("payment_status", %{"beneficiary" => ben}, state, _from)
       when is_binary(ben) do
    case list_payments_lookup(state.store_mod, ben) do
      {:error, _why} ->
        {:reply, Jason.encode!(%{ok: false, error: "store_unavailable"}), state}

      lookup ->
        {rows, durable?} =
          case lookup do
            {:ok, rows} -> {rows, true}
            :no_store -> {[], false}
          end

        payments =
          Enum.map(rows, &(Map.take(&1, [:amount_usd, :method, :ref, :at]) |> stringify()))

        binding = Map.get(state.bindings, ben)

        {:reply,
         Jason.encode!(%{
           ok: true,
           beneficiary: ben,
           address: binding && binding.address,
           payments: payments,
           durable: durable?
         }), state}
    end
  end

  defp handle_action("settlements_since", msg, state, from) do
    cond do
      not Enum.member?(state.targets, to_string(from)) ->
        emit_metric(state, "payments_read_refused", %{
          action: "settlements_since",
          reason: "not_a_consumer"
        })

        {:reply,
         Jason.encode!(%{
           action: "settlements_since",
           ok: false,
           error: "not_a_consumer"
         }), state}

      state.degraded_boot ->
        read_refusal(state, "settlements_since", "degraded_boot")

      true ->
        with {:ok, after_seq} <- action_non_negative_integer(msg, "after_seq", 0),
             {:ok, raw_limit} <- action_non_negative_integer(msg, "limit", 100) do
          limit = clamp_integer(raw_limit, 1, 500)

          case state_outbox_page(state, after_seq, limit) do
            {:ok, %{settlements: rows, next_seq: next_seq, max_seq: max_seq, complete: complete}} ->
              render_reply(state, "settlements_since", %{
                action: "settlements_since",
                ok: true,
                settlements: Enum.map(rows, &json_safe/1),
                next_seq: next_seq,
                max_seq: max_seq,
                complete: complete
              })

            {:error, :no_outbox_store} ->
              read_refusal(state, "settlements_since", "no_outbox_store")

            {:error, :invalid_store_result} ->
              read_refusal(state, "settlements_since", "invalid_store_result")

            {:error, _why} ->
              read_refusal(state, "settlements_since", "store_unavailable")
          end
        else
          {:error, :bad_request} ->
            read_refusal(state, "settlements_since", "bad_request")
        end
    end
  end

  defp handle_action("reconcile", msg, state, _from) do
    cond do
      state.degraded_boot ->
        read_refusal(state, "reconcile", "degraded_boot")

      true ->
        with {:ok, raw_limit} <- action_non_negative_integer(msg, "limit", 50) do
          limit = clamp_integer(raw_limit, 1, 200)
          started_ms = System.monotonic_time(:millisecond)

          case recent_settlements(state, limit) do
            {:ok, rows} ->
              result = reconcile_rows(rows, state, limit)
              elapsed_ms = max(System.monotonic_time(:millisecond) - started_ms, 0)

              render_reply(
                state,
                "reconcile",
                result |> Map.put(:ok, true) |> Map.put(:elapsed_ms, elapsed_ms)
              )

            {:error, :no_outbox_store} ->
              read_refusal(state, "reconcile", "no_outbox_store")

            {:error, :invalid_store_result} ->
              read_refusal(state, "reconcile", "invalid_store_result")

            {:error, _why} ->
              read_refusal(state, "reconcile", "store_unavailable")
          end
        else
          {:error, :bad_request} ->
            read_refusal(state, "reconcile", "bad_request")
        end
    end
  end

  defp handle_action("ingest_event", _msg, state, _from) do
    # Push modalities land in Task 8 (method verifies signature before settle).
    {:reply, Jason.encode!(%{ok: false, error: "no_push_methods"}), state}
  end

  defp handle_action(_, _msg, state, _from),
    do: {:reply, Jason.encode!(%{ok: false, error: "bad_request"}), state}

  # ── outbox read ─────────────────────────────────────────────────────────────

  defp state_outbox_page(state, after_seq, limit) do
    cond do
      exported?(state.store_mod, :list_settlements_since, 2) ->
        durable_namespace_page(state.store_mod, state.namespace, after_seq, limit)

      state.ephemeral_outbox ->
        memory_namespace_page(state, after_seq, limit)

      true ->
        {:error, :no_outbox_store}
    end
  end

  defp durable_namespace_page(store_mod, namespace, after_seq, limit) do
    scan_namespace_page(store_mod, namespace, after_seq, after_seq, limit, [], nil)
  end

  defp scan_namespace_page(
         store_mod,
         namespace,
         original_after,
         cursor,
         limit,
         acc,
         known_max
       ) do
    case outbox_store_read(store_mod, cursor, limit) do
      {:ok, %{settlements: raw_rows, max_seq: max_seq}} ->
        max_seq = if is_integer(known_max), do: max(known_max, max_seq), else: max_seq
        raw_rows = Enum.sort_by(raw_rows, &outbox_seq/1)
        room = limit - length(acc)

        selected =
          raw_rows
          |> Enum.filter(&namespace_match?(&1, namespace))
          |> Enum.filter(&(outbox_seq(&1) > original_after))
          |> Enum.take(room)

        acc = acc ++ selected
        raw_cursor = Enum.reduce(raw_rows, cursor, &max(outbox_seq(&1), &2))

        cond do
          length(acc) == limit ->
            next_seq = highest_seq(acc, original_after)

            with {:ok, more?} <-
                   namespace_exists_after?(store_mod, namespace, next_seq, max_seq, limit) do
              {:ok,
               %{
                 settlements: acc,
                 next_seq: next_seq,
                 max_seq: max_seq,
                 complete: not more?
               }}
            end

          raw_rows == [] or raw_cursor >= max_seq or length(raw_rows) < limit ->
            {:ok,
             %{
               settlements: acc,
               next_seq: highest_seq(acc, original_after),
               max_seq: max_seq,
               complete: true
             }}

          raw_cursor == cursor ->
            {:error, :bad_store_return}

          true ->
            scan_namespace_page(
              store_mod,
              namespace,
              original_after,
              raw_cursor,
              limit,
              acc,
              max_seq
            )
        end

      {:error, _why} = error ->
        error
    end
  end

  defp namespace_exists_after?(_store_mod, _namespace, cursor, max_seq, _limit)
       when cursor >= max_seq,
       do: {:ok, false}

  defp namespace_exists_after?(store_mod, namespace, cursor, max_seq, limit) do
    case outbox_store_read(store_mod, cursor, limit) do
      {:ok, %{settlements: rows, max_seq: observed_max}} ->
        max_seq = max(max_seq, observed_max)

        cond do
          Enum.any?(rows, &namespace_match?(&1, namespace)) ->
            {:ok, true}

          rows == [] ->
            {:ok, false}

          true ->
            next_cursor = Enum.reduce(rows, cursor, &max(outbox_seq(&1), &2))

            if next_cursor == cursor do
              {:error, :bad_store_return}
            else
              namespace_exists_after?(store_mod, namespace, next_cursor, max_seq, limit)
            end
        end

      {:error, _why} = error ->
        error
    end
  end

  defp memory_namespace_page(state, after_seq, limit) do
    rows =
      state.settlement_mirror
      |> Enum.filter(&(is_integer(outbox_seq(&1)) and outbox_seq(&1) > after_seq))
      |> Enum.filter(&namespace_match?(&1, state.namespace))
      |> Enum.sort_by(&outbox_seq/1)

    page = Enum.take(rows, limit)

    {:ok,
     %{
       settlements: page,
       next_seq: highest_seq(page, after_seq),
       max_seq: max(state.next_outbox_seq - 1, 0),
       complete: length(rows) <= limit
     }}
  end

  defp outbox_store_read(store_mod, after_seq, limit) do
    try do
      case apply(store_mod, :list_settlements_since, [after_seq, limit]) do
        {:ok, %{settlements: rows, max_seq: max_seq}}
        when is_list(rows) and is_integer(max_seq) and max_seq >= 0 ->
          if Enum.all?(
               rows,
               &(is_integer(outbox_seq(&1)) and outbox_seq(&1) > after_seq and
                   outbox_seq(&1) <= max_seq)
             ) do
            {:ok, %{settlements: rows, max_seq: max_seq}}
          else
            {:error, :invalid_store_result}
          end

        {:error, _why} = error ->
          error

        _other ->
          {:error, :bad_store_return}
      end
    catch
      kind, reason ->
        Logger.error("payments: store list_settlements_since #{kind}-ed: #{inspect(reason)}")
        {:error, {kind, reason}}
    end
  end

  defp highest_seq(rows, default) do
    Enum.reduce(rows, default, &max(outbox_seq(&1), &2))
  end

  defp outbox_seq(row), do: row_get(row, :outbox_seq)

  defp namespace_match?(row, namespace) do
    case row_get(row, :namespace) do
      nil -> false
      row_namespace -> to_string(row_namespace) == to_string(namespace)
    end
  end

  # ── independent-chain reconciliation ────────────────────────────────────────

  defp recent_settlements(state, limit) do
    cond do
      exported?(state.store_mod, :list_settlements_since, 2) ->
        scan_recent_settlements(state.store_mod, state.namespace, 0, limit, [])

      state.ephemeral_outbox ->
        rows =
          state.settlement_mirror
          |> Enum.filter(&(is_integer(outbox_seq(&1)) and namespace_match?(&1, state.namespace)))
          |> Enum.sort_by(&outbox_seq/1, :desc)
          |> Enum.take(limit)

        {:ok, rows}

      true ->
        {:error, :no_outbox_store}
    end
  end

  # The pinned store seam is forward-only. Reconciliation therefore streams
  # sequenced pages and retains only the newest `limit` namespace rows; this
  # remains correct across sequence gaps and interleaved namespaces.
  defp scan_recent_settlements(store_mod, namespace, cursor, limit, newest) do
    case outbox_store_read(store_mod, cursor, limit) do
      {:ok, %{settlements: rows, max_seq: max_seq}} ->
        newest =
          (newest ++ Enum.filter(rows, &namespace_match?(&1, namespace)))
          |> Enum.sort_by(&outbox_seq/1, :desc)
          |> Enum.take(limit)

        next_cursor = Enum.reduce(rows, cursor, &max(outbox_seq(&1), &2))

        cond do
          rows == [] or next_cursor >= max_seq or length(rows) < limit ->
            {:ok, newest}

          next_cursor == cursor ->
            {:error, :bad_store_return}

          true ->
            scan_recent_settlements(store_mod, namespace, next_cursor, limit, newest)
        end

      {:error, _why} = error ->
        error
    end
  end

  defp reconcile_rows(rows, state, rpc_limit) do
    # Each complete row makes at most one receipt call. Taking the action's
    # clamped limit here makes the wall-clock bound explicit even if a future
    # read implementation accidentally returns too many rows. The production
    # Rpc.call/3 seam retains its existing per-call curl timeout.
    rows
    |> Enum.take(rpc_limit)
    |> Enum.reduce(
      %{checked: 0, drift: [], unverifiable: 0, legacy: 0, incomplete: 0},
      fn row, acc ->
        key = row_get(row, :idempotency_key)

        case chain_fact_status(row) do
          :complete ->
            case reconcile_row(row, state) do
              :ok ->
                %{acc | checked: acc.checked + 1}

              {:drift, reasons} ->
                emit_metric(state, "payments_reconcile_drift", %{
                  idempotency_key: key,
                  reasons: reasons
                })

                Logger.error(
                  "payments: reconciliation drift for #{inspect(key)}: #{Enum.join(reasons, ", ")}"
                )

                %{acc | checked: acc.checked + 1, drift: acc.drift ++ [key]}

              {:unverifiable, reason} ->
                emit_metric(state, "payments_reconcile_unverifiable", %{
                  idempotency_key: key,
                  reason: reason
                })

                Logger.error(
                  "payments: reconciliation unverifiable for #{inspect(key)}: #{inspect(reason)}"
                )

                %{acc | unverifiable: acc.unverifiable + 1}
            end

          :incomplete ->
            missing_facts = missing_chain_facts(row)

            emit_metric(state, "payments_reconcile_incomplete", %{
              idempotency_key: key,
              missing_facts: missing_facts
            })

            Logger.error(
              "payments: incomplete 0.2.0 settlement #{inspect(key)}; missing #{inspect(missing_facts)}"
            )

            %{acc | incomplete: acc.incomplete + 1}

          :legacy ->
            emit_metric(state, "payments_reconcile_unverifiable", %{
              idempotency_key: key,
              reason: "legacy_unverifiable"
            })

            %{acc | legacy: acc.legacy + 1}
        end
      end
    )
  end

  @chain_fact_keys [
    :raw_amount,
    :decimals,
    :token_contract,
    :chain,
    :chain_id,
    :block_number,
    :log_index,
    :tx_hash,
    :from_address
  ]

  defp chain_fact_status(row) do
    cond do
      missing_chain_facts(row) == [] -> :complete
      present_fact?(row_get(row, :chain_id)) -> :incomplete
      true -> :legacy
    end
  end

  defp missing_chain_facts(row),
    do: Enum.reject(@chain_fact_keys, &present_fact?(row_get(row, &1)))

  defp present_fact?(value), do: not is_nil(value) and value != ""

  defp reconcile_row(row, state) do
    case reconcile_chain(row, state.chains) do
      nil ->
        {:unverifiable, :chain_not_configured}

      chain ->
        case Map.get(chain, :reconcile_rpc_url) do
          nil ->
            {:unverifiable, :reconcile_rpc_url_missing}

          reconcile_rpc_url ->
            second_chain = Map.put(chain, :rpc_url, reconcile_rpc_url)

            case state.rpc_fn.(second_chain, "eth_getTransactionReceipt", [
                   row_get(row, :tx_hash)
                 ]) do
              {:ok, receipt} when is_map(receipt) ->
                compare_receipt(row, receipt, state)

              {:ok, nil} ->
                {:drift, ["transaction_missing"]}

              {:ok, _other} ->
                {:drift, ["malformed_receipt"]}

              {:error, why} ->
                {:unverifiable, {:rpc_error, why}}
            end
        end
    end
  catch
    kind, reason -> {:unverifiable, {kind, reason}}
  end

  defp reconcile_chain(row, chains) do
    Enum.find(chains, fn chain ->
      to_string(Map.get(chain, :name)) == to_string(row_get(row, :chain)) and
        normalize_integer(Map.get(chain, :chain_id)) == normalize_integer(row_get(row, :chain_id))
    end)
  end

  defp compare_receipt(row, receipt, state) do
    stored_log_index = normalize_integer(row_get(row, :log_index))

    log =
      receipt
      |> rpc_get(:logs)
      |> case do
        logs when is_list(logs) ->
          Enum.find(logs, &(normalize_integer(rpc_get(&1, :log_index)) == stored_log_index))

        _other ->
          nil
      end

    if is_nil(log) do
      {:drift, ["log_missing"]}
    else
      binding_address =
        state.bindings
        |> Map.get(to_string(row_get(row, :beneficiary)))
        |> case do
          nil -> nil
          binding -> row_get(binding, :address)
        end

      stored_comparisons = [
        {"raw_amount",
         normalize_integer(rpc_get(log, :data)) == normalize_integer(row_get(row, :raw_amount))},
        {"token_contract", same_hex?(rpc_get(log, :address), row_get(row, :token_contract))},
        {"from_address",
         same_hex?(topic_address(rpc_get(log, :topics), 1), row_get(row, :from_address))},
        {"block_number",
         normalize_integer(rpc_get(log, :block_number)) ==
           normalize_integer(row_get(row, :block_number)) and
           normalize_integer(rpc_get(receipt, :block_number)) ==
             normalize_integer(row_get(row, :block_number))},
        {"log_index", normalize_integer(rpc_get(log, :log_index)) == stored_log_index},
        {"tx_hash",
         present_same_hex?(rpc_get(receipt, :transaction_hash), row_get(row, :tx_hash)) and
           present_same_hex?(rpc_get(log, :transaction_hash), row_get(row, :tx_hash))}
      ]

      stored_reasons = for {name, false} <- stored_comparisons, do: name

      cond do
        stored_reasons != [] ->
          {:drift, stored_reasons}

        is_nil(binding_address) ->
          {:unverifiable, :binding_missing}

        not same_hex?(topic_address(rpc_get(log, :topics), 2), binding_address) ->
          {:drift, ["to_address"]}

        true ->
          :ok
      end
    end
  end

  defp topic_address(topics, index) when is_list(topics) do
    case Enum.at(topics, index) do
      topic when is_binary(topic) and byte_size(topic) >= 40 ->
        "0x" <> String.slice(String.downcase(topic), -40, 40)

      _other ->
        nil
    end
  end

  defp topic_address(_topics, _index), do: nil

  defp present_same_hex?(left, right) when is_binary(left) and left != "" and is_binary(right),
    do: same_hex?(left, right)

  defp present_same_hex?(_left, _right), do: false

  defp same_hex?(left, right) when is_binary(left) and is_binary(right),
    do: String.downcase(left) == String.downcase(right)

  defp same_hex?(_left, _right), do: false

  defp normalize_integer(value) when is_integer(value), do: value

  defp normalize_integer("0x" <> hex) do
    case Integer.parse(hex, 16) do
      {value, ""} -> value
      _other -> nil
    end
  end

  defp normalize_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _other -> nil
    end
  end

  defp normalize_integer(_value), do: nil

  # ── telemetry and wire normalization ────────────────────────────────────────

  defp render_reply(state, action, payload) do
    try do
      {:reply, Jason.encode!(payload), state}
    catch
      kind, reason ->
        Logger.error(
          "payments: #{action} reply encoding failed (#{kind}: #{safe_inspect(reason)}); refusing"
        )

        read_refusal(state, action, "encode_failed")
    end
  end

  defp read_refusal(state, action, error) do
    emit_metric(state, "payments_read_refused", %{action: action, reason: error})
    {:reply, Jason.encode!(%{action: action, ok: false, error: error}), state}
  end

  defp emit_metric(%{metrics_fn: metrics_fn}, event, meta),
    do: emit_metric(metrics_fn, event, meta)

  defp emit_metric(metrics_fn, event, meta) when is_function(metrics_fn, 2) do
    try do
      metrics_fn.(event, meta)
    catch
      kind, reason ->
        Logger.error(
          "payments: metrics_fn failed for #{event} (#{kind}: #{inspect(reason)}); money path unaffected"
        )

        :ok
    end
  end

  defp emit_metric(_invalid_metrics_fn, event, meta) do
    emit_metric(&default_metrics_fn/2, event, meta)
  end

  defp default_metrics_fn(event, meta) do
    Logger.info("payments metric #{event}: #{inspect(meta)}")
  end

  defp json_safe(value) do
    try do
      json_safe_value(value)
    catch
      _kind, _reason -> safe_inspect(value)
    end
  end

  defp json_safe_value(%Decimal{} = value), do: Decimal.to_string(value)
  defp json_safe_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_safe_value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp json_safe_value(%Date{} = value), do: Date.to_iso8601(value)
  defp json_safe_value(%Time{} = value), do: Time.to_iso8601(value)
  defp json_safe_value(%_{} = value), do: safe_inspect(value)
  defp json_safe_value(value) when is_list(value), do: Enum.map(value, &json_safe/1)

  defp json_safe_value(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {json_safe_key(key), json_safe(nested)} end)
  end

  defp json_safe_value(value)
       when is_tuple(value) or is_pid(value) or is_function(value) or is_reference(value) or
              is_port(value),
       do: safe_inspect(value)

  defp json_safe_value(value) do
    case Jason.encode(value) do
      {:ok, _json} -> value
      {:error, _reason} -> safe_inspect(value)
    end
  end

  defp json_safe_key(key) when is_atom(key), do: Atom.to_string(key)

  defp json_safe_key(key) when is_binary(key) do
    if String.valid?(key), do: key, else: safe_inspect(key)
  end

  defp json_safe_key(key), do: safe_inspect(key)

  defp safe_inspect(value) do
    try do
      inspect(value)
    catch
      _kind, _reason -> "#Inspect.Error<unrenderable>"
    end
  end

  defp action_non_negative_integer(msg, key, default) do
    case Map.fetch(msg, key) do
      :error -> {:ok, default}
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _invalid} -> {:error, :bad_request}
    end
  end

  defp clamp_integer(value, minimum, maximum),
    do: value |> max(minimum) |> min(maximum)

  defp row_get(nil, _key), do: nil

  defp row_get(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key)))
  end

  defp rpc_get(map, key) do
    case row_get(map, key) do
      nil ->
        camel_key =
          key
          |> Atom.to_string()
          |> String.split("_")
          |> case do
            [head | tail] -> head <> Enum.map_join(tail, &String.capitalize/1)
          end

        Map.get(map, camel_key)

      value ->
        value
    end
  end

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
                Logger.error(
                  "payments: binding persist failed (#{inspect(why)}) — refusing allocation"
                )

                emit_metric(state, "payments_hold", %{
                  stage: "address_binding",
                  beneficiary: ben
                })

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
      catch
        kind, reason ->
          Logger.error("payments: store #{fun} #{kind}-ed: #{inspect(reason)}")
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
      catch
        kind, reason -> {:error, {kind, reason}}
      end
    else
      :ok
    end
  end

  defp record_payment_write(nil, _row), do: :memory

  defp record_payment_write(mod, row) do
    if exported?(mod, :record_payment, 1) do
      try do
        case apply(mod, :record_payment, [row]) do
          :ok -> :ok
          {:ok, seq} when is_integer(seq) and seq > 0 -> {:ok, seq}
          {:error, why} -> {:error, why}
          other -> {:error, {:bad_return, other}}
        end
      catch
        kind, reason -> {:error, {kind, reason}}
      end
    else
      :memory
    end
  end

  # Distinguishes NOT-EXPORTED (falls back to the memory path, same as a nil
  # store) from EXPORTED-BUT-ERRORED (fails closed) for an optional read
  # callback — used by settle_one/2's payment_seen? dedup and
  # payment_status's list_payments so a coherence-legal store implementing
  # only PART of the optional surface (e.g. bindings but not settlement)
  # doesn't get treated as "erroring" forever on the half it doesn't
  # implement.
  defp exported?(nil, _fun, _arity), do: false

  defp exported?(mod, fun, arity) do
    Code.ensure_loaded(mod)
    function_exported?(mod, fun, arity)
  end

  defp durable_settlement_store?(store_mod) do
    exported?(store_mod, :payment_seen?, 1) and exported?(store_mod, :record_payment, 1)
  end

  defp stringify(map), do: Map.new(map, fn {k, v} -> {to_string(k), to_string(v)} end)

  defp default_deliver_fn(swarm_name) do
    # Genswarms.Objects.ObjectServer is a host-provided peer module, not a
    # compile-time dep of this package — dispatch via apply/3 so this module
    # compiles standalone (a direct remote call would warn/fail under
    # --warnings-as-errors since the module isn't available at compile time).
    # The peer call's RESULT is propagated through map_peer_delivery_result/1
    # — hardcoding :ok here would count any error-shaped RETURN from
    # deliver_message as delivered, silently hiding a failed one-shot push
    # from telemetry (raises/EXITs are caught by deliver_one).
    fn target, from, content ->
      Genswarms.Objects.ObjectServer
      |> apply(:deliver_message, [swarm_name, target, from, content])
      |> map_peer_delivery_result()
    end
  end

  @doc false
  # Maps a host-defined ObjectServer.deliver_message/4 return onto the
  # deliver_fn contract (:ok | {:error, term()}). deliver_message's return
  # shape is host-provided and not pinned by this package, so only exactly
  # :ok and {:ok, _} count as delivered; an {:error, _} passes through and
  # anything else becomes {:error, {:bad_return, other}} — deliver_one then
  # logs and meters the failed push. Public (doc: false) so the
  # mapping itself is pinnable by checks without a live ObjectServer.
  def map_peer_delivery_result(:ok), do: :ok
  def map_peer_delivery_result({:ok, _}), do: :ok
  def map_peer_delivery_result({:error, _} = err), do: err
  def map_peer_delivery_result(other), do: {:error, {:bad_return, other}}
end
