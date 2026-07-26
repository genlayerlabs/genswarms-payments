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

  ## Bounded blast radius (C1) and the two depths (C2)

  Every settlement passes two caps. Above `max_payment_usd`, or past
  `max_issuance_per_window_usd` inside the trailing `issuance_window_hours`
  window, it is recorded `quarantined` instead of settled: durable, deduped,
  alarmed, notified to targets as `payment_held`, and never creditable
  (`outbox_seq` stays NULL, AND the outbox read itself drops any row a store
  returns whose status says it is not settled — the read is the path that
  credits, so it defends itself). The aggregate
  cap carries a per-beneficiary `small_topup_usd` carve-out so one whale
  cannot deny everyone else's small top-ups for the rest of the window.

  Credit happens at each chain's `fast_credit_depth` — shallow and fast on
  purpose, bounded by those caps. FINALITY is a separate, queried thing: the
  `reconcile` action asks each chain's `finalized` tag (or
  `{:confirmations, n}`) and reports rows above that head as unfinalized. A
  chain that cannot answer is `finality_unverifiable`, never "finalized".

  ## Boot and first-tick gates

  - A publicly known test xpub is refused at `init/1` unless
    `allow_test_xpub: true` (local rigs only, never mainnet).
  - A binding loaded under a namespace other than this hub's is watched but
    its settlements are HELD — money is never silently re-namespaced.
  - At the first tick per chain, the endpoint must prove itself:
    `eth_chainId` equal to the configured `chain_id` and the token's
    `decimals()` equal to the configured `decimals`. A mismatch or an
    unverifiable answer holds that chain (retried next tick); a pass is cached.

  ## Authorization lane (EIP-3009 → treasury)

  A chain with a configured `treasury_address` gets a second identity: not a
  watched deposit address with one beneficiary, but a shared wallet that
  user-signed EIP-3009 authorizations (minted by a trusted source via the
  `issue_authorization` action) and, eventually, other lanes (a future
  deposit-sweep collection) can BOTH pay into. The hub owns the issued-
  authorization registry end to end (issuance, lookup, consumption) via five
  `Store` callbacks (REQUIRED to boot once a `treasury_address` is set — an
  authorization lane that cannot register a nonce acknowledges money it can
  never credit), and credits a treasury `Transfer` ONLY when EXACTLY ONE
  `AuthorizationUsed` event in the SAME transaction has that Transfer's
  `from` as its `authorizer` AND a nonce this hub issued and can still
  resolve — see `authorization_disposition/2` for the credit rule spec §4.4
  requires: without it, a deposit-sweep collection landing in the same wallet
  would read as a user payment and be credited twice, and a batched
  transaction (anyone may submit an EIP-3009 authorization) could steer one
  user's credit to another. The beneficiary comes from the ISSUED ROW and the
  credit is capped at the amount that row authorized. Anything else — no
  correlation, a foreign nonce, an authorizer that is not the sender, or an
  ambiguous match — lands as an unrecognised inflow: recorded with its
  refusal reason, metered, never credited, never held against the chain's
  cursor.
  """

  require Logger
  alias Genswarms.Payments.HD

  @known_test_xpubs MapSet.new([
                      "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"
                    ])
  @money_pattern ~r/\A(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\z/
  # 32 bytes hex, "0x"-prefixed — an EIP-3009 nonce, and the exact shape
  # usdc.ex's AuthorizationUsed correlation produces (it downcases its side
  # too; normalizing at issue_authorization/6, the one place a nonce ever
  # enters the registry, is what makes that later correlation an exact string
  # match instead of a case-sensitivity trap).
  @nonce_pattern ~r/\A0x[0-9a-fA-F]{64}\z/

  # D3/D4 operator-surface bounds. Held rows inside a `payment_status` answer
  # are a per-beneficiary tail; the standalone queue and the sweep are the
  # operator's paged views. All three are clamped so no operator keystroke can
  # ask the store (or the chain) for unbounded work.
  @quarantined_status_limit 20
  @quarantined_action_limit 100

  # (R4-P4-I6) The sweep is bounded in COUNT and in TIME, and the time bound is
  # the one that matters. Each address is one sequential synchronous RPC inside
  # this object's GenServer callback, and the transport's own deadline is 20s,
  # so a 200-address cap is a 66-minute mailbox stall on an operator keystroke —
  # taken, by construction, exactly when the RPC endpoint is degraded, i.e.
  # during the incident that prompted the sweep. Every `/topup` deposit_address
  # and every chain-scan tick queues behind it.
  #
  # So: a small hard cap AND a wall-clock budget. When the budget is spent the
  # report returns what it measured with `complete: false` and `remaining`, the
  # same honest-partial stance `unreadable` already takes. No new async
  # machinery — the work is bounded, not moved.
  @sweep_address_limit 25
  @sweep_default_limit 10
  @sweep_budget_ms 20_000
  @sweep_row_limit 50

  # Bound on the "another allocator took that HD index" retry (see
  # ensure_binding/2). Deliberately small: it exists to step over indices a
  # concurrent hub has just taken, not to walk a large gap.
  @binding_index_attempts 25

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
    validate_boolean_config!(config, :allow_test_xpub, false)
    validate_boolean_config!(config, :allow_ephemeral, false)

    raw_xpub = Map.fetch!(config, :xpub) |> to_string()
    validate_test_xpub!(raw_xpub, Map.get(config, :allow_test_xpub, false))

    xpub =
      case HD.parse_xpub(raw_xpub) do
        {:ok, parsed} -> parsed
        {:error, why} -> raise ArgumentError, "payments: invalid xpub (#{why})"
      end

    store_mod = Map.get(config, :store_mod)
    validate_store_coherence!(store_mod)

    chains = Map.get(config, :chains, [])
    unless is_list(chains), do: raise(ArgumentError, "payments: chains must be a list")
    Enum.each(chains, &validate_chain!/1)
    validate_unique_chain_field!(chains, :name)
    validate_unique_chain_field!(chains, :chain_id)

    targets = Map.get(config, :targets, []) |> Enum.map(&to_string/1)
    allow_ephemeral? = Map.get(config, :allow_ephemeral) == true
    validate_durable_settlement_store!(store_mod, targets, Map.get(config, :allow_ephemeral))
    validate_authorization_store!(store_mod, chains, allow_ephemeral?)

    namespace = Map.get(config, :namespace, "default") |> to_string()
    metrics_fn = Map.get(config, :metrics_fn, &default_metrics_fn/2)

    max_payment_usd =
      strict_money_config!(config, :max_payment_usd, "10000", allow_nil?: false, positive?: true)

    max_issuance_per_window_usd =
      strict_money_config!(
        config,
        :max_issuance_per_window_usd,
        nil,
        allow_nil?: true,
        positive?: true
      )

    small_topup_usd =
      strict_money_config!(config, :small_topup_usd, "5", allow_nil?: false, positive?: false)

    issuance_window_hours = positive_integer_config!(config, :issuance_window_hours, 24)
    validate_issuance_window_store!(store_mod, max_issuance_per_window_usd)

    trusted = MapSet.new(Map.get(config, :trusted_sources, []) |> Enum.map(&to_string/1))
    operator_sources = validate_operator_sources!(config, trusted)

    max_authorization_window_seconds =
      positive_integer_config!(config, :max_authorization_window_seconds, 3600)

    {bindings, degraded_boot?, foreign_namespace_bindings} =
      init_bindings(store_mod, namespace, metrics_fn)

    validate_treasury_not_bound!(chains, bindings)

    next_index =
      bindings |> Map.values() |> Enum.map(& &1.index) |> Enum.max(fn -> -1 end) |> Kernel.+(1)

    %{
      name: Map.get(config, :name, :payments),
      swarm_name: Map.get(config, :swarm_name, "swarm"),
      xpub: xpub,
      trusted_sources: trusted,
      operator_sources: operator_sources,
      targets: targets,
      namespace: namespace,
      store_mod: store_mod,
      deliver_fn:
        Map.get(config, :deliver_fn, default_deliver_fn(Map.get(config, :swarm_name, "swarm"))),
      now_fn: Map.get(config, :now_fn, &DateTime.utc_now/0),
      rpc_fn: Map.get(config, :rpc_fn, &Genswarms.Payments.Rpc.call/3),
      metrics_fn: metrics_fn,
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
      chain_self_checks: MapSet.new(),
      foreign_namespace_bindings: foreign_namespace_bindings,
      max_payment_usd: max_payment_usd,
      max_issuance_per_window_usd: max_issuance_per_window_usd,
      small_topup_usd: small_topup_usd,
      issuance_window_hours: issuance_window_hours,
      max_authorization_window_seconds: max_authorization_window_seconds,
      allow_ephemeral: allow_ephemeral?,
      # (R4-P4-I6) Wall-clock budget for one sweep_report, in ms. Configurable
      # because the right number is the host's RPC latency times a handful of
      # addresses, not a package constant — but it is bounded either way.
      sweep_budget_ms: positive_integer_config!(config, :sweep_budget_ms, @sweep_budget_ms),
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
  defp init_bindings(nil, _namespace, _metrics_fn), do: {%{}, false, MapSet.new()}

  defp init_bindings(mod, namespace, metrics_fn) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, :list_address_bindings, 0) do
      try do
        case apply(mod, :list_address_bindings, []) do
          {:ok, rows} ->
            bindings = Map.new(rows, fn b -> {b.beneficiary, Map.delete(b, :beneficiary)} end)

            foreign_rows =
              Enum.filter(rows, &(to_string(row_get(&1, :namespace)) != namespace))

            Enum.each(foreign_rows, fn row ->
              Logger.error(
                "payments: binding #{inspect(row_get(row, :beneficiary))} has namespace #{inspect(row_get(row, :namespace))}, configured hub namespace is #{inspect(namespace)} — watching the address but HOLDING its settlements (never re-namespaced silently)"
              )

              emit_metric(metrics_fn, "payments_namespace_mismatch", %{
                stage: "binding_load",
                beneficiary: row_get(row, :beneficiary),
                binding_namespace: row_get(row, :namespace),
                hub_namespace: namespace
              })
            end)

            foreign =
              foreign_rows
              |> Enum.map(&to_string(row_get(&1, :beneficiary)))
              |> MapSet.new()

            {bindings, false, foreign}

          {:error, why} ->
            Logger.error(
              "payments: list_address_bindings failed at boot (#{inspect(why)}) — DEGRADED BOOT: polling and allocation refused until restart"
            )

            {%{}, true, MapSet.new()}
        end
      catch
        kind, reason ->
          Logger.error(
            "payments: list_address_bindings #{kind}-ed at boot (#{inspect(reason)}) — DEGRADED BOOT: polling and allocation refused until restart"
          )

          {%{}, true, MapSet.new()}
      end
    else
      {%{}, false, MapSet.new()}
    end
  end

  # D3: value-affecting and operator-scope actions get their OWN allowlist,
  # never `trusted_sources`. `trusted_sources` is the ordinary bot glue — the
  # cron that ticks the watcher, the consumer that reads the outbox, the
  # command object that mints deposit addresses. Releasing quarantined money
  # (and enumerating every beneficiary's address and balance) is a different
  # kind of authority, so it defaults to the EMPTY set: an operator surface
  # nobody configured is an operator surface nobody has.
  #
  # A source that is operator-but-not-trusted can never act (both gates
  # apply). That is a config mistake, so it is loud at boot rather than
  # silently dead at 3am — but it is not fatal: refusing to boot the money
  # path over a stale name in a list is the worse failure.
  #
  # (R4-P4-I7) WHAT THIS ALLOWLIST IS AND IS NOT, stated plainly so no reader
  # takes it for more than it is. It is a SOURCE-IDENTITY gate: it decides
  # which objects may send operator actions. It is a real barrier against the
  # cron and the outbox consumer, which are on `trusted_sources` and are NOT on
  # this list — neither can release money, by construction.
  #
  # It is NOT a second factor against the object that IS on it. When a host
  # lists an object that also relays ordinary end-user traffic, every message
  # that object sends carries the same source identity, so this hub cannot tell
  # an operator-authorized action from any other action that object was talked
  # into sending. In that shape the REAL control is the caller-side operator
  # gate, and this list only narrows WHICH object holds it. A host that wants
  # two independent factors needs either a dedicated operator-only object
  # between the glue and this hub, or a config-injected shared secret carried in
  # the action payload and validated here — neither is invented for the host,
  # and no combination of the two lists here substitutes for it.
  defp validate_operator_sources!(config, trusted) do
    raw = Map.get(config, :operator_sources, [])

    unless is_list(raw) do
      raise ArgumentError, "payments: operator_sources must be a list of source names"
    end

    operator = MapSet.new(raw, &to_string/1)
    orphans = MapSet.difference(operator, trusted)

    unless MapSet.size(orphans) == 0 do
      Logger.error(
        "payments: operator_sources #{inspect(MapSet.to_list(orphans))} are not in trusted_sources — they can never act (both gates apply); fix the config"
      )
    end

    if MapSet.size(operator) > 0 and MapSet.equal?(operator, trusted) do
      Logger.warning(
        "payments: operator_sources equals trusted_sources — every ordinary caller can release quarantined money; the separation is the point of the second allowlist"
      )
    end

    operator
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

    # The authorization lane's own two round trips. record_issued_authorization
    # without issued_authorization would issue rows nobody could ever look up
    # (the credit rule always sees nil ⇒ every treasury inflow becomes
    # "unrecognised" forever); live_authorization_nonces without
    # mark_authorization_consumed would ask the chain about a nonce set that
    # can never shrink except by expiry. Either half alone is worse than
    # neither, exactly like the two groups above.
    validate_group!(mod, [{:record_issued_authorization, 1}, {:issued_authorization, 1}])
    validate_group!(mod, [{:live_authorization_nonces, 1}, {:mark_authorization_consumed, 1}])

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

  # The authorization lane's five callbacks, as ONE unit. Unlike
  # validate_store_coherence!/1 (which asks "is this store internally
  # consistent?"), this gate asks "can this hub actually do what it is
  # configured to do?" — the same question validate_durable_settlement_store!/3
  # asks about the settlement ledger, and it has the same answer shape.
  @authorization_callbacks [
    {:record_issued_authorization, 1},
    {:issued_authorization, 1},
    {:live_authorization_nonces, 1},
    {:mark_authorization_consumed, 1},
    {:record_unrecognised_inflow, 1}
  ]

  # A chain with `treasury_address` set IS the authorization lane switched on:
  # the watcher adds the treasury to its Transfer filter and runs the §4.4
  # credit rule on everything that lands there. Without a durable registry
  # behind it, that rule can only ever answer "not ours" — so the hub would
  # acknowledge `issue_authorization` with ok:true, the user would sign, money
  # would move on chain, and EVERY payment would be buried as an unrecognised
  # inflow. That is the same class rev-4 already refuses to boot for the
  # settlement ledger ("acked money that cannot be credited"), so it gets the
  # same treatment and the same explicit `allow_ephemeral: true` opt-out.
  defp validate_authorization_store!(store_mod, chains, allow_ephemeral?) do
    lane_on? = Enum.any?(chains, &(not is_nil(Map.get(&1, :treasury_address))))

    missing =
      Enum.reject(@authorization_callbacks, fn {fun, arity} ->
        exported?(store_mod, fun, arity)
      end)

    if lane_on? and missing != [] and not allow_ephemeral? do
      names = Enum.map_join(missing, ", ", fn {fun, arity} -> "#{fun}/#{arity}" end)

      raise ArgumentError,
            "payments: a chain configures treasury_address, which turns ON the authorization lane, but the store does not export #{names} — such a hub answers issue_authorization with ok:true and can never credit the resulting payment (every treasury Transfer lands unrecognised); configure a store implementing all five authorization callbacks, or set allow_ephemeral: true explicitly"
    end

    :ok
  end

  # (I3) A binding whose address IS the treasury address bypasses the credit
  # rule completely: the watcher would resolve that ONE beneficiary for every
  # user's authorization Transfer and settle it as an ordinary deposit, with no
  # nonce involved at all. The hub — not the host — owns this registry
  # precisely so a mis-written row cannot mint a wrong credit, so a collision
  # is a refusal to boot, not a warning. (Usdc.transfer_settlement/6 also
  # resolves the treasury branch FIRST, for a collision written after boot.)
  defp validate_treasury_not_bound!(chains, bindings) do
    treasuries =
      chains
      |> Enum.flat_map(&List.wrap(Map.get(&1, :treasury_address)))
      |> MapSet.new(&String.downcase(to_string(&1)))

    unless MapSet.size(treasuries) == 0 do
      Enum.each(bindings, fn {beneficiary, binding} ->
        address = to_string(Map.get(binding, :address, ""))

        if MapSet.member?(treasuries, String.downcase(address)) do
          raise ArgumentError,
                "payments: beneficiary #{inspect(beneficiary)} is bound to #{inspect(address)}, which is a configured treasury_address — a watched binding resolves BEFORE the treasury credit rule, so every user's authorization payment would be credited to this one beneficiary; repair the binding before booting"
        end
      end)
    end

    :ok
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
    unless is_map(chain), do: raise(ArgumentError, "payments: each chain must be a map")

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
    validate_non_negative_integer!(chain, :confirmations, 12)
    validate_non_negative_integer!(chain, :fast_credit_depth, Map.get(chain, :confirmations, 12))
    validate_non_negative_integer!(chain, :decimals, 6)
    validate_finality!(chain)
    validate_treasury_address!(chain)
  end

  # Optional; nil turns the authorization lane off for this chain (the
  # watcher never adds it to the Transfer filter, never queries
  # AuthorizationUsed logs). Present, it must be a plain non-empty string —
  # it is compared against `topic_address/1`'s downcased output, never fed to
  # curl, so it needs none of `validate_rpc_url!`'s injection guard.
  defp validate_treasury_address!(chain) do
    case Map.get(chain, :treasury_address) do
      nil ->
        :ok

      addr when is_binary(addr) and addr != "" ->
        :ok

      invalid ->
        raise ArgumentError,
              "payments: chain #{inspect(Map.get(chain, :name, chain))} has invalid treasury_address: #{inspect(invalid)}"
    end
  end

  defp validate_finality!(chain) do
    case Map.get(chain, :finality, :finalized) do
      :finalized ->
        :ok

      {:confirmations, confirmations}
      when is_integer(confirmations) and confirmations >= 0 ->
        :ok

      invalid ->
        raise ArgumentError,
              "payments: chain #{inspect(Map.get(chain, :name, chain))} finality must be :finalized or {:confirmations, non_negative_integer}, got: #{inspect(invalid)}"
    end
  end

  defp validate_non_negative_integer!(config, key, default) do
    case Map.get(config, key, default) do
      value when is_integer(value) and value >= 0 ->
        :ok

      invalid ->
        raise ArgumentError,
              "payments: #{key} must be a non-negative integer, got: #{inspect(invalid)}"
    end
  end

  defp validate_boolean_config!(config, key, default) do
    case Map.get(config, key, default) do
      value when is_boolean(value) ->
        :ok

      invalid ->
        raise ArgumentError, "payments: #{key} must be boolean, got: #{inspect(invalid)}"
    end
  end

  defp positive_integer_config!(config, key, default) do
    case Map.get(config, key, default) do
      value when is_integer(value) and value > 0 ->
        value

      invalid ->
        raise ArgumentError,
              "payments: #{key} must be a positive integer, got: #{inspect(invalid)}"
    end
  end

  defp strict_money_config!(config, key, default, opts) do
    value = Map.get(config, key, default)
    allow_nil? = Keyword.fetch!(opts, :allow_nil?)
    positive? = Keyword.fetch!(opts, :positive?)

    cond do
      is_nil(value) and allow_nil? ->
        nil

      not is_binary(value) ->
        raise ArgumentError,
              "payments: #{key} must be a plain non-negative Decimal string#{if allow_nil?, do: " or nil", else: ""}, got: #{inspect(value)}"

      not Regex.match?(@money_pattern, value) ->
        raise ArgumentError,
              "payments: #{key} must be a plain non-negative Decimal string without exponent notation, got: #{inspect(value)}"

      true ->
        decimal = Decimal.new(value)

        if positive? and Decimal.compare(decimal, Decimal.new(0)) != :gt do
          raise ArgumentError, "payments: #{key} must be greater than zero"
        end

        decimal
    end
  end

  # C1's aggregate cap needs the trailing-window issuance total. A durable
  # settlement store owns that history, so a hub configured with the cap over
  # a durable store that cannot answer `issuance_totals_since/3` could only
  # ever fail closed on EVERY settlement — a permanent runtime hold nobody
  # asked for. That is a static configuration error, so it is refused at init
  # (loud, immediate, fixable) instead of at settle time. Memory-mode hubs
  # compute the window from their own settlement mirror and are exempt.
  defp validate_issuance_window_store!(_store_mod, nil), do: :ok

  defp validate_issuance_window_store!(store_mod, %Decimal{}) do
    if durable_settlement_store?(store_mod) and
         not exported?(store_mod, :issuance_totals_since, 3) do
      raise ArgumentError,
            "payments: max_issuance_per_window_usd requires the store to export issuance_totals_since/3 (namespace, beneficiary, since) — without it the aggregate cap can only hold every settlement"
    end

    :ok
  end

  # The denylist's source of truth is the published serialization, but the
  # thing that is compromised is the KEY MATERIAL: every child private key
  # under this xpub is f(the publicly known parent private key, the chain code
  # carried in the clear inside the xpub). Re-serializing the same 33-byte
  # compressed pubkey under a different chain code (or a different version
  # prefix) yields a fresh-looking base58 string whose funds are just as
  # sweepable, so match the decoded pubkey rather than the string. String
  # comparison stays as the cheap first leg (it also catches a string whose
  # decode we would reject anyway).
  defp validate_test_xpub!(xpub, allow_test_xpub?) do
    if allow_test_xpub? != true and known_test_key_material?(xpub) do
      raise ArgumentError,
            "payments: refusing publicly known test xpub (its key material is on the denylist even under a re-encoded serialization); set allow_test_xpub: true only for an explicit local test rig (never on mainnet)"
    end

    :ok
  end

  defp known_test_key_material?(xpub) do
    MapSet.member?(@known_test_xpubs, xpub) or
      case HD.parse_xpub(xpub) do
        {:ok, %{pubkey: pubkey}} -> MapSet.member?(known_test_pubkeys(), pubkey)
        {:error, _why} -> false
      end
  end

  # Decoded on demand from the string list (one entry today): keeping the
  # serializations as the source keeps the denylist auditable against the
  # published test vectors, while the comparison happens on key material.
  # An entry that fails to decode is a denylist bug, not a boot failure for
  # the operator — it simply cannot match anything, and the string leg above
  # still catches its exact serialization.
  defp known_test_pubkeys do
    @known_test_xpubs
    |> Enum.flat_map(fn known ->
      case HD.parse_xpub(known) do
        {:ok, %{pubkey: pubkey}} -> [pubkey]
        {:error, _why} -> []
      end
    end)
    |> MapSet.new()
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
    metrics_fn = Map.get(config, :metrics_fn, &default_metrics_fn/2)

    result =
      if exported?(store_mod, :list_settlements_since, 2) do
        outbox_store_read(store_mod, after_seq, limit, metrics_fn)
      else
        {:error, :no_outbox_store}
      end

    case result do
      {:ok, %{settlements: rows, max_seq: max_seq, raw_count: raw_count, raw_max_seq: raw_max}} ->
        # Paging is described by the RAW store page: a dropped poisoned row
        # must not make the consumer think the page ended or replay a seq.
        next_seq = max(after_seq, raw_max)

        {:ok,
         %{
           settlements: Enum.filter(rows, &namespace_match?(&1, namespace)),
           max_seq: max_seq,
           next_seq: next_seq,
           complete: raw_count == 0 or raw_count < limit or next_seq >= max_seq
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

      # D3 operator actions: trusted AND on the separate operator allowlist.
      # An untrusted or merely-trusted sender gets an explicit refusal (these
      # are typed by a human at a console and a silent drop is indistinguishable
      # from a broken hub) plus a metric — a probe at the release door is worth
      # seeing.
      {:ok, %{"action" => action} = msg}
      when action in ~w(release_payment quarantined sweep_report) ->
        cond do
          not trusted?(from, state) ->
            operator_refusal(state, action, msg, from, "untrusted_source")

          not operator?(from, state) ->
            operator_refusal(state, action, msg, from, "not_an_operator")

          true ->
            handle_action(action, msg, state, from)
        end

      # (M5) KEEP THIS GUARD ON ONE LINE. checks/payments_contract_shape_check.exs
      # derives the set of accepted actions with a regex that requires the
      # whole `action in ~w` list unbroken on ONE line; wrapping it silently
      # empties that derived set. The pin fails loudly, so this is a footgun and
      # not a hole — but the comment is cheaper than rediscovering it.
      {:ok, %{"action" => action} = msg}
      when action in ~w(deposit_address payment_status ingest_event settlements_since reconcile issue_authorization) ->
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
  Settle confirmed payments: durable-dedup each, classify it against the C1
  caps, record it, then deliver the stamped `payment_confirmed` to every
  allowlisted target. FAIL CLOSED: if the configured store errors on the dedup
  read, the cap read, OR the record write, the settlement is skipped this round
  (the watcher will re-present it — the scan cursor only advances on full
  success). A capped settlement is instead recorded `quarantined` and
  announced as `payment_held`: durable and resolved (the cursor may advance),
  but never credited. Returns {settled_count, state} — quarantined rows are
  not settled and do not count.
  """
  def settle(settlements, state) when is_list(settlements) do
    Enum.reduce(settlements, {0, state}, fn s, {n, st} ->
      case dispatch_settlement(s, st) do
        {:settled, st} -> {n + 1, st}
        {:quarantined, st} -> {n, st}
        {:skipped, st} -> {n, st}
      end
    end)
  end

  # ── entry A's credit rule (spec §4.4) ───────────────────────────────────────
  #
  # A `"usdc_authorization"` candidate arrives with `beneficiary`/`namespace`
  # UNRESOLVED (usdc.ex cannot resolve them — the treasury has no single
  # beneficiary, see Usdc.transfer_settlement/5) and, instead, a `nonce_hex`
  # that is either nil (no correlated AuthorizationUsed log at all) or the
  # nonce the correlated log carried. Resolution happens HERE, once, before
  # the settlement ever reaches the ordinary dedup/cap/record pipeline below —
  # every other settlement shape skips straight to it.
  defp dispatch_settlement(%{method: "usdc_authorization", idempotency_key: key} = s, state) do
    if MapSet.member?(state.seen_keys, key) do
      {:skipped, state}
    else
      case authorization_disposition(s, state) do
        {:settle, nonce_hex, beneficiary, authorized_usd} ->
          s
          |> Map.put(:beneficiary, beneficiary)
          |> Map.put(:namespace, state.namespace)
          |> cap_credit_at_authorized(authorized_usd, state)
          |> settle_one(state)
          |> consume_when_resolved(nonce_hex)

        {:ignore, reason} ->
          ignore_inflow(s, state, reason)
      end
    end
  end

  defp dispatch_settlement(s, state), do: settle_one(s, state)

  # Credit ONLY an inflow whose nonce THIS hub issued and can still look up.
  # Without this rule the treasury wallet's Transfers would be indistinguishable
  # from any watched deposit address's — and the future deposit-sweep lane
  # (Task 6) sends its own on-chain-authorized collections to this SAME
  # wallet, which would then read as a user payment and get credited a second
  # time for money already credited once. `nil` covers both "no correlated
  # AuthorizationUsed log at all" and "a nonce that correlates to a DIFFERENT
  # domain's authorization" identically — `issued_authorization/1` only ever
  # answers for nonces THIS hub minted via `issue_authorization`.
  # A SINGLE refusal clause, deliberately: "no correlated nonce at all", "a
  # nonce that correlates to something we never issued" and "a nonce whose
  # authorizer was not this Transfer's sender" ALL produce an empty match list
  # and fall through the SAME `[] ->` clause below, because they are the same
  # fact from this hub's point of view — "not ours" — and must refuse
  # identically. Splitting them into separate refusal branches would let a
  # future edit fix one path's mutation and miss the other's.
  #
  # AMBIGUITY FAILS CLOSED, it never guesses. Two issued authorizations whose
  # authorizer both equal this Transfer's `from`, or two treasury Transfers in
  # one tx sharing a `from` (`:authorization_conflict`, set by
  # Usdc.mark_correlation_conflicts/1), leave the correlation genuinely
  # undecidable — and log order inside a batched tx is attacker-controlled, so
  # "pick the first" is a money bug wearing a heuristic. Refused as
  # unrecognised: an operator can repair an under-credit; nobody can un-pay a
  # wrong beneficiary.
  defp authorization_disposition(inflow, state) do
    if row_get(inflow, :authorization_conflict) == true do
      {:ignore, :ambiguous_correlation}
    else
      case issued_authorization_matches(inflow, state) do
        [{nonce_hex, issued}] ->
          authorized_credit(nonce_hex, issued)

        [] ->
          {:ignore, :not_issued}

        _ambiguous ->
          {:ignore, :ambiguous_correlation}
      end
    end
  end

  # Every nonce this Transfer could belong to (see Usdc.treasury_inflow/4:
  # AuthorizationUsed events in the SAME tx whose `authorizer` is this
  # Transfer's `from`), narrowed to the ones THIS hub actually issued and can
  # still resolve. `issued_authorization/1` answering nil is what refuses a
  # stranger's Transfer, including another domain's own on-chain authorization
  # landing in the same wallet.
  #
  # `nonce_candidates` is what the in-tree watcher supplies; the `nonce_hex`
  # fallback keeps the public `settle/2` usable by a host pushing a
  # single-correlation row of its own (it gets the registry check, but the
  # authorizer check is the watcher's to make — it needs the tx's other logs).
  defp issued_authorization_matches(inflow, state) do
    inflow
    |> authorization_candidates()
    |> Enum.map(&{&1, issued_authorization_lookup(&1, state)})
    |> Enum.filter(fn {_nonce_hex, issued} -> is_map(issued) end)
  end

  defp authorization_candidates(inflow) do
    case row_get(inflow, :nonce_candidates) do
      candidates when is_list(candidates) -> Enum.uniq(candidates)
      _absent -> inflow |> row_get(:nonce_hex) |> List.wrap()
    end
  end

  # The beneficiary comes from the ISSUED ROW, keyed by nonce — never from log
  # position, never from the Transfer's own `from`. A row that cannot supply a
  # usable beneficiary or a usable authorized amount fails closed like every
  # other store defect in this module: crediting `nil`, or crediting an
  # unbounded amount because the cap could not be read, are both worse than an
  # unrecognised inflow an operator reissues by hand.
  defp authorized_credit(nonce_hex, issued) do
    beneficiary = row_get(issued, :beneficiary)

    case {is_binary(beneficiary) and beneficiary != "", issued_amount_usd(issued)} do
      {true, {:ok, authorized}} -> {:settle, nonce_hex, beneficiary, authorized}
      _unusable -> {:ignore, :issued_row_unusable}
    end
  end

  defp issued_amount_usd(issued) do
    case row_get(issued, :amount_usd) do
      %Decimal{} = amount ->
        if Decimal.compare(amount, Decimal.new(0)) == :gt, do: {:ok, amount}, else: :error

      amount when is_binary(amount) ->
        if Regex.match?(@money_pattern, amount),
          do: issued_amount_usd(%{amount_usd: Decimal.new(amount)}),
          else: :error

      _invalid ->
        :error
    end
  end

  # (I2) A correct `receiveWithAuthorization` moves EXACTLY the signed value,
  # so `moved > authorized` is not a generosity to pass on — it is proof that
  # either the correlation or the token contract is not what this hub assumes.
  # Credit the authorized amount (never more than the user signed for) and make
  # the anomaly loud: it is also the cheapest independent detector for a
  # mis-correlation that slipped every check above.
  defp cap_credit_at_authorized(row, authorized, state) do
    moved = row_get(row, :amount_usd)

    if match?(%Decimal{}, moved) and Decimal.compare(moved, authorized) == :gt do
      key = row_get(row, :idempotency_key)

      Logger.error(
        "payments: treasury inflow #{key} moved #{money_text(moved)} against an authorization for #{money_text(authorized)} — crediting the AUTHORIZED amount; a Transfer exceeding its own authorization means the correlation or the token contract is not what this hub assumes"
      )

      emit_metric(state, "payments_authorization_overpay", %{
        idempotency_key: key,
        moved_usd: money_text(moved),
        authorized_usd: money_text(authorized)
      })

      row
      |> Map.put(:amount_usd, authorized)
      |> Map.put(:moved_amount_usd, moved)
      |> Map.put(:credit_capped, true)
    else
      row
    end
  end

  # (C1) Consumption is marked ONLY once the settlement is durably resolved —
  # `:settled` or `:quarantined`, both of which are recorded rows the dedup
  # will recognise forever. A HELD settlement (store blip on the dedup read,
  # the record, or the cap evaluation) must leave the nonce LIVE: the whole
  # fail-closed design assumes the chain's cursor stays put and the Transfer is
  # re-presented next tick, but re-presentation is only lossless if the
  # AuthorizationUsed leg is still fetched — and that leg is filtered on
  # `live_authorization_nonces`. Retiring the nonce first turned every held
  # authorization settlement into a permanent under-credit: the retry arrived
  # with no correlation at all and was buried as unrecognised while the cursor
  # advanced past it.
  defp consume_when_resolved({outcome, state} = result, nonce_hex)
       when outcome in [:settled, :quarantined] do
    mark_nonce_consumed(nonce_hex, state)
    result
  end

  defp consume_when_resolved(result, _nonce_hex), do: result

  defp issued_authorization_lookup(nil, _state), do: nil

  defp issued_authorization_lookup(nonce_hex, state) do
    if exported?(state.store_mod, :issued_authorization, 1) do
      store_result(state.store_mod, :issued_authorization, [nonce_hex], nil)
    else
      nil
    end
  end

  # Retires the nonce from the watcher's live filter once the linked
  # settlement is durably resolved — independent of whether the caps settled
  # or quarantined it (see Store.mark_authorization_consumed/1's doc; the
  # nonce is spent ON CHAIN either way, and this hub's cap policy is a
  # separate decision made afterwards). Best-effort: a failure here cannot
  # double-credit (the settlement dedups on its own idempotency_key
  # regardless), so it is logged rather than held.
  defp mark_nonce_consumed(nonce_hex, state) do
    case store_write(state.store_mod, :mark_authorization_consumed, [nonce_hex]) do
      :ok ->
        :ok

      {:error, why} ->
        Logger.error(
          "payments: mark_authorization_consumed failed for #{nonce_hex} (#{inspect(why)}) — nonce stays in the live filter until it expires; no double-credit risk"
        )
    end
  end

  # Not a payment: never recorded as settled or quarantined, never delivered.
  # The key is still added to seen_keys so the chain's cursor can advance past
  # it exactly as it would past a settled or quarantined row — holding the
  # WHOLE chain hostage to routine treasury noise (a deposit-sweep collection,
  # a stray transfer) would also block every genuine deposit-address payment
  # scanned in the same round.
  defp ignore_inflow(%{idempotency_key: key} = s, state, reason) do
    row = Map.put(unrecognised_row(s, state), :reason, reason_text(reason))

    case store_write(state.store_mod, :record_unrecognised_inflow, [row]) do
      :ok ->
        :ok

      {:error, why} ->
        Logger.error(
          "payments: record_unrecognised_inflow failed (#{inspect(why)}) for #{key} — logged only; nothing here was ever creditable, so the chain's progress is not held on it"
        )
    end

    emit_metric(state, "payments_unrecognised_inflow", %{
      chain: to_string(row_get(s, :chain)),
      idempotency_key: key,
      reason: reason_text(reason),
      # M2: `settle/2` is public and push modalities are the declared future —
      # a non-Decimal amount on a "usdc_authorization" row must not RAISE out
      # of settle/2 here while every other method holds it as :invalid_amount.
      amount_usd: money_text(row_get(s, :amount_usd)),
      nonce_hex: row_get(s, :nonce_hex)
    })

    {:skipped, %{state | seen_keys: MapSet.put(state.seen_keys, key)}}
  end

  defp unrecognised_row(s, state) do
    %{
      idempotency_key: row_get(s, :idempotency_key),
      chain: row_get(s, :chain),
      chain_id: row_get(s, :chain_id),
      tx_hash: row_get(s, :tx_hash),
      log_index: row_get(s, :log_index),
      block_number: row_get(s, :block_number),
      amount_usd: row_get(s, :amount_usd),
      raw_amount: row_get(s, :raw_amount),
      decimals: row_get(s, :decimals),
      token_contract: row_get(s, :token_contract),
      from_address: row_get(s, :from_address),
      nonce_hex: row_get(s, :nonce_hex),
      # The full candidate set, not only the single-candidate mirror: for the
      # ambiguous refusals it is the ONLY record of what the hub declined to
      # guess between, and that is exactly the row an operator reads back.
      nonce_candidates: authorization_candidates(s),
      namespace: state.namespace,
      at: state.now_fn.()
    }
  end

  defp settle_one(%{idempotency_key: key} = s, state) do
    if foreign_namespace_binding?(s, state) do
      Logger.error(
        "payments: settlement #{key} belongs to binding #{inspect(row_get(s, :beneficiary))}, loaded under namespace #{inspect(row_get(s, :namespace))} while this hub is #{inspect(state.namespace)} — HELD (fix the binding's namespace; it is never re-namespaced here)"
      )

      emit_metric(state, "payments_namespace_mismatch", %{
        stage: "settle",
        idempotency_key: key,
        beneficiary: row_get(s, :beneficiary),
        binding_namespace: row_get(s, :namespace),
        hub_namespace: state.namespace
      })

      {:skipped, state}
    else
      seen_memory? = MapSet.member?(state.seen_keys, key)

      case {seen_memory?, payment_seen_lookup(state.store_mod, key)} do
        {true, _} ->
          {:skipped, state}

        {_, {:ok, true}} ->
          {:skipped, %{state | seen_keys: MapSet.put(state.seen_keys, key)}}

        {false, {:ok, false}} ->
          classify_and_record(s, state)

        {false, :no_store} ->
          classify_and_record(s, state)

        {false, {:error, _why}} ->
          Logger.error("payments: dedup read failed for #{key} — FAIL CLOSED, holding settlement")
          emit_metric(state, "payments_hold", %{idempotency_key: key, stage: "dedup_read"})
          {:skipped, state}
      end
    end
  end

  # D2: a binding loaded at boot under a namespace that is not this hub's is
  # watched (its address stays visible, money is never invisible) but its
  # settlements are HELD — crediting them would silently re-namespace someone
  # else's money. Held, not dropped: the cursor stays put and the alarm
  # repeats every tick until an operator repairs the binding.
  defp foreign_namespace_binding?(settlement, state) do
    MapSet.member?(
      state.foreign_namespace_bindings,
      to_string(row_get(settlement, :beneficiary))
    )
  end

  defp classify_and_record(s, state) do
    case settlement_disposition(s, state) do
      :settled ->
        record_settlement(s, "settled", nil, state)

      {:quarantined, reason} ->
        record_settlement(s, "quarantined", reason, state)

      {:hold, why} ->
        key = row_get(s, :idempotency_key)

        Logger.error(
          "payments: cap evaluation failed for #{key} (#{inspect(why)}) — FAIL CLOSED, holding settlement"
        )

        emit_metric(state, "payments_hold", %{
          idempotency_key: key,
          stage: "cap_evaluation",
          reason: reason_text(why)
        })

        {:skipped, state}
    end
  end

  defp settlement_disposition(s, state) do
    amount = row_get(s, :amount_usd)

    cond do
      not match?(%Decimal{}, amount) ->
        {:hold, :invalid_amount}

      # A non-positive amount is never a payment. Zero is already filtered by
      # the in-tree watcher and a negative one cannot come off a uint256, but
      # `settle/2` is public and push modalities are the declared future: a
      # negative Decimal would clear BOTH caps (never `:gt`) and then, recorded
      # "settled", SUBTRACT from the trailing-window totals — widening the
      # window for a later over-credit. Held, not quarantined: it is a
      # malformed input, not a policy decision about real money.
      Decimal.compare(amount, Decimal.new(0)) != :gt ->
        {:hold, :invalid_amount}

      Decimal.compare(amount, state.max_payment_usd) == :gt ->
        {:quarantined, :max_payment}

      is_nil(state.max_issuance_per_window_usd) ->
        :settled

      true ->
        case issuance_totals(state, row_get(s, :beneficiary)) do
          {:ok, %{total_usd: total, beneficiary_usd: beneficiary_total}} ->
            within_carve_out? =
              Decimal.compare(
                Decimal.add(beneficiary_total, amount),
                state.small_topup_usd
              ) != :gt

            exceeds_aggregate? =
              Decimal.compare(
                Decimal.add(total, amount),
                state.max_issuance_per_window_usd
              ) == :gt

            if exceeds_aggregate? and not within_carve_out?,
              do: {:quarantined, :aggregate},
              else: :settled

          {:error, why} ->
            {:hold, why}
        end
    end
  end

  # The trailing-window totals come from the durable ledger when the store can
  # answer for it, and from the in-memory mirror otherwise (memory mode, which
  # is only reachable for a crediting hub behind the explicit allow_ephemeral
  # opt-out). A durable store without the callback is refused at init; the
  # branch here is defense in depth for a store swapped in after boot.
  defp issuance_totals(state, beneficiary) do
    since = DateTime.add(state.now_fn.(), -state.issuance_window_hours * 3600, :second)

    cond do
      exported?(state.store_mod, :issuance_totals_since, 3) ->
        state.store_mod
        |> store_result(
          :issuance_totals_since,
          [state.namespace, to_string(beneficiary), since],
          {:error, :store_failed}
        )
        |> validate_issuance_totals()

      durable_settlement_store?(state.store_mod) ->
        emit_metric(state, "payments_store_version_skew", %{
          stage: "issuance_totals_since",
          reason: "callback_missing"
        })

        {:error, :issuance_totals_callback_missing}

      true ->
        memory_issuance_totals(state, beneficiary, since)
    end
  end

  defp validate_issuance_totals(
         {:ok, %{total_usd: %Decimal{} = total, beneficiary_usd: %Decimal{} = per_beneficiary}}
       ) do
    zero = Decimal.new(0)

    if Decimal.compare(total, zero) != :lt and Decimal.compare(per_beneficiary, zero) != :lt do
      {:ok, %{total_usd: total, beneficiary_usd: per_beneficiary}}
    else
      {:error, :invalid_store_result}
    end
  end

  defp validate_issuance_totals({:error, why}), do: {:error, why}
  defp validate_issuance_totals(_invalid), do: {:error, :invalid_store_result}

  defp memory_issuance_totals(state, beneficiary, since) do
    settled =
      Enum.filter(state.settlement_mirror, fn row ->
        row_get(row, :status) == "settled" and
          namespace_match?(row, state.namespace) and
          match?(%DateTime{}, row_get(row, :at)) and
          DateTime.compare(row_get(row, :at), since) != :lt
      end)

    total =
      Enum.reduce(settled, Decimal.new(0), fn row, acc ->
        Decimal.add(acc, row_get(row, :amount_usd))
      end)

    beneficiary_total =
      settled
      |> Enum.filter(&(to_string(row_get(&1, :beneficiary)) == to_string(beneficiary)))
      |> Enum.reduce(Decimal.new(0), fn row, acc ->
        Decimal.add(acc, row_get(row, :amount_usd))
      end)

    {:ok, %{total_usd: total, beneficiary_usd: beneficiary_total}}
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

  defp record_settlement(%{idempotency_key: key} = s, status, reason, state) do
    row =
      s
      |> Map.put(:at, state.now_fn.())
      |> Map.put(:outbox_seq, nil)
      |> Map.put(:status, status)
      # D3: the cap decision travels WITH the row, not only into a metric and a
      # one-shot cast. The operator queue is read back days later ("why is this
      # $120 held?"), and a queue that cannot answer that question sends the
      # operator to a log search. Stores keep unknown keys as audit facts.
      |> maybe_put_quarantine_reason(status, reason)

    case record_payment_write(state.store_mod, row) do
      result when result in [:memory, :ok] ->
        finish_recorded_settlement(row, s, key, result, reason, state)

      {:ok, seq} = result when is_integer(seq) and seq > 0 ->
        if status == "settled" do
          finish_recorded_settlement(row, s, key, result, reason, state)
        else
          # A3: the sequence marks CREDITABLE, not recorded. A store that mints
          # one for a quarantined row would publish it to the outbox and credit
          # exactly what the cap just refused — alarm and hold rather than
          # deliver. The row is already durable, so dedup skips it next tick and
          # the cursor is free again; only an operator can repair the store.
          Logger.error(
            "payments: store assigned outbox sequence #{seq} to QUARANTINED row #{key} — quarantined rows must carry a NULL sequence; holding"
          )

          emit_metric(state, "payments_store_version_skew", %{
            idempotency_key: key,
            attempted_status: status,
            reason: "quarantine_sequence_assigned"
          })

          {:skipped, state}
        end

      {:error, why} ->
        # A store older than this hub can reject a status it has no column for.
        # That is a version skew, not a payment problem: hold (fail closed, the
        # watcher re-presents next tick) and make the skew visible.
        if why == :unsupported_status do
          emit_metric(state, "payments_store_version_skew", %{
            idempotency_key: key,
            attempted_status: status,
            reason: "unsupported_status"
          })
        end

        Logger.error(
          "payments: record_payment failed (#{inspect(why)}) — FAIL CLOSED, holding #{key}"
        )

        emit_metric(state, "payments_hold", %{idempotency_key: key, stage: "record_payment"})
        {:skipped, state}
    end
  end

  defp maybe_put_quarantine_reason(row, "quarantined", reason) when not is_nil(reason),
    do: Map.put(row, :quarantine_reason, reason_text(reason))

  defp maybe_put_quarantine_reason(row, _status, _reason), do: row

  defp finish_recorded_settlement(
         %{status: "settled"} = row,
         s,
         key,
         result,
         _reason,
         state
       ) do
    {row, state} = keep_creditable_settlement(row, result, state)

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
      deliver_one(state, target, state.name, content, key, "payment_confirmed")
    end)

    {:settled, state}
  end

  # C1: a quarantined row is durable, deduped, alarmed — and never creditable.
  # It keeps a NULL outbox_seq (A3), so it is invisible to the outbox read and
  # to reconciliation until an operator releases it (phase 4). The
  # `payment_held` cast is the user-visible notice hook: best effort, because
  # the operator queue — not this message — is the authoritative record. It
  # carries the SAME stamp as `payment_confirmed` (method, namespace, at) plus
  # the reason: a consumer must be able to reject a foreign-namespace hold and
  # to key the hold under the very `"<method>:<ref>"` string the eventual
  # release will credit under — a shorter payload forces it to guess.
  defp finish_recorded_settlement(
         %{status: "quarantined"} = row,
         s,
         key,
         _result,
         reason,
         state
       ) do
    state = %{
      state
      | seen_keys: MapSet.put(state.seen_keys, key),
        settlement_mirror: [row | state.settlement_mirror]
    }

    emit_metric(state, "payments_quarantined", %{
      idempotency_key: key,
      beneficiary: s.beneficiary,
      amount_usd: Decimal.to_string(s.amount_usd),
      reason: Atom.to_string(reason)
    })

    Logger.error(
      "payments: QUARANTINED #{key} (#{reason}) — #{Decimal.to_string(s.amount_usd)} USD for #{inspect(s.beneficiary)} recorded, NOT credited; release is an operator action"
    )

    content =
      Jason.encode!(%{
        action: "payment_held",
        beneficiary: s.beneficiary,
        amount_usd: Decimal.to_string(s.amount_usd),
        method: s.method,
        ref: s.ref,
        namespace: s.namespace,
        at: DateTime.to_iso8601(row.at),
        reason: Atom.to_string(reason)
      })

    Enum.each(state.targets, fn target ->
      deliver_one(state, target, state.name, content, key, "payment_held")
    end)

    {:quarantined, state}
  end

  defp keep_creditable_settlement(row, {:ok, seq}, state)
       when is_integer(seq) and seq > 0 do
    {Map.put(row, :outbox_seq, seq), state}
  end

  defp keep_creditable_settlement(row, :ok, state), do: {row, state}

  defp keep_creditable_settlement(row, :memory, state) do
    {Map.put(row, :outbox_seq, state.next_outbox_seq),
     %{state | next_outbox_seq: state.next_outbox_seq + 1}}
  end

  # Push is a one-shot latency optimisation. A failure is isolated per target,
  # logged, and metered; the durable outbox read is the recovery mechanism for
  # `payment_confirmed`, and the operator queue for `payment_held`.
  # `catch` is deliberate: a GenServer timeout is an EXIT, not an exception.
  defp deliver_one(state, target, from, content, idempotency_key, action) do
    try do
      case state.deliver_fn.(target, from, content) do
        :ok ->
          :ok

        other ->
          Logger.error(
            "payments: one-shot #{action} delivery to #{target} returned #{inspect(other)} (not :ok); recover via settlements_since"
          )

          emit_metric(state, "payments_push_failed", %{
            action: action,
            idempotency_key: idempotency_key,
            target: target,
            reason: inspect(other)
          })

          :error
      end
    catch
      kind, reason ->
        Logger.error(
          "payments: one-shot #{action} delivery to #{target} failed (#{kind}: #{inspect(reason)}); recover via settlements_since"
        )

        emit_metric(state, "payments_push_failed", %{
          action: action,
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
    {chains, state} = self_checked_chains(state)

    core = %{
      chains: chains,
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
      end,
      # Entry A leg 2's nonce filter: issued ∧ unconsumed ∧ unexpired, as of
      # THIS tick's clock. Not exported ⇒ the authorization lane is simply
      # off (no store to own the registry) — every treasury chain's watcher
      # query for it is skipped, same shortcut as an empty watched set. An
      # EXPORTED-but-erroring read fails the WHOLE chain's scan round for
      # this tick, exactly like get_last_scanned_block above — a store that
      # cannot say which nonces are live must never let the watcher silently
      # ask for none.
      live_authorization_nonces: fn ->
        if exported?(state.store_mod, :live_authorization_nonces, 1) do
          case store_result(
                 state.store_mod,
                 :live_authorization_nonces,
                 [DateTime.to_unix(state.now_fn.())],
                 {:error, :store_failed}
               ) do
            list when is_list(list) -> {:ok, list}
            {:error, why} -> {:error, why}
            other -> {:error, {:bad_return, other}}
          end
        else
          {:ok, []}
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

  # ── D4: on-chain self-check ─────────────────────────────────────────────────
  # Before a chain is ever scanned, the endpoint must PROVE it is the chain
  # the config claims: `eth_chainId` equal to the configured `chain_id`, and —
  # for a chain with a token contract — that contract's `decimals()` equal to
  # the configured `decimals`. Both are silent money bugs otherwise: the wrong
  # endpoint credits deposits that never arrived on the intended chain, and a
  # decimals mismatch mis-scales every amount by orders of magnitude.
  #
  # It runs at the first tick rather than at init because an RPC may simply be
  # down at boot, and a boot-time refusal would crash-loop the object over a
  # transient outage. A failed or unverifiable check HOLDS that chain only (no
  # scanning, no settling) and is retried next tick, so a healed RPC recovers
  # by itself; a passed check is cached per chain and never re-issued.
  defp self_checked_chains(state) do
    Enum.reduce(state.chains, {[], state}, fn chain, {kept, st} ->
      key = to_string(Map.get(chain, :name))

      if MapSet.member?(st.chain_self_checks, key) do
        {kept ++ [chain], st}
      else
        case self_check_chain(chain, st) do
          :ok ->
            {kept ++ [chain], %{st | chain_self_checks: MapSet.put(st.chain_self_checks, key)}}

          {:error, reason, meta} ->
            Logger.error(
              "payments: chain #{key} failed its on-chain self-check (#{reason}: #{inspect(meta)}) — chain HELD this tick (no scanning, no settling); retried next tick"
            )

            emit_metric(
              st,
              "payments_chain_self_check_failed",
              Map.merge(%{chain: key, reason: reason_text(reason)}, meta)
            )

            {kept, st}
        end
      end
    end)
  end

  @decimals_selector "0x313ce567"

  defp self_check_chain(chain, state) do
    with {:ok, observed_chain_id} <- self_check_rpc(chain, state, "eth_chainId", []),
         :ok <- compare_chain_id(chain, observed_chain_id) do
      check_decimals(chain, state)
    end
  end

  defp compare_chain_id(chain, observed) do
    configured = Map.get(chain, :chain_id)

    if normalize_integer(observed) == configured do
      :ok
    else
      {:error, :chain_id_mismatch, %{configured: configured, observed: inspect(observed)}}
    end
  end

  # A chain with no token contract has nothing whose decimals could disagree
  # (no in-tree method can convert money on it); chain_id alone gates it.
  defp check_decimals(chain, state) do
    case Map.get(chain, :usdc_contract) do
      nil ->
        :ok

      contract ->
        params = [%{"to" => contract, "data" => @decimals_selector}, "latest"]

        with {:ok, observed} <- self_check_rpc(chain, state, "eth_call", params) do
          configured = Map.get(chain, :decimals, 6)

          # "The call came back with nothing readable" and "the token says a
          # different number" are the same hold but VERY different operator
          # work: the first is a broken/mis-pointed endpoint (a proxy address
          # with no code answers `0x`), the second is a wrong config that will
          # mis-scale every amount. Label them apart.
          case normalize_integer(observed) do
            nil ->
              {:error, :decimals_unverifiable,
               %{
                 method: "eth_call",
                 configured: configured,
                 detail: "unparseable decimals() answer: #{inspect(observed)}",
                 token_contract: to_string(contract)
               }}

            ^configured ->
              :ok

            other ->
              {:error, :decimals_mismatch,
               %{
                 configured: configured,
                 observed: inspect(observed),
                 observed_decimals: other,
                 token_contract: to_string(contract)
               }}
          end
        end
    end
  end

  # Any RPC trouble — an {:error, _}, a raise, an EXIT, or an unparseable
  # answer — is UNVERIFIABLE, never "probably fine": the chain stays held.
  defp self_check_rpc(chain, state, method, params) do
    case state.rpc_fn.(chain, method, params) do
      {:ok, value} -> {:ok, value}
      {:error, why} -> {:error, :unverifiable, %{method: method, detail: inspect(why)}}
      other -> {:error, :unverifiable, %{method: method, detail: inspect(other)}}
    end
  catch
    kind, reason ->
      {:error, :unverifiable, %{method: method, detail: "#{kind}: #{safe_inspect(reason)}"}}
  end

  defp trusted?(from, state), do: MapSet.member?(state.trusted_sources, to_string(from))

  defp operator?(from, state),
    do: MapSet.member?(Map.get(state, :operator_sources, MapSet.new()), to_string(from))

  defp operator_refusal(state, action, msg, from, reason) do
    Logger.warning("payments: #{reason} #{inspect(from)} sent operator action #{action}")

    emit_metric(state, "payments_operator_refused", %{
      action: action,
      reason: reason,
      source: to_string(from)
    })

    {:reply,
     Jason.encode!(
       %{action: action, ok: false, error: reason}
       |> Map.merge(operator_echo(msg))
     ), state}
  end

  # Every operator reply — success or refusal — echoes the thing the request
  # named, so the caller can correlate an async reply without guessing.
  defp operator_echo(msg) do
    %{}
    |> maybe_echo(msg, "idempotency_key")
    |> maybe_echo(msg, "beneficiary")
  end

  defp maybe_echo(acc, msg, key) do
    case Map.get(msg, key) do
      value when is_binary(value) and value != "" -> Map.put(acc, key, value)
      _ -> acc
    end
  end

  defp handle_action(
         "deposit_address",
         %{"beneficiary" => _ben},
         %{degraded_boot: true} = state,
         _from
       ) do
    emit_metric(state, "payments_hold", %{stage: "degraded_boot", action: "deposit_address"})
    {:reply, Jason.encode!(%{ok: false, error: "degraded_boot"}), state}
  end

  # D2's other end: a beneficiary whose binding was loaded under a FOREIGN
  # namespace has every settlement HELD (see `foreign_namespace_binding?/2`),
  # and the hold freezes that chain's cursor too. Re-serving its address would
  # invite a deposit into a black hole — money watched, never credited, and
  # the chain stuck behind it. Refuse the read the same way the settle path
  # refuses the credit, and alarm with the same metric so one repair closes
  # both.
  defp handle_action("deposit_address", %{"beneficiary" => ben}, state, _from)
       when is_binary(ben) and ben != "" do
    if MapSet.member?(state.foreign_namespace_bindings, ben) do
      Logger.error(
        "payments: refusing deposit_address for #{inspect(ben)} — its binding is loaded under a foreign namespace, so settlements to that address would be HELD (repair the binding's namespace first)"
      )

      emit_metric(state, "payments_namespace_mismatch", %{
        stage: "deposit_address",
        beneficiary: ben,
        binding_namespace: to_string(row_get(Map.get(state.bindings, ben, %{}), :namespace)),
        hub_namespace: state.namespace
      })

      {:reply, Jason.encode!(%{ok: false, error: "namespace_mismatch"}), state}
    else
      deposit_address_reply(ben, state)
    end
  end

  # ── entry A: the hub OWNS the issued-authorization registry ────────────────
  #
  # A trusted-source action, exactly like `deposit_address` — NOT an operator
  # action. It writes via the store, never directly: a host that wrote rows
  # "by hand" would have to guess this shape, and a mis-shaped row produces
  # credits the credit rule (settle_one/authorization_disposition below) can
  # never see are wrong. No `degraded_boot` gate: that flag protects the
  # BINDINGS watched-set/HD-index invariant, which this registry is entirely
  # independent of.
  #
  # Idempotent by `order_ref` — the STORE decides duplicate detection
  # (`record_issued_authorization/1`'s `{:ok, :duplicate}`); the hub always
  # echoes back the SAME row content it was just asked to write, whether this
  # was the first call or a replay, so "repeat the issuance" and "read back
  # what was issued" are the same request from the caller's side.
  defp handle_action("issue_authorization", msg, state, _from) do
    with {:ok, nonce_hex} <- validate_nonce_hex(Map.get(msg, "nonce")),
         {:ok, order_ref} <- validate_present_string(Map.get(msg, "order_ref"), :bad_order_ref),
         {:ok, beneficiary} <-
           validate_present_string(Map.get(msg, "beneficiary"), :bad_beneficiary),
         {:ok, amount_usd} <- validate_positive_money(Map.get(msg, "amount_usd")),
         {:ok, valid_before} <-
           validate_valid_before(
             Map.get(msg, "valid_before"),
             DateTime.to_unix(state.now_fn.()),
             state.max_authorization_window_seconds
           ) do
      issue_authorization(nonce_hex, order_ref, beneficiary, amount_usd, valid_before, state)
    else
      {:error, reason} ->
        emit_metric(state, "payments_read_refused", %{
          action: "issue_authorization",
          reason: to_string(reason)
        })

        {:reply, Jason.encode!(%{action: "issue_authorization", ok: false, error: to_string(reason)}),
         state}
    end
  end

  # payment_status is README's reconciliation path — it must refuse rather
  # than fail open. During degraded_boot init couldn't establish the true
  # watched set, so an "ok:true, payments:[]" answer here would be
  # indistinguishable from "genuinely zero payments" when it's really "we
  # don't know". Checked ahead of the generic clause below.
  #
  # (R4-P4-I1) TAGGED and ECHOED like every other operator reply. This was the
  # ONE operator answer that carried neither, and an untagged refusal is
  # indistinguishable on the wire from the untagged `deposit_address` refusal a
  # caller routes to an END USER: a host correlating replies by shape answered
  # an unrelated user's top-up with this, and the operator who asked got
  # silence — in the exact state (a degraded hub) where an operator most needs
  # an answer. The tag is what makes the two reply families separable at all;
  # the echo is what makes the correlation exact rather than most-recent.
  defp handle_action(
         "payment_status",
         %{"beneficiary" => _ben} = msg,
         %{degraded_boot: true} = state,
         _from
       ) do
    {:reply,
     Jason.encode!(
       %{action: "payment_status", ok: false, error: "degraded_boot"}
       |> Map.merge(operator_echo(msg))
     ), state}
  end

  # D3: settled money AND held money, in one answer. The two legs come from
  # two different callbacks because the contracts are different — `list_payments`
  # is "money this beneficiary received" and must never carry a quarantined row
  # — but an operator asking "what happened to my user's payment" needs both,
  # and a status surface that shows only the settled half is how a held payment
  # becomes invisible.
  #
  # Either leg erroring refuses the WHOLE reply: "no held rows" and "I could not
  # read held rows" are different sentences, and only one of them is true.
  defp handle_action("payment_status", %{"beneficiary" => ben} = msg, state, _from)
       when is_binary(ben) do
    with {:ok, rows, durable?} <- status_settled_rows(state, ben),
         {:ok, held_rows, held_durable?} <- status_held_rows(state, ben) do
      payments =
        Enum.map(rows, &(Map.take(&1, [:amount_usd, :method, :ref, :at]) |> stringify()))

      binding = Map.get(state.bindings, ben)

      {:reply,
       Jason.encode!(%{
         action: "payment_status",
         ok: true,
         beneficiary: ben,
         address: binding && binding.address,
         payments: payments,
         durable: durable?,
         held: Enum.map(held_rows, &held_row_view/1),
         held_durable: held_durable?
       }), state}
    else
      # (R4-P4-M1) Echoed like every other refusal: a refusal that names nothing
      # can only be correlated by "most recent", which cross-delivers when two
      # operators ask at once.
      {:error, _why} ->
        {:reply,
         Jason.encode!(
           %{action: "payment_status", ok: false, error: "store_unavailable"}
           |> Map.merge(operator_echo(msg))
         ), state}
    end
  end

  # The operator's held-money queue for the whole namespace (`/payments held`),
  # capped. Refuses rather than answering an empty queue it cannot see.
  defp handle_action("quarantined", msg, %{degraded_boot: true} = state, _from) do
    {:reply,
     Jason.encode!(
       %{action: "quarantined", ok: false, error: "degraded_boot"}
       |> Map.merge(operator_echo(msg))
     ), state}
  end

  defp handle_action("quarantined", msg, state, _from) do
    with {:ok, raw_limit} <- action_non_negative_integer(msg, "limit", 20) do
      limit = clamp_integer(raw_limit, 1, @quarantined_action_limit)
      beneficiary = optional_string(msg, "beneficiary")

      case quarantined_lookup(state, beneficiary, limit) do
        {:ok, rows} ->
          {total, unparsable} = sum_amount_with_defects(rows)

          render_reply(state, "quarantined", %{
            action: "quarantined",
            ok: true,
            namespace: state.namespace,
            beneficiary: beneficiary,
            count: length(rows),
            total_usd: Decimal.to_string(total),
            amounts_unparsable: unparsable,
            held: Enum.map(rows, &held_row_view/1),
            complete: length(rows) < limit
          })

        :no_store ->
          read_refusal(state, "quarantined", "no_quarantine_store")

        {:error, _why} ->
          read_refusal(state, "quarantined", "store_unavailable")
      end
    else
      {:error, :bad_request} -> read_refusal(state, "quarantined", "bad_request")
    end
  end

  # ── D3: release ─────────────────────────────────────────────────────────────
  #
  # The ONE action that turns held money back into creditable money, and the
  # only one in this package that changes a settled/quarantined decision after
  # the fact. Three things it deliberately does NOT do:
  #
  #   * it does not credit. It flips the durable row and emits the SAME
  #     `payment_confirmed` a normal settlement emits, so the consumer credits
  #     through its own validating path (and dedups on its own key). There is
  #     no bypass around that path anywhere in this repo, on purpose;
  #   * it does not mint the sequence itself. The store mints it at RELEASE
  #     time from the same monotone generator, which is what puts the row at
  #     the HEAD of the outbox — above every consumer cursor, including one
  #     that already advanced past the row's original position;
  #   * it does not double-credit. A row that is already settled is an
  #     idempotent success with no push: the outbox already carries it, and
  #     the consumer's key dedup would refuse a second credit anyway.
  defp handle_action("release_payment", msg, %{degraded_boot: true} = state, _from) do
    emit_metric(state, "payments_hold", %{stage: "degraded_boot", action: "release_payment"})

    {:reply,
     Jason.encode!(
       %{action: "release_payment", ok: false, error: "degraded_boot"}
       |> Map.merge(operator_echo(msg))
     ), state}
  end

  defp handle_action("release_payment", msg, state, _from) do
    case optional_string(msg, "idempotency_key") do
      nil ->
        {:reply, Jason.encode!(%{action: "release_payment", ok: false, error: "bad_request"}),
         state}

      key ->
        release_one(key, state)
    end
  end

  # ── D4: sweep MEASUREMENT (never a sweep) ───────────────────────────────────
  #
  # "Measure the sweep, don't design it": how many derived addresses hold a
  # balance and how much, so consolidation becomes a decision with a number
  # attached. This hub is watch-only — it holds an xPUB, not an xprv — so
  # there is nothing here that could move a coin even if it wanted to. The
  # only chain traffic is `balanceOf` reads, bounded by `limit`.
  defp handle_action("sweep_report", _msg, %{degraded_boot: true} = state, _from) do
    {:reply, Jason.encode!(%{action: "sweep_report", ok: false, error: "degraded_boot"}), state}
  end

  defp handle_action("sweep_report", msg, state, _from) do
    with {:ok, raw_limit} <- action_non_negative_integer(msg, "limit", @sweep_default_limit),
         {:ok, chain} <- sweep_chain(msg, state) do
      limit = clamp_integer(raw_limit, 1, @sweep_address_limit)
      render_reply(state, "sweep_report", sweep_report(chain, limit, state))
    else
      {:error, :bad_request} -> read_refusal(state, "sweep_report", "bad_request")
      {:error, :chain_required} -> read_refusal(state, "sweep_report", "chain_required")
      {:error, :unknown_chain} -> read_refusal(state, "sweep_report", "unknown_chain")
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

  # ── entry A: issue_authorization's field validation + the write itself ─────

  defp validate_nonce_hex(nonce) when is_binary(nonce) do
    if Regex.match?(@nonce_pattern, nonce) do
      {:ok, String.downcase(nonce)}
    else
      {:error, :bad_nonce}
    end
  end

  defp validate_nonce_hex(_invalid), do: {:error, :bad_nonce}

  defp validate_present_string(value, _error) when is_binary(value) and value != "",
    do: {:ok, value}

  defp validate_present_string(_value, error), do: {:error, error}

  # Reuses @money_pattern — the same plain-non-negative-Decimal-string gate
  # `strict_money_config!/4` applies to config, applied here to a per-message
  # field instead. Zero is not a payment.
  defp validate_positive_money(value) when is_binary(value) do
    if Regex.match?(@money_pattern, value) do
      decimal = Decimal.new(value)

      if Decimal.compare(decimal, Decimal.new(0)) == :gt do
        {:ok, decimal}
      else
        {:error, :bad_amount}
      end
    else
      {:error, :bad_amount}
    end
  end

  defp validate_positive_money(_invalid), do: {:error, :bad_amount}

  # Unix seconds, matching EIP-3009's `validBefore` and the same clock
  # `live_authorization_nonces/1` is queried against (`DateTime.to_unix/1`).
  #
  # (I4) The window is BOUNDED HERE, at the one place a nonce ever enters the
  # registry — the property the whole live-nonce-filter design rests on
  # ("bounded by construction: every issued authorization expires") was
  # previously delegated entirely to the caller, which accepted a `valid_before`
  # a century out (a nonce that never leaves the getLogs filter) and one
  # already in the past (acked ok:true, dead on arrival — the money lands
  # unrecognised because the nonce is never live at scan time). Both are
  # refused; `max_authorization_window_seconds` (default 1h, the plan's window)
  # is the bound.
  defp validate_valid_before(value, now_unix, max_window) when is_integer(value) do
    cond do
      value <= now_unix -> {:error, :expired_valid_before}
      value - now_unix > max_window -> {:error, :valid_before_too_far}
      true -> {:ok, value}
    end
  end

  defp validate_valid_before(_invalid, _now_unix, _max_window), do: {:error, :bad_valid_before}

  defp issue_authorization(nonce_hex, order_ref, beneficiary, amount_usd, valid_before, state) do
    row = %{
      nonce_hex: nonce_hex,
      order_ref: order_ref,
      beneficiary: beneficiary,
      amount_usd: amount_usd,
      # SEALED: the hub's own namespace, never whatever (if anything) the
      # caller's message carried — same stance as every other write in this
      # module (D2's binding namespace, the settlement's own namespace).
      namespace: state.namespace,
      valid_before: valid_before,
      issued_at: state.now_fn.()
    }

    case record_issued_authorization_write(state, row) do
      written when written in [:ok, :memory] ->
        issuance_reply(state, row)

      # (C3) A replay answers with the STORED row, never with the request that
      # lost the race. Echoing the request's own fields made issuance
      # NON-idempotent in the one way that matters: a caller that timed out on
      # the first reply and retried the order with a freshly minted nonce (the
      # normal thing a retrying caller does) got ok:true naming THAT nonce —
      # which was never registered, is never in the getLogs filter, and buries
      # the user's payment as unrecognised.
      {:ok, :duplicate, stored} ->
        duplicate_issuance_reply(state, order_ref, stored)

      {:error, why} ->
        Logger.error(
          "payments: record_issued_authorization failed (#{inspect(why)}) for order_ref #{inspect(order_ref)} — refusing"
        )

        emit_metric(state, "payments_hold", %{
          stage: "issue_authorization",
          order_ref: order_ref,
          reason: reason_text(why)
        })

        {:reply,
         Jason.encode!(%{action: "issue_authorization", ok: false, error: "store_unavailable"}),
         state}
    end
  end

  defp issuance_reply(state, row) do
    render_reply(state, "issue_authorization", %{
      action: "issue_authorization",
      ok: true,
      nonce: row_get(row, :nonce_hex),
      order_ref: row_get(row, :order_ref),
      beneficiary: row_get(row, :beneficiary),
      amount_usd: money_text(row_get(row, :amount_usd)),
      namespace: state.namespace,
      valid_before: row_get(row, :valid_before)
    })
  end

  # The stored row is validated before it is echoed: an ok:true naming a nonce
  # this hub cannot prove is registered is precisely the failure C3 describes,
  # and a store that cannot hand back the row it deduped against cannot make
  # that proof. REFUSE rather than echo unverified fields — the caller retries
  # or the operator looks, and no user is handed a nonce that will never
  # correlate.
  defp duplicate_issuance_reply(state, order_ref, stored) do
    case stored_issuance_row(stored, state) do
      {:ok, row} ->
        issuance_reply(state, row)

      {:error, why} ->
        Logger.error(
          "payments: record_issued_authorization answered :duplicate for order_ref #{inspect(order_ref)} with an unusable row (#{reason_text(why)}: #{safe_inspect(stored)}) — refusing rather than echoing the request's own unregistered nonce back as if it were on record"
        )

        emit_metric(state, "payments_hold", %{
          stage: "issue_authorization",
          order_ref: order_ref,
          reason: reason_text(why)
        })

        {:reply,
         Jason.encode!(%{action: "issue_authorization", ok: false, error: "store_unavailable"}),
         state}
    end
  end

  defp stored_issuance_row(stored, state) when is_map(stored) do
    nonce_hex = row_get(stored, :nonce_hex)
    order_ref = row_get(stored, :order_ref)
    beneficiary = row_get(stored, :beneficiary)
    valid_before = row_get(stored, :valid_before)
    namespace = row_get(stored, :namespace)

    cond do
      not (is_binary(nonce_hex) and Regex.match?(@nonce_pattern, nonce_hex)) ->
        {:error, :duplicate_row_bad_nonce}

      not (is_binary(order_ref) and order_ref != "") ->
        {:error, :duplicate_row_bad_order_ref}

      not (is_binary(beneficiary) and beneficiary != "") ->
        {:error, :duplicate_row_bad_beneficiary}

      not is_integer(valid_before) ->
        {:error, :duplicate_row_bad_valid_before}

      not is_nil(namespace) and to_string(namespace) != state.namespace ->
        {:error, :duplicate_row_foreign_namespace}

      true ->
        case issued_amount_usd(stored) do
          {:ok, amount} ->
            {:ok,
             %{
               nonce_hex: String.downcase(nonce_hex),
               order_ref: order_ref,
               beneficiary: beneficiary,
               amount_usd: amount,
               valid_before: valid_before
             }}

          :error ->
            {:error, :duplicate_row_bad_amount}
        end
    end
  end

  defp stored_issuance_row(_stored, _state), do: {:error, :duplicate_row_missing}

  # (I1, runtime half) A store that cannot register the nonce cannot back an
  # ok:true. Memory mode stays reachable behind the SAME explicit
  # `allow_ephemeral: true` opt-out the settlement ledger uses — never by
  # default, because the default silently acknowledged money it could never
  # credit.
  defp record_issued_authorization_write(state, row) do
    mod = state.store_mod

    cond do
      exported?(mod, :record_issued_authorization, 1) ->
        try do
          case apply(mod, :record_issued_authorization, [row]) do
            :ok -> :ok
            {:ok, :duplicate, stored} -> {:ok, :duplicate, stored}
            # The row-less duplicate shape cannot be honored: this hub has no
            # order_ref lookup, so it literally cannot read back what it
            # deduped against. Treated as a store defect, not as success.
            {:ok, :duplicate} -> {:error, :duplicate_row_missing}
            {:error, why} -> {:error, why}
            other -> {:error, {:bad_return, other}}
          end
        catch
          kind, reason -> {:error, {kind, reason}}
        end

      state.allow_ephemeral ->
        :memory

      true ->
        {:error, :no_authorization_store}
    end
  end

  # ── outbox read ─────────────────────────────────────────────────────────────

  defp state_outbox_page(state, after_seq, limit) do
    cond do
      exported?(state.store_mod, :list_settlements_since, 2) ->
        durable_namespace_page(
          state.store_mod,
          state.namespace,
          after_seq,
          limit,
          state.metrics_fn
        )

      state.ephemeral_outbox ->
        memory_namespace_page(state, after_seq, limit)

      true ->
        {:error, :no_outbox_store}
    end
  end

  defp durable_namespace_page(store_mod, namespace, after_seq, limit, metrics_fn) do
    scan_namespace_page(store_mod, namespace, after_seq, after_seq, limit, [], nil, metrics_fn)
  end

  defp scan_namespace_page(
         store_mod,
         namespace,
         original_after,
         cursor,
         limit,
         acc,
         known_max,
         metrics_fn
       ) do
    case outbox_store_read(store_mod, cursor, limit, metrics_fn) do
      {:ok, %{settlements: rows, max_seq: max_seq, raw_count: raw_count, raw_max_seq: raw_max}} ->
        max_seq = if is_integer(known_max), do: max(known_max, max_seq), else: max_seq
        rows = Enum.sort_by(rows, &outbox_seq/1)
        room = limit - length(acc)

        selected =
          rows
          |> Enum.filter(&namespace_match?(&1, namespace))
          |> Enum.filter(&(outbox_seq(&1) > original_after))
          |> Enum.take(room)

        acc = acc ++ selected
        # Cursor and page-exhaustion come from the RAW page, so dropping a
        # poisoned row can never stall paging or hide the rows behind it.
        raw_cursor = max(cursor, raw_max)

        cond do
          length(acc) == limit ->
            next_seq = highest_seq(acc, original_after)

            with {:ok, more?} <-
                   namespace_exists_after?(
                     store_mod,
                     namespace,
                     next_seq,
                     max_seq,
                     limit,
                     metrics_fn
                   ) do
              {:ok,
               %{
                 settlements: acc,
                 next_seq: next_seq,
                 max_seq: max_seq,
                 complete: not more?
               }}
            end

          raw_count == 0 or raw_cursor >= max_seq or raw_count < limit ->
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
              max_seq,
              metrics_fn
            )
        end

      {:error, _why} = error ->
        error
    end
  end

  defp namespace_exists_after?(_store_mod, _namespace, cursor, max_seq, _limit, _metrics_fn)
       when cursor >= max_seq,
       do: {:ok, false}

  defp namespace_exists_after?(store_mod, namespace, cursor, max_seq, limit, metrics_fn) do
    case outbox_store_read(store_mod, cursor, limit, metrics_fn) do
      {:ok,
       %{
         settlements: rows,
         max_seq: observed_max,
         raw_count: raw_count,
         raw_max_seq: raw_max
       }} ->
        max_seq = max(max_seq, observed_max)

        cond do
          # Only CREDITABLE rows count as "there is more for you" — a page of
          # nothing but poisoned rows must not promise a consumer a next page.
          Enum.any?(rows, &namespace_match?(&1, namespace)) ->
            {:ok, true}

          raw_count == 0 ->
            {:ok, false}

          true ->
            next_cursor = max(cursor, raw_max)

            if next_cursor == cursor do
              {:error, :bad_store_return}
            else
              namespace_exists_after?(
                store_mod,
                namespace,
                next_cursor,
                max_seq,
                limit,
                metrics_fn
              )
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
      |> Enum.filter(&creditable_status?/1)
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

  # THE choke point for every durable outbox read (the message action, the
  # host seam, and reconciliation). Two jobs: validate the store's page shape,
  # and refuse to serve a row that is not creditable.
  #
  # A1/C1 keep `outbox_seq` NULL on a quarantined row, so a correct store
  # simply never returns one here — but the whole point of C1 is that the row
  # is held even when the store is DEFECTIVE. `record_settlement` already
  # alarms and refuses the push when a store mints a sequence for a
  # quarantined row; by then the row is durably persisted with that sequence,
  # and this read is the path that would credit it. Drop it, alarm per row.
  # The returned page keeps the RAW shape (`raw_count`/`raw_max_seq`) so
  # dropping rows never disturbs paging.
  #
  # The filter is "status present and not settled", not "status != settled":
  # pre-0.2.0 rows carry no status at all and must keep flowing.
  defp outbox_store_read(store_mod, after_seq, limit, metrics_fn) do
    try do
      case apply(store_mod, :list_settlements_since, [after_seq, limit]) do
        {:ok, %{settlements: rows, max_seq: max_seq}}
        when is_list(rows) and is_integer(max_seq) and max_seq >= 0 ->
          if Enum.all?(
               rows,
               &(is_integer(outbox_seq(&1)) and outbox_seq(&1) > after_seq and
                   outbox_seq(&1) <= max_seq)
             ) do
            {:ok,
             %{
               settlements: reject_uncreditable_rows(rows, metrics_fn),
               max_seq: max_seq,
               raw_count: length(rows),
               raw_max_seq: highest_seq(rows, after_seq)
             }}
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

  defp reject_uncreditable_rows(rows, metrics_fn) do
    Enum.reject(rows, fn row ->
      if creditable_status?(row) do
        false
      else
        key = row_get(row, :idempotency_key)
        status = to_string(row_get(row, :status))

        Logger.error(
          "payments: durable outbox row #{inspect(key)} has status #{inspect(status)} but carries outbox_seq #{inspect(outbox_seq(row))} — NOT creditable, dropped from the read (store defect: only a settled row may hold a sequence)"
        )

        emit_metric(metrics_fn, "payments_outbox_poisoned_row", %{
          idempotency_key: key,
          status: status,
          outbox_seq: outbox_seq(row)
        })

        true
      end
    end)
  end

  defp creditable_status?(row) do
    case row_get(row, :status) do
      nil -> true
      status -> to_string(status) == "settled"
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
        scan_recent_settlements(state.store_mod, state.namespace, 0, limit, [], state.metrics_fn)

      state.ephemeral_outbox ->
        rows =
          state.settlement_mirror
          |> Enum.filter(
            &(is_integer(outbox_seq(&1)) and creditable_status?(&1) and
                namespace_match?(&1, state.namespace))
          )
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
  defp scan_recent_settlements(store_mod, namespace, cursor, limit, newest, metrics_fn) do
    case outbox_store_read(store_mod, cursor, limit, metrics_fn) do
      {:ok, %{settlements: rows, max_seq: max_seq, raw_count: raw_count, raw_max_seq: raw_max}} ->
        newest =
          (newest ++ Enum.filter(rows, &namespace_match?(&1, namespace)))
          |> Enum.sort_by(&outbox_seq/1, :desc)
          |> Enum.take(limit)

        next_cursor = max(cursor, raw_max)

        cond do
          raw_count == 0 or next_cursor >= max_seq or raw_count < limit ->
            {:ok, newest}

          next_cursor == cursor ->
            {:error, :bad_store_return}

          true ->
            scan_recent_settlements(store_mod, namespace, next_cursor, limit, newest, metrics_fn)
        end

      {:error, _why} = error ->
        error
    end
  end

  defp reconcile_rows(rows, state, rpc_limit) do
    # Each complete row makes at most one receipt call, plus at most one
    # finality-head call PER CHAIN for the whole run (memoised below). Taking
    # the action's clamped limit here makes the wall-clock bound explicit even
    # if a future read implementation accidentally returns too many rows. The
    # production Rpc.call/3 seam retains its existing per-call curl timeout.
    {result, _finality_heads} =
      rows
      |> Enum.take(rpc_limit)
      |> Enum.reduce(
        {%{
           checked: 0,
           drift: [],
           unverifiable: 0,
           legacy: 0,
           incomplete: 0,
           unfinalized: 0,
           finality_unverifiable: 0
         }, %{}},
        fn row, {acc, heads} ->
          key = row_get(row, :idempotency_key)

          case chain_fact_status(row) do
            :complete ->
              acc =
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

              finality_leg(row, state, acc, heads)

            :incomplete ->
              missing_facts = missing_chain_facts(row)

              emit_metric(state, "payments_reconcile_incomplete", %{
                idempotency_key: key,
                missing_facts: missing_facts
              })

              Logger.error(
                "payments: incomplete 0.2.0 settlement #{inspect(key)}; missing #{inspect(missing_facts)}"
              )

              {%{acc | incomplete: acc.incomplete + 1}, heads}

            :legacy ->
              emit_metric(state, "payments_reconcile_unverifiable", %{
                idempotency_key: key,
                reason: "legacy_unverifiable"
              })

              {%{acc | legacy: acc.legacy + 1}, heads}
          end
        end
      )

    result
  end

  # ── C2: the finality leg ────────────────────────────────────────────────────
  # Credit runs at `fast_credit_depth` — shallow and fast on purpose, because
  # the feature exists to unblock a user NOW; C1's caps bound what that
  # shallowness can cost. Finality is answered by the CHAIN here, on the
  # reconciliation leg: `finality: :finalized` asks the reconcile endpoint for
  # the `finalized` block tag (~10-20 min behind the head on Base),
  # `{:confirmations, n}` serves chains that do not publish the tag. A row
  # above that head is reported `unfinalized` — informational, never a credit
  # reversal. A null/absent/unparseable answer is `finality_unverifiable` and
  # is NEVER read as "finalized"; a reorged-out row surfaces through the
  # existing receipt leg as drift.
  defp finality_leg(row, state, acc, heads) do
    chain_key = to_string(row_get(row, :chain))

    {head_result, heads} =
      case Map.fetch(heads, chain_key) do
        {:ok, cached} ->
          {cached, heads}

        :error ->
          chain = reconcile_chain(row, state.chains)
          computed = finality_head(chain, state)

          case computed do
            {:ok, _head} ->
              :ok

            {:error, reason} ->
              Logger.error(
                "payments: finality is UNVERIFIABLE for chain #{chain_key} (#{inspect(reason)}) — rows on it are not treated as finalized"
              )

              unless finality_opted_out?(chain, reason) do
                emit_metric(state, "payments_reconcile_finality_unverifiable", %{
                  chain: chain_key,
                  reason: reason_text(reason)
                })
              end
          end

          {computed, Map.put(heads, chain_key, computed)}
      end

    key = row_get(row, :idempotency_key)
    block_number = normalize_integer(row_get(row, :block_number))

    case head_result do
      {:ok, head} when is_integer(block_number) and block_number > head ->
        emit_metric(state, "payments_reconcile_unfinalized", %{
          idempotency_key: key,
          chain: chain_key,
          block_number: block_number,
          finality_head: head
        })

        {%{acc | unfinalized: acc.unfinalized + 1}, heads}

      {:ok, head} when is_integer(head) and is_integer(block_number) ->
        {acc, heads}

      # Head answered, ROW unreadable: the head-level metric above never fires
      # for this, so without an emit here the row is a number in the reply and
      # nothing in telemetry — the one shape an operator cannot chase.
      {:ok, _head} ->
        Logger.error(
          "payments: finality is UNVERIFIABLE for #{inspect(key)} on chain #{chain_key} — block_number #{inspect(row_get(row, :block_number))} is unparseable; not treated as finalized"
        )

        emit_metric(state, "payments_reconcile_finality_unverifiable", %{
          idempotency_key: key,
          chain: chain_key,
          reason: "block_number_unparseable",
          block_number: inspect(row_get(row, :block_number))
        })

        {%{acc | finality_unverifiable: acc.finality_unverifiable + 1}, heads}

      {:error, _reason} ->
        {%{acc | finality_unverifiable: acc.finality_unverifiable + 1}, heads}
    end
  end

  # M6: a chain configured `finality: {:confirmations, n}` with no second
  # endpoint has not FAILED to answer — its operator declined the finality
  # leg. The reply counter still says "not verified" (never "finalized"), but
  # the alarm is reserved for endpoints that were asked and could not answer,
  # so upgrading does not hand legacy configs a metric that repeats forever
  # and can never be cleared without new configuration.
  defp finality_opted_out?(chain, :reconcile_rpc_url_missing) when is_map(chain) do
    match?({:confirmations, _n}, Map.get(chain, :finality, :finalized))
  end

  defp finality_opted_out?(_chain, _reason), do: false

  defp finality_head(nil, _state), do: {:error, :chain_not_configured}

  defp finality_head(chain, state) do
    case Map.get(chain, :reconcile_rpc_url) do
      nil ->
        {:error, :reconcile_rpc_url_missing}

      reconcile_rpc_url ->
        second_chain = Map.put(chain, :rpc_url, reconcile_rpc_url)

        case Map.get(chain, :finality, :finalized) do
          :finalized ->
            finalized_tag_head(second_chain, state)

          {:confirmations, confirmations} ->
            confirmations_head(second_chain, state, confirmations)
        end
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp finalized_tag_head(chain, state) do
    case state.rpc_fn.(chain, "eth_getBlockByNumber", ["finalized", false]) do
      {:ok, block} when is_map(block) ->
        case normalize_integer(rpc_get(block, :number)) do
          nil -> {:error, :finalized_head_unparseable}
          head -> {:ok, head}
        end

      {:ok, _absent} ->
        {:error, :finalized_tag_unsupported}

      {:error, why} ->
        {:error, {:rpc_error, why}}

      other ->
        {:error, {:bad_rpc_return, other}}
    end
  end

  defp confirmations_head(chain, state, confirmations) do
    case state.rpc_fn.(chain, "eth_blockNumber", []) do
      {:ok, latest} ->
        case normalize_integer(latest) do
          nil -> {:error, :latest_head_unparseable}
          head -> {:ok, max(head - confirmations, 0)}
        end

      {:error, why} ->
        {:error, {:rpc_error, why}}

      other ->
        {:error, {:bad_rpc_return, other}}
    end
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

  # Metric metadata must survive any reason shape: an atom stays readable,
  # anything else (an {:rpc_error, why} tuple, a caught EXIT) is inspected
  # rather than crashing String.Chars inside a telemetry emit.
  defp reason_text(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_text(reason) when is_binary(reason), do: reason
  defp reason_text(reason), do: safe_inspect(reason)

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

  defp deposit_address_reply(ben, state) do
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

  defp ensure_binding(ben, state) do
    case Map.fetch(state.bindings, ben) do
      {:ok, binding} ->
        {:ok, binding, state}

      :error ->
        allocate_binding(ben, state, @binding_index_attempts)
    end
  end

  # I2: `next_index` is an in-memory allocator over a SHARED durable table, so
  # two orchestrators (any rolling restart) can hold the same value. Whoever
  # loses the insert must step over the taken index — the previous code
  # returned the error with `next_index` UNCHANGED, so the loser re-offered the
  # same permanently-taken index to every subsequent new user and `/topup`
  # stayed broken on that instance long after the restart finished. That is a
  # livelock, not a race: nothing about it self-heals.
  #
  # So: on `:index_taken` advance and retry, bounded. A store outage still
  # fails closed (never hand out an address whose binding is not durable).
  #
  # (R4-P4-I4) `:binding_conflict` no longer refuses blindly. It means THIS
  # beneficiary is already bound durably — to an address this process does not
  # have in `state.bindings` because a peer instance (or a pre-restart
  # incarnation of this one) wrote it. Refusing there was a permanent break,
  # not a race: `state.bindings` was never repaired, so every later `/topup` by
  # that user on this instance failed identically for the life of the process,
  # reported as "the store is down" when the store was in fact holding the exact
  # answer the user asked for. So: ADOPT the stored binding — read it back and
  # serve it. Never re-derive, never rebind. The invariant the old refusal was
  # protecting ("a beneficiary must never end up with two deposit addresses") is
  # strictly better served by adoption, because adoption serves the ONE address
  # the durable table already committed to, and derivation is never consulted.
  # We refuse only when the read-back cannot prove which address that is.
  defp allocate_binding(ben, state, attempts_left) do
    index = state.next_index

    case HD.address(state.xpub, index) do
      {:ok, address} ->
        binding = %{index: index, address: address, namespace: state.namespace}
        row = Map.put(binding, :beneficiary, ben)

        case store_write(state.store_mod, :put_address_binding, [row]) do
          :ok ->
            {:ok, binding,
             %{state | bindings: Map.put(state.bindings, ben, binding), next_index: index + 1}}

          {:error, :index_taken} when attempts_left > 1 ->
            Logger.warning(
              "payments: HD index #{index} is already taken by another allocator — advancing and retrying (#{attempts_left - 1} left)"
            )

            emit_metric(state, "payments_binding_index_taken", %{
              beneficiary: ben,
              index: index
            })

            allocate_binding(ben, %{state | next_index: index + 1}, attempts_left - 1)

          {:error, :index_taken} ->
            Logger.error(
              "payments: exhausted HD index retries at #{index} — every candidate was taken; refusing allocation"
            )

            emit_metric(state, "payments_hold", %{
              stage: "address_binding",
              beneficiary: ben,
              reason: "index_retries_exhausted"
            })

            {:error, :store_unavailable, %{state | next_index: index + 1}}

          {:error, :binding_conflict} ->
            adopt_binding(ben, state)

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

  # (R4-P4-I4) The read half of the conflict. `get_address_binding/1` existed in
  # the contract for exactly this and had no caller; this is the caller.
  #
  # Four outcomes, and only one of them serves an address:
  #
  #   * a row under THIS namespace with a usable index+address — adopt it into
  #     `state.bindings` (repairing the in-memory map, so the NEXT `/topup` by
  #     this beneficiary is a plain hit) and serve it. The address is the
  #     store's, never a fresh derivation;
  #   * a row under a FOREIGN namespace — refuse with `namespace_mismatch` and
  #     remember it, the same way `deposit_address` already refuses a binding
  #     loaded under a foreign namespace at boot: settlements to that address
  #     would be HELD, so serving it would invite a deposit into a black hole;
  #   * no row / an unusable row — the store said "this beneficiary is bound"
  #     and then could not say to what. That is a store defect, not an answer;
  #   * the store not exporting the read, or erroring — refuse. Never derive a
  #     second address for a beneficiary the store says is already bound.
  defp adopt_binding(ben, state) do
    case binding_lookup(state.store_mod, ben) do
      {:ok, row} when is_map(row) ->
        index = normalize_integer(row_get(row, :index) || row_get(row, :hd_index))
        address = row_get(row, :address)
        namespace = to_string(row_get(row, :namespace) || state.namespace)

        cond do
          not (is_integer(index) and index >= 0 and is_binary(address) and address != "") ->
            Logger.error(
              "payments: binding conflict for #{inspect(ben)} and the store's read-back is unusable (#{inspect(row)}) — refusing rather than deriving a second address"
            )

            emit_metric(state, "payments_hold", %{
              stage: "address_binding",
              beneficiary: ben,
              reason: "unusable_binding_readback"
            })

            {:error, :store_unavailable, state}

          namespace != state.namespace ->
            Logger.error(
              "payments: beneficiary #{inspect(ben)} is bound under namespace #{inspect(namespace)} while this hub is #{inspect(state.namespace)} — refusing to serve an address whose settlements would be HELD"
            )

            emit_metric(state, "payments_namespace_mismatch", %{
              stage: "address_binding",
              beneficiary: ben,
              binding_namespace: namespace,
              hub_namespace: state.namespace
            })

            {:error, :namespace_mismatch,
             %{
               state
               | foreign_namespace_bindings:
                   MapSet.put(state.foreign_namespace_bindings, ben)
             }}

          true ->
            binding = %{index: index, address: address, namespace: namespace}

            Logger.warning(
              "payments: adopting the durable binding for #{inspect(ben)} (index #{index}) — it was written by another instance; serving the stored address, never a new one"
            )

            emit_metric(state, "payments_binding_adopted", %{
              beneficiary: ben,
              index: index
            })

            {:ok, binding,
             %{
               state
               | bindings: Map.put(state.bindings, ben, binding),
                 next_index: max(state.next_index, index + 1)
             }}
        end

      {:ok, nil} ->
        Logger.error(
          "payments: store answered :binding_conflict for #{inspect(ben)} and then reported no binding — refusing"
        )

        emit_metric(state, "payments_hold", %{
          stage: "address_binding",
          beneficiary: ben,
          reason: "conflict_without_binding"
        })

        {:error, :store_unavailable, state}

      other ->
        Logger.error(
          "payments: binding conflict for #{inspect(ben)} and the binding read-back failed (#{inspect(other)}) — refusing"
        )

        emit_metric(state, "payments_hold", %{
          stage: "address_binding",
          beneficiary: ben,
          reason: "binding_readback_failed"
        })

        {:error, :store_unavailable, state}
    end
  end

  defp binding_lookup(store_mod, ben) do
    if exported?(store_mod, :get_address_binding, 1) do
      store_result(store_mod, :get_address_binding, [ben], {:error, :store_failed})
    else
      {:error, :no_binding_read}
    end
  end

  # ── D3 operator surface: release + held queue ───────────────────────────────

  defp status_settled_rows(state, ben) do
    case list_payments_lookup(state.store_mod, ben) do
      {:ok, rows} -> {:ok, rows, true}
      :no_store -> {:ok, [], false}
      {:error, why} -> {:error, why}
    end
  end

  defp status_held_rows(state, ben) do
    case quarantined_lookup(state, ben, @quarantined_status_limit) do
      {:ok, rows} -> {:ok, rows, true}
      :no_store -> {:ok, [], false}
      {:error, why} -> {:error, why}
    end
  end

  defp release_one(key, state) do
    if exported?(state.store_mod, :release_quarantined_payment, 2) do
      state.store_mod
      |> store_result(:release_quarantined_payment, [state.namespace, key], {:error, :store_failed})
      |> release_result(key, state)
    else
      Logger.error(
        "payments: release_payment refused for #{inspect(key)} — the configured store exports no release_quarantined_payment/2; the row stays quarantined"
      )

      {:reply,
       Jason.encode!(%{
         action: "release_payment",
         ok: false,
         error: "no_release_store",
         idempotency_key: key
       }), state}
    end
  end

  defp release_result({:ok, :released, row}, key, state) when is_map(row) do
    released_settlement(key, row, state)
  end

  defp release_result({:ok, :already_settled, _row}, key, state) do
    # Idempotent success. No push: the row already carries a sequence, so the
    # outbox already offers it to every consumer, and a second credit would be
    # refused by the consumer's own key dedup anyway.
    Logger.info("payments: release_payment #{inspect(key)} — already settled, no-op")

    {:reply,
     Jason.encode!(%{
       action: "release_payment",
       ok: true,
       released: false,
       already: "settled",
       idempotency_key: key
     }), state}
  end

  defp release_result({:error, :not_found}, key, state) do
    {:reply,
     Jason.encode!(%{
       action: "release_payment",
       ok: false,
       error: "unknown_key",
       idempotency_key: key
     }), state}
  end

  defp release_result({:error, {:not_releasable, status}}, key, state) do
    {:reply,
     Jason.encode!(%{
       action: "release_payment",
       ok: false,
       error: "not_quarantined",
       status: to_string(status),
       idempotency_key: key
     }), state}
  end

  defp release_result({:error, why}, key, state) do
    Logger.error("payments: release_payment #{inspect(key)} failed: #{inspect(why)}")

    emit_metric(state, "payments_release_failed", %{
      idempotency_key: key,
      reason: reason_text(why)
    })

    {:reply,
     Jason.encode!(%{
       action: "release_payment",
       ok: false,
       error: "store_unavailable",
       idempotency_key: key
     }), state}
  end

  defp release_result(_other, key, state) do
    release_result({:error, :bad_store_return}, key, state)
  end

  # The released row is the store's word about money, so it is checked like
  # every other store return before anything is delivered on the strength of
  # it. The SEQUENCE is the one field that must be right: with it the poll
  # credits the row whatever happens to this push, without it nothing ever
  # will.
  defp released_settlement(key, row, state) do
    seq = outbox_seq(row)
    status = to_string(row_get(row, :status))
    namespace = row_get(row, :namespace)

    cond do
      not (is_integer(seq) and seq > 0) ->
        Logger.error(
          "payments: store released #{inspect(key)} without a fresh outbox sequence (#{inspect(seq)}) — a row with no sequence is invisible to every consumer; refusing to report a release"
        )

        emit_metric(state, "payments_release_defect", %{
          idempotency_key: key,
          reason: "no_outbox_seq"
        })

        {:reply,
         Jason.encode!(%{
           action: "release_payment",
           ok: false,
           error: "invalid_store_result",
           idempotency_key: key
         }), state}

      status != "settled" ->
        emit_metric(state, "payments_release_defect", %{
          idempotency_key: key,
          reason: "status_#{status}"
        })

        {:reply,
         Jason.encode!(%{
           action: "release_payment",
           ok: false,
           error: "invalid_store_result",
           idempotency_key: key
         }), state}

      not namespace_match?(row, state.namespace) ->
        # Scoping is the store's job (the callback takes the namespace); a row
        # from elsewhere coming back means the store ignored it.
        Logger.error(
          "payments: store released #{inspect(key)} under namespace #{inspect(namespace)} while this hub is #{inspect(state.namespace)} — refusing to announce another namespace's money"
        )

        emit_metric(state, "payments_namespace_mismatch", %{
          stage: "release",
          idempotency_key: key,
          binding_namespace: namespace,
          hub_namespace: state.namespace
        })

        {:reply,
         Jason.encode!(%{
           action: "release_payment",
           ok: false,
           error: "namespace_mismatch",
           idempotency_key: key
         }), state}

      true ->
        emit_metric(state, "payments_released", %{
          idempotency_key: key,
          outbox_seq: seq,
          beneficiary: row_get(row, :beneficiary),
          amount_usd: money_text(row_get(row, :amount_usd))
        })

        Logger.warning(
          "payments: RELEASED #{key} — #{money_text(row_get(row, :amount_usd))} USD for #{inspect(row_get(row, :beneficiary))} is creditable again at outbox_seq #{seq}"
        )

        pushed? = push_released_settlement(key, row, seq, state)

        state = %{state | seen_keys: MapSet.put(state.seen_keys, key)}

        {:reply,
         Jason.encode!(%{
           action: "release_payment",
           ok: true,
           released: true,
           idempotency_key: key,
           beneficiary: row_get(row, :beneficiary),
           amount_usd: money_text(row_get(row, :amount_usd)),
           outbox_seq: seq,
           pushed: pushed?,
           # (R4-P4-M4) `pushed: false` has exactly ONE cause here — a row
           # missing beneficiary/method/ref/amount (a delivery failure still
           # counts as pushed; the outbox is the authority either way). The
           # consumer's poll validates the SAME four fields, so such a row is
           # not creditable by the poll either: it will be classified permanent
           # and quarantined consumer-side. Saying "the poll will credit it"
           # there is fabricated success generated by the branch that detected
           # the problem, so the reply states the truth and lets the render say
           # it.
           creditable: pushed?
         }), state}
    end
  end

  # The release announces itself with exactly the `payment_confirmed` a normal
  # settlement announces — same fields, same one-shot best-effort cast, same
  # consumer-side validation and dedup. A release-specific credit message would
  # be a second, less-tested way to move money; there isn't one.
  #
  # An incomplete row cannot be announced (the consumer would refuse it as
  # malformed), but it HAS been released: the poll picks it up from the outbox,
  # which is the authoritative path anyway. Say that plainly instead of
  # reporting a push that did not happen.
  defp push_released_settlement(key, row, seq, state) do
    beneficiary = row_get(row, :beneficiary)
    method = row_get(row, :method)
    ref = row_get(row, :ref)
    amount = money_text(row_get(row, :amount_usd))

    if is_binary(beneficiary) and beneficiary != "" and is_binary(method) and method != "" and
         is_binary(ref) and ref != "" and amount != nil do
      content =
        Jason.encode!(%{
          action: "payment_confirmed",
          beneficiary: beneficiary,
          amount_usd: amount,
          method: method,
          ref: ref,
          namespace: to_string(row_get(row, :namespace)),
          at: at_text(row_get(row, :at)) || DateTime.to_iso8601(state.now_fn.()),
          outbox_seq: seq
        })

      Enum.each(state.targets, fn target ->
        deliver_one(state, target, state.name, content, key, "payment_confirmed")
      end)

      true
    else
      Logger.error(
        "payments: released row #{inspect(key)} is missing push fields (beneficiary/method/ref/amount) — it IS released (outbox_seq #{seq}) and the consumer's poll will credit it; no push sent"
      )

      emit_metric(state, "payments_release_defect", %{
        idempotency_key: key,
        reason: "incomplete_push_row"
      })

      false
    end
  end

  defp quarantined_lookup(state, beneficiary, limit) do
    if exported?(state.store_mod, :list_quarantined_payments, 3) do
      case store_result(
             state.store_mod,
             :list_quarantined_payments,
             [state.namespace, beneficiary, limit],
             {:error, :store_failed}
           ) do
        {:ok, rows} when is_list(rows) -> {:ok, rows}
        {:error, why} -> {:error, why}
        _other -> {:error, :store_failed}
      end
    else
      :no_store
    end
  end

  defp held_row_view(row) do
    %{
      idempotency_key: row_get(row, :idempotency_key),
      beneficiary: row_get(row, :beneficiary),
      amount_usd: money_text(row_get(row, :amount_usd)),
      method: row_get(row, :method),
      ref: row_get(row, :ref),
      reason: row_get(row, :quarantine_reason),
      at: at_text(row_get(row, :at))
    }
  end

  # (R4-P4-M7) A total is a money number, so an amount this hub cannot parse is
  # COUNTED, never folded in as zero — the same stance the sweep takes with an
  # unreadable balance. Unreachable from a `Decimal`-returning store, which is
  # why it is a count next to the total rather than a refusal: silence about a
  # row that was silently valued at $0 is the pattern, not this instance of it.
  defp sum_amount_with_defects(rows) do
    Enum.reduce(rows, {Decimal.new(0), 0}, fn row, {acc, unparsable} ->
      case row_get(row, :amount_usd) do
        %Decimal{} = amount ->
          {Decimal.add(acc, amount), unparsable}

        value when is_binary(value) ->
          if Regex.match?(@money_pattern, value) do
            {Decimal.add(acc, Decimal.new(value)), unparsable}
          else
            {acc, unparsable + 1}
          end

        _ ->
          {acc, unparsable + 1}
      end
    end)
  end

  defp money_text(%Decimal{} = amount), do: Decimal.to_string(amount)
  defp money_text(value) when is_binary(value), do: value
  defp money_text(_), do: nil

  defp at_text(%DateTime{} = at), do: DateTime.to_iso8601(at)
  defp at_text(value) when is_binary(value), do: value
  defp at_text(_), do: nil

  defp optional_string(msg, key) do
    case Map.get(msg, key) do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  # ── D4 sweep measurement ────────────────────────────────────────────────────

  defp sweep_chain(msg, state) do
    case optional_string(msg, "chain") do
      nil ->
        case state.chains do
          [chain] -> {:ok, chain}
          _ -> {:error, :chain_required}
        end

      name ->
        case Enum.find(state.chains, &(to_string(Map.get(&1, :name)) == name)) do
          nil -> {:error, :unknown_chain}
          chain -> {:ok, chain}
        end
    end
  end

  defp sweep_report(chain, limit, state) do
    ordered = state.bindings |> Enum.sort_by(fn {_ben, binding} -> binding.index end)
    total_bindings = length(ordered)
    selected = Enum.take(ordered, limit)
    decimals = Map.get(chain, :decimals, 6)
    started_ms = System.monotonic_time(:millisecond)
    budget_ms = Map.get(state, :sweep_budget_ms) || @sweep_budget_ms
    deadline_ms = started_ms + budget_ms

    # The budget is checked BEFORE each call, never mid-call: one in-flight RPC
    # can still run to the transport's own deadline, so the true worst case is
    # @sweep_budget_ms + one RPC timeout — bounded, and bounded by a number an
    # operator can hold in their head.
    {rows, unreadable, checked, budget_spent?} =
      Enum.reduce_while(selected, {[], 0, 0, false}, fn {beneficiary, binding},
                                                        {acc, unreadable, checked, _spent} ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          {:halt, {acc, unreadable, checked, true}}
        else
          case token_balance(chain, binding.address, decimals, state) do
            {:ok, amount} ->
              if Decimal.compare(amount, Decimal.new(0)) == :gt do
                {:cont, {[{beneficiary, binding, amount} | acc], unreadable, checked + 1, false}}
              else
                {:cont, {acc, unreadable, checked + 1, false}}
              end

            {:error, why} ->
              # NEVER counted as zero: an unreadable balance is an unknown, and
              # a sweep decision made on a silently-zeroed unknown is exactly the
              # wrong decision.
              Logger.warning(
                "payments: sweep balance unreadable for #{binding.address} on #{Map.get(chain, :name)}: #{inspect(why)}"
              )

              {:cont, {acc, unreadable + 1, checked + 1, false}}
          end
        end
      end)

    if budget_spent? do
      Logger.warning(
        "payments: sweep_report spent its #{budget_ms}ms budget after #{checked} of #{total_bindings} addresses on #{Map.get(chain, :name)} — returning a PARTIAL report rather than holding the money hub's mailbox"
      )

      emit_metric(state, "payments_sweep_truncated", %{
        chain: to_string(Map.get(chain, :name)),
        addresses_checked: checked,
        bindings_total: total_bindings
      })
    end

    rows = Enum.sort_by(rows, fn {_ben, _binding, amount} -> Decimal.to_float(amount) end, :desc)
    total = Enum.reduce(rows, Decimal.new(0), fn {_b, _bi, amount}, acc -> Decimal.add(acc, amount) end)

    largest =
      case rows do
        [{_ben, binding, amount} | _] -> %{address: binding.address, balance_usd: Decimal.to_string(amount)}
        [] -> nil
      end

    emit_metric(state, "payments_sweep_report", %{
      chain: to_string(Map.get(chain, :name)),
      addresses_checked: checked,
      nonzero: length(rows),
      unreadable: unreadable,
      total_usd: Decimal.to_string(total)
    })

    %{
      action: "sweep_report",
      ok: true,
      chain: to_string(Map.get(chain, :name)),
      token_contract: Map.get(chain, :usdc_contract),
      bindings_total: total_bindings,
      addresses_checked: checked,
      complete: checked >= total_bindings,
      remaining: max(total_bindings - checked, 0),
      budget_spent: budget_spent?,
      unreadable: unreadable,
      nonzero: length(rows),
      total_usd: Decimal.to_string(total),
      largest: largest,
      addresses:
        rows
        |> Enum.take(@sweep_row_limit)
        |> Enum.map(fn {beneficiary, binding, amount} ->
          %{
            beneficiary: beneficiary,
            index: binding.index,
            address: binding.address,
            balance_usd: Decimal.to_string(amount)
          }
        end),
      elapsed_ms: max(System.monotonic_time(:millisecond) - started_ms, 0)
    }
  end

  # ERC-20 `balanceOf(address)` — selector 0x70a08231 + the 32-byte padded
  # address. A read, and only ever a read: this object holds an xPUB, so it
  # could not move a token if the report asked it to.
  defp token_balance(chain, address, decimals, state) do
    data = "0x70a08231" <> String.pad_leading(strip_hex_prefix(address), 64, "0")

    try do
      case state.rpc_fn.(chain, "eth_call", [
             %{"to" => Map.get(chain, :usdc_contract), "data" => data},
             "latest"
           ]) do
        {:ok, raw} ->
          case normalize_integer(raw) do
            value when is_integer(value) and value >= 0 ->
              {:ok, Decimal.div(Decimal.new(value), Decimal.new(Integer.pow(10, decimals)))}

            _ ->
              {:error, {:bad_balance, safe_inspect(raw)}}
          end

        {:error, why} ->
          {:error, why}

        other ->
          {:error, {:bad_rpc_return, safe_inspect(other)}}
      end
    catch
      kind, reason -> {:error, {kind, safe_inspect(reason)}}
    end
  end

  defp strip_hex_prefix("0x" <> hex), do: String.downcase(hex)
  defp strip_hex_prefix("0X" <> hex), do: String.downcase(hex)
  defp strip_hex_prefix(value) when is_binary(value), do: String.downcase(value)

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
