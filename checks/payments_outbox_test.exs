Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

defmodule OutboxStore do
  def reset(rows \\ []) do
    :persistent_term.put({__MODULE__, :rows}, rows)
    :persistent_term.put({__MODULE__, :calls}, [])
    :persistent_term.put({__MODULE__, :error}, nil)
    :persistent_term.put({__MODULE__, :max_seq}, nil)
  end

  def rows, do: :persistent_term.get({__MODULE__, :rows}, [])
  def calls, do: :persistent_term.get({__MODULE__, :calls}, [])
  def fail!(reason), do: :persistent_term.put({__MODULE__, :error}, reason)
  def max_seq!(max_seq), do: :persistent_term.put({__MODULE__, :max_seq}, max_seq)

  def list_settlements_since(after_seq, limit) do
    :persistent_term.put({__MODULE__, :calls}, calls() ++ [{after_seq, limit}])

    case :persistent_term.get({__MODULE__, :error}, nil) do
      nil ->
        sequenced = Enum.filter(rows(), &is_integer(&1.outbox_seq))

        {:ok,
         %{
           settlements:
             sequenced
             |> Enum.filter(&(&1.outbox_seq > after_seq))
             |> Enum.sort_by(& &1.outbox_seq)
             |> Enum.take(limit),
           max_seq:
             :persistent_term.get(
               {__MODULE__, :max_seq},
               Enum.reduce(sequenced, 0, &max(&1.outbox_seq, &2))
             ) || Enum.reduce(sequenced, 0, &max(&1.outbox_seq, &2))
         }}

      reason ->
        {:error, reason}
    end
  end

  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_row), do: {:ok, 99}
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_binding), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
end

defmodule NoOutboxStore do
  def payment_seen?(_key), do: {:ok, false}
  def record_payment(_row), do: :ok
  def list_address_bindings, do: {:ok, []}
  def put_address_binding(_binding), do: :ok
  def get_last_scanned_block(_chain), do: {:ok, nil}
  def put_last_scanned_block(_chain, _block), do: :ok
end

full_row = %{
  beneficiary: "budget:abc",
  amount_usd: Decimal.new("2.5"),
  method: "usdc_base",
  ref: "0xT1:0",
  idempotency_key: "8453:0xT1:0",
  namespace: "llm_quota",
  at: ~U[2026-07-25 10:00:00Z],
  outbox_seq: 1,
  raw_amount: 2_500_000,
  decimals: 6,
  token_contract: "0xCONTRACT",
  chain: "base",
  chain_id: 8453,
  block_number: 150,
  log_index: 0,
  tx_hash: "0xT1",
  from_address: "0xFROM"
}

other_namespace = %{full_row | idempotency_key: "8453:0xT2:0", namespace: "other", outbox_seq: 2}

last_row = %{
  full_row
  | idempotency_key: "8453:0xT3:0",
    ref: "0xT3:0",
    tx_hash: "0xT3",
    outbox_seq: 3
}

OutboxStore.reset([full_row, other_namespace, last_row])

Check.check(
  f,
  "stateless read filters rows by namespace while preserving whole-table max_seq",
  Payments.settlements_since(
    %{store_mod: OutboxStore, namespace: "llm_quota"},
    0,
    10
  ) ==
    {:ok,
     %{
       settlements: [full_row, last_row],
       max_seq: 3,
       next_seq: 3,
       complete: true
     }}
)

foreign_rows =
  for seq <- 1..5 do
    %{
      full_row
      | idempotency_key: "8453:0xFOREIGN#{seq}:0",
        namespace: "other",
        outbox_seq: seq
    }
  end

own_after_foreign = %{
  full_row
  | idempotency_key: "8453:0xOWN:0",
    ref: "0xOWN:0",
    tx_hash: "0xOWN",
    outbox_seq: 6
}

OutboxStore.reset(foreign_rows ++ [own_after_foreign])

{:ok, first_foreign_page} =
  Payments.settlements_since(
    %{store_mod: OutboxStore, namespace: "llm_quota"},
    0,
    3
  )

{:ok, second_foreign_page} =
  Payments.settlements_since(
    %{store_mod: OutboxStore, namespace: "llm_quota"},
    first_foreign_page.next_seq,
    3
  )

Check.check(
  f,
  "stateless consumer advances across a full foreign-namespace page and reaches its own row",
  first_foreign_page.settlements == [] and first_foreign_page.next_seq == 3 and
    first_foreign_page.complete == false and
    second_foreign_page.settlements == [own_after_foreign] and
    second_foreign_page.next_seq == 6 and second_foreign_page.complete == true
)

OutboxStore.fail!(:db_down)

Check.check(
  f,
  "stateless read passes store errors through",
  Payments.settlements_since(
    %{store_mod: OutboxStore, namespace: "llm_quota"},
    0,
    10
  ) == {:error, :db_down}
)

Check.check(
  f,
  "stateless read refuses a missing callback",
  Payments.settlements_since(
    %{store_mod: NoOutboxStore, namespace: "llm_quota"},
    0,
    10
  ) == {:error, :no_outbox_store}
)

OutboxStore.reset([full_row, other_namespace, last_row])

state =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    trusted_sources: ["consumer", "trusted_non_target"],
    targets: ["consumer"],
    namespace: "llm_quota",
    store_mod: OutboxStore,
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end
  })

untrusted =
  Payments.handle_message(
    "stranger",
    Jason.encode!(%{action: "settlements_since"}),
    state
  )

Check.check(f, "settlements_since is trusted-source gated", match?({:noreply, ^state}, untrusted))

{:reply, non_target_json, _} =
  Payments.handle_message(
    "trusted_non_target",
    Jason.encode!(%{action: "settlements_since"}),
    state
  )

Check.check(
  f,
  "trusted non-target is refused as not_a_consumer",
  Jason.decode!(non_target_json) == %{
    "action" => "settlements_since",
    "ok" => false,
    "error" => "not_a_consumer"
  }
)

{:reply, page_json, _} =
  Payments.handle_message(
    "consumer",
    Jason.encode!(%{action: "settlements_since", after_seq: 0, limit: 1}),
    state
  )

page = Jason.decode!(page_json)
wire_row = hd(page["settlements"])

Check.check(
  f,
  "target receives a namespace-filtered page with exact cursor metadata",
  page["ok"] == true and page["next_seq"] == 1 and page["max_seq"] == 3 and
    page["complete"] == false and length(page["settlements"]) == 1
)

Check.check(
  f,
  "full truthful row is JSON encodable without losing chain facts",
  wire_row["amount_usd"] == "2.5" and wire_row["at"] == "2026-07-25T10:00:00Z" and
    Enum.all?(
      ~w(raw_amount decimals token_contract chain chain_id block_number log_index tx_hash from_address),
      &Map.has_key?(wire_row, &1)
    )
)

tuple_protocol_probe =
  try do
    Jason.encode!(%{poison: {:ok, :tuple}})
    :unexpectedly_encoded
  rescue
    error in Protocol.UndefinedError -> {:protocol_undefined, error}
  end

poisoned_row =
  Map.merge(full_row, %{
    tuple_value: {:ok, :tuple},
    struct_value: 1..3
  })

OutboxStore.reset([poisoned_row])

poisoned_reply =
  try do
    {:reply, json, _} =
      Payments.handle_message(
        "consumer",
        Jason.encode!(%{action: "settlements_since"}),
        state
      )

    {:ok, Jason.decode!(json)}
  rescue
    error -> {:raised, error}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(
  f,
  "tuple Protocol.UndefinedError probe is contained and tuple/struct row values render",
  match?({:protocol_undefined, %Protocol.UndefinedError{}}, tuple_protocol_probe) and
    match?(
      {:ok,
       %{
         "ok" => true,
         "settlements" => [
           %{"tuple_value" => "{:ok, :tuple}", "struct_value" => "1..3"}
         ]
       }},
      poisoned_reply
    )
)

OutboxStore.reset([full_row])

{:reply, _clamped_high, _} =
  Payments.handle_message(
    "consumer",
    Jason.encode!(%{action: "settlements_since", limit: 50_000}),
    state
  )

high_clamped? = Enum.any?(OutboxStore.calls(), &(&1 == {0, 500}))

OutboxStore.reset([full_row])

{:reply, _clamped_low, _} =
  Payments.handle_message(
    "consumer",
    Jason.encode!(%{action: "settlements_since", limit: 0}),
    state
  )

Check.check(
  f,
  "action defaults after_seq and clamps limit to [1, 500]",
  high_clamped? and Enum.any?(OutboxStore.calls(), &(&1 == {0, 1}))
)

OutboxStore.reset([full_row])

invalid_param_replies =
  [
    %{after_seq: 1.0},
    %{after_seq: "1"},
    %{after_seq: -1},
    %{limit: 1.0},
    %{limit: "1"},
    %{limit: -1}
  ]
  |> Enum.map(fn params ->
    {:reply, json, _} =
      Payments.handle_message(
        "consumer",
        Jason.encode!(Map.put(params, :action, "settlements_since")),
        state
      )

    Jason.decode!(json)
  end)

Check.check(
  f,
  "non-integer and negative action cursors/limits refuse as bad_request",
  Enum.all?(
    invalid_param_replies,
    &match?(%{"ok" => false, "error" => "bad_request"}, &1)
  ) and OutboxStore.calls() == []
)

lying_row = %{full_row | outbox_seq: 10}
OutboxStore.reset([lying_row])
OutboxStore.max_seq!(3)

invalid_stateless =
  Payments.settlements_since(
    %{store_mod: OutboxStore, namespace: "llm_quota"},
    0,
    10
  )

{:reply, invalid_action_json, _} =
  Payments.handle_message(
    "consumer",
    Jason.encode!(%{action: "settlements_since"}),
    state
  )

Check.check(
  f,
  "rows above max_seq are invalid_store_result in stateless and action reads",
  invalid_stateless == {:error, :invalid_store_result} and
    match?(
      %{"ok" => false, "error" => "invalid_store_result"},
      Jason.decode!(invalid_action_json)
    )
)

{:reply, degraded_json, _} =
  Payments.handle_message(
    "consumer",
    Jason.encode!(%{action: "settlements_since"}),
    %{state | degraded_boot: true}
  )

Check.check(
  f,
  "degraded boot refuses the outbox read distinctly",
  Jason.decode!(degraded_json) == %{
    "action" => "settlements_since",
    "ok" => false,
    "error" => "degraded_boot"
  }
)

OutboxStore.fail!(:db_down)

{:reply, store_error_json, _} =
  Payments.handle_message(
    "consumer",
    Jason.encode!(%{action: "settlements_since"}),
    state
  )

Check.check(
  f,
  "store failure is a refusal, never empty success",
  Jason.decode!(store_error_json) == %{
    "action" => "settlements_since",
    "ok" => false,
    "error" => "store_unavailable"
  }
)

no_callback_state = %{state | store_mod: NoOutboxStore}

{:reply, no_callback_json, _} =
  Payments.handle_message(
    "consumer",
    Jason.encode!(%{action: "settlements_since"}),
    no_callback_state
  )

Check.check(
  f,
  "non-ephemeral hub with no callback refuses distinctly",
  Jason.decode!(no_callback_json) == %{
    "action" => "settlements_since",
    "ok" => false,
    "error" => "no_outbox_store"
  }
)

{:ok, metric_events} = Agent.start_link(fn -> [] end)

ephemeral =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    trusted_sources: ["consumer"],
    targets: ["consumer"],
    allow_ephemeral: true,
    namespace: "llm_quota",
    auto_tick: false,
    deliver_fn: fn _, _, _ -> {:error, :push_lost} end,
    metrics_fn: fn event, meta -> Agent.update(metric_events, &[{event, meta} | &1]) end
  })

settlement = Map.drop(full_row, [:at, :outbox_seq])
{1, ephemeral} = Payments.settle([settlement], ephemeral)

{1, ephemeral} =
  Payments.settle([%{settlement | idempotency_key: "8453:0xT4:0", tx_hash: "0xT4"}], ephemeral)

{:reply, ephemeral_json, _} =
  Payments.handle_message(
    "consumer",
    Jason.encode!(%{action: "settlements_since", after_seq: 1}),
    ephemeral
  )

ephemeral_page = Jason.decode!(ephemeral_json)
events = Agent.get(metric_events, & &1)

Check.check(
  f,
  "ephemeral mirror serves sequenced rows and reports the highest minted counter",
  ephemeral_page["ok"] == true and ephemeral_page["next_seq"] == 2 and
    ephemeral_page["max_seq"] == 2 and ephemeral_page["complete"] == true and
    hd(ephemeral_page["settlements"])["idempotency_key"] == "8453:0xT4:0"
)

Check.check(
  f,
  "one-shot push failure is metered and the failed row remains readable",
  Enum.any?(events, fn {event, meta} ->
    event == "payments_push_failed" and meta.idempotency_key == "8453:0xT4:0"
  end)
)

raising_metrics =
  Payments.init!(%{
    name: :payments,
    xpub: xpub,
    targets: ["consumer"],
    allow_ephemeral: true,
    namespace: "llm_quota",
    auto_tick: false,
    deliver_fn: fn _, _, _ -> :ok end,
    metrics_fn: fn _, _ -> raise "metrics down" end
  })

raising_result =
  try do
    Payments.settle([settlement], raising_metrics)
  rescue
    error -> {:raised, error}
  catch
    kind, reason -> {kind, reason}
  end

Check.check(
  f,
  "raising metrics_fn never breaks settlement",
  match?({1, %{settlement_mirror: [%{idempotency_key: "8453:0xT1:0"}]}}, raising_result)
)

Check.finish(f)
