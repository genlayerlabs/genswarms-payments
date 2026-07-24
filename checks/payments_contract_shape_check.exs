Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

valid_config = %{
  xpub: xpub,
  trusted_sources: ["trusted"],
  targets: [],
  auto_tick: false,
  chains: []
}

valid_init = Payments.init(valid_config)

Check.check(f, "init/1 valid config returns {:ok, state_map}",
  match?({:ok, %{}}, valid_init))

invalid_init =
  try do
    Payments.init(%{})
  rescue
    error -> {:raised, error}
  catch
    kind, reason -> {:caught, kind, reason}
  end

Check.check(f, "init/1 invalid config returns {:error, term} without raising",
  match?({:error, _}, invalid_init))

{:ok, initial_state} = valid_init

shape_ok? = fn
  {:reply, json, state} when is_binary(json) and is_map(state) ->
    match?({:ok, _}, Jason.decode(json))

  {:noreply, state} when is_map(state) ->
    true

  _other ->
    false
end

# Derive the accepted action strings from the real handler source. This makes
# a newly added action fail here until it has an explicit shape-driving case.
source = File.read!(Path.expand("../lib/genswarms/payments.ex", __DIR__))

literal_actions =
  Regex.scan(~r/%\{"action"\s*=>\s*"([^"]+)"\}/, source, capture: :all_but_first)
  |> List.flatten()

grouped_actions =
  Regex.scan(~r/action in ~w\(([^)]+)\)/, source, capture: :all_but_first)
  |> Enum.flat_map(fn [actions] -> String.split(actions) end)

implemented_actions = MapSet.new(literal_actions ++ grouped_actions)

action_messages = %{
  "health" => %{"action" => "health"},
  "tick" => %{"action" => "tick"},
  "deposit_address" => %{"action" => "deposit_address", "beneficiary" => "budget:shape"},
  "payment_status" => %{"action" => "payment_status", "beneficiary" => "budget:shape"},
  "ingest_event" => %{"action" => "ingest_event"}
}

Check.check(f, "every implemented action string has an explicit shape-driving case",
  implemented_actions == MapSet.new(Map.keys(action_messages)))

{known_shapes_ok?, state_after_actions} =
  Enum.reduce(action_messages, {true, initial_state}, fn {_action, message}, {ok?, state} ->
    result = Payments.handle_message("trusted", Jason.encode!(message), state)

    next_state =
      case result do
        {:reply, _json, next_state} -> next_state
        {:noreply, next_state} -> next_state
        _other -> state
      end

    {ok? and shape_ok?.(result), next_state}
  end)

Check.check(f, "every known action returns an ObjectHandler message shape",
  known_shapes_ok?)

unknown_result =
  Payments.handle_message("trusted", Jason.encode!(%{"action" => "future_unknown"}), state_after_actions)

Check.check(f, "unknown action returns an ObjectHandler message shape",
  shape_ok?.(unknown_result))

malformed_result = Payments.handle_message("trusted", "{not-json", state_after_actions)

Check.check(f, "malformed JSON returns an ObjectHandler message shape",
  shape_ok?.(malformed_result))

untrusted_result =
  Payments.handle_message(
    "stranger",
    Jason.encode!(%{"action" => "deposit_address", "beneficiary" => "budget:untrusted"}),
    state_after_actions
  )

Check.check(f, "untrusted source returns an ObjectHandler message shape",
  shape_ok?.(untrusted_result))

untrusted_tick_result =
  Payments.handle_message("stranger", Jason.encode!(%{"action" => "tick"}), state_after_actions)

Check.check(f, "untrusted tick returns an ObjectHandler message shape",
  shape_ok?.(untrusted_tick_result))

bad_known_result =
  Payments.handle_message("trusted", Jason.encode!(%{"action" => "deposit_address"}), state_after_actions)

Check.check(f, "malformed known action returns a JSON reply and state map",
  shape_ok?.(bad_known_result) and match?({:reply, _, _}, bad_known_result))

Check.finish(f)
