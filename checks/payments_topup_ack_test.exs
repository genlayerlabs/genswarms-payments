# Standalone — NO Postgres, NO network.  mix run checks/payments_topup_ack_test.exs
#
# Genswarms.Payments.TopupAck — the package-side presenter for keeper results
# and credit notices. Ported from the first host after the lane's live runs
# (2026-07-27); this check pins the ORCHESTRATION the package now owns:
#
#   1. a MINED result finds the card via order_ref -> row -> conversation and
#      EDITS it in place (retiring the payment link); a row without card
#      columns falls back to `send` — a money message is never dropped for
#      want of an edit target;
#   2. every terminal has the right default copy: mined, reverted,
#      rpc_timeout, and BOTH refused shapes — the insufficient-balance
#      refusal gets the actionable sentence and the raw contract string
#      never reaches the chat;
#   3. `:text_fn` overrides copy without re-owning orchestration (the host-
#      voice seam);
#   4. non-terminal / malformed notifications say nothing at all;
#   5. a store or conversation_fn that raises costs the one message, never
#      the caller's process;
#   6. the credit notice edits the SAME card when the settlement resolves,
#      and still speaks (send) when it does not.
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
check = fn label, ok -> Check.check(f, label, ok) end

alias Genswarms.Payments.TopupAck

defmodule AckSeams do
  # One process-local mailbox for delivered payloads + a swappable store.
  def reset do
    Process.put(:ack_delivered, [])
    :ok
  end

  def deliver(payload) do
    Process.put(:ack_delivered, Process.get(:ack_delivered, []) ++ [payload])
    :ok
  end

  def delivered, do: Process.get(:ack_delivered, [])
end

defmodule AckRowStore do
  # The row store honours ONLY the exact key it was seeded with — the
  # fake-that-answers-anything defect is how the first host shipped a
  # broken ack behind a green check.
  def seed(row), do: Process.put(:ack_row, row)

  def authorization_by_order_ref(order_ref) do
    case Process.get(:ack_row) do
      %{order_ref: ^order_ref} = row -> row
      _ -> nil
    end
  end

  def authorization_by_settlement(method, ref) do
    case Process.get(:ack_row) do
      %{settlement: {^method, ^ref}} = row -> row
      _ -> nil
    end
  end
end

defmodule AckRaisingStore do
  def authorization_by_order_ref(_), do: raise("store fault")
  def authorization_by_settlement(_, _), do: raise("store fault")
end

order_ref = "ref-abc"

card_row = %{
  order_ref: order_ref,
  beneficiary: "user:42",
  amount_usd: "1.0",
  card_chat_id: "tg:5:0",
  card_message_id: 99,
  settlement: {"base_sepolia", "0xtx"}
}

bare_row = Map.merge(card_row, %{card_chat_id: nil, card_message_id: nil})

conversation_fn = fn
  "user:42" -> {:ok, %{conversation_id: "tg:5:0"}}
  _ -> :error
end

ack = fn store, extra_opts ->
  TopupAck.result_fn(
    Keyword.merge(
      [store: store, conversation_fn: conversation_fn, deliver_fn: &AckSeams.deliver/1],
      extra_opts
    )
  )
end

notification = fn result -> %{order_id: "0xrandom", order_ref: order_ref, result: result} end

# ── 1. edit-vs-send ─────────────────────────────────────────────────────────
AckSeams.reset()
AckRowStore.seed(card_row)
ack.(AckRowStore, []).(notification.({:mined, "0xhash"}))

case AckSeams.delivered() do
  [%{action: "edit_message", conversation_id: "tg:5:0", message_id: 99, text: text}] ->
    check.("a mined result EDITS the recorded card", true)
    check.("…with the amount the user came for", String.contains?(text, "1.00 USDC"))
    check.("…and the crediting copy", String.contains?(text, "Crediting your balance"))

  other ->
    check.("a mined result EDITS the recorded card (got #{inspect(other)})", false)
end

AckSeams.reset()
AckRowStore.seed(bare_row)
ack.(AckRowStore, []).(notification.({:mined, "0xhash"}))

check.(
  "a row without card columns falls back to send",
  match?([%{action: "send", conversation_id: "tg:5:0"}], AckSeams.delivered())
)

# ── 2. the default copy matrix ──────────────────────────────────────────────
copy_cases = [
  {{:failed, :reverted}, "didn't go through on the chain", nil},
  {{:failed, :rpc_timeout}, "couldn't reach the network", nil},
  {{:failed, :submit_rejected}, "network fee couldn't be covered", nil},
  {{:refused, "FiatToken: transfer amount exceeds balance"}, "more than that wallet holds",
   "FiatToken"},
  {{:refused, nil}, "refused before sending", nil}
]

for {result, must, must_not} <- copy_cases do
  AckSeams.reset()
  AckRowStore.seed(card_row)
  ack.(AckRowStore, []).(notification.(result))

  case AckSeams.delivered() do
    [%{text: text}] ->
      check.("#{inspect(result)} speaks with the default copy", String.contains?(text, must))

      if must_not,
        do:
          check.(
            "…and never leaks the raw contract string",
            not String.contains?(text, must_not)
          )

    other ->
      check.("#{inspect(result)} speaks (got #{length(other)} messages)", false)
  end
end

# ── 3. the host-voice seam ──────────────────────────────────────────────────
AckSeams.reset()
AckRowStore.seed(card_row)

ack.(AckRowStore, text_fn: fn {:mined, _}, amt -> "custom #{amt}" end).(
  notification.({:mined, "0xhash"})
)

check.(
  "text_fn overrides copy without re-owning orchestration",
  match?([%{action: "edit_message", text: "custom 1.00"}], AckSeams.delivered())
)

# ── 4. silence where silence is the answer ──────────────────────────────────
AckSeams.reset()
AckRowStore.seed(card_row)
handler = ack.(AckRowStore, [])
handler.(notification.({:pending, "0x"}))
handler.(notification.(:weird))
handler.(%{order_id: "only-keeper-id", result: {:mined, "0x"}})
handler.(:not_a_notification)
check.("non-terminal / malformed notifications say nothing", AckSeams.delivered() == [])

check.(
  "an order_ref the store does not know says nothing",
  (AckRowStore.seed(%{card_row | order_ref: "other"})
   handler.(notification.({:mined, "0x"}))
   AckSeams.delivered() == [])
)

# ── 5. faults cost the message, never the caller ────────────────────────────
AckSeams.reset()

check.(
  "a raising store is swallowed (keeper survives)",
  ack.(AckRaisingStore, []).(notification.({:mined, "0x"})) == :ok
)

AckSeams.reset()
AckRowStore.seed(card_row)

check.(
  "a raising conversation_fn is swallowed",
  TopupAck.result_fn(
    store: AckRowStore,
    conversation_fn: fn _ -> raise "resolver down" end,
    deliver_fn: &AckSeams.deliver/1
  ).(notification.({:mined, "0x"})) == :ok
)

check.("…and nothing was delivered either way", AckSeams.delivered() == [])

# ── 6. the credit closes the same card ──────────────────────────────────────
credit = fn store ->
  TopupAck.credit_notice_fn(
    store: store,
    conversation_fn: conversation_fn,
    deliver_fn: &AckSeams.deliver/1
  )
end

AckSeams.reset()
AckRowStore.seed(card_row)

credit.(AckRowStore).(%{
  budget_identity: "user:42",
  credited: "1.0",
  balance: "1.0",
  method: "base_sepolia",
  ref: "0xtx"
})

case AckSeams.delivered() do
  [%{action: "edit_message", message_id: 99, text: text}] ->
    check.("the credit EDITS the same card into its final state", true)
    check.("…stating credited amount and balance", String.contains?(text, "1.00 USDC credited"))

  other ->
    check.("the credit edits the card (got #{inspect(other)})", false)
end

AckSeams.reset()
AckRowStore.seed(card_row)

credit.(AckRowStore).(%{
  budget_identity: "user:42",
  credited: "2.5",
  balance: "3.5"
})

check.(
  "a credit with no settlement facts still SPEAKS (send fallback)",
  match?([%{action: "send", text: "✅ 2.50 USDC credited. Balance: $3.50."}], AckSeams.delivered())
)

AckSeams.reset()

check.(
  "a raising store on the credit path is swallowed",
  credit.(AckRaisingStore).(%{budget_identity: "user:42", credited: "1", balance: "1"}) == :ok
)

Check.finish(f)
IO.puts("PAYMENTS_TOPUP_ACK: ALL PASS")
