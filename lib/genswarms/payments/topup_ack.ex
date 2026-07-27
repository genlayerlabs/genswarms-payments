defmodule Genswarms.Payments.TopupAck do
  @moduledoc """
  Tells the paying user what happened to their top-up, seconds after they
  sign — the presenter half of the authorization lane, generic over hosts.

  ## Why this lives in the package

  A top-up credits through two independent one-minute cron ticks: the chain
  watcher notices the treasury Transfer, then the credit poll applies it.
  From the user's side that is up to two and a half minutes of a completely
  silent chat after they signed — long enough, measured on the lane's first
  live runs (2026-07-27, the first host), that a person reasonably concludes
  it did not work. The keeper already knows the outcome within seconds and
  calls `result_fn` on every terminal result; this module is that callback,
  written once, so the second host does not rediscover the silence.

  It does not credit anything and it is not part of the money path — the
  credit still comes from the settlement, on its own schedule, as the only
  authority on the balance. This only removes the silence.

  ## What the host injects

  Everything transport- or host-shaped is a seam; the orchestration and the
  default English copy are the package's:

    * `:store` — a module answering `authorization_by_order_ref/1` and
      `authorization_by_settlement/2` (see `Genswarms.Payments.Store`).
    * `:conversation_fn` — `(beneficiary -> {:ok, %{conversation_id: cid}}
      | term)`: where does this budget identity live? The first host answers
      it from the LLM proxy's budget-origin binding.
    * `:deliver_fn` — `(payload_map -> term)`: actually says it. The payload
      is `%{action: "send" | "edit_message", conversation_id: cid, text: t}`
      (+ `:message_id` on edits); encoding and transport are the host's.
    * `:text_fn` / `:credit_text_fn` (optional) — a host that wants its own
      voice overrides copy without re-owning the orchestration.

  ## The card

  When the store row carries `card_chat_id`/`card_message_id` (recorded by
  the host's delivery effect when the original top-up card lands), each
  stage EDITS that card instead of stacking messages — which is also what
  retires the payment link: the card that carried it becomes the outcome.
  A row without a card falls back to `send`; a money confirmation is never
  dropped because an edit target is missing.

  ## Failure stance

  Every path is best effort and silent on failure. This runs inside the
  keeper's (or proxy's) process: a missing row, an unresolvable
  conversation or a delivery error must cost this one chat message and
  nothing else. The money is already on chain either way.
  """

  require Logger

  @doc """
  Builds the `result_fn` the keeper calls on every terminal result.

  The keeper hands a map carrying BOTH identifiers; resolution is on
  `order_ref` — the ref the host minted and stored. `order_id` is the
  keeper's own random internal id and nothing host-side is keyed by it (an
  earlier host revision keyed on it and acknowledged nobody, silently, on
  every real payment).
  """
  def result_fn(opts) do
    store = Keyword.fetch!(opts, :store)
    conversation_fn = Keyword.fetch!(opts, :conversation_fn)
    deliver_fn = Keyword.fetch!(opts, :deliver_fn)
    text_fn = Keyword.get(opts, :text_fn, &default_text/2)

    fn
      %{order_ref: order_ref, result: result} when is_binary(order_ref) ->
        deliver(order_ref, result, store, conversation_fn, deliver_fn, text_fn)

      _other ->
        :ok
    end
  end

  @doc """
  Builds the `credit_notice_fn` the LLM proxy (or any credit authority)
  calls when a credit lands — the card's LAST state.

  Falls back to a plain `send` whenever the card cannot be found: a credit
  the user is never told about is the one outcome worse than an untidy chat.
  """
  def credit_notice_fn(opts) do
    store = Keyword.fetch!(opts, :store)
    conversation_fn = Keyword.fetch!(opts, :conversation_fn)
    deliver_fn = Keyword.fetch!(opts, :deliver_fn)
    credit_text_fn = Keyword.get(opts, :credit_text_fn, &default_credit_text/2)

    fn payload -> deliver_credit(payload, store, conversation_fn, deliver_fn, credit_text_fn) end
  end

  @doc """
  Default copy for one terminal keeper result, or nil when the result is
  not one the user should hear about.

  `nil` is a real answer, not a gap: only outcomes the user can act on — or
  must stop waiting for — are worth a message.

  `amount` is the top-up's own amount label when known. It matters most on
  the edit path, where this text REPLACES the original card: without it the
  card would lose the one number the user came for.
  """
  def default_text(result, amount \\ nil)

  def default_text({:mined, _hash}, amount) do
    "⏳ #{topup_of(amount)}signed and on the chain. Crediting your balance…"
  end

  def default_text({:failed, :reverted}, amount) do
    "❌ #{topup_of(amount)}didn't go through on the chain. Nothing was charged — send /topup to retry."
  end

  def default_text({:failed, :rpc_timeout}, amount) do
    "❌ #{topup_of(amount)}couldn't reach the network. Nothing was charged — send /topup to retry."
  end

  # The node refused the broadcast — OUR side couldn't pay the network fee
  # (dry relayer, the 2026-07-27 live test) or the transaction was invalid.
  # The user's signature cost them nothing and the cause is operator-side,
  # so the copy points at retrying later, not at anything they did wrong.
  def default_text({:failed, :submit_rejected}, amount) do
    "❌ #{topup_of(amount)}couldn't be submitted — the network fee couldn't be covered right now. " <>
      "Nothing was charged — try /topup again in a few minutes."
  end

  # Refused by the pre-broadcast simulation: nothing was sent at all, and
  # the keeper hands over the contract's own reason. The one reason a person
  # can act on directly — not enough USDC in the signing wallet — gets its
  # own sentence; everything else gets an honest generic refusal. The raw
  # contract string ("FiatToken: transfer amount exceeds balance") never
  # reaches a chat user.
  def default_text({:refused, reason}, amount) when is_binary(reason) do
    if reason =~ "exceeds balance" do
      "❌ #{topup_of(amount)}is more than that wallet holds. Nothing was sent — /topup a smaller amount."
    else
      "❌ #{topup_of(amount)}was refused before sending. Nothing was charged — send /topup to retry."
    end
  end

  def default_text({:refused, _reason}, amount) do
    "❌ #{topup_of(amount)}was refused before sending. Nothing was charged — send /topup to retry."
  end

  def default_text(_other, _amount), do: nil

  @doc "Default copy for the card's final state: what was credited, and the new balance."
  def default_credit_text(credited, balance) do
    "✅ #{money(credited)} USDC credited. Balance: $#{money(balance)}."
  end

  defp topup_of(nil), do: "Top-up "
  defp topup_of(amount), do: "#{amount} USDC "

  # order_ref -> the issued authorization -> the budget identity -> the
  # conversation that identity was last bound to. Every hop is a read the
  # system already keeps for other reasons; nothing here is a new source of
  # truth about who owns what.
  defp deliver(order_ref, result, store, conversation_fn, deliver_fn, text_fn) do
    with row when is_map(row) <- store.authorization_by_order_ref(order_ref),
         message when is_binary(message) <- text_fn.(result, amount_label(row)),
         {:ok, %{conversation_id: cid}} when is_binary(cid) <-
           conversation_fn.(row.beneficiary) do
      deliver_fn.(payload(row, cid, message))
      :ok
    else
      _ -> :ok
    end
  rescue
    e ->
      Logger.warning("topup ack delivery failed: #{Exception.message(e)}")
      :ok
  catch
    kind, reason ->
      Logger.warning("topup ack delivery #{kind}: #{inspect(reason)}")
      :ok
  end

  # EDIT the card when we know which message it is, otherwise send a new one.
  # Editing is what retires the payment link; falling back to `send` is not a
  # nicety — a card whose delivery effect never recorded an id (delivery
  # failed, or a send predating card recording) must still get its answer.
  defp payload(%{card_chat_id: chat_id, card_message_id: message_id}, cid, message)
       when is_binary(chat_id) and is_integer(message_id) do
    %{
      action: "edit_message",
      # The recorded chat, not the resolved one: the message id is only
      # meaningful in the chat it was posted to.
      conversation_id: chat_id,
      message_id: message_id,
      text: message
    }
    |> tap(fn _ -> if chat_id != cid, do: log_chat_drift(chat_id, cid) end)
  end

  defp payload(_row, cid, message) do
    %{action: "send", conversation_id: cid, text: message}
  end

  defp log_chat_drift(card_chat, resolved_chat) do
    Logger.warning(
      "topup ack: card lives in #{card_chat} but the budget resolves to " <>
        "#{resolved_chat} — editing the card"
    )
  end

  defp amount_label(%{amount_usd: amount}) when not is_nil(amount) do
    amount |> Decimal.new() |> Decimal.round(2) |> Decimal.to_string(:normal)
  rescue
    _ -> nil
  end

  defp amount_label(_row), do: nil

  # The credit's own settlement identifies the order, which identifies the
  # card. When any hop misses we still speak — to the conversation the
  # budget identity was last bound to, the same route a built-in notice
  # would have used.
  defp deliver_credit(payload, store, conversation_fn, deliver_fn, credit_text_fn) do
    %{budget_identity: identity, credited: credited, balance: balance} = payload
    message = credit_text_fn.(credited, balance)

    row =
      with method when is_binary(method) <- Map.get(payload, :method),
           ref when is_binary(ref) <- Map.get(payload, :ref) do
        store.authorization_by_settlement(method, ref)
      else
        _ -> nil
      end

    case conversation_fn.(identity) do
      {:ok, %{conversation_id: cid}} when is_binary(cid) ->
        deliver_fn.(payload(row || %{}, cid, message))
        :ok

      _ ->
        :ok
    end
  rescue
    e ->
      Logger.warning("topup credit card update failed: #{Exception.message(e)}")
      :ok
  catch
    kind, reason ->
      Logger.warning("topup credit card update #{kind}: #{inspect(reason)}")
      :ok
  end

  defp money(value) do
    value |> Decimal.new() |> Decimal.round(2) |> Decimal.to_string(:normal)
  rescue
    _ -> to_string(value)
  end
end
