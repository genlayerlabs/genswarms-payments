# Standalone — NO Postgres, NO network.  mix run checks/payments_foundations_test.exs
Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()

callbacks = Genswarms.Payments.Store.behaviour_info(:callbacks)

expected = [
  put_address_binding: 1,
  get_address_binding: 1,
  list_address_bindings: 0,
  payment_seen?: 1,
  record_payment: 1,
  get_last_scanned_block: 1,
  put_last_scanned_block: 2,
  list_payments: 1,
  list_settlements_since: 2,
  issuance_totals_since: 3,
  # D3 operator surface (phase 4): release is the ONLY writer that turns a
  # quarantined row back into creditable money, and the quarantined read is the
  # operator's held-money queue.
  release_quarantined_payment: 2,
  list_quarantined_payments: 3,
  # Entry A (Task 5): the hub owns the issued-authorization registry end to
  # end (issuance, lookup, the live-nonce filter, consumption) plus the
  # unrecognised-inflow audit trail the §4.4 credit rule writes to.
  record_issued_authorization: 1,
  issued_authorization: 1,
  live_authorization_nonces: 1,
  mark_authorization_consumed: 1,
  # N1 (re-review fix wave): one-credit-per-nonce as a hub-state fact, not an
  # assumption borrowed from mark_authorization_consumed/1 never failing.
  authorization_settled?: 1,
  record_unrecognised_inflow: 1,
  # Presenter reads (2026-07-27): the TopupAck chat card resolves the issued
  # row by the keeper's order_ref and by the landed settlement.
  authorization_by_order_ref: 1,
  authorization_by_settlement: 2,
  # Dashboard reads (2026-07-27): the package's Top-ups and Deposits pages.
  list_issued_authorizations: 1,
  list_unrecognised_inflows: 1,
  list_deposit_balances: 1
]

Check.check(
  f,
  "Store behaviour declares all 23 callbacks",
  Enum.all?(expected, &(&1 in callbacks))
)

Check.check(
  f,
  "all Store callbacks are optional",
  Enum.sort(Genswarms.Payments.Store.behaviour_info(:optional_callbacks)) ==
    Enum.sort(expected)
)

Check.finish(f)
