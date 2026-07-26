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
  record_unrecognised_inflow: 1
]

Check.check(
  f,
  "Store behaviour declares all 17 callbacks",
  Enum.all?(expected, &(&1 in callbacks))
)

Check.check(
  f,
  "all Store callbacks are optional",
  Enum.sort(Genswarms.Payments.Store.behaviour_info(:optional_callbacks)) ==
    Enum.sort(expected)
)

Check.finish(f)
