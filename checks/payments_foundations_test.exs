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
  list_settlements_since: 2
]

Check.check(
  f,
  "Store behaviour declares all 9 callbacks",
  Enum.all?(expected, &(&1 in callbacks))
)

Check.check(
  f,
  "all Store callbacks are optional",
  Enum.sort(Genswarms.Payments.Store.behaviour_info(:optional_callbacks)) ==
    Enum.sort(expected)
)

Check.finish(f)
