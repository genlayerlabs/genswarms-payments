# genswarms-payments

Payment settlement hub object for [genswarms](https://github.com/genlayerlabs/genswarms)
swarms. It owns beneficiary identity (a stable HD deposit address per
beneficiary), the idempotent settlement ledger, and stamped
`payment_confirmed` delivery to allowlisted downstream targets. Payment
modalities (in-tree USDC today; Stripe, x402, etc. as sibling packages
tomorrow) implement `Genswarms.Payments.Method` and plug into the same core —
a hub-and-adapter design: one settlement ledger, one delivery path, any
number of ways money can arrive.

## Trust model

Every capability is fail-closed, gated by two allowlists:

- **`trusted_sources`** — who may talk to the object at all. An untrusted
  `deposit_address`, `payment_status`, `tick`, or `ingest_event` message gets
  silent `{:noreply, _}` (only `health` is unauthenticated). Empty
  `trusted_sources` means nobody can act.
- **`targets`** — who may receive `payment_confirmed`. Empty `targets` means
  nobody is ever credited, even though settlement still records durably.

Push modalities add a second gate before settlement ever sees a payload: the
`Method.ingest_event/2` callback must verify the payload's authenticity
(webhook signature, facilitator signature, ...) itself — the core trusts
whatever settlements a method returns, so an unverified method is a hole. No
push method ships yet; `ingest_event` currently always replies
`{"ok": false, "error": "no_push_methods"}`.

## Config

```elixir
%{
  name: :payments,                    # object name, stamped on every delivered message (default :payments)
  swarm_name: "my_swarm",             # used by the default deliver_fn (default "swarm")
  xpub: System.fetch_env!("PAYMENTS_XPUB"),   # required — watch-only, see Custody below
  trusted_sources: ["telegram_ingress", "cron"],  # required for anything to work (default [])
  targets: ["downstream_object"],     # required for anyone to get credited (default [])
  namespace: "default",               # stamped on bindings/deliveries; caller-defined meaning (default "default")
  store_mod: MyApp.PaymentsStore,     # optional — see Store contract (default nil = memory)
  chains: [
    %{
      name: "base",
      rpc_url: System.fetch_env!("BASE_RPC_URL"),
      usdc_contract: "0x...",
      confirmations: 12,              # default 12
      decimals: 6,                    # default 6
      start_block: 0,                 # default 0 — cold-start scan floor
      max_block_range: 2000,          # default 2000 — cap per poll round
      address_chunk: 200              # default 200 — addresses per eth_getLogs call
    }
  ],
  methods: [Genswarms.Payments.Usdc], # pluggable modalities (default [Genswarms.Payments.Usdc])
  deliver_fn: fn target, from, content -> ... end,  # default dispatches via the host ObjectServer
  now_fn: &DateTime.utc_now/0,        # injection seam for checks (default)
  rpc_fn: &Genswarms.Payments.Rpc.call/3,  # injection seam for checks (default)
  auto_tick: true,                    # currently INERT — see below (default true)
  poll_interval_ms: 60_000            # currently INERT — see below (default 60_000)
}
```

`auto_tick` and `poll_interval_ms` are accepted and stored but nothing in
this package reads them to schedule anything — a poll round only happens
when a trusted source sends `{"action": "tick"}`. In practice that means
wiring a scheduler object (e.g. genswarms-cron) to deliver `tick` on an
interval; this package owns the settlement/watch logic, not the clock.

## Object protocol

- `{"action": "health"}` — unauthenticated; `{"ok": true, "bindings": N}`.
- `{"action": "tick"}` — trusted only; runs one poll round (every configured
  method scans, settlements settle, cursors advance per the fail-closed rule
  below). No reply.
- `{"action": "deposit_address", "beneficiary": "..."}` — trusted only;
  returns the beneficiary's stable address, minting one on first ask.
- `{"action": "payment_status", "beneficiary": "..."}` — trusted only;
  returns the address plus recorded payments (empty list if unbound or the
  store has none).
- `{"action": "ingest_event", ...}` — trusted only; reserved for future push
  methods, currently always refuses.

## Custody model

The object holds an **xpub only** — watch-only BIP32 public derivation
(`Genswarms.Payments.HD`), pure Elixir, no NIFs. It can compute deposit
addresses and watch them; it can never sign a transaction, because it never
sees or accepts an xprv (a private key). If the host ever misconfigures the
wrong chain for a contract, funds sent are still recoverable — the address
itself is a standard EIP-55 Ethereum account controlled by whoever holds the
matching xprv offline, not something this object can lose custody of by
misbehaving.

## Store contract (`Genswarms.Payments.Store`)

Every callback is optional; missing ones fall back to an in-memory mirror
(fine in dev, lost on restart).

| Callback | Purpose |
|---|---|
| `put_address_binding/1` | persist `%{beneficiary, index, address, namespace}` |
| `get_address_binding/1` | fetch a binding by beneficiary |
| `list_address_bindings/0` | boot: rebuild the watched set + next index |
| `payment_seen?/1` | settlement dedup by idempotency key — must be durable in prod |
| `record_payment/1` | record one settled payment |
| `get_last_scanned_block/1` | last fully-settled block for a chain |
| `put_last_scanned_block/2` | advance a chain's scan cursor |
| `list_payments/1` | settled payments for a beneficiary, newest first |

Unlike budget *reads* in sibling packages, settlement **writes** fail closed:
if a configured store errors on the dedup read or the record write, the
round holds that settlement rather than risk crediting it twice or losing
it. No store at all is a legitimate dev mode — memory dedup still works
within a single run.

## Settlement fail-closed rule and the cursor invariant

`settle/2` durably dedups each settlement before recording it, then delivers
`payment_confirmed` to every target. A settlement is skipped (never
recorded, never delivered) only when the store errors on the dedup read or
the write — the watcher will re-present it next round.

`poll/1` advances a chain's scan cursor (`put_last_scanned_block`) **only**
when the round found something to advance to (a non-nil `safe_to`) **and**
every settlement scanned for that chain actually settled (recorded, or
already-seen — dedup counts). If even one of that chain's settlements was
held back by a store failure, the cursor stays put, so the next `tick`
re-scans and re-presents it. This is the invariant that makes the whole
pipeline safe against a flaky store: nothing is ever double-credited, and
nothing is ever silently skipped.

## In-tree USDC watcher

`Genswarms.Payments.Usdc` is a pull method: per `tick`, per configured
chain, it fetches `eth_blockNumber`, computes `safe_to = latest -
confirmations`, and pulls `eth_getLogs` for the ERC-20 `Transfer` topic
against the chain's `usdc_contract`, chunked over watched addresses
(`address_chunk`) and capped in range (`max_block_range`) so a cold start
never issues an unbounded query. Two client-side defenses run even though
the RPC is asked to filter: logs are re-filtered by `blockNumber <= to`
(never trust a provider to honor `toBlock`) and by exact contract address
match (never trust a `Transfer`-shaped log to actually be USDC — a
misbehaving or compromised RPC could hand back logs from an unrelated
contract). `Genswarms.Payments.Rpc` shells out to `curl` (the engine has no
`:inets`); the RPC URL — which may embed a provider API key — rides a
chmod-600 `--config` tempfile, never argv where `ps` would expose it, and is
scrubbed from both successful and error output before it's logged.

## Method behaviour

Future modalities (Stripe, x402, ...) ship as sibling packages implementing
`Genswarms.Payments.Method`: `id/0`, `capabilities/0`, and either `poll/2`
(pull: scan and return `{chain, settlements, safe_to}` per configured
target) or `ingest_event/2` (push: verify then return settlements). Both
callbacks are optional so a method can be pull-only or push-only.

## Verification

```sh
mix deps.get
./checks/run.sh        # every checks/payments_*.exs — no Postgres, no network
```
