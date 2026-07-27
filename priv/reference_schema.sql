-- ============================================================================
-- Genswarms.Payments — REFERENCE PostgreSQL schema for the Store contract
-- ============================================================================
--
-- This file is the schema the contract's semantics were proven against (the
-- first host's live Base Sepolia runs, 2026-07). It is a REFERENCE, not a
-- migration runner: the host owns its schema and runs its own migrations.
-- Copy these statements into your migration system, rename nothing you don't
-- have to, and then prove the result with
-- `Genswarms.Payments.StoreConformance.run!(YourStore)` against a throwaway
-- database — the conformance suite, not this file, is the authority on
-- whether your store behaves.
--
-- Column notes that carry money semantics are inline. Tables owned by OTHER
-- packages (e.g. the LLM proxy's credit ledger) are deliberately absent:
-- this file covers exactly what `Genswarms.Payments.Store` callbacks touch.

-- ── deposit-address bindings (entry B: per-user HD deposit addresses) ───────
CREATE TABLE IF NOT EXISTS payment_address_bindings (
  beneficiary TEXT PRIMARY KEY,
  hd_index     INTEGER NOT NULL,
  address      TEXT NOT NULL,
  namespace    TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Both uniques are load-bearing: one HD index and one address must never be
-- bound to two beneficiaries.
CREATE UNIQUE INDEX IF NOT EXISTS payment_address_bindings_hd_index_key
  ON payment_address_bindings (hd_index);

CREATE UNIQUE INDEX IF NOT EXISTS payment_address_bindings_address_key
  ON payment_address_bindings (address);

-- ── settlements + transactional outbox ──────────────────────────────────────
-- The outbox sequence is minted ONLY for creditable ("settled") rows;
-- quarantined rows persist with outbox_seq NULL and must never receive one at
-- record time (release is an operator action that mints a FRESH sequence).
CREATE SEQUENCE IF NOT EXISTS payment_settlement_outbox_seq;

CREATE TABLE IF NOT EXISTS payment_settlements (
  id              BIGSERIAL PRIMARY KEY,
  outbox_seq      BIGINT UNIQUE,
  -- settlement dedup wall: one idempotency_key settles exactly once, ever
  idempotency_key TEXT UNIQUE NOT NULL,
  beneficiary     TEXT NOT NULL,
  namespace       TEXT,
  amount_usd      NUMERIC NOT NULL,
  method          TEXT,
  ref             TEXT,
  status          TEXT NOT NULL DEFAULT 'settled',
  -- full method-supplied audit facts; authorization settlements carry
  -- facts->>'nonce_hex', which authorization_settled?/1 and
  -- authorization_by_settlement/2 join on EXACTLY
  facts           JSONB NOT NULL DEFAULT '{}'::jsonb,
  at              TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payment_settlements_beneficiary
  ON payment_settlements (beneficiary, created_at DESC);

-- issuance_totals_since/3 (the C1 aggregate cap) scans settled rows by
-- namespace + window; without this partial index the cap query table-scans.
CREATE INDEX IF NOT EXISTS idx_payment_settlements_window
  ON payment_settlements (namespace, at)
  WHERE status = 'settled';

-- ── chain scan cursor ───────────────────────────────────────────────────────
-- Advanced ONLY after every settlement of the scanned range is recorded
-- (fail-closed scanning); one row per chain name.
CREATE TABLE IF NOT EXISTS payment_scan_cursor (
  chain       TEXT PRIMARY KEY,
  last_block  BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── issued authorizations (entry A: EIP-3009 top-ups) ───────────────────────
CREATE TABLE IF NOT EXISTS topup_authorizations (
  id            BIGSERIAL PRIMARY KEY,
  -- lowercase "0x" + 64 hex — exactly the AuthorizationUsed log topic form
  nonce_hex     TEXT NOT NULL UNIQUE,
  -- issuance identity: replays of one order_ref return the row ON RECORD
  order_ref     TEXT NOT NULL UNIQUE,
  beneficiary   TEXT NOT NULL,
  namespace     TEXT NOT NULL,
  amount_usd    NUMERIC(20, 6) NOT NULL,
  valid_before  BIGINT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  consumed_at   TIMESTAMPTZ,
  -- the chat card this top-up progresses through (TopupAck edits it in
  -- place, retiring the payment link). NULL = not editable; every stage
  -- falls back to sending its own message. Written only by the host's
  -- delivery effect, observing the card actually landing.
  card_chat_id    TEXT,
  card_message_id BIGINT
);

CREATE INDEX IF NOT EXISTS topup_authorizations_beneficiary_idx
  ON topup_authorizations (beneficiary);

-- live_authorization_nonces/1: unconsumed and still inside the window
CREATE INDEX IF NOT EXISTS topup_authorizations_live_idx
  ON topup_authorizations (valid_before)
  WHERE consumed_at IS NULL;

-- ── unrecognised treasury inflows (the §4.4 audit trail) ────────────────────
-- Money that arrived at the treasury without a matching issued authorization.
-- Never credited; the UNIQUE is what lets a rescan re-observe the same
-- transfer without erroring the scan (record_unrecognised_inflow dedupes).
CREATE TABLE IF NOT EXISTS treasury_unrecognised_inflows (
  id          BIGSERIAL PRIMARY KEY,
  chain       TEXT NOT NULL,
  tx_hash     TEXT NOT NULL,
  log_index   INTEGER NOT NULL,
  from_addr   TEXT NOT NULL,
  amount_usd  NUMERIC(20, 6) NOT NULL,
  nonce_hex   TEXT,
  reason      TEXT,
  seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (chain, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS treasury_unrecognised_inflows_seen_idx
  ON treasury_unrecognised_inflows (seen_at DESC);
