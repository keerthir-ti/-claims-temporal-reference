-- MOCK application data: stands in for ClaimsPro, eClaims, Comm Hub, Policy Gate and the change feed
-- the event bridge reads. Only mock activities and the mock tools touch this schema.
CREATE SCHEMA IF NOT EXISTS mock;
CREATE SEQUENCE IF NOT EXISTS mock.ids START 1000;

CREATE TABLE IF NOT EXISTS mock.claims (
  claim_id             text PRIMARY KEY,
  scenario             text   NOT NULL,
  status               text   NOT NULL DEFAULT 'OPEN',
  line_of_business     text   NOT NULL,
  company              text   NOT NULL,
  channel              text   NOT NULL,
  state                text   NOT NULL,
  change_version       bigint NOT NULL DEFAULT 1,
  injury_reported      boolean NOT NULL DEFAULT false,
  prior_claims         int    NOT NULL DEFAULT 0,
  claimed_amount_cents bigint NOT NULL DEFAULT 0,
  adjuster_id          text   NOT NULL DEFAULT '',
  documents_received   boolean NOT NULL DEFAULT false,
  closed_reason        text,
  behaviour            jsonb  NOT NULL DEFAULT '{}',
  updated_at           timestamptz NOT NULL DEFAULT now()
);

-- Adjuster tasks (the ClaimsPro side of a human decision).
CREATE TABLE IF NOT EXISTS mock.tasks (
  task_id                  text PRIMARY KEY DEFAULT 'TASK-' || nextval('mock.ids'),
  decision_id              text UNIQUE NOT NULL,
  workflow_id              text NOT NULL,
  claim_id                 text NOT NULL,
  kind                     text NOT NULL,
  choices                  text[] NOT NULL,
  required_role            text NOT NULL DEFAULT '',
  required_authority_cents bigint NOT NULL DEFAULT 0,
  requested_by             text NOT NULL DEFAULT '',
  level                    int  NOT NULL DEFAULT 0,
  context                  text NOT NULL DEFAULT '',
  due_at_ms                bigint NOT NULL DEFAULT 0,
  status                   text NOT NULL DEFAULT 'OPEN',
  outcome                  text,
  created_at               timestamptz NOT NULL DEFAULT now(),
  closed_at                timestamptz
);

CREATE TABLE IF NOT EXISTS mock.reserves (
  idempotency_key text PRIMARY KEY,
  claim_id        text NOT NULL,
  reserve_id      text NOT NULL DEFAULT 'RES-' || nextval('mock.ids'),
  amount_cents    bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS mock.payments (
  payment_key  text PRIMARY KEY,
  claim_id     text NOT NULL,
  payment_id   text NOT NULL DEFAULT 'PAY-' || nextval('mock.ids'),
  amount_cents bigint NOT NULL,
  payee_ref    text NOT NULL DEFAULT '',
  status       text NOT NULL DEFAULT 'CREATED',
  ack_lost     boolean NOT NULL DEFAULT false,
  created_at   timestamptz NOT NULL DEFAULT now(),
  closed_at    timestamptz
);

CREATE TABLE IF NOT EXISTS mock.contacts (
  idempotency_key text PRIMARY KEY,
  claim_id        text NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mock.dispatches (
  idempotency_key text PRIMARY KEY,
  claim_id        text NOT NULL,
  accepted        boolean,
  vendor_id       text,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mock.appraisals (
  idempotency_key text PRIMARY KEY,
  claim_id        text NOT NULL,
  appraisal_id    text NOT NULL DEFAULT 'APR-' || nextval('mock.ids')
);

CREATE TABLE IF NOT EXISTS mock.estimates (
  claim_id     text PRIMARY KEY,
  amount_cents bigint NOT NULL,
  available_at timestamptz NOT NULL
);

-- Database change events of the claims systems, as the event bridge would read them over JDBC.
CREATE TABLE IF NOT EXISTS mock.change_events (
  position      bigserial PRIMARY KEY,
  event_id      text UNIQUE NOT NULL,
  claim_id      text NOT NULL,
  type          text NOT NULL,
  source_system text NOT NULL,
  occurred_at   timestamptz NOT NULL DEFAULT now(),
  visible_at    timestamptz NOT NULL DEFAULT now(),
  delivered_at  timestamptz,
  result        text
);
CREATE INDEX IF NOT EXISTS change_events_pending ON mock.change_events (visible_at) WHERE delivered_at IS NULL;
