-- Business history kept outside Temporal (real: keep). Written only through the Projection activities.
CREATE SCHEMA IF NOT EXISTS projection;

-- Current stage version per claim. Writes carrying an older stage version are refused.
CREATE TABLE IF NOT EXISTS projection.claim_stage (
  claim_id      text PRIMARY KEY,
  stage_version int  NOT NULL,
  stage         text NOT NULL,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

-- One row per step, decision and event acted on. seq is 1..n per run with a closing row at the end.
CREATE TABLE IF NOT EXISTS projection.claim_events (
  claim_id      text    NOT NULL,
  workflow_id   text    NOT NULL,
  run_id        text    NOT NULL,
  workflow_type text    NOT NULL,
  seq           int     NOT NULL,
  stage_version int     NOT NULL,
  step          text    NOT NULL,
  outcome       text    NOT NULL DEFAULT '',
  trigger_id    text    NOT NULL DEFAULT '',
  actor         text    NOT NULL DEFAULT '',
  reason        text    NOT NULL DEFAULT '',
  refs          jsonb   NOT NULL DEFAULT '{}',
  closing       boolean NOT NULL DEFAULT false,
  occurred_at   timestamptz NOT NULL,
  recorded_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, seq),
  UNIQUE (claim_id, stage_version, workflow_id, step)
);
CREATE INDEX IF NOT EXISTS claim_events_claim ON projection.claim_events (claim_id, occurred_at);
CREATE INDEX IF NOT EXISTS claim_events_actor ON projection.claim_events (actor) WHERE actor <> '';
