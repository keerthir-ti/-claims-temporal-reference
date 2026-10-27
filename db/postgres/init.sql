-- Application-owned business projection: the long-term, queryable record of what happened to a
-- claim. Written only through the idempotent recordProjection activity (key claimId:stageVersion:step).
CREATE TABLE IF NOT EXISTS claim_projection (
  projection_key TEXT PRIMARY KEY,
  claim_id       TEXT        NOT NULL,
  stage_version  INT         NOT NULL,
  track          TEXT,
  step           TEXT        NOT NULL,
  status         TEXT        NOT NULL,
  detail         TEXT,
  workflow_id    TEXT,
  run_id         TEXT,
  occurred_at    TIMESTAMPTZ NOT NULL,
  recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS claim_projection_claim ON claim_projection (claim_id, occurred_at);
CREATE INDEX IF NOT EXISTS claim_projection_step ON claim_projection (step, occurred_at);

-- Current business state per claim (latest transition).
CREATE OR REPLACE VIEW claim_current AS
SELECT DISTINCT ON (claim_id) claim_id, track AS current_track, stage_version, occurred_at AS since, detail AS reason
FROM claim_projection
WHERE step LIKE 'TRANSITION:%'
ORDER BY claim_id, occurred_at DESC, stage_version DESC;
