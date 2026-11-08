-- One table is enough: the job state machine lives in the `state` column.
-- ready -> running -> done
--                   \-> ready (retry, run_at pushed back) -> dead (attempts exhausted)
CREATE TABLE IF NOT EXISTS jobs (
    id           BIGSERIAL PRIMARY KEY,
    queue        TEXT        NOT NULL DEFAULT 'default',
    payload      JSONB       NOT NULL,
    state        TEXT        NOT NULL DEFAULT 'ready',
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts     INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 5,
    locked_until TIMESTAMPTZ,
    locked_by    TEXT,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT jobs_state_valid CHECK (state IN ('ready', 'running', 'done', 'dead'))
);

-- Claim path: only rows that are ready and due. Partial, so it stays small
-- no matter how many finished rows accumulate.
CREATE INDEX IF NOT EXISTS jobs_ready_idx ON jobs (queue, run_at) WHERE state = 'ready';
-- Recovery path: find leases that have expired.
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs (locked_until) WHERE state = 'running';
