-- +goose Up
-- Fork-owned execution provider bindings. An execution_instance_binding row is
-- the durable association between a control-plane session (runtime_instance)
-- and the concrete external compute it runs on for one generation. It is
-- written before a runtime credential is issued and read by the E2B runtime
-- authenticator to authorize callback writes. Substrate sessions do not use
-- this table: their binding remains the actor_uid column on session.
CREATE TABLE execution_instance_binding (
    id              UUID        PRIMARY KEY,
    session_id      UUID        NOT NULL,
    provider        TEXT        NOT NULL CHECK (provider = 'e2b'),
    installation    TEXT        NOT NULL CHECK (installation <> ''),
    -- Concrete external compute identity, e.g. the E2B sandbox id. Unique per
    -- installation so one external sandbox cannot serve two sessions.
    external_id     TEXT        NOT NULL CHECK (external_id <> ''),
    generation      BIGINT      NOT NULL CHECK (generation > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at      TIMESTAMPTZ,
    UNIQUE (session_id, generation),
    UNIQUE (provider, installation, external_id),
    FOREIGN KEY (session_id) REFERENCES session(id) ON DELETE CASCADE,
    CHECK ((revoked_at IS NULL) OR (revoked_at >= created_at))
);
-- Only one live (unrevoked) binding may exist per session.
CREATE UNIQUE INDEX execution_instance_binding_live_idx
    ON execution_instance_binding (session_id) WHERE revoked_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS execution_instance_binding_live_idx;
DROP TABLE IF EXISTS execution_instance_binding;
