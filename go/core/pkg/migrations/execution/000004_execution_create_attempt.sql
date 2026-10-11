-- +goose Up
-- Fork-owned create-attempt records. An execution_create_attempt row is the
-- durable "create was issued" marker for one session CREATE operation: it is
-- written in the same transaction that claims the operation, BEFORE the
-- control plane calls the provider's CreateSandbox. Its existence is the
-- difference between a retry that reconciles an in-flight create and a retry
-- that issues a second one -- an ambiguous create (timeout, dropped response)
-- leaves the operation pending, and without this record a later attempt has
-- no way to know compute may already exist and would create a duplicate.
--
-- Keyed to (operation_id): the CREATE operation is the durable intent; a
-- superseded executor re-claiming the same operation still reconciles the
-- same issued attempt rather than treating the operation as fresh. outcome
-- records issued/known (a sandbox id was observed) so a reconcile can prefer
-- a known-external-id match over a metadata-only one. One row per operation:
-- once a create is issued for an operation no second issue is recorded.
CREATE TABLE execution_create_attempt (
    id           UUID PRIMARY KEY,
    session_id   UUID NOT NULL,
    operation_id UUID NOT NULL,
    provider     TEXT NOT NULL CHECK (provider = 'e2b'),
    installation TEXT NOT NULL CHECK (installation <> ''),
    -- The external sandbox id once known; NULL while the create outcome is
    -- still issued-but-unobserved. Not unique: an unobserved attempt carries
    -- no external id yet, and one is written only after a listing confirms it.
    external_id  TEXT,
    -- 'issued' = create issued, outcome unobserved; 'known' = a sandbox id was
    -- observed/bound; 'rejected' = the provider definitively refused the
    -- create (auth/validation), so it is resolved, never reconciled.
    outcome      TEXT NOT NULL CHECK (outcome IN ('issued', 'known', 'rejected')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (operation_id),
    FOREIGN KEY (session_id) REFERENCES session(id) ON DELETE CASCADE
);

-- Fast lookup of the issued attempt for a session's open CREATE: the create
-- path checks this before deciding whether to issue or reconcile.
CREATE INDEX execution_create_attempt_session_idx
    ON execution_create_attempt (session_id);

-- +goose Down
DROP TABLE IF EXISTS execution_create_attempt;
