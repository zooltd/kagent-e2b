-- The E2B artifact binding also carries the revision's resolved non-secret
-- process environment — image-level and harness-injected variables such as the
-- compiled model base URL — so the bootstrap launch exports them per revision
-- without re-reading operator config. Secret-bearing variables never reach
-- this column: the workload credential broker injects those outside the guest,
-- so only literal values are stored here. JSONB preserves order-insensitive
-- object form keyed by variable name.
-- +goose Up
ALTER TABLE execution_revision_binding
    ADD COLUMN environment JSONB NOT NULL DEFAULT '{}'::jsonb;

-- +goose Down
ALTER TABLE execution_revision_binding
    DROP COLUMN IF EXISTS environment;
