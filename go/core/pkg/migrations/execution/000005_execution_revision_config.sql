-- The E2B artifact binding carries everything the activation probe needs to
-- reproduce this revision's launch without re-reading live operator config:
-- the compiled harness config written to the guest's ConfigPath, and the
-- resolved launch contract (supervisor command + guest file paths) the
-- workload mapping selected. Storing both on the binding — not on
-- runtime_revision.source_snapshot and not re-resolved from the installation
-- map — keeps provenance and launch config distinct, and freezes the contract
-- per revision so a later change to the installation's workload mapping cannot
-- silently change an existing revision's bootstrap. Both columns are NOT NULL:
-- a revision may not bind without launchable config and contract.
-- +goose Up
ALTER TABLE execution_revision_binding
    ADD COLUMN config_json JSONB NOT NULL,
    ADD COLUMN launch_contract JSONB NOT NULL;

-- +goose Down
ALTER TABLE execution_revision_binding
    DROP COLUMN IF EXISTS config_json,
    DROP COLUMN IF EXISTS launch_contract;
