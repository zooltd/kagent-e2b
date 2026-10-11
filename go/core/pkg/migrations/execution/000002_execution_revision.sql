-- +goose Up
-- Provider discrimination on runtime_revision plus the E2B artifact binding.
-- E2B objects are unpublished and fork-owned, so they are shaped directly for
-- the end state: an E2B revision holds NULL ActorTemplate fields and records
-- its genuine artifact (installation + external template/build) in
-- execution_revision_binding. runtime_revision is upstream-owned and
-- published, so it is ALTERed here rather than edited in 000001_initial.

ALTER TABLE runtime_revision
    ADD COLUMN provider TEXT NOT NULL DEFAULT 'substrate'
        CHECK (provider IN ('substrate', 'e2b'));

ALTER TABLE runtime_revision
    ALTER COLUMN actor_template_atespace DROP NOT NULL,
    ALTER COLUMN actor_template_name DROP NOT NULL,
    ALTER COLUMN actor_template_uid DROP NOT NULL;

-- Substrate revisions keep their required ActorTemplate identity; E2B
-- revisions must hold NULL ActorTemplate fields so they can never be read as
-- a valid Substrate artifact.
ALTER TABLE runtime_revision
    ADD CONSTRAINT runtime_revision_provider_actor_template_check CHECK (
        (provider = 'substrate' AND actor_template_atespace IS NOT NULL
            AND actor_template_name IS NOT NULL AND actor_template_uid IS NOT NULL)
        OR (provider = 'e2b' AND actor_template_atespace IS NULL
            AND actor_template_name IS NULL AND actor_template_uid IS NULL)
    );

-- ActorTemplate uniqueness applies only to Substrate revisions; E2B rows hold
-- NULLs and are excluded so many E2B revisions may coexist.
ALTER TABLE runtime_revision
    DROP CONSTRAINT runtime_revision_actor_template_namespace_actor_template_na_key;
CREATE UNIQUE INDEX runtime_revision_substrate_actor_template_key
    ON runtime_revision (actor_template_atespace, actor_template_name)
    WHERE provider = 'substrate';

-- execution_revision_binding ties an E2B revision to the concrete external
-- build it runs (installation + external template + build). It is written in
-- the same transaction that records the revision, before the revision is
-- promoted ready. UNIQUE(revision) gives each revision exactly one binding;
-- no template-wide exclusivity — many agent revisions may legitimately reuse
-- one external artifact. provider is always 'e2b' and the composite foreign
-- key forces it to equal the bound revision's provider, so a Substrate
-- revision can never hold an artifact binding.
CREATE TABLE execution_revision_binding (
    id           UUID PRIMARY KEY,
    revision     TEXT NOT NULL,
    provider     TEXT NOT NULL CHECK (provider = 'e2b'),
    installation TEXT NOT NULL CHECK (installation <> ''),
    external_id  TEXT NOT NULL CHECK (external_id <> ''),
    build_id     TEXT NOT NULL CHECK (build_id <> ''),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (revision)
);

-- Backs the composite FK: (revision, provider) is unique because revision is
-- the primary key. The composite reference forces a binding's provider to
-- match its revision's, so provider='e2b' bindings only ever attach to
-- provider='e2b' revisions.
CREATE UNIQUE INDEX runtime_revision_revision_provider_key
    ON runtime_revision (revision, provider);

ALTER TABLE execution_revision_binding
    ADD CONSTRAINT execution_revision_binding_provider_fkey
    FOREIGN KEY (revision, provider)
    REFERENCES runtime_revision (revision, provider) ON DELETE CASCADE
    DEFERRABLE INITIALLY DEFERRED;

-- The agent_runtime_revision view was created with runtime_revision.* before
-- provider existed, so it does not expose the new column. CREATE OR REPLACE
-- cannot change column order, so drop and recreate it to expose provider.
DROP VIEW agent_runtime_revision;
CREATE VIEW agent_runtime_revision AS
    SELECT r.*, a.agent_name, a.agent_uid, a.agent_card
    FROM runtime_revision r JOIN agent_revision a USING (revision) WHERE r.kind = 'agent';

-- Require every E2B revision to keep its artifact binding for as long as it
-- exists. A foreign key cannot express "the revision must be referenced", so
-- two deferred triggers close both directions at COMMIT:
--   * on runtime_revision: a provider='e2b' row may not commit without a
--     binding row (fires on insert and on provider flips to 'e2b');
--   * on execution_revision_binding: a binding may not be deleted or
--     re-pointed while its provider='e2b' revision still exists. The guard
--     re-checks revision existence, so deleting a revision still cascades the
--     binding delete without tripping the constraint.
-- Together with the binding's provider FK they make the revision<->binding
-- pairing atomic and undeletable-in-isolation.
-- +goose StatementBegin
CREATE FUNCTION execution_revision_requires_binding() RETURNS trigger AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM execution_revision_binding b
        WHERE b.revision = NEW.revision AND b.provider = 'e2b'
    ) THEN
        RAISE EXCEPTION 'E2B runtime revision % has no artifact binding', NEW.revision;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER runtime_revision_e2b_binding_required
    AFTER INSERT OR UPDATE OF provider ON runtime_revision
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    WHEN (NEW.provider = 'e2b')
    EXECUTE FUNCTION execution_revision_requires_binding();

-- +goose StatementBegin
CREATE FUNCTION execution_binding_not_orphaned() RETURNS trigger AS $$
DECLARE
    remaining_provider TEXT;
BEGIN
    SELECT provider INTO remaining_provider
    FROM runtime_revision WHERE revision = OLD.revision;
    IF remaining_provider = 'e2b' THEN
        RAISE EXCEPTION 'cannot remove artifact binding from retained E2B revision %', OLD.revision;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER execution_revision_binding_not_orphaned
    AFTER DELETE OR UPDATE OF revision, provider ON execution_revision_binding
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION execution_binding_not_orphaned();

-- +goose Down
-- Refuse to roll back while E2B rows remain: they have no Substrate identity
-- to restore into, and dropping provider would fabricate one.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_revision WHERE provider = 'e2b') THEN
        RAISE EXCEPTION 'cannot roll back execution migration while E2B revisions exist';
    END IF;
END;
$$;
-- +goose StatementEnd
DROP TRIGGER IF EXISTS execution_revision_binding_not_orphaned ON execution_revision_binding;
DROP TRIGGER IF EXISTS runtime_revision_e2b_binding_required ON runtime_revision;
DROP FUNCTION IF EXISTS execution_binding_not_orphaned;
DROP FUNCTION IF EXISTS execution_revision_requires_binding;
DROP TABLE IF EXISTS execution_revision_binding;
DROP INDEX IF EXISTS runtime_revision_revision_provider_key;
DROP INDEX IF EXISTS runtime_revision_substrate_actor_template_key;
ALTER TABLE runtime_revision
    ADD CONSTRAINT runtime_revision_actor_template_namespace_actor_template_na_key
        UNIQUE (actor_template_atespace, actor_template_name);
ALTER TABLE runtime_revision DROP CONSTRAINT runtime_revision_provider_actor_template_check;
ALTER TABLE runtime_revision
    ALTER COLUMN actor_template_atespace SET NOT NULL,
    ALTER COLUMN actor_template_name SET NOT NULL,
    ALTER COLUMN actor_template_uid SET NOT NULL;
-- Restore the original provider-free view before dropping the column it
-- references; the original 000001 definition had no provider.
DROP VIEW agent_runtime_revision;
CREATE VIEW agent_runtime_revision AS
    SELECT r.revision, r.kind, r.namespace, r.source_snapshot, r.egress_destinations,
        r.credentials, r.actor_template_atespace, r.actor_template_name, r.actor_template_uid,
        r.created_at, r.updated_at, r.deleted_at,
        a.agent_name, a.agent_uid, a.agent_card
    FROM runtime_revision r JOIN agent_revision a USING (revision) WHERE r.kind = 'agent';
ALTER TABLE runtime_revision DROP COLUMN provider;
