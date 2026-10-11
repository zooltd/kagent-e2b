-- +goose Up
-- Provider-discriminated checkpoints. Rather than a fork-owned
-- execution_checkpoint table, an E2B checkpoint is a session_checkpoint row
-- carrying provider='e2b' whose durable artifact is an exported /data archive
-- in the session_checkpoint_archive child table — one row per checkpoint.
--
-- This is the design the reviewer mandated over the separate table, and it is
-- the load-bearing choice for correctness: every admission guard that blocks
-- task writes, lifecycle operations, and quiescence claims during a
-- checkpoint keys on `session_checkpoint.state = 'CREATING'` (session_tasks,
-- session_quiescence, dispatch). A separate table would have inserted
-- reservations those guards never see, letting a checkpoint run concurrently
-- with the very writes it must freeze. Sharing the row keeps identity,
-- state, authorization, and admission semantics identical across providers.
--
-- A provider discriminator plus per-provider CHECK constraints make the wrong
-- state unrepresentable at the column level, so relaxing the Substrate
-- snapshot NOT NULLs cannot let an E2B row masquerade as a Substrate
-- checkpoint — the failure correction-A forbade. The archive blob is the only
-- E2B-specific field; it lives in a child keyed by checkpoint id so a list or
-- get never drags the blob, and the session_checkpoint row keeps its existing
-- shape.

ALTER TABLE session_checkpoint
    ADD COLUMN provider TEXT NOT NULL DEFAULT 'substrate';

-- E2B checkpoints need no ate-api snapshot; Substrate ones keep requiring one.
ALTER TABLE session_checkpoint ALTER COLUMN snapshot_atespace DROP NOT NULL;
ALTER TABLE session_checkpoint ALTER COLUMN snapshot_uri DROP NOT NULL;
ALTER TABLE session_checkpoint ALTER COLUMN snapshot_content_scope DROP NOT NULL;
ALTER TABLE session_checkpoint ALTER COLUMN tag_uid DROP NOT NULL;
-- Drop the implicit default: an absent tag must read NULL, not a fabricated
-- empty identity that a reader could mistake for a real Substrate tag.
ALTER TABLE session_checkpoint ALTER COLUMN tag_uid DROP DEFAULT;

-- Per-provider shape. The provider discriminates which artifact columns are
-- required and which must be absent, so an E2B row can never look like a
-- valid Substrate checkpoint and vice versa. READY is gated separately below
-- because it depends on the archive landing in the child table, which a
-- row-local CHECK cannot see.
ALTER TABLE session_checkpoint
    ADD CONSTRAINT session_checkpoint_provider_check
        CHECK (provider IN ('substrate', 'e2b')),
    ADD CONSTRAINT session_checkpoint_substrate_snapshot_check
        CHECK (provider <> 'substrate' OR (
            snapshot_atespace IS NOT NULL AND snapshot_uri IS NOT NULL
            AND snapshot_content_scope IS NOT NULL
            AND snapshot_content_scope IN ('FULL', 'DATA'))),
    ADD CONSTRAINT session_checkpoint_e2b_no_snapshot_check
        CHECK (provider <> 'e2b' OR (
            snapshot_atespace IS NULL AND snapshot_uri IS NULL
            AND snapshot_content_scope IS NULL AND tag_uid IS NULL));

-- A durable checkpoint must survive its source session's deletion: the
-- source_session_id is provenance, not an ownership cascade. Substrate
-- session_checkpoint already carries no source FK for exactly this reason
-- (an idle-timeout delete hard-removes the session row), so no change is
-- needed here — but the archive child must not introduce one either. The
-- child's only FK is to its owning checkpoint, deleted explicitly when the
-- checkpoint itself is deleted.

CREATE TABLE session_checkpoint_archive (
    checkpoint_id    UUID  PRIMARY KEY
        REFERENCES session_checkpoint(id) ON DELETE CASCADE,
    -- The exported /data archive and its integrity digest. NULL while the
    -- checkpoint is CREATING; both must be present before READY — the same
    -- "no READY without its artifact" rule the session binding enforces. The
    -- archive is bounded so a checkpoint is a bounded /data, not uncontrolled
    -- growth: 64 MiB compressed matches the during-collection ExportDataLimit
    -- the envd client enforces, so the column bound is the same bound the
    -- export applies — the last line of defense, not a different one.
    archive          BYTEA,
    archive_sha256   BYTEA CHECK (archive_sha256 IS NULL OR octet_length(archive_sha256) = 32),
    -- What the archive restores onto: the installation that serves the fresh
    -- sandbox. Recorded at reserve from the session's live binding so the
    -- fork path resolves it without re-reading the source's (possibly gone)
    -- sandbox.
    installation     TEXT  NOT NULL CHECK (installation <> ''),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (archive IS NULL OR octet_length(archive) <= 67108864),
    CHECK ((archive IS NULL) = (archive_sha256 IS NULL))
);

-- READY requires the retained artifact for BOTH providers. This trigger is
-- the deferred "no READY without its artifact" rule: a substrate row needs
-- its snapshot+tag committed at finalize, an e2b row needs a child row whose
-- archive and digest are both present. It validates EVERY row whose resulting
-- state is READY — on INSERT and on UPDATE — so neither an insert-as-READY
-- nor a READY-to-READY mutation that strips the artifact (e.g. nulling a
-- Substrate tag) escapes the check.
-- +goose StatementBegin
CREATE FUNCTION session_checkpoint_ready_requires_artifact() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = 'READY' THEN
        IF NEW.provider = 'e2b' THEN
            IF NOT EXISTS (SELECT 1 FROM session_checkpoint_archive a
                           WHERE a.checkpoint_id = NEW.id
                             AND a.archive IS NOT NULL AND a.archive_sha256 IS NOT NULL) THEN
                RAISE EXCEPTION 'e2b checkpoint % cannot be READY without its retained archive', NEW.id;
            END IF;
        ELSIF NEW.snapshot_uri IS NULL OR NEW.tag_uid IS NULL THEN
            RAISE EXCEPTION 'substrate checkpoint % cannot be READY without a retained snapshot tag', NEW.id;
        END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd

CREATE TRIGGER session_checkpoint_ready_gate
    BEFORE INSERT OR UPDATE ON session_checkpoint
    FOR EACH ROW EXECUTE FUNCTION session_checkpoint_ready_requires_artifact();

-- The archive's identity is immutable: checkpoint_id is the child's primary
-- key and its only link to the parent, so reparenting the row away from a
-- READY checkpoint would strip that checkpoint's artifact. Block any change
-- to the identity column outright — no legitimate write ever reparents it.
-- +goose StatementBegin
CREATE FUNCTION session_checkpoint_archive_identity_immutable() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'checkpoint archive identity % is immutable', OLD.checkpoint_id;
END $$;
-- +goose StatementEnd

CREATE TRIGGER session_checkpoint_archive_immutable
    BEFORE UPDATE OF checkpoint_id ON session_checkpoint_archive
    FOR EACH ROW EXECUTE FUNCTION session_checkpoint_archive_identity_immutable();

-- A READY E2B checkpoint's archive is immutable and required: it cannot be
-- deleted or its blob/digest nulled while the parent is READY. Once the
-- parent leaves READY (DELETING, or the checkpoint row itself being deleted
-- — which cascades the child after the parent row is gone), the archive may
-- go.
--
-- Serialization, not deadlock-freedom: the guard takes the parent row
-- FOR UPDATE before deciding, so a child mutation serializes on the parent's
-- lock. Normal store code keeps mutations parent-first (lockCheckpoint, then
-- write the child), so a child op and a promotion contend on the same parent
-- lock — whichever holds it commits its outcome and the loser re-reads the
-- settled state. A genuinely opposite-order pair (child-lock held while
-- waiting on the parent, parent held while writing the child) is resolved by
-- PostgreSQL's own deadlock detection aborting one transaction: the
-- invariant holds under abort, not under a global lock order. This is the
-- honest serialization model — correctness comes from the invariant, not a
-- claimed absence of deadlock.
-- +goose StatementBegin
CREATE FUNCTION session_checkpoint_archive_guard() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
    parent_state TEXT;
BEGIN
    SELECT state INTO parent_state
        FROM session_checkpoint WHERE id = OLD.checkpoint_id
        FOR UPDATE;
    -- A missing parent means the checkpoint row is already gone (cascade
    -- from its own delete); nothing to protect.
    IF parent_state = 'READY' THEN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'cannot delete the archive of READY checkpoint %', OLD.checkpoint_id;
        END IF;
        IF NEW.archive IS NULL OR NEW.archive_sha256 IS NULL THEN
            RAISE EXCEPTION 'cannot null the archive of READY checkpoint %', OLD.checkpoint_id;
        END IF;
        IF NEW.archive IS DISTINCT FROM OLD.archive
           OR NEW.archive_sha256 IS DISTINCT FROM OLD.archive_sha256 THEN
            RAISE EXCEPTION 'cannot modify the archive of READY checkpoint %', OLD.checkpoint_id;
        END IF;
    END IF;
    RETURN COALESCE(NEW, OLD);
END $$;
-- +goose StatementEnd

CREATE TRIGGER session_checkpoint_archive_guard
    BEFORE DELETE OR UPDATE OF archive, archive_sha256 ON session_checkpoint_archive
    FOR EACH ROW EXECUTE FUNCTION session_checkpoint_archive_guard();

-- +goose Down
DROP TRIGGER IF EXISTS session_checkpoint_archive_immutable ON session_checkpoint_archive;
DROP FUNCTION IF EXISTS session_checkpoint_archive_identity_immutable;
DROP TRIGGER IF EXISTS session_checkpoint_archive_guard ON session_checkpoint_archive;
DROP FUNCTION IF EXISTS session_checkpoint_archive_guard;
DROP TRIGGER IF EXISTS session_checkpoint_ready_gate ON session_checkpoint;
DROP FUNCTION IF EXISTS session_checkpoint_ready_requires_artifact;
DROP TABLE IF EXISTS session_checkpoint_archive;
ALTER TABLE session_checkpoint
    DROP CONSTRAINT IF EXISTS session_checkpoint_e2b_no_snapshot_check,
    DROP CONSTRAINT IF EXISTS session_checkpoint_substrate_snapshot_check,
    DROP CONSTRAINT IF EXISTS session_checkpoint_provider_check;
ALTER TABLE session_checkpoint ALTER COLUMN tag_uid SET DEFAULT '';
ALTER TABLE session_checkpoint ALTER COLUMN tag_uid SET NOT NULL;
ALTER TABLE session_checkpoint ALTER COLUMN snapshot_content_scope SET NOT NULL;
ALTER TABLE session_checkpoint ALTER COLUMN snapshot_uri SET NOT NULL;
ALTER TABLE session_checkpoint ALTER COLUMN snapshot_atespace SET NOT NULL;
ALTER TABLE session_checkpoint DROP COLUMN IF EXISTS provider;
