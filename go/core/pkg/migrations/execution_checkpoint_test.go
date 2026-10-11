package migrations

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestExecutionCheckpointConstraints locks the provider-discriminated
// checkpoint schema against migrated PostgreSQL: an E2B checkpoint is a
// session_checkpoint row with provider='e2b' whose artifact is a bounded
// archive in session_checkpoint_archive, READY only once archive+digest land.
// The Substrate row keeps requiring a snapshot; the providers cannot
// masquerade as each other at the column level.
func TestExecutionCheckpointConstraints(t *testing.T) {
	dsn := startTestDB(t)
	sources := append(BuiltinSourcesInSchema(false, "public", "public"), ExecutionSourceInSchema("public"))
	if err := RunUp(context.Background(), dsn, sources); err != nil {
		t.Fatalf("RunUp: %v", err)
	}
	if !testTableExists(t, dsn, "session_checkpoint_archive") {
		t.Fatal("session_checkpoint_archive missing after Up")
	}
	if !testColumnExists(t, dsn, "session_checkpoint", "provider") {
		t.Fatal("session_checkpoint.provider missing after Up")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sid := "c0ffee00-0000-4000-8000-000000000001"
	ckID := "c0ffee00-0000-4000-8000-0000000000c1"

	// Seed an E2B revision + agent_revision + a READY session bound to it.
	execSQL(t, dsn, `WITH b AS (
		INSERT INTO execution_revision_binding (id, revision, provider, installation, external_id, build_id, config_json, launch_contract)
		VALUES (gen_random_uuid(), 'ck-rev', 'e2b', 'lab', 'tpl', 'bld', '{"v":5}','{}') RETURNING revision)
		INSERT INTO runtime_revision (revision, kind, namespace, source_snapshot, provider,
			actor_template_atespace, actor_template_name, actor_template_uid)
		SELECT 'ck-rev', 'agent', 'ns', '{}', 'e2b', NULL, NULL, NULL FROM b`)
	execSQL(t, dsn, `INSERT INTO agent_revision (revision, agent_name, agent_uid, agent_card)
		VALUES ('ck-rev', 'a', 'u', 'x')`)
	execSQL(t, dsn, `WITH history AS (
		INSERT INTO a2a_context (id, context_id) VALUES ($1, $1) RETURNING id, context_id
	), runtime AS (
		INSERT INTO runtime_instance (id, kind, user_id, request_id, state, operation, prepared_revision)
		VALUES ($1, 'agent', 'alice', 'r1', 'RUNTIME_STATE_READY', 'RUNTIME_OPERATION_NONE', 'ck-rev')
		RETURNING id
	)
	INSERT INTO session (id, context_id, history_id, data)
	SELECT runtime.id, history.context_id, history.id, 'x' FROM runtime CROSS JOIN history`, sid)

	// An E2B CREATING checkpoint: NULL snapshot columns are required by the
	// e2b shape check; a NULL archive is allowed while the export runs.
	execSQL(t, dsn, `INSERT INTO session_checkpoint
		(id, source_session_id, user_id, request_id, head_task_id, history_sequence,
		 source_history_id, prepared_revision, provider, state, data)
		SELECT $2, s.id, 'alice', 'ck1', 't1', 1, s.history_id,
			'ck-rev', 'e2b', 'CREATING', 'x' FROM session s WHERE s.id = $1`, sid, ckID)
	execSQL(t, dsn, `INSERT INTO session_checkpoint_archive (checkpoint_id, installation)
		VALUES ($1, 'lab')`, ckID)

	// An E2B row may NOT carry Substrate snapshot identity (masquerade guard).
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_checkpoint
		(id, source_session_id, user_id, request_id, head_task_id, history_sequence,
		 source_history_id, prepared_revision, provider, snapshot_atespace, snapshot_uri,
		 snapshot_content_scope, state, data)
		SELECT gen_random_uuid(), s.id, 'alice', 'ckm', 't1', 1, s.history_id,
			'ck-rev', 'e2b', 'ate', 'uri', 'DATA', 'CREATING', 'x' FROM session s WHERE s.id = $1`, sid); err == nil ||
		!strings.Contains(err.Error(), "e2b_no_snapshot") {
		t.Fatalf("an E2B row carrying a Substrate snapshot was allowed: %v", err)
	}

	// A Substrate row with NULL snapshot is rejected by the substrate check.
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_checkpoint
		(id, source_session_id, user_id, request_id, head_task_id, history_sequence,
		 source_history_id, prepared_revision, provider, state, data)
		SELECT gen_random_uuid(), s.id, 'alice', 'cks', 't1', 1, s.history_id,
			'ck-rev', 'substrate', 'CREATING', 'x' FROM session s WHERE s.id = $1`, sid); err == nil ||
		!strings.Contains(err.Error(), "substrate_snapshot") {
		t.Fatalf("a Substrate row without a snapshot was allowed: %v", err)
	}

	// A Substrate row with a NULL content scope but non-NULL atespace/uri is
	// rejected: NULL IN ('FULL','DATA') would otherwise silently pass the CHECK.
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_checkpoint
		(id, source_session_id, user_id, request_id, head_task_id, history_sequence,
		 source_history_id, prepared_revision, provider, snapshot_atespace, snapshot_uri,
		 snapshot_content_scope, state, data)
		SELECT gen_random_uuid(), s.id, 'alice', 'ckn', 't1', 1, s.history_id,
			'ck-rev', 'substrate', 'ate', 'uri', NULL, 'CREATING', 'x' FROM session s WHERE s.id = $1`, sid); err == nil ||
		!strings.Contains(err.Error(), "substrate_snapshot") {
		t.Fatalf("a Substrate row with NULL content scope was allowed: %v", err)
	}

	// A row cannot be inserted directly as READY past its artifact — the gate
	// fires on INSERT, not only on the transition into READY.
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_checkpoint
		(id, source_session_id, user_id, request_id, head_task_id, history_sequence,
		 source_history_id, prepared_revision, provider, state, data)
		SELECT gen_random_uuid(), s.id, 'alice', 'ckr', 't1', 1, s.history_id,
			'ck-rev', 'e2b', 'READY', 'x' FROM session s WHERE s.id = $1`, sid); err == nil {
		t.Fatal("an E2B row was inserted directly as READY without an archive")
	}

	// A second CREATING on the same source session is rejected while the first
	// is still CREATING (the shared one_creating guard).
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_checkpoint
		(id, source_session_id, user_id, request_id, head_task_id, history_sequence,
		 source_history_id, prepared_revision, provider, state, data)
		SELECT gen_random_uuid(), s.id, 'alice', 'ck2', 't2', 2, s.history_id,
			'ck-rev', 'e2b', 'CREATING', 'x' FROM session s WHERE s.id = $1`, sid); err == nil ||
		!strings.Contains(err.Error(), "one_creating") {
		t.Fatalf("a second CREATING checkpoint was allowed: %v", err)
	}

	// READY without the archive committed is rejected by the ready gate.
	if _, err := db.ExecContext(context.Background(), `UPDATE session_checkpoint
		SET state = 'READY' WHERE id = $1`, ckID); err == nil {
		t.Fatal("E2B READY without a committed archive was allowed")
	}

	// Committing archive + digest lets READY land.
	execSQL(t, dsn, `UPDATE session_checkpoint_archive
		SET archive = 'blob', archive_sha256 = decode(repeat('ab', 32), 'hex')
		WHERE checkpoint_id = $1`, ckID)
	execSQL(t, dsn, `UPDATE session_checkpoint SET state = 'READY' WHERE id = $1`, ckID)

	// A READY checkpoint's archive is immutable and required: it cannot be
	// deleted or its blob/digest nulled while the parent is READY.
	if _, err := db.ExecContext(context.Background(),
		`DELETE FROM session_checkpoint_archive WHERE checkpoint_id = $1`, ckID); err == nil {
		t.Fatal("deleting a READY checkpoint's archive was allowed")
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_checkpoint_archive SET archive = NULL, archive_sha256 = NULL
		 WHERE checkpoint_id = $1`, ckID); err == nil {
		t.Fatal("nulling a READY checkpoint's archive was allowed")
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_checkpoint_archive SET archive = 'different'
		 WHERE checkpoint_id = $1`, ckID); err == nil {
		t.Fatal("modifying a READY checkpoint's archive was allowed")
	}
	// Reparenting the archive row away from the READY checkpoint is an
	// identity change and rejected outright, whatever the target.
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_checkpoint_archive SET checkpoint_id = gen_random_uuid()
		 WHERE checkpoint_id = $1`, ckID); err == nil {
		t.Fatal("reparenting a READY checkpoint's archive was allowed")
	}
	// A READY-to-READY mutation cannot strip the artifact: nulling the row's
	// Substrate tag must re-fail the gate, not slide through because the state
	// did not transition.
	subCk := "c0ffee00-0000-4000-8000-0000000000c2"
	execSQL(t, dsn, `INSERT INTO session_checkpoint
		(id, source_session_id, user_id, request_id, head_task_id, history_sequence,
		 source_history_id, prepared_revision, provider, snapshot_atespace, snapshot_uri,
		 snapshot_content_scope, tag_uid, state, data)
		SELECT $2, s.id, 'alice', 'subck', 't1', 1, s.history_id,
			'ck-rev', 'substrate', 'ate', 'uri', 'DATA', 'tag', 'READY', 'x'
		FROM session s WHERE s.id = $1`, sid, subCk)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_checkpoint SET tag_uid = NULL WHERE id = $1`, subCk); err == nil {
		t.Fatal("a READY-to-READY update stripping the Substrate tag was allowed")
	}

	// A durable checkpoint survives its source session's deletion: the archive
	// is provenance, not an ownership cascade.
	execSQL(t, dsn, `DELETE FROM session WHERE id = $1`, sid)
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_checkpoint WHERE id = $1`, ckID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("checkpoint did not survive its source session: %d rows", n)
	}
	var arch int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_checkpoint_archive WHERE checkpoint_id = $1`, ckID).Scan(&arch); err != nil {
		t.Fatal(err)
	}
	if arch != 1 {
		t.Fatalf("archive did not survive its source session: %d rows", arch)
	}

	// Deleting the checkpoint itself (once out of READY — DELETING) removes
	// its archive through the child cascade; the guard only protects a READY
	// parent's archive.
	execSQL(t, dsn, `UPDATE session_checkpoint SET state = 'DELETING' WHERE id = $1`, ckID)
	execSQL(t, dsn, `DELETE FROM session_checkpoint WHERE id = $1`, ckID)
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_checkpoint_archive WHERE checkpoint_id = $1`, ckID).Scan(&arch); err != nil {
		t.Fatal(err)
	}
	if arch != 0 {
		t.Fatalf("archive outlived its checkpoint's delete: %d rows", arch)
	}
}
