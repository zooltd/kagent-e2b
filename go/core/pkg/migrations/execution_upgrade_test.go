package migrations

import (
	"context"
	"database/sql"
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// These tests exercise the two real upgrade paths the plan requires, using
// the actual runner against real PostgreSQL:
//
//  1. fresh upstream -> extension
//  2. existing extension -> new upstream -> next extension
//
// "New upstream" and "next extension" are simulated by layering one extra
// migration file over the embedded track, so the test drives the real RunUp /
// WithProvider machinery — tracking tables, advisory locks, goose versioning —
// rather than a parallel fake. The stub bodies are deliberate no-ops; the
// point is ordering and crash/restart, not their SQL.

// materializeTrack copies one embedded track dir into a MapFS so a test can
// append a simulated next-version file to the real migration set.
func materializeTrack(t *testing.T, dir string) fstest.MapFS {
	t.Helper()
	sub, err := fs.Sub(FS, dir)
	if err != nil {
		t.Fatalf("fs.Sub(%q): %v", dir, err)
	}
	out := fstest.MapFS{}
	err = fs.WalkDir(sub, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := fs.ReadFile(sub, path)
		if err != nil {
			return err
		}
		out[path] = &fstest.MapFile{Data: data}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%q): %v", dir, err)
	}
	return out
}

func coreSource(t *testing.T, fsys fstest.MapFS) Source {
	t.Helper()
	return Source{Name: "core", Schema: "public", TrackingTable: coreTrackingTable, FS: fsys, Dir: "."}
}

func executionSource(t *testing.T, fsys fstest.MapFS) Source {
	t.Helper()
	return Source{Name: "execution", Schema: "public", TrackingTable: executionTrackingTable, FS: fsys, Dir: "."}
}

// TestUpgradeFreshUpstreamThenExtension covers path 1: a database that has only
// the built-in upstream tracks applied takes the fork's execution extension
// cleanly, and VerifyMigrated reports the full set migrated.
func TestUpgradeFreshUpstreamThenExtension(t *testing.T) {
	dsn := startTestDB(t)
	core := BuiltinSourcesInSchema(false, "public", "public")
	if err := RunUp(context.Background(), dsn, core); err != nil {
		t.Fatalf("upstream RunUp: %v", err)
	}

	exec := ExecutionSourceInSchema("public")
	if err := RunUp(context.Background(), dsn, []Source{exec}); err != nil {
		t.Fatalf("extension RunUp: %v", err)
	}
	if !testTableExists(t, dsn, "execution_revision_binding") {
		t.Fatal("extension tables missing after upgrade")
	}
	if !testColumnExists(t, dsn, "runtime_revision", "provider") {
		t.Fatal("provider column missing after extension")
	}
	if err := VerifyMigrated(context.Background(), dsn, append(core, exec)); err != nil {
		t.Fatalf("VerifyMigrated: %v", err)
	}
}

// TestUpgradeExistingExtensionNewUpstreamNextExtension covers path 2: a
// database that already runs the extension takes a NEWER upstream core (one
// extra migration), then the NEXT extension version, in order, and a
// crash/restart mid-extension resumes cleanly.
func TestUpgradeExistingExtensionNewUpstreamNextExtension(t *testing.T) {
	dsn := startTestDB(t)

	// Baseline: current upstream + current extension already applied.
	baseline := append(BuiltinSourcesInSchema(false, "public", "public"), ExecutionSourceInSchema("public"))
	if err := RunUp(context.Background(), dsn, baseline); err != nil {
		t.Fatalf("baseline RunUp: %v", err)
	}

	// New upstream core gains one migration. Materialize the real set and add
	// 000002 so this is the real runner over the real files plus a stub — not
	// a parallel fake ledger.
	nextCoreFS := materializeTrack(t, "core")
	nextCoreFS["000002_upstream_next.sql"] = &fstest.MapFile{Data: migrationSQL(
		"CREATE TABLE upstream_next (id bigint PRIMARY KEY);",
		"DROP TABLE IF EXISTS upstream_next;",
	)}
	newCore := coreSource(t, nextCoreFS)
	if err := RunUp(context.Background(), dsn, append([]Source{newCore}, ExecutionSourceInSchema("public"))); err != nil {
		t.Fatalf("new-upstream RunUp: %v", err)
	}
	if !testTableExists(t, dsn, "upstream_next") {
		t.Fatal("new upstream migration did not apply")
	}

	// Next extension version appends 000008 to the real execution track (which
	// now reaches 000007).
	nextExecFS := materializeTrack(t, "execution")
	nextExecFS["000008_next_extension.sql"] = &fstest.MapFile{Data: migrationSQL(
		"ALTER TABLE execution_revision_binding ADD COLUMN next_col TEXT;",
		"ALTER TABLE execution_revision_binding DROP COLUMN IF EXISTS next_col;",
	)}
	nextExec := executionSource(t, nextExecFS)

	// Simulate a crash mid-extension: apply only up to version 6, then resume.
	if err := WithProvider(context.Background(), dsn, nextExec, func(p *goose.Provider) error {
		_, err := p.UpTo(context.Background(), 7)
		return err
	}); err != nil {
		t.Fatalf("partial extension apply: %v", err)
	}
	if got := testVersions(t, dsn, executionTrackingTable); !slices.Equal(got, []int64{0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("extension versions after partial apply = %v", got)
	}
	// Restart: the runner picks up at 8 without replaying 1-7.
	if err := RunUp(context.Background(), dsn, []Source{nextExec}); err != nil {
		t.Fatalf("resume extension RunUp: %v", err)
	}
	if got := testVersions(t, dsn, executionTrackingTable); !slices.Equal(got, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("extension versions after resume = %v", got)
	}
	if !testColumnExists(t, dsn, "execution_revision_binding", "next_col") {
		t.Fatal("next extension migration did not apply after restart")
	}

	// The whole set verifies as migrated against the composed source list.
	if err := VerifyMigrated(context.Background(), dsn, []Source{newCore, nextExec}); err != nil {
		t.Fatalf("VerifyMigrated: %v", err)
	}

	// Prepared inline SQL still runs against the upgraded schema: the provider
	// CHECK accepts an e2b revision row end to end.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execSQL(t, dsn, `WITH b AS (
		INSERT INTO execution_revision_binding (id, revision, provider, installation, external_id, build_id, config_json, launch_contract)
		VALUES (gen_random_uuid(), 'up-e2b', 'e2b', 'lab', 'tpl', 'bld', '{"v":5}','{}') RETURNING revision)
		INSERT INTO runtime_revision (revision, kind, namespace, source_snapshot, provider,
			actor_template_atespace, actor_template_name, actor_template_uid)
		SELECT 'up-e2b', 'agent', 'ns', '{}', 'e2b', NULL, NULL, NULL FROM b`)
}
