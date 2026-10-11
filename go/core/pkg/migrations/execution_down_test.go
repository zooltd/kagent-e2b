package migrations

import (
	"context"
	"database/sql"
	"slices"
	"testing"

	"github.com/pressly/goose/v3"
)

// The execution Down must refuse while E2B rows remain and roll the schema
// back to the pre-provider shape when none do.
func TestExecutionDownRollback(t *testing.T) {
	dsn := startTestDB(t)
	exec := ExecutionSource()
	sources := append(BuiltinSources(false), exec)
	if err := RunUp(context.Background(), dsn, sources); err != nil {
		t.Fatalf("RunUp: %v", err)
	}
	if !testColumnExists(t, dsn, "runtime_revision", "provider") {
		t.Fatal("provider column missing after Up")
	}
	if !testTableExists(t, dsn, "execution_revision_binding") {
		t.Fatal("execution_revision_binding missing after Up")
	}
	// An E2B revision blocks rollback.
	execSQL(t, dsn, `WITH b AS (
		INSERT INTO execution_revision_binding (id, revision, provider, installation, external_id, build_id, config_json, launch_contract)
		VALUES (gen_random_uuid(), 'e2b-r', 'e2b', 'lab', 'tpl', 'bld', '{"v":5}','{}') RETURNING revision)
		INSERT INTO runtime_revision (revision, kind, namespace, source_snapshot, provider, actor_template_atespace, actor_template_name, actor_template_uid)
		SELECT 'e2b-r', 'agent', 'ns', '{}', 'e2b', NULL, NULL, NULL FROM b`)
	err := WithProvider(context.Background(), dsn, exec, func(provider *goose.Provider) error {
		_, err := provider.DownTo(context.Background(), 0)
		return err
	})
	if err == nil {
		t.Fatal("Down succeeded while an E2B revision remained")
	}
	execSQL(t, dsn, `DELETE FROM runtime_revision WHERE revision = 'e2b-r'`)
	if err := WithProvider(context.Background(), dsn, exec, func(provider *goose.Provider) error {
		_, err := provider.DownTo(context.Background(), 0)
		return err
	}); err != nil {
		t.Fatalf("DownTo: %v", err)
	}
	if testColumnExists(t, dsn, "runtime_revision", "provider") {
		t.Fatal("provider column survived Down")
	}
	if testTableExists(t, dsn, "execution_revision_binding") {
		t.Fatal("binding table survived Down")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execSQL(t, dsn, `INSERT INTO runtime_revision (revision, kind, namespace, source_snapshot, actor_template_atespace, actor_template_name, actor_template_uid) VALUES ('r1','agent','ns','{}','a','n','u')`)
	if _, err := db.ExecContext(context.Background(), `INSERT INTO runtime_revision (revision, kind, namespace, source_snapshot, actor_template_atespace, actor_template_name, actor_template_uid) VALUES ('r2','agent','ns','{}','a','n','u')`); err == nil {
		t.Fatal("duplicate atespace+name allowed after Down — unique constraint not restored")
	}
	if versions := testVersions(t, dsn, exec.TrackingTable); !slices.Equal(versions, []int64{0}) {
		t.Fatalf("versions = %v", versions)
	}
	// Re-migrating must work after the rollback.
	if err := RunUp(context.Background(), dsn, []Source{exec}); err != nil {
		t.Fatalf("re-Up after Down: %v", err)
	}
}
