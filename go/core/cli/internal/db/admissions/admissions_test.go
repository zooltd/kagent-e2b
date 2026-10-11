package admissions

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	"github.com/kagent-dev/kagent/go/core/pkg/env"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runCLI executes `admissions <args>` on a fresh command tree and returns
// stdout, stderr, and the command error.
func runCLI(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}

// --- argument / connection validation (no database needed) ---

func TestAdmissionsRejectsUnknownSubcommand(t *testing.T) {
	_, _, err := runCLI(t, "bogus")
	require.Error(t, err)
}

func TestAdmissionsRequireDSN(t *testing.T) {
	// No --db-url and no env var: must fail nonzero before touching the network.
	t.Setenv(env.PostgresDatabaseURL.Name(), "")
	for _, args := range [][]string{{"status"}, {"close"}, {"open"}} {
		_, _, err := runCLI(t, args...)
		require.Error(t, err, "%v must require a database URL", args)
		assert.Contains(t, err.Error(), env.PostgresDatabaseURL.Name(),
			"missing-DSN error should name the env var, not print a value")
	}
}

func TestAdmissionsStatusRejectsArgs(t *testing.T) {
	_, _, err := runCLI(t, "status", "extra")
	require.Error(t, err)
}

func TestAdmissionsBadDSNFailsNonzero(t *testing.T) {
	// A syntactically bad URL must error, not hang or report open.
	_, _, err := runCLI(t, "status", "--db-url", "not-a-url")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "not-a-url", "error must not echo the DSN")
}

func TestAdmissionsInvalidOptionDSNDoesNotLeak(t *testing.T) {
	// A structurally valid postgres URL carrying an invalid option reaches pgx's
	// config parser, whose error embeds the raw DSN. The command must refuse
	// without echoing the credential-bearing value.
	dsn := "postgres://operator:s3cr3t-pass@db.internal:5432/kagent?sslmode=bogus-option"
	_, _, err := runCLI(t, "status", "--db-url", dsn)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "s3cr3t-pass", "error must not echo the password")
	assert.NotContains(t, err.Error(), "bogus-option", "error must not echo the DSN")
}

// --- real Postgres coverage ---

// TestAdmissionsAgainstPostgres drives the operator surface end to end on a
// migrated real Postgres: status -> close -> status -> open, repeatability, and
// the missing-row error path. The schema is the real migrated schema.
func TestAdmissionsAgainstPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}
	ctx := context.Background()
	dsn := dbtest.StartT(ctx, t)
	dbtest.MigrateT(t, dsn, false) // execution track carries admission_barrier

	status := func() string {
		t.Helper()
		out, _, err := runCLI(t, "status", "--db-url", dsn)
		require.NoError(t, err)
		return out
	}
	mustRun := func(args ...string) string {
		t.Helper()
		out, _, err := runCLI(t, append(args, "--db-url", dsn)...)
		require.NoError(t, err, "%v", args)
		return out
	}

	t.Run("starts open after migration", func(t *testing.T) {
		assert.Contains(t, status(), "OPEN")
	})

	t.Run("close reports closed and persists", func(t *testing.T) {
		assert.Contains(t, mustRun("close", "--reason", "drain window"), "CLOSED")
		assert.Contains(t, status(), "CLOSED")
	})

	t.Run("close is idempotent", func(t *testing.T) {
		// Repeated close reports the committed state; a concurrent operator's
		// write makes "already closed" unknowable, so the command reports the
		// barrier's resulting state rather than claiming a no-op.
		assert.Contains(t, mustRun("close"), "CLOSED")
		assert.Contains(t, status(), "CLOSED")
	})

	t.Run("open restores admission", func(t *testing.T) {
		assert.Contains(t, mustRun("open"), "OPEN")
		assert.Contains(t, status(), "OPEN")
	})

	t.Run("open is idempotent", func(t *testing.T) {
		assert.Contains(t, mustRun("open"), "OPEN")
		assert.Contains(t, status(), "OPEN")
	})

	t.Run("missing barrier row is an error not open", func(t *testing.T) {
		// Remove the singleton row against the same DB, then restore it.
		pool, err := database.Connect(ctx, &database.PostgresConfig{URL: dsn})
		require.NoError(t, err)
		defer pool.Close()
		_, err = pool.Exec(ctx, `DELETE FROM admission_barrier WHERE id = 1`)
		require.NoError(t, err)
		defer func() {
			_, _ = pool.Exec(context.Background(),
				`INSERT INTO admission_barrier (id, closed) VALUES (1, false) ON CONFLICT (id) DO NOTHING`)
		}()

		out, _, err := runCLI(t, "status", "--db-url", dsn)
		require.Error(t, err, "missing barrier row must fail nonzero")
		assert.NotContains(t, strings.ToUpper(out), "OPEN",
			"missing row must not be reported as open")
	})
}

func TestAdmissionsReportRejectsArgs(t *testing.T) {
	_, _, err := runCLI(t, "report", "extra")
	require.Error(t, err)
}

func TestAdmissionsReportRejectsBadOutput(t *testing.T) {
	t.Setenv(env.PostgresDatabaseURL.Name(), "postgres://unused")
	_, _, err := runCLI(t, "report", "--output", "yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--output")
}

// errWriter fails every write, standing in for a broken pipe / full disk so a
// rendered report cannot silently drop output while the command still exits 0.
type errWriter struct{ err error }

func (w errWriter) Write([]byte) (int, error) { return 0, w.err }

func TestAdmissionsReportTextWriterFailurePropagates(t *testing.T) {
	report := &database.AdmissionReport{AdmissionsClosed: false}
	err := writeReportText(errWriter{err: assertErr}, report)
	require.ErrorIs(t, err, assertErr, "a failing text writer must surface an error")
	err = writeReportJSON(errWriter{err: assertErr}, report)
	require.ErrorIs(t, err, assertErr, "a failing JSON writer must surface an error")
}

var assertErr = errors.New("sink write failed")

// TestAdmissionsReportAgainstPostgres proves the report prints the barrier
// state and every section header against a real migrated database, and that a
// broken schema fails rather than emitting an empty success report.
func TestAdmissionsReportAgainstPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}
	ctx := context.Background()
	dsn := dbtest.StartT(ctx, t)
	dbtest.MigrateT(t, dsn, false)

	out, _, err := runCLI(t, "report", "--db-url", dsn)
	require.NoError(t, err)
	// Barrier state and the observation-only disclaimer must be present, plus
	// every section header so an operator can distinguish "empty section" from
	// "section not produced."
	assert.Contains(t, out, "admission barrier: OPEN")
	assert.Contains(t, out, "database observation only")
	for _, section := range []string{
		"non-terminal tasks", "open lifecycle operations",
		"outstanding dispatch reservations", "unsettled publication",
		"pending quiescence", "retained runtime bindings",
	} {
		assert.Contains(t, out, section, "report must print the %q header", section)
	}
	assert.NotContains(t, strings.ToUpper(out), "TRUNCATED")

	// JSON output is valid and machine-readable.
	out, _, err = runCLI(t, "report", "--db-url", dsn, "--output", "json")
	require.NoError(t, err)
	assert.Contains(t, out, `"admissionsClosed"`)
	assert.Contains(t, out, `"nonTerminalTasks"`)

	// A missing barrier row is a broken schema: the command must fail nonzero,
	// not print a clean report that a drain would mistakenly trust.
	pool, err := database.Connect(ctx, &database.PostgresConfig{URL: dsn})
	require.NoError(t, err)
	defer pool.Close()
	_, err = pool.Exec(ctx, `DELETE FROM admission_barrier WHERE id = 1`)
	require.NoError(t, err)
	defer func() {
		_, _ = pool.Exec(context.Background(),
			`INSERT INTO admission_barrier (id, closed) VALUES (1, false) ON CONFLICT (id) DO NOTHING`)
	}()
	_, _, err = runCLI(t, "report", "--db-url", dsn)
	require.Error(t, err, "missing barrier row must fail nonzero")
}
