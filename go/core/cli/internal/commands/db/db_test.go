package db

import (
	"context"
	"testing"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	"github.com/kagent-dev/kagent/go/core/pkg/env"
	"github.com/kagent-dev/kagent/go/core/pkg/migrations"
	"github.com/stretchr/testify/require"
)

func TestMigrationSourcesFromEnvironment(t *testing.T) {
	// The execution track always follows the built-ins: it carries the
	// E2B/provider schema ordinary store operations depend on, matching how the
	// controller registers it via Options.ExtraMigrations.
	for _, tt := range []struct {
		input string
		want  []string
	}{
		{input: "false", want: []string{"core", "execution"}},
		{input: " TrUe ", want: []string{"core", "vector", "execution"}},
		{input: "invalid", want: []string{"core", "execution"}},
	} {
		t.Run(tt.input, func(t *testing.T) {
			t.Setenv(env.DatabaseVectorEnabled.Name(), tt.input)
			namespace := "unused"
			sources, err := migrationSources(&namespace)(t.Context())
			require.NoError(t, err)
			var names []string
			for _, source := range sources {
				names = append(names, source.Name)
			}
			require.Equal(t, tt.want, names)
		})
	}
}

// TestMigrationSourcesApplyExecutionTrack proves the CLI's `db migrate` path
// creates the fork-owned execution schema — including admission_barrier — that
// the operator `db admissions` commands and the store depend on.
func TestMigrationSourcesApplyExecutionTrack(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}
	ctx := context.Background()
	dsn := dbtest.StartT(ctx, t)

	// Pin vector selection: without the env var, migrationSources consults the
	// developer's current Kubernetes context, making this offline test depend
	// on local cluster configuration.
	t.Setenv(env.DatabaseVectorEnabled.Name(), "false")

	namespace := "unused"
	sources, err := migrationSources(&namespace)(ctx)
	require.NoError(t, err)
	require.NoError(t, migrations.RunUp(ctx, dsn, sources))

	pool, err := database.Connect(ctx, &database.PostgresConfig{URL: dsn})
	require.NoError(t, err)
	defer pool.Close()
	var present bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'admission_barrier')`).Scan(&present))
	require.True(t, present, "CLI migration must create the execution-track admission_barrier table")
}
