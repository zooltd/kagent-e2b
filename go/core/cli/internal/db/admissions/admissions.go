// Package admissions provides the `kagent db admissions` operator commands.
//
// They flip and read the single global admission_barrier row that gates new
// agent-session admission (session create, dispatch, resume, fork) across both
// execution providers. This is an operator maintenance switch only — closing
// admissions does not drain or quiesce in-flight work, and reopening does not
// certify that maintenance succeeded. The commands talk to Postgres directly
// via --db-url / KAGENT_POSTGRES_DATABASE_URL; they run no migrations, make no
// runtime or Kubernetes calls, and never print the connection string.
package admissions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/pkg/consts"
	"github.com/kagent-dev/kagent/go/core/pkg/env"
	"github.com/spf13/cobra"
)

// opTimeout bounds connection setup plus the operation. The store's own connect
// retry window is two minutes, which is too long for an interactive operator
// command; this deadline makes a dead endpoint or hung query fail nonzero
// quickly instead of blocking the terminal.
const opTimeout = 15 * time.Second

type commandState struct {
	dbURL string
}

// NewCommand returns the `admissions` command with status, close, and open.
func NewCommand() *cobra.Command {
	state := &commandState{}
	cmd := &cobra.Command{
		Use:   "admissions",
		Short: "Inspect and control agent-session admission",
		Long: `Inspect and control agent-session admission for maintenance.

The barrier gates only NEW agent-session work — creating a session, starting a
new turn, resuming a suspended session, forking a checkpoint — across both the
Substrate and E2B providers. Closing admissions refuses that new work with a
temporary-unavailable response; already-accepted tasks, settle/cancel/delete,
callbacks, and reconciliation keep running. Closing is not a drain: sessions
already in flight are unaffected. Opening resumes admission and says nothing
about whether the maintenance that prompted the closure succeeded.

The connection string comes from --db-url or ` + env.PostgresDatabaseURL.Name() + `.`,
	}
	cmd.PersistentFlags().StringVar(&state.dbURL, "db-url", "", "PostgreSQL connection URL")
	cmd.AddCommand(newStatusCmd(state))
	cmd.AddCommand(newCloseCmd(state))
	cmd.AddCommand(newOpenCmd(state))
	cmd.AddCommand(newReportCmd(state))
	return cmd
}

// resolveDSN reads --db-url, then KAGENT_POSTGRES_DATABASE_URL. It returns the
// value for connection use only; it is never printed or embedded in errors, so
// embedded credentials cannot leak into output.
func (s *commandState) resolveDSN() (string, error) {
	if dsn := strings.TrimSpace(s.dbURL); dsn != "" {
		return dsn, nil
	}
	if value, set := env.PostgresDatabaseURL.Lookup(); set {
		if dsn := strings.TrimSpace(value); dsn != "" {
			return dsn, nil
		}
	}
	return "", fmt.Errorf("set the database URL with --db-url or %s", env.PostgresDatabaseURL.Name())
}

// withClient connects with a bounded deadline and hands the operator's command
// a store client. Connection failures, timeouts, and a missing barrier schema
// surface as errors (nonzero exit), never as a false "open" report.
func (s *commandState) withClient(command *cobra.Command, fn func(ctx context.Context, c *database.Client) error) error {
	dsn, err := s.resolveDSN()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(command.Context(), opTimeout)
	defer cancel()
	// Validate the URL without echoing its contents: the parse error retains
	// the DSN, so report only that the value was invalid — never its text. A
	// DSN that parses reaches Connect, whose remaining failures are
	// network-level and name the host, not the credential.
	if err := database.ValidateURL(dsn); err != nil {
		return fmt.Errorf("invalid database URL supplied via --db-url or %s", env.PostgresDatabaseURL.Name())
	}
	// The barrier lives in the same schema migrations target; connect under
	// the configured schema and role so `db migrate` and `db admissions`
	// address the same admission_barrier row.
	pool, err := database.Connect(ctx, &database.PostgresConfig{
		URL:    dsn,
		Role:   env.DatabaseRole.Get(),
		Schema: env.DatabaseSchema.Get(),
	})
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()
	return fn(ctx, database.NewClient(pool, consts.DefaultPgvectorSchema))
}

func newStatusCmd(state *commandState) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show whether agent-session admission is open or closed",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return state.withClient(command, func(ctx context.Context, c *database.Client) error {
				closed, err := c.AdmissionsClosed(ctx)
				if err != nil {
					return fmt.Errorf("read admission barrier: %w", err)
				}
				if closed {
					fmt.Fprintln(command.OutOrStdout(), "agent-session admissions are CLOSED")
				} else {
					fmt.Fprintln(command.OutOrStdout(), "agent-session admissions are OPEN")
				}
				return nil
			})
		},
	}
}

func newCloseCmd(state *commandState) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:   "close",
		Short: "Close agent-session admission to new work",
		Long: `Close agent-session admission to new work for maintenance.

New sessions, new turns, resumes, and checkpoint forks are refused until
` + "`kagent db admissions open`" + ` runs. Work already admitted keeps running;
this does not drain it.`,
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return state.withClient(command, func(ctx context.Context, c *database.Client) error {
				closed, err := c.SetAdmissionsClosed(ctx, true, reason)
				if err != nil {
					return fmt.Errorf("close admission barrier: %w", err)
				}
				if !closed {
					return errors.New("close reported open after the write committed")
				}
				// Report only the committed result; a concurrent operator may have
				// changed the barrier first, so "already closed" is not knowable
				// from a separate pre-read. Repeated closes stay idempotent.
				fmt.Fprintln(command.OutOrStdout(), "agent-session admissions CLOSED")
				return nil
			})
		},
	}
	cmd.Flags().StringVar(&reason, "reason", "", "free-text reason recorded with the closure")
	return cmd
}

func newOpenCmd(state *commandState) *cobra.Command {
	return &cobra.Command{
		Use:   "open",
		Short: "Reopen agent-session admission to new work",
		Long: `Reopen agent-session admission to new work.

This resumes admission only; it does not certify that the maintenance that
prompted the closure succeeded.`,
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return state.withClient(command, func(ctx context.Context, c *database.Client) error {
				closed, err := c.SetAdmissionsClosed(ctx, false, "")
				if err != nil {
					return fmt.Errorf("open admission barrier: %w", err)
				}
				if closed {
					return errors.New("open reported closed after the write committed")
				}
				fmt.Fprintln(command.OutOrStdout(), "agent-session admissions OPEN")
				return nil
			})
		},
	}
}

// newReportCmd returns `kagent db admissions report`: a read-only audit
// baseline of durable agent-session work for a drain. It makes one coherent
// repeatable-read snapshot of the database and prints it. It performs NO
// provider, Kubernetes, envd, or runtime calls, no claims, locks, or writes —
// it is a database observation, and empty output means no listed durable work
// was seen, not that the drain or the compute underneath it is done.
func newReportCmd(state *commandState) *cobra.Command {
	var output string
	command := &cobra.Command{
		Use:   "report",
		Short: "Report durable unresolved agent-session work for a drain",
		Long: `Report durable unresolved agent-session work for a maintenance drain.

This is a read-only audit baseline: one consistent Postgres snapshot listing
non-terminal tasks, open lifecycle operations, outstanding dispatch
reservations, unsettled publication/quiescence, and retained runtime bindings
across both providers. It makes no provider, runtime, or Kubernetes calls and
changes nothing.

The output is a database observation only. It does not verify provider
compute, drain completion, or that any session is paused or safe to upgrade.
An empty report means no listed durable work was observed — not that every
stream or runtime is inactive.`,
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if output != "text" && output != "json" {
				return fmt.Errorf("invalid --output %q. Use text or json", output)
			}
			return state.withClient(command, func(ctx context.Context, c *database.Client) error {
				report, err := c.AdmissionReportSnapshot(ctx)
				if err != nil {
					// A missing/unmigrated schema or query failure must surface as a
					// nonzero exit, never an empty "success" report.
					return fmt.Errorf("read admission report: %w", err)
				}
				if output == "json" {
					return writeReportJSON(command.OutOrStdout(), report)
				}
				return writeReportText(command.OutOrStdout(), report)
			})
		},
	}
	command.Flags().StringVar(&output, "output", "text", `Output format: "text" or "json"`)
	return command
}

func writeReportJSON(out io.Writer, report *database.AdmissionReport) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

// writeReportText prints a compact, operator-readable inventory. Every section
// header is printed even when empty so an operator can tell "no durable work
// observed" from a section that was skipped. The body is built into a buffer
// and emitted in one write so a failing writer propagates an error rather than
// silently producing a truncated audit report (e.g. dropping the truncation
// warning) while the command still exits zero.
func writeReportText(out io.Writer, report *database.AdmissionReport) error {
	var buf strings.Builder
	w := &buf
	state := "OPEN"
	if report.AdmissionsClosed {
		state = "CLOSED"
	}
	fmt.Fprintf(w, "admission barrier: %s\n", state)
	fmt.Fprintf(w, "snapshot at:       %s\n", report.GeneratedAt.UTC().Format(time.RFC3339))
	fmt.Fprintln(w, "(database observation only; provider state and drain completion are not verified)")

	section := func(title string, n int) {
		fmt.Fprintf(w, "\n%s (%d)\n", title, n)
	}

	section("non-terminal tasks", len(report.NonTerminalTasks))
	for _, task := range report.NonTerminalTasks {
		note := ""
		if task.Waiting {
			note = " (waiting for input/authentication)"
		}
		fmt.Fprintf(w, "  session=%s task=%s state=%s provider=%s%s\n",
			task.SessionID, task.TaskID, task.State, task.Provider, note)
	}

	section("open lifecycle operations", len(report.OpenOperations))
	for _, op := range report.OpenOperations {
		var extra strings.Builder
		if op.ExecutorID != nil {
			fmt.Fprintf(&extra, " executor=%s", *op.ExecutorID)
		}
		if op.ExecutorExpiresAt != nil {
			fmt.Fprintf(&extra, " claim-expires=%s", op.ExecutorExpiresAt.UTC().Format(time.RFC3339))
		}
		if op.CreateAttemptID != nil {
			fmt.Fprintf(&extra, " create-attempt=%s outcome=%s", *op.CreateAttemptID, deref(op.CreateOutcome))
			if op.CreateExternalID != nil {
				fmt.Fprintf(&extra, " external=%s", *op.CreateExternalID)
			}
		}
		fmt.Fprintf(&extra, " bound=%t", op.Bound)
		fmt.Fprintf(w, "  session=%s op=%s op-id=%s state=%s provider=%s%s\n",
			op.SessionID, op.Operation, op.OperationID, op.State, op.Provider, extra.String())
	}

	section("outstanding dispatch reservations", len(report.DispatchReservations))
	for _, d := range report.DispatchReservations {
		fmt.Fprintf(w, "  session=%s dispatch=%s expires=%s\n",
			d.SessionID, d.DispatchID, d.DispatchExpiresAt.UTC().Format(time.RFC3339))
	}

	section("unsettled publication / claimed quiescence", len(report.UnsettledEvents))
	for _, u := range report.UnsettledEvents {
		fmt.Fprintf(w, "  session=%s task=%s seq=%d kind=%s\n",
			u.SessionID, u.TaskID, u.Sequence, u.Kind)
	}

	section("pending quiescence", len(report.PendingQuiescence))
	for _, q := range report.PendingQuiescence {
		label := "unclaimed"
		if q.Claimed {
			label = "claimed"
		}
		fmt.Fprintf(w, "  session=%s task=%s seq=%d %s\n",
			q.SessionID, q.TaskID, q.Sequence, label)
	}

	section("retained runtime bindings", len(report.RetainedBindings))
	for _, b := range report.RetainedBindings {
		note := " (idle-but-bound; retained runtime identity, not paused or safe to upgrade)"
		if b.ActiveTask {
			note = " (active task)"
		}
		if b.Provider == "e2b" {
			fmt.Fprintf(w, "  session=%s provider=%s installation=%s external=%s gen=%d%s\n",
				b.SessionID, b.Provider, b.Installation, b.ExternalID, derefInt(b.Generation), note)
		} else {
			fmt.Fprintf(w, "  session=%s provider=%s actor=%s%s\n",
				b.SessionID, b.Provider, b.ActorUID, note)
		}
	}

	if report.Truncated {
		fmt.Fprintln(w, "\nWARNING: one or more sections hit the row cap; this is a partial inventory, not exhaustive.")
	}
	_, err := io.WriteString(out, buf.String())
	return err
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
