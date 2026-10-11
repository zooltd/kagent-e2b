package controller

import (
	"context"
	"time"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/kagent-dev/kagent/go/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const e2bTimeoutRenewInterval = 30 * time.Second

// MinRenewableLifetimeSeconds is the smallest sandbox lifetime the renewer
// will accept. A renewal sweeps every e2bTimeoutRenewInterval, so a lifetime
// shorter than the cadence can let an admitted task's sandbox pause or die
// between sweeps — an admitted task must always renew to at least this floor.
const MinRenewableLifetimeSeconds int32 = int32(e2bTimeoutRenewInterval / time.Second)

// e2bTimeoutRenewerStore resolves the bound sessions whose compute must be
// kept alive. It is satisfied by *database.Client.
type e2bTimeoutRenewerStore interface {
	ListE2BSessionsWithRunningTasks(context.Context) ([]database.RunningE2BTask, error)
}

// E2BTimeoutSetter is the provider effect the renewer needs: extending one
// sandbox's lifetime. *e2b.Client satisfies it in production.
type E2BTimeoutSetter interface {
	SetSandboxTimeout(context.Context, string, int32) error
}

// E2BTimeoutSetterLookup resolves a timeout setter for one installation plus
// the lifetime to write. It is a function so the renewer is testable without
// real provider clients.
type E2BTimeoutSetterLookup func(installation string) (setter E2BTimeoutSetter, lifetimeSeconds int32, err error)

// E2BTimeoutRenewer keeps a bound sandbox's lifetime ahead of its running
// tasks. The plan requires renewing the timeout during admitted tasks —
// including waiting native turns that remain physically running — so a task's
// compute is never killed or paused mid-turn. It sweeps the sessions that have
// physically running work (non-terminal tasks, including input/auth-required
// waits) and renews each bound sandbox's lifetime to the configured value. A
// sandbox that stops having running work simply stops being renewed and ages
// to its timeout-to-pause bound. Leader-elected; a per-sandbox failure is
// logged and retried next sweep.
type E2BTimeoutRenewer struct {
	store    e2bTimeoutRenewerStore
	lookup   E2BTimeoutSetterLookup
	interval time.Duration
}

var (
	_ manager.Runnable               = (*E2BTimeoutRenewer)(nil)
	_ manager.LeaderElectionRunnable = (*E2BTimeoutRenewer)(nil)
)

// NewE2BTimeoutRenewer builds the renewer. A nil lookup disables it.
func NewE2BTimeoutRenewer(store e2bTimeoutRenewerStore, lookup E2BTimeoutSetterLookup) *E2BTimeoutRenewer {
	return &E2BTimeoutRenewer{store: store, lookup: lookup, interval: e2bTimeoutRenewInterval}
}

func (r *E2BTimeoutRenewer) NeedLeaderElection() bool { return true }

func (r *E2BTimeoutRenewer) Start(ctx context.Context) error {
	if r.lookup == nil {
		<-ctx.Done()
		return nil
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		r.sweep(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}

// sweep renews the timeout on every bound sandbox with running work.
func (r *E2BTimeoutRenewer) sweep(ctx context.Context) {
	running, err := r.store.ListE2BSessionsWithRunningTasks(ctx)
	if err != nil {
		logging.FromContext(ctx).ErrorContext(ctx, "e2b timeout-renew list failed", "error", err)
		return
	}
	log := logging.FromContext(ctx)
	for _, task := range running {
		if ctx.Err() != nil {
			return
		}
		if err := r.renew(ctx, task); err != nil {
			log.ErrorContext(ctx, "e2b timeout renew failed",
				"session", task.SessionID, "sandbox", task.ExternalID, "error", err)
		}
	}
}

// renew resolves the installation's setter and lifetime and extends the bound
// sandbox. An installation that cannot renew (unconfigured or no api) leaves
// the sandbox to age toward its timeout-to-pause bound rather than erroring.
func (r *E2BTimeoutRenewer) renew(ctx context.Context, task database.RunningE2BTask) error {
	setter, lifetime, err := r.lookup(task.Installation)
	if err != nil || setter == nil || lifetime <= 0 {
		return err
	}
	renewCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// A gone sandbox is skipped, not retried — the binding/dialer path owns
	// runtime-loss reporting.
	if err := setter.SetSandboxTimeout(renewCtx, task.ExternalID, lifetime); err != nil && !e2b.IsNotFound(err) {
		return err
	}
	return nil
}
