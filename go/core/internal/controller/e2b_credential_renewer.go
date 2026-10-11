package controller

import (
	"context"
	"time"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const e2bCredentialRenewInterval = 30 * time.Second

// e2bCredentialRenewerStore lists the live E2B bindings whose runtime
// credential the renewer keeps ahead of expiry. Satisfied by
// *database.Client.
type e2bCredentialRenewerStore interface {
	ListE2BCredentialBindings(context.Context) ([]database.E2BCredentialBinding, error)
}

// E2BCredentialRefresh is the shared refresh effect the renewer delegates to.
// RefreshRunning refreshes a session's bound credential only while its
// sandbox is running; a paused sandbox is skipped rather than woken.
// Satisfied by *sessionsvc.E2BCredentialRefresher in production.
type E2BCredentialRefresh interface {
	RefreshRunning(ctx context.Context, sessionID string) (refreshed bool, err error)
}

// E2BCredentialRenewer keeps each live E2B session's bound runtime credential
// ahead of its expiry. The bootstrap issues a credential bounded by a TTL;
// without renewal a long-running task or a session resumed after the TTL
// would lose signed TaskStore access while its binding stays valid. The
// renewer sweeps every live binding and refreshes only sandboxes the provider
// reports running — a paused sandbox is never woken just to renew a
// credential; its refresh happens when work actually wakes it through
// dispatch or explicit resume. Leader-elected so only one controller sweeps;
// a per-session failure is logged and retried next sweep.
type E2BCredentialRenewer struct {
	store    e2bCredentialRenewerStore
	refresh  E2BCredentialRefresh
	interval time.Duration
}

var (
	_ manager.Runnable               = (*E2BCredentialRenewer)(nil)
	_ manager.LeaderElectionRunnable = (*E2BCredentialRenewer)(nil)
)

// NewE2BCredentialRenewer builds the renewer over the store and shared
// refresher. A nil refresher yields a no-op worker.
func NewE2BCredentialRenewer(store e2bCredentialRenewerStore, refresh E2BCredentialRefresh) *E2BCredentialRenewer {
	return &E2BCredentialRenewer{store: store, refresh: refresh, interval: e2bCredentialRenewInterval}
}

func (r *E2BCredentialRenewer) NeedLeaderElection() bool { return true }

func (r *E2BCredentialRenewer) Start(ctx context.Context) error {
	if r.refresh == nil {
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

// sweep refreshes every live binding whose sandbox is running. RefreshRunning
// applies the running/paused gate, so the renewer passes the full live set and
// lets the shared refresher skip paused compute rather than duplicating the
// state check here.
func (r *E2BCredentialRenewer) sweep(ctx context.Context) {
	bindings, err := r.store.ListE2BCredentialBindings(ctx)
	if err != nil {
		logging.FromContext(ctx).ErrorContext(ctx, "e2b credential-renewal list failed", "error", err)
		return
	}
	log := logging.FromContext(ctx)
	for _, b := range bindings {
		if ctx.Err() != nil {
			return
		}
		if _, err := r.refresh.RefreshRunning(ctx, b.SessionID.String()); err != nil {
			log.ErrorContext(ctx, "e2b credential renew failed",
				"session", b.SessionID, "sandbox", b.ExternalID, "error", err)
		}
	}
}

// ReconcileOnce runs one sweep synchronously so fault-injection tests can
// drive a renewal without standing up the leader-elected loop.
func (r *E2BCredentialRenewer) ReconcileOnce(ctx context.Context) {
	r.sweep(ctx)
}
