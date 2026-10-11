package controller

import (
	"context"
	"time"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/kagent-dev/kagent/go/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const e2bLossReaperInterval = 30 * time.Second

// e2bLossReaperStore resolves the live bindings whose bound compute must be
// verified, and revokes a binding whose compute is gone. It is satisfied by
// *database.Client.
type e2bLossReaperStore interface {
	ListLiveE2BExecutionBindings(context.Context) ([]database.ExecutionInstanceBinding, error)
	RevokeExecutionBinding(context.Context, string) error
}

// E2BLossInspector resolves one external sandbox's liveness. It is satisfied
// by a per-installation *e2b.Client in production and a fake in tests.
type E2BLossInspector interface {
	GetSandbox(ctx context.Context, sandboxID string) (*e2b.Sandbox, error)
}

// E2BLossInspectorLookup resolves an inspector for one installation. It is a
// function so the reaper is testable without real provider clients; an
// unknown installation returns an error so liveness fails closed — the
// binding is left live rather than revoked on an unverifiable installation.
type E2BLossInspectorLookup func(installation string) (E2BLossInspector, error)

// E2BLossReaper revokes execution bindings whose bound sandbox no longer
// exists. Revoking the binding is what kills the session's live credential:
// the runtime authenticator refuses a callback whose session has no live
// binding, so a credential minted against compute that was killed out-of-band
// (provider eviction, manual delete, install wipe) stops authorizing work
// instead of silently targeting a replacement sandbox or failing open.
//
// Only a definitive 404 revokes. A transient list/get failure, an
// unconfigured installation, or a paused-but-present sandbox leaves the
// binding live — the gateway's runtime-loss pre-flight already reports a gone
// sandbox as ErrRuntimeLost at dial time, so revocation here is the durable
// record of loss, never a guess. Leader-elected; a per-binding failure is
// logged and retried next sweep.
type E2BLossReaper struct {
	store    e2bLossReaperStore
	lookup   E2BLossInspectorLookup
	interval time.Duration
}

var (
	_ manager.Runnable               = (*E2BLossReaper)(nil)
	_ manager.LeaderElectionRunnable = (*E2BLossReaper)(nil)
)

// NewE2BLossReaper builds the sweep. A nil lookup disables it.
func NewE2BLossReaper(store e2bLossReaperStore, lookup E2BLossInspectorLookup) *E2BLossReaper {
	return &E2BLossReaper{store: store, lookup: lookup, interval: e2bLossReaperInterval}
}

func (r *E2BLossReaper) NeedLeaderElection() bool { return true }

func (r *E2BLossReaper) Start(ctx context.Context) error {
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

// ReconcileOnce runs one sweep synchronously. It is exported so tests can
// drive a reconcile without standing up the leader-elected loop.
func (r *E2BLossReaper) ReconcileOnce(ctx context.Context) {
	r.sweep(ctx)
}

// sweep verifies every live E2B binding's bound sandbox still exists.
func (r *E2BLossReaper) sweep(ctx context.Context) {
	bindings, err := r.store.ListLiveE2BExecutionBindings(ctx)
	if err != nil {
		logging.FromContext(ctx).ErrorContext(ctx, "e2b loss-reaper list failed", "error", err)
		return
	}
	log := logging.FromContext(ctx)
	for _, binding := range bindings {
		if ctx.Err() != nil {
			return
		}
		if err := r.verify(ctx, binding); err != nil {
			log.ErrorContext(ctx, "e2b loss-reaper verify failed",
				"session", binding.SessionID, "sandbox", binding.ExternalID, "error", err)
		}
	}
}

// verify checks one binding's bound sandbox and revokes the binding on a
// definitive 404. Anything short of proof of absence leaves the binding live.
func (r *E2BLossReaper) verify(ctx context.Context, binding database.ExecutionInstanceBinding) error {
	inspector, err := r.lookup(binding.Installation)
	if err != nil || inspector == nil {
		return err
	}
	getCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = inspector.GetSandbox(getCtx, binding.ExternalID)
	cancel()
	if err == nil {
		return nil // sandbox present (running or paused): still bound
	}
	if !e2b.IsNotFound(err) {
		return err // transient: never revoke on uncertainty
	}
	if err := r.store.RevokeExecutionBinding(ctx, binding.SessionID.String()); err != nil {
		return err
	}
	logging.FromContext(ctx).InfoContext(ctx, "revoked e2b binding for lost sandbox",
		"session", binding.SessionID, "sandbox", binding.ExternalID,
		"installation", binding.Installation)
	return nil
}
