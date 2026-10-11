package controller

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/kagent-dev/kagent/go/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const e2bLateBindingInterval = 30 * time.Second

// e2bLateBindingStore resolves the durable operations a late-arriving sandbox
// can still bind to, plus the artifact binding that scopes the template check.
// It is satisfied by *database.Client.
type e2bLateBindingStore interface {
	ListOpenE2BCreateOperations(context.Context) ([]database.OpenE2BCreate, error)
	GetE2BRevisionBinding(context.Context, string) (*database.ExecutionRevisionBinding, error)
	ClaimSessionOperation(context.Context, string, uuid.UUID, uuid.UUID) (bool, error)
	ReleaseRuntimeOperation(context.Context, string, uuid.UUID, uuid.UUID) error
	BindE2BSessionCreate(context.Context, string, uuid.UUID, uuid.UUID, database.E2BCreateBinding) error
}

// E2BLateBindingReconciler binds a provider sandbox whose create response was
// lost to the open CREATE operation that still awaits it. The inline create
// path reconciles the common case; this sweep covers the late-response case the
// plan calls out: a create that lands after the caller gave up, whose listing
// lagged the inline reconcile, or whose executor crashed before binding. The
// orphan reaper alone would delete such a sandbox as unbound; this sweep runs
// the bind first so a legitimately-created sandbox is adopted, not reaped.
//
// It claims the operation the same way a later executor would — the executor
// CAS after the original claim expires — so at most one binder wins, then
// binds a single verified correlation+template match through the same atomic
// FinishE2BSessionCreate a clean response uses. Multiple matches are left for
// the orphan reaper's quarantine; zero matches are left pending, never
// re-created.
type E2BLateBindingReconciler struct {
	store         e2bLateBindingStore
	installations []string
	lookup        E2BOrphanLookup
	interval      time.Duration
}

var (
	_ manager.Runnable               = (*E2BLateBindingReconciler)(nil)
	_ manager.LeaderElectionRunnable = (*E2BLateBindingReconciler)(nil)
)

// NewE2BLateBindingReconciler builds the sweep for the configured
// installations. It shares the orphan reaper's installation lookup; an empty
// installation set yields a no-op worker.
func NewE2BLateBindingReconciler(store e2bLateBindingStore, installations []string, lookup E2BOrphanLookup) *E2BLateBindingReconciler {
	return &E2BLateBindingReconciler{store: store, installations: installations, lookup: lookup, interval: e2bLateBindingInterval}
}

func (r *E2BLateBindingReconciler) NeedLeaderElection() bool { return true }

func (r *E2BLateBindingReconciler) Start(ctx context.Context) error {
	if len(r.installations) == 0 {
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

// sweep resolves each open E2B CREATE operation and tries to bind a late
// sandbox to it. A failure is logged and retried next sweep.
func (r *E2BLateBindingReconciler) sweep(ctx context.Context) {
	opens, err := r.store.ListOpenE2BCreateOperations(ctx)
	if err != nil {
		logging.FromContext(ctx).ErrorContext(ctx, "e2b late-binding list failed", "error", err)
		return
	}
	for _, op := range opens {
		if ctx.Err() != nil {
			return
		}
		if err := r.reconcileOperation(ctx, op); err != nil {
			logging.FromContext(ctx).ErrorContext(ctx, "e2b late-binding reconcile failed",
				"session", op.SessionID, "operation", op.OperationID, "error", err)
		}
	}
}

// ReconcileOnce runs one sweep synchronously. It is exported so fault-injection
// tests can drive a reconcile without standing up the leader-elected loop.
func (r *E2BLateBindingReconciler) ReconcileOnce(ctx context.Context) {
	r.sweep(ctx)
}

// reconcileOperation claims one open operation and binds a single verified
// match. The artifact binding supplies the installation + template the match
// must carry; the list is scoped to this operation's correlation tag.
func (r *E2BLateBindingReconciler) reconcileOperation(ctx context.Context, op database.OpenE2BCreate) error {
	artifact, err := r.store.GetE2BRevisionBinding(ctx, op.Revision)
	if err != nil {
		return err
	}
	lister, ok := r.lookupLister(artifact.Installation)
	if !ok {
		return nil // installation not configured for listing; leave pending
	}
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	listed, err := lister.ListSandboxes(listCtx, map[string]string{
		"kagent.dev/operation": op.OperationID.String(),
	})
	cancel()
	if err != nil {
		return err
	}
	var matches []e2b.ListedSandbox
	for _, sb := range listed {
		if sb.TemplateID == artifact.ExternalID {
			matches = append(matches, sb)
		}
	}
	// Exactly one verified match can bind. Zero stays pending (not proof of
	// absence); multiple is the orphan reaper's quarantine case, not a bind.
	if len(matches) != 1 {
		return nil
	}
	// Claim the operation under a fresh executor: the original claim may be
	// expired or its executor gone. If another binder already holds it, this
	// sweep steps aside rather than racing a concurrent completion.
	executorID := uuid.New()
	claimed, err := r.store.ClaimSessionOperation(ctx, op.SessionID.String(), op.OperationID, executorID)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	// Adopt the sandbox: commit its binding while the CREATE stays open. The
	// reconciler does NOT complete READY — it cannot activate the bound guest
	// (bootstrap is the workflow's step and needs the revision payload), so a
	// finish here would land a READY session with no harness. The still-open
	// CREATE is completed by the next session Create attempt, which resumes
	// and activates this same bound sandbox.
	bindCtx, bindCancel := context.WithTimeout(ctx, 10*time.Second)
	err = r.store.BindE2BSessionCreate(bindCtx, op.SessionID.String(), op.OperationID, executorID, database.E2BCreateBinding{
		Installation: artifact.Installation,
		ExternalID:   matches[0].SandboxID,
		Generation:   1,
	})
	bindCancel()
	if err != nil {
		releaseCtx, releaseCancel := context.WithTimeout(ctx, 5*time.Second)
		_ = r.store.ReleaseRuntimeOperation(releaseCtx, op.SessionID.String(), op.OperationID, executorID)
		releaseCancel()
		return err
	}
	// Release the claim so the next Create attempt can re-claim and finish:
	// the binding is durable, the operation stays open for activation.
	releaseCtx, releaseCancel := context.WithTimeout(ctx, 5*time.Second)
	_ = r.store.ReleaseRuntimeOperation(releaseCtx, op.SessionID.String(), op.OperationID, executorID)
	releaseCancel()
	logging.FromContext(ctx).InfoContext(ctx, "adopted late e2b sandbox to open create (binding committed, awaiting activation)",
		"session", op.SessionID, "sandbox", matches[0].SandboxID)
	return nil
}

// lookupLister resolves a lister for an installation, tolerating an
// unconfigured one by returning ok=false so the sweep skips rather than fails.
func (r *E2BLateBindingReconciler) lookupLister(installation string) (E2BOrphanLister, bool) {
	l, err := r.lookup(installation)
	if err != nil {
		return nil, false
	}
	return l, true
}
