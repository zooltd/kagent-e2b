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

const e2bOrphanReaperInterval = 30 * time.Second

// e2bOrphanReaperStore resolves whether a provider-reported sandbox is still
// claimed by a live session binding, whether an operation id still names an
// open CREATE that could adopt a late sandbox, and — the deletion fence — the
// serialized final decision. It is satisfied by *database.Client.
type e2bOrphanReaperStore interface {
	ListOpenE2BCreateOperations(context.Context) ([]database.OpenE2BCreate, error)
	// E2BOrphanDeletionGate is the durable fence: it takes the owning
	// session's row lock — the same serialization point every adoption path
	// takes — and re-verifies under it that no live binding claims the
	// sandbox and no open CREATE can still adopt it. Without it the reaper's
	// reads are a snapshot an adoption can commit behind, and the delete
	// would destroy just-bound compute.
	E2BOrphanDeletionGate(context.Context, uuid.UUID, string, string) (bool, error)
}

// e2bOwnerOperationKey is the sandbox metadata tag the create path stamps with
// the CREATE operation id that owns the sandbox. The tag is only a claim —
// another kagent deployment uses the same key — so the reaper verifies the
// tagged operation id resolves to a create attempt in THIS database before it
// treats the sandbox as owned. A sandbox whose tag is absent, unparseable, or
// resolves to no local attempt was placed by a different owner and is never
// this controller's to delete.
const e2bOwnerOperationKey = "kagent.dev/operation"

// E2BOrphanLister is the narrow provider effect the reaper needs: listing the
// running sandboxes in one installation and removing one. *e2b.Client
// satisfies it in production.
type E2BOrphanLister interface {
	ListSandboxes(context.Context, map[string]string) ([]e2b.ListedSandbox, error)
	DeleteSandbox(context.Context, string) error
}

// E2BOrphanLookup resolves one installation's sandbox lister. It is a function
// so the reaper can be tested without constructing real provider clients; an
// unknown installation returns an error so that installation's sweep fails
// rather than silently skipping orphan detection.
type E2BOrphanLookup func(installation string) (E2BOrphanLister, error)

// E2BOrphanReaper removes provider sandboxes that are not claimed by any live
// session binding. An E2B sandbox can outlive its create when the create
// response is lost after allocation, or its binding can be revoked by session
// deletion while the sandbox is still running; in both cases the sandbox is
// untracked compute the deployment pays for and a stale credential could
// reach. The reaper reconciles provider truth against the durable bindings and
// deletes the difference. It runs under leader election so only one controller
// instance sweeps.
type E2BOrphanReaper struct {
	store         e2bOrphanReaperStore
	installations []string
	lookup        E2BOrphanLookup
	interval      time.Duration
}

var (
	_ manager.Runnable               = (*E2BOrphanReaper)(nil)
	_ manager.LeaderElectionRunnable = (*E2BOrphanReaper)(nil)
)

// NewE2BOrphanReaper builds the sweeper for the configured installation ids.
// An empty installation set yields a no-op worker.
func NewE2BOrphanReaper(store e2bOrphanReaperStore, installations []string, lookup E2BOrphanLookup) *E2BOrphanReaper {
	return &E2BOrphanReaper{store: store, installations: installations, lookup: lookup, interval: e2bOrphanReaperInterval}
}

func (r *E2BOrphanReaper) NeedLeaderElection() bool { return true }

func (r *E2BOrphanReaper) Start(ctx context.Context) error {
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

// sweep lists every running sandbox in each installation and deletes those no
// live binding claims. A listing or per-sandbox failure is logged and retried
// next sweep; it never blocks the other installations.
func (r *E2BOrphanReaper) sweep(ctx context.Context) {
	log := logging.FromContext(ctx)
	// Resolve the still-open CREATE operation set once for the whole sweep so
	// each candidate's ownership tag can be checked against durable intent
	// rather than re-listing per sandbox. A list failure is indeterminate:
	// rather than risk deleting a sandbox an open create could still adopt,
	// the entire sweep is skipped this round.
	openOps, err := r.openOperationSet(ctx)
	if err != nil {
		log.ErrorContext(ctx, "e2b orphan sweep cannot resolve open creates; skipping", "error", err)
		return
	}
	for _, installation := range r.installations {
		if ctx.Err() != nil {
			return
		}
		if err := r.sweepInstallation(ctx, installation, openOps); err != nil {
			log.ErrorContext(ctx, "e2b orphan sweep failed", "installation", installation, "error", err)
		}
	}
}

// openOperationSet returns the set of operation ids that still have an open
// CREATE — the durable operations a late-arriving sandbox can still bind to.
func (r *E2BOrphanReaper) openOperationSet(ctx context.Context) (map[string]struct{}, error) {
	opens, err := r.store.ListOpenE2BCreateOperations(ctx)
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(opens))
	for _, op := range opens {
		set[op.OperationID.String()] = struct{}{}
	}
	return set, nil
}

func (r *E2BOrphanReaper) sweepInstallation(ctx context.Context, installation string, openOps map[string]struct{}) error {
	lister, err := r.lookup(installation)
	if err != nil {
		return err
	}
	listCtx, cancel := context.WithTimeout(ctx, time.Minute)
	sandboxes, err := lister.ListSandboxes(listCtx, nil)
	cancel()
	if err != nil {
		return err
	}
	log := logging.FromContext(ctx)
	for _, sb := range sandboxes {
		if ctx.Err() != nil {
			return nil
		}
		if sb.SandboxID == "" {
			continue
		}
		if err := r.reapIfOrphan(ctx, installation, sb, lister, openOps); err != nil {
			log.ErrorContext(ctx, "e2b orphan reap failed", "installation", installation, "sandbox", sb.SandboxID, "error", err)
		}
	}
	return nil
}

// reapIfOrphan deletes a sandbox only when this deployment affirmatively owns
// it, no live binding claims it, and no still-open CREATE could adopt it.
// Ownership is the create-path operation tag: a sandbox carrying no
// kagent.dev/operation tag was placed by another client or control plane
// sharing the installation and is never deleted, no matter how old it is. An
// untagged, malformed, or unparsed StartedAt is treated as a young sandbox —
// the conservative outcome is always to leave the sandbox.
//
// The final orphan decision is not made from the sweep's earlier reads — those
// are a snapshot an adoption can commit behind. It is made by
// E2BOrphanDeletionGate, which takes the owning session's row lock (the same
// lock every adoption path serializes on) and re-verifies under it that the
// sandbox is still unbound and un-adoptable. Only a gate-approved sandbox is
// passed to DeleteSandbox.
func (r *E2BOrphanReaper) reapIfOrphan(ctx context.Context, installation string, sb e2b.ListedSandbox, lister E2BOrphanLister, openOps map[string]struct{}) error {
	sandboxID := sb.SandboxID
	// Ownership gate: the tag is only a claim — a co-tenant deployment stamps
	// the same key — so it must resolve to a create attempt in this database
	// before the sandbox counts as owned. Absent, unparseable, or foreign tags
	// are never reaped. A tag naming a still-open CREATE is recoverable: the
	// late-binding sweep can still adopt it, so deleting races that adoption.
	ownerOp := sb.Metadata[e2bOwnerOperationKey]
	ownerOpID, parseErr := uuid.Parse(ownerOp)
	if ownerOp == "" || parseErr != nil {
		return nil // no valid ownership tag; not this deployment's
	}
	if _, open := openOps[ownerOpID.String()]; open {
		return nil // an open CREATE can still adopt it (cheap pre-filter; the
		// gate re-checks this under the lock)
	}
	// Skip owned sandboxes too young to be safely declared orphans: a create
	// still completing its binding within the operation timeout is in flight,
	// not orphaned. A missing or unparsed StartedAt is conservative — leave it.
	started, err := time.Parse(time.RFC3339, sb.StartedAt)
	if err != nil {
		return nil // cannot establish age; do not delete on uncertainty
	}
	if time.Since(started) < database.RuntimeOperationTimeout {
		return nil
	}
	// The deletion fence: under the owning session's row lock, re-verify the
	// attempt correlates to this installation+sandbox, that the owning CREATE
	// is no longer adoptable, and that no live binding claims the external id.
	// A gate that returns false or errors leaves the sandbox — an adoption may
	// have committed behind the sweep's snapshot, or the state is indeterminate.
	allowed, err := r.store.E2BOrphanDeletionGate(ctx, ownerOpID, installation, sandboxID)
	if err != nil {
		return err // indeterminate claim state: do not delete on uncertainty
	}
	if !allowed {
		return nil // foreign, still-adoptable, or already bound
	}
	// The gate serialized the decision: no adoption could commit behind it.
	// Delete tolerates an already-gone sandbox.
	if err := lister.DeleteSandbox(ctx, sandboxID); err != nil && !e2b.IsNotFound(err) {
		return err
	}
	logging.FromContext(ctx).InfoContext(ctx, "reaped orphan e2b sandbox", "installation", installation, "sandbox", sandboxID)
	return nil
}
