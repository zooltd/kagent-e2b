package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kagent-dev/kagent/go/pkg/logging"

	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var _ manager.Runnable = (*ActorWorkflow)(nil)
var _ manager.LeaderElectionRunnable = (*ActorWorkflow)(nil)

// Every API replica can process idle work; PostgreSQL grants each claim once.
func (*ActorWorkflow) NeedLeaderElection() bool { return false }

// wakeQuiescence is a bounded, nonblocking hint. A buffered signal survives a
// settlement racing with a worker's empty scan or a burst while all workers are
// busy. PostgreSQL, rather than the channel, holds the work to drain.
func (w *ActorWorkflow) wakeQuiescence() {
	select {
	case w.quiescenceWake <- struct{}{}:
	default:
	}
}

// Start pauses or suspends idle sessions independently of task publication.
// Startup and periodic recovery scans discover work missed by local signals.
// Workers drain claims with bounded concurrency; task reads never wait for them.
func (w *ActorWorkflow) Start(ctx context.Context) error {
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			timer := time.NewTimer(w.recoveryInterval)
			defer timer.Stop()
			for ctx.Err() == nil {
				work, err := w.store.ClaimSessionQuiescence(ctx)
				if err == nil {
					// Hand off a hint before runtime I/O so a single settlement
					// burst can enlist all workers, even if this actor is slow.
					w.wakeQuiescence()
					w.quiesceIdleSession(ctx, work)
					continue
				}
				delay := min(time.Second, w.recoveryInterval)
				if errors.Is(err, database.ErrNotFound) {
					pending, pendingErr := w.store.HasPendingSessionQuiescence(ctx)
					if pendingErr != nil {
						err = pendingErr
					} else if !pending {
						delay = w.recoveryInterval
					}
				}
				if !errors.Is(err, database.ErrNotFound) && ctx.Err() == nil {
					logging.FromContext(ctx).ErrorContext(ctx, "claim runtime boundary", "error", err)
				}
				timer.Reset(delay)
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				case <-w.quiescenceWake:
				}
			}
		})
	}
	workers.Wait()
	return nil
}

func (w *ActorWorkflow) quiesceIdleSession(ctx context.Context, work *database.SessionQuiescence) {
	runtimeCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	var snapshot *database.SessionTaskSnapshot
	var err error
	// The backend is determined by the durable revision provider, not by a
	// live binding: a binding the loss reaper already revoked still belongs
	// to an E2B session and must not fall through to the Substrate actor
	// path. An unreadable revision is UNKNOWN — propagated as an error that
	// retains the claim, never silently treated as Substrate.
	e2bSession, err := w.isE2BSession(runtimeCtx, work.Session)
	e2bBoundary := false
	var pauseErr error
	if err == nil && e2bSession {
		pauseErr = w.pauseE2BSession(runtimeCtx, work.Session)
		switch {
		case pauseErr == nil:
			// Confirmed pause: no checkpoint is retained.
			e2bBoundary = true
		case errors.Is(pauseErr, database.ErrNotFound), e2b.IsNotFound(pauseErr):
			// Confirmed loss, not a pause: the binding-resolution proves the
			// binding was revoked (not merely absent), or the provider proved
			// the sandbox gone. Nothing remains to pause or lose, so the
			// boundary resolves without a checkpoint. Transient provider
			// errors keep the claim for retry.
			logging.FromContext(ctx).InfoContext(ctx, "e2b idle boundary resolves as runtime loss",
				"session_id", work.Session.Id, "version", work.Version, "error", pauseErr)
			e2bBoundary = true
		default:
			// Transient/unknown pause outcome: retain the claim.
			err = pauseErr
		}
	} else if err == nil && work.State.Terminal() {
		snapshot, err = w.Quiesce(runtimeCtx, work.Session)
	} else if err == nil {
		err = w.Pause(runtimeCtx, work.Session)
	}
	cancel()
	if err != nil {
		// No timeout-based takeover: the Substrate request may still complete.
		// Keep admission closed until its outcome can be safely reconciled.
		logging.FromContext(ctx).ErrorContext(ctx, "runtime boundary outcome unknown", "session_id", work.Session.Id, "version", work.Version, "error", err)
		return
	}
	// Keep a known snapshot until its reference is stored. Retry database failures
	// without repeating runtime work; give shutdown one bounded completion attempt.
	for delay := 100 * time.Millisecond; ; delay = min(2*delay, 5*time.Second) {
		finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if e2bSession && e2bBoundary {
			err = w.store.FinishE2BSessionQuiescence(finishCtx, work)
		} else {
			err = w.store.FinishSessionQuiescence(finishCtx, work, snapshot)
		}
		finishCancel()
		if err == nil {
			return
		}
		logging.FromContext(ctx).ErrorContext(ctx, "record idle runtime outcome", "session_id", work.Session.Id, "version", work.Version, "error", err)
		if errors.Is(err, database.ErrNotFound) || errors.Is(err, database.ErrConflict) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// isE2BSession reports whether the session's prepared revision is an E2B
// revision — the durable backend identity, independent of whether a live
// binding still resolves (the loss reaper may already have revoked it). A
// revision that cannot be read returns an error: the backend is then
// UNKNOWN, not Substrate, and the caller must not guess.
func (w *ActorWorkflow) isE2BSession(ctx context.Context, session *apiv1alpha1.Session) (bool, error) {
	revision, err := w.store.GetRuntimeRevision(ctx, session.GetPreparedRevision())
	if err != nil {
		return false, fmt.Errorf("resolve session %s backend provider: %w", session.Id, err)
	}
	return revision.Provider == database.ExecutionProviderE2B, nil
}

// pauseE2BSession pauses the bound external sandbox through its installation's
// provider. The provider's pause is the pinned memory-preserving kind the
// INPUT_REQUIRED/AUTH_REQUIRED continuation relies on; a terminal task uses
// the same pause since no checkpoint is retained either way.
//
// Confirmed loss is distinguished from mere absence through the binding
// resolution: a live binding yields a real pause; a REVOKED binding (revoked_at
// set, external_id recorded) proves the compute is durably recorded-gone and
// returns database.ErrNotFound for the caller to resolve as loss. A session
// that never bound, or whose resolution cannot be proven, is NOT loss — it is
// an inconsistent state the caller retains as unknown rather than guessing.
// A missing provider config is an error, never a silent Substrate pause.
func (w *ActorWorkflow) pauseE2BSession(ctx context.Context, session *apiv1alpha1.Session) error {
	res, err := w.store.GetExecutionBindingResolution(ctx, session.Id)
	if err != nil {
		return err
	}
	switch {
	case res.RevokedAt != nil:
		// The binding was durably revoked: confirmed runtime loss.
		return database.ErrNotFound
	case res.Provider != database.ExecutionProviderE2B || res.ExternalID == "":
		// No live binding and no proven revocation — the session is in an
		// inconsistent state (never bound, or bound to another provider).
		// That is unknown, not loss, and never a Substrate pause.
		return fmt.Errorf("E2B session %s has no live binding and no proven revocation (state %s)", session.Id, res.State)
	default:
		provider, err := w.e2b(res.Installation)
		if err != nil {
			return err
		}
		return provider.Creator.PauseSandbox(ctx, res.ExternalID)
	}
}
