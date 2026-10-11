package session

import (
	"context"
	"fmt"

	"golang.org/x/sync/singleflight"

	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
)

// credentialRefreshStore is the durable-read surface the refresher needs:
// resolve the session's live binding, its prepared revision, and the frozen
// launch contract that carries the credential path. Satisfied by
// *database.Client.
type credentialRefreshStore interface {
	GetLiveExecutionBinding(context.Context, string) (*database.ExecutionInstanceBinding, error)
	GetE2BSessionPreparedRevision(context.Context, string) (string, error)
	GetE2BRevisionBinding(context.Context, string) (*database.ExecutionRevisionBinding, error)
}

// refreshOutcome is the single typed singleflight result so concurrent callers
// of the same binding never receive an incompatible payload. refreshed reports
// whether a credential was actually installed; a skipped (paused, not woken)
// refresh reports false.
type refreshOutcome struct {
	refreshed bool
	binding   database.ExecutionInstanceBinding
}

// E2BCredentialRefresher installs a fresh runtime credential on a session's
// bound sandbox so a long-running task or a paused-then-woken session never
// loses signed TaskStore access while its binding stays valid. The session
// service owns orchestration; per-sandbox signing and the atomic file replace
// stay in the envd bootstrapper, and no database transaction spans the envd
// calls.
//
// Wake permission is explicit and part of the singleflight key, never
// inferred: a background sweep's "paused — skipped" result shares a key with
// no dispatch or resume call, so a non-waking refresh can never satisfy a
// caller that must actually wake and refresh. Dispatch/resume calls may wake a
// paused sandbox to refresh before forwarding.
//
// Refreshes are serialized per (binding identity + wake intent) so an older
// in-flight refresh cannot overwrite a newer one, and the live binding is
// revalidated around the envd call so a revoked or rebound session is never
// refreshed into a credential the binding no longer owns — and a dispatch is
// never authorized against a binding the refresh did not cover.
type E2BCredentialRefresher struct {
	store credentialRefreshStore
	e2b   E2BProviderLookup
	sf    singleflight.Group
}

// NewE2BCredentialRefresher builds the refresher over the session store and
// the same per-installation provider lookup the create path uses.
func NewE2BCredentialRefresher(store credentialRefreshStore, e2b E2BProviderLookup) *E2BCredentialRefresher {
	return &E2BCredentialRefresher{store: store, e2b: e2b}
}

// sfKey serializes refreshes on the full binding identity plus the wake
// intent. The intent is part of the key so a background "skipped paused"
// result is never returned to a dispatch/resume caller that requires a wake.
func sfKey(b *database.ExecutionInstanceBinding, wake bool) string {
	return fmt.Sprintf("%s/%s/%s/%d/wake=%t", b.SessionID, b.Installation, b.ExternalID, b.Generation, wake)
}

// EnsureForDispatch guarantees the session's bound credential is fresh before
// work is forwarded. It may wake a paused sandbox and returns a transient
// error on refresh failure so the caller does not dispatch against an expired
// credential. It returns the live binding the caller may route on, and fails
// if the binding changed under the refresh — the refreshed identity must be
// the one dispatched to.
func (r *E2BCredentialRefresher) EnsureForDispatch(ctx context.Context, sessionID string) (*database.ExecutionInstanceBinding, error) {
	binding, err := r.liveBinding(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	res, err, _ := r.sf.Do(sfKey(binding, true), func() (any, error) {
		return r.refresh(ctx, sessionID, true)
	})
	if err != nil {
		return nil, err
	}
	out := res.(refreshOutcome)
	if !out.refreshed {
		return nil, fmt.Errorf("credential refresh for session %s did not run (paused or absent sandbox)", sessionID)
	}
	// Revalidate before returning dispatch permission: the binding the refresh
	// installed against must still be the live one — a rebind or revoke under
	// the refresh must not be reported as dispatchable on the old sandbox.
	current, err := r.liveBinding(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if !sameBindingIdentity(binding, current) {
		return nil, fmt.Errorf("session %s binding changed during credential refresh: %w", sessionID, database.ErrConflict)
	}
	return current, nil
}

// RefreshRunning refreshes a bound session's credential only when its sandbox
// is currently running; a paused sandbox is left alone and reported as
// refreshed=false rather than woken. Used by the leader-elected background
// renewer.
func (r *E2BCredentialRefresher) RefreshRunning(ctx context.Context, sessionID string) (refreshed bool, err error) {
	binding, err := r.liveBinding(ctx, sessionID)
	if err != nil {
		return false, err
	}
	res, err, _ := r.sf.Do(sfKey(binding, false), func() (any, error) {
		return r.refresh(ctx, sessionID, false)
	})
	if err != nil {
		return false, err
	}
	return res.(refreshOutcome).refreshed, nil
}

// refreshOnSandbox installs a fresh credential on a sandbox record the caller
// already holds — e.g. e2bResume's just-resumed sandbox — so the resume path
// refreshes without a second provider Resume call. The sandbox must match the
// live binding's external id, and the binding is revalidated after.
func (r *E2BCredentialRefresher) refreshOnSandbox(ctx context.Context, sessionID string, sandbox *e2b.Sandbox) error {
	binding, err := r.liveBinding(ctx, sessionID)
	if err != nil {
		return err
	}
	res, err, _ := r.sf.Do(sfKey(binding, true), func() (any, error) {
		return r.refreshOn(ctx, sessionID, binding, sandbox)
	})
	if err != nil {
		return err
	}
	out := res.(refreshOutcome)
	current, err := r.liveBinding(ctx, sessionID)
	if err != nil {
		return err
	}
	if !sameBindingIdentity(binding, current) {
		return fmt.Errorf("session %s binding changed during credential refresh: %w", sessionID, database.ErrConflict)
	}
	_ = out
	return nil
}

// sameBindingIdentity reports whether two bindings are the same instance:
// session, provider, installation, external id, and generation all equal. A
// refresh is only valid for the exact binding it ran against.
func sameBindingIdentity(a, b *database.ExecutionInstanceBinding) bool {
	return a.SessionID == b.SessionID &&
		a.Provider == b.Provider &&
		a.Installation == b.Installation &&
		a.ExternalID == b.ExternalID &&
		a.Generation == b.Generation
}

// refresh is the shared body. It re-reads the live binding inside the
// singleflight so the work runs against current state, resolves the bound
// sandbox (waking only when permitted), and installs a fresh credential via
// the bootstrapper's atomic write.
func (r *E2BCredentialRefresher) refresh(ctx context.Context, sessionID string, wake bool) (refreshOutcome, error) {
	if r.e2b == nil {
		return refreshOutcome{}, fmt.Errorf("no E2B provider configured")
	}
	binding, err := r.liveBinding(ctx, sessionID)
	if err != nil {
		return refreshOutcome{}, err
	}
	provider, err := r.e2b(binding.Installation)
	if err != nil {
		return refreshOutcome{}, fmt.Errorf("resolve E2B installation %q: %w", binding.Installation, err)
	}
	if provider.Bootstrapper == nil {
		return refreshOutcome{}, fmt.Errorf("E2B installation %q has no bootstrapper; cannot refresh credential", binding.Installation)
	}

	// Resolve the bound sandbox and its current envd token. A paused sandbox
	// only resumes when wake is permitted (dispatch); a background sweep skips
	// it. A gone sandbox is a runtime-loss condition, not a refreshable one.
	sandbox, err := r.resolveSandbox(ctx, provider, binding.ExternalID, wake)
	if err != nil {
		return refreshOutcome{}, err
	}
	if sandbox == nil {
		return refreshOutcome{refreshed: false, binding: *binding}, nil // paused and wake not permitted: skip
	}
	if sandbox.SandboxID != binding.ExternalID {
		return refreshOutcome{}, fmt.Errorf("resolved sandbox %s does not match bound %s: %w", sandbox.SandboxID, binding.ExternalID, database.ErrConflict)
	}
	out, err := r.refreshOn(ctx, sessionID, binding, sandbox)
	if err != nil {
		return refreshOutcome{}, err
	}
	return out, nil
}

// refreshOn performs the binding revalidation and atomic install on an
// already-resolved sandbox record. Shared by the sweep/dispatch body and the
// resume path that already holds a just-resumed sandbox.
func (r *E2BCredentialRefresher) refreshOn(ctx context.Context, sessionID string, binding *database.ExecutionInstanceBinding, sandbox *e2b.Sandbox) (refreshOutcome, error) {
	if r.e2b == nil {
		return refreshOutcome{}, fmt.Errorf("no E2B provider configured")
	}
	provider, err := r.e2b(binding.Installation)
	if err != nil {
		return refreshOutcome{}, fmt.Errorf("resolve E2B installation %q: %w", binding.Installation, err)
	}
	revision, err := r.store.GetE2BSessionPreparedRevision(ctx, sessionID)
	if err != nil {
		return refreshOutcome{}, fmt.Errorf("resolve prepared revision for session %s: %w", sessionID, err)
	}
	artifact, err := r.store.GetE2BRevisionBinding(ctx, revision)
	if err != nil {
		return refreshOutcome{}, fmt.Errorf("load E2B revision binding for refresh: %w", err)
	}
	if provider.Bootstrapper == nil {
		return refreshOutcome{}, fmt.Errorf("E2B installation %q has no bootstrapper; cannot refresh credential", binding.Installation)
	}
	// Revalidate the binding is still the live one before issuing to this
	// guest — a revoke or rebind between the first read and now must not mint a
	// credential onto compute the binding no longer owns.
	current, err := r.liveBinding(ctx, sessionID)
	if err != nil {
		return refreshOutcome{}, err
	}
	if !sameBindingIdentity(binding, current) {
		return refreshOutcome{}, fmt.Errorf("session %s binding changed during credential refresh: %w", sessionID, database.ErrConflict)
	}
	bootstrapper, err := provider.Bootstrapper(sandbox, artifact.LaunchContract)
	if err != nil {
		return refreshOutcome{}, fmt.Errorf("build credential refresher for sandbox %s: %w", sandbox.SandboxID, err)
	}
	if err := bootstrapper.RefreshCredential(ctx, binding.Installation, sessionID, binding.ExternalID, binding.Generation); err != nil {
		return refreshOutcome{}, err
	}
	return refreshOutcome{refreshed: true, binding: *current}, nil
}

// resolveSandbox returns the bound sandbox's live record with a usable envd
// token. With wake permitted it resumes a paused sandbox (the dispatch wake)
// and returns the post-resume record; without it a paused or absent sandbox
// returns nil,nil so the background sweep skips it rather than waking it.
func (r *E2BCredentialRefresher) resolveSandbox(ctx context.Context, provider *E2BProvider, externalID string, wake bool) (*e2b.Sandbox, error) {
	creator := provider.Creator
	if !wake {
		// Background path: refresh only a sandbox the provider reports running.
		// GetSandbox carries no state field, so paused/running is read off the
		// metadata-scoped listing; a sandbox not listed as running (paused, or
		// absent) is skipped rather than woken.
		if provider.Lister == nil {
			return nil, fmt.Errorf("E2B installation has no sandbox lister; cannot confirm running state for %s", externalID)
		}
		listed, lerr := provider.Lister.ListSandboxes(ctx, nil)
		if lerr != nil {
			return nil, fmt.Errorf("list sandboxes to confirm %s running: %w", externalID, lerr)
		}
		for _, sb := range listed {
			if sb.SandboxID == externalID && sb.State == e2b.SandboxStateRunning {
				return creator.GetSandbox(ctx, externalID)
			}
		}
		return nil, nil // paused or absent: skip, never wake
	}
	// Dispatch path: ensure the compute is up and return the record carrying
	// the post-resume token. GetSandbox confirms presence; ResumeSandbox is
	// idempotent on a running sandbox and a 404 on either is definitive loss.
	if _, err := creator.GetSandbox(ctx, externalID); err != nil {
		if e2b.IsNotFound(err) {
			return nil, fmt.Errorf("bound sandbox %s is gone: %w", externalID, database.ErrNotFound)
		}
		return nil, fmt.Errorf("resolve bound sandbox %s: %w", externalID, err)
	}
	resumed, rerr := creator.ResumeSandbox(ctx, externalID)
	if rerr != nil {
		if e2b.IsNotFound(rerr) {
			return nil, fmt.Errorf("bound sandbox %s is gone: %w", externalID, database.ErrNotFound)
		}
		return nil, fmt.Errorf("resume bound sandbox %s: %w", externalID, rerr)
	}
	return resumed, nil
}

// liveBinding re-reads the session's live binding, failing closed when it is
// revoked or gone.
func (r *E2BCredentialRefresher) liveBinding(ctx context.Context, sessionID string) (*database.ExecutionInstanceBinding, error) {
	b, err := r.store.GetLiveExecutionBinding(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("resolve live E2B binding for session %s: %w", sessionID, err)
	}
	if b == nil || b.Provider != database.ExecutionProviderE2B || b.ExternalID == "" {
		return nil, fmt.Errorf("session %s has no live E2B binding: %w", sessionID, database.ErrNotFound)
	}
	return b, nil
}
