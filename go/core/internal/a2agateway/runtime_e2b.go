package a2agateway

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aext"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

// E2BIngressPort is re-exported for callers that configure the fixed ingress
// port; the constant's source of truth is the e2b package.
const E2BIngressPort = e2b.E2BIngressPort

const e2bSandboxIDHeader = "E2b-Sandbox-Id"
const e2bSandboxPortHeader = "E2b-Sandbox-Port"

// The E2B installation/workload/harness configuration types are owned by the
// e2b package — a leaf that carries no service/session dependency — so both the
// controller (revision preparation) and this gateway (dialing) can consume the
// same operator config without an import cycle. They are aliased here so the
// gateway's existing references keep a single spelling.
type (
	E2BInstallation = e2b.E2BInstallation
	E2BWorkload     = e2b.E2BWorkload
	E2BHarnessSpec  = e2b.E2BHarnessSpec
)

// E2BInstallations delegates to the e2b package's parser/validator.
func E2BInstallations(raw string) (map[string]E2BInstallation, error) {
	return e2b.E2BInstallations(raw)
}

// E2BBindingReader resolves a session's live execution binding. It is satisfied
// by the database store.
type E2BBindingReader interface {
	GetLiveExecutionBinding(ctx context.Context, sessionID string) (*database.ExecutionInstanceBinding, error)
	// GetExecutionBindingResolution classifies a session whose live-binding
	// read returned not-found: its lifecycle state and most recent binding row.
	// It lets the dialer distinguish reaper-confirmed runtime loss (a revoked
	// binding on a live session) from an in-flight CREATE or a deleted session
	// without a second round-trip through unrelated tables.
	GetExecutionBindingResolution(ctx context.Context, sessionID string) (*database.ExecutionBindingResolution, error)
}

// E2BSandboxInspector resolves one external sandbox's liveness in its
// installation. It is satisfied by a per-installation *e2b.Client in
// production and a fake in tests. A nil inspector means the dialer cannot
// distinguish a paused sandbox (which the proxy auto-resumes) from a killed
// one — it dials and lets the proxy surface the failure.
type E2BSandboxInspector interface {
	GetSandbox(ctx context.Context, sandboxID string) (*e2b.Sandbox, error)
}

// E2BInspectorLookup resolves an inspector for one installation. It is a
// function so the dialer is testable without real provider clients; an
// unknown installation returns an error so liveness fails closed.
type E2BInspectorLookup func(installation string) (E2BSandboxInspector, error)

// ErrRuntimeLost is the explicit runtime-loss status the plan requires when a
// bound sandbox is gone: the external compute no longer exists, so the session
// cannot be woken or dialed, but its history stays readable. It is distinct
// from a transient proxy/dial failure so callers do not retry a dead sandbox
// as if it were reachable.
var ErrRuntimeLost = errors.New("e2b runtime compute lost")

// E2BDialer connects public gateway calls for an E2B session to the bound
// sandbox through the installation's private client proxy. It resolves the
// destination only from the durable live binding: the e2b:// authority is a
// consistency check, not routing authority, and no caller-supplied host,
// header or session field selects the sandbox. Routing headers are generated
// here from the binding's external sandbox id and the fixed ingress port.
type E2BDialer struct {
	bindings      E2BBindingReader
	installations map[string]E2BInstallation
	authenticator auth.AuthProvider
	httpClient    *http.Client
	// inspectors optionally resolves a bound sandbox's liveness before dialing
	// so a killed sandbox is reported as ErrRuntimeLost rather than an opaque
	// proxy failure. nil means liveness is not pre-flighted; the proxy's
	// auto-resume still wakes a paused sandbox on ingress.
	inspectors E2BInspectorLookup
	// refresher optionally renews the bound sandbox's runtime credential before
	// dispatch so a paused-or-expired session never forwards work on a dead
	// token. It is the session service's shared refresh; the gateway calls it,
	// never signs or writes a credential itself. nil disables renewal.
	refresher E2BCredentialEnsure
}

// E2BCredentialEnsure is the dispatch-time refresh the gateway calls before
// routing to a bound sandbox. It may wake a paused sandbox and returns the
// live binding to route on; a refresh failure is a transient error that must
// stop forwarding. Satisfied by *sessionsvc.E2BCredentialRefresher.
type E2BCredentialEnsure interface {
	EnsureForDispatch(ctx context.Context, sessionID string) (*database.ExecutionInstanceBinding, error)
}

// NewE2BDialer builds the dialer against the configured installation map. Each
// proxy endpoint must be an http/https URL; an empty or invalid map still
// yields a usable dialer that simply fails E2B sessions closed at Dial time.
func NewE2BDialer(bindings E2BBindingReader, installations map[string]E2BInstallation, authenticator auth.AuthProvider, refresher E2BCredentialEnsure, inspectors ...E2BInspectorLookup) (*E2BDialer, error) {
	if bindings == nil {
		return nil, fmt.Errorf("E2B binding reader is not configured")
	}
	if authenticator == nil {
		return nil, fmt.Errorf("E2B runtime authentication is not configured")
	}
	// The shared client honors https proxy endpoints with system trust; http is
	// accepted for the private-embed deployment the proxy is documented for.
	// Redirects are disabled: a 3xx from a sandbox/proxy must not replay this
	// request (and its routing headers) onto a redirect-supplied host.
	return &E2BDialer{
		bindings:      bindings,
		installations: installations,
		authenticator: authenticator,
		refresher:     refresher,
		inspectors:    firstOrNil(inspectors),
		httpClient: &http.Client{
			Transport:     &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func firstOrNil(v []E2BInspectorLookup) E2BInspectorLookup {
	if len(v) == 0 {
		return nil
	}
	return v[0]
}

// IsE2BAuthority reports whether an a2a_authority selects the E2B provider. It
// delegates to the store's scheme definition so there is one source of truth
// for the e2b://session/ handle the dialer and the writer share.
func IsE2BAuthority(authority string) bool {
	return database.IsE2BSessionAuthority(authority)
}

func (d *E2BDialer) Dial(ctx context.Context, session *apiv1alpha1.Session) (*a2aclient.Client, error) {
	authority := session.GetA2AAuthority()
	if !IsE2BAuthority(authority) {
		return nil, fmt.Errorf("session %s is not an E2B session (authority %q)", session.GetId(), authority)
	}
	// The authority's session id must equal the session being routed; a binding
	// is only ever resolved for session.Id, never for an authority-supplied id.
	if authority != database.E2BSessionAuthority(session.GetId()) {
		return nil, fmt.Errorf("E2B authority %q does not match session %s", authority, session.GetId())
	}
	binding, err := d.bindings.GetLiveExecutionBinding(ctx, session.GetId())
	if err != nil {
		// Missing, revoked or mismatched bindings fail closed with no Substrate
		// fallback: an E2B session never silently routes elsewhere. A not-found
		// miss is classified so reaper-confirmed loss surfaces as ErrRuntimeLost
		// rather than an opaque resolution error.
		if !errors.Is(err, database.ErrNotFound) {
			return nil, fmt.Errorf("resolve E2B session %s binding: %w", session.GetId(), err)
		}
		return nil, d.resolveMissingBinding(ctx, session.GetId(), err)
	}
	if binding.Provider != database.ExecutionProviderE2B {
		return nil, fmt.Errorf("session %s binding is not an E2B binding (provider %q)", session.GetId(), binding.Provider)
	}
	if binding.ExternalID == "" {
		return nil, fmt.Errorf("session %s E2B binding has no external sandbox id", session.GetId())
	}
	inst, ok := d.installations[binding.Installation]
	if !ok {
		return nil, fmt.Errorf("E2B installation %q for session %s is not configured", binding.Installation, session.GetId())
	}
	proxyURL, err := e2b.ParseE2BProxy(inst.ClientProxy)
	if err != nil {
		return nil, fmt.Errorf("E2B installation %q: %w", binding.Installation, err)
	}
	// Pre-flight the bound sandbox's liveness when an inspector is configured:
	// a killed sandbox is reported as ErrRuntimeLost — explicit runtime-loss
	// while history stays readable — instead of an opaque proxy failure. A
	// paused sandbox still resolves (the proxy auto-resumes on ingress), so
	// only a definitive not-found is treated as loss; any other inspector
	// error is a transient lookup failure, not proof of loss.
	if d.inspectors != nil {
		inspector, ierr := d.inspectors(binding.Installation)
		if ierr == nil && inspector != nil {
			if _, gerr := inspector.GetSandbox(ctx, binding.ExternalID); e2b.IsNotFound(gerr) {
				return nil, fmt.Errorf("session %s bound sandbox %s is gone: %w", session.GetId(), binding.ExternalID, ErrRuntimeLost)
			}
		}
	}
	// Renew the bound credential before dispatch so a paused-or-expired session
	// never forwards work on a dead token. The refresher may wake a paused
	// sandbox (the dispatch wake, ahead of proxy auto-resume); its failure is a
	// transient error that stops forwarding rather than dispatching unauthed.
	if d.refresher != nil {
		refreshed, rerr := d.refresher.EnsureForDispatch(ctx, session.GetId())
		if rerr != nil {
			return nil, fmt.Errorf("refresh E2B credential before dispatch: %w", rerr)
		}
		// Route on the revalidated binding — a rebind during refresh must not be
		// masked by the earlier read. If it moved to a different installation,
		// its proxy URL changes too; resolve the target from the new binding's
		// installation rather than the stale one.
		binding = refreshed
		inst, ok = d.installations[binding.Installation]
		if !ok {
			return nil, fmt.Errorf("E2B installation %q for session %s is not configured", binding.Installation, session.GetId())
		}
		proxyURL, err = e2b.ParseE2BProxy(inst.ClientProxy)
		if err != nil {
			return nil, fmt.Errorf("E2B installation %q: %w", binding.Installation, err)
		}
	}
	target := &e2bTarget{
		proxyURL:  proxyURL,
		sandboxID: binding.ExternalID,
	}
	return a2aclient.NewFromEndpoints(ctx, []*a2atype.AgentInterface{{
		URL:             target.proxyURL,
		ProtocolBinding: a2atype.TransportProtocolJSONRPC,
		ProtocolVersion: a2atype.Version,
	}},
		a2aclient.WithJSONRPCTransport(d.httpClient),
		a2aclient.WithCallInterceptors(
			a2aext.NewClientPropagator(nil),
			&e2bRoutingInterceptor{authenticator: d.authenticator, session: session, target: target},
		),
	)
}

// resolveMissingBinding classifies a GetLiveExecutionBinding not-found result.
// The loss reaper revokes a binding on a definitive provider 404 while leaving
// the session live, so a revoked binding on a non-deleted session is the
// durable record of confirmed loss — not an error to retry. It is reported as
// ErrRuntimeLost, but only after the inspector confirms the bound sandbox is
// actually gone: revocation records loss the reaper already proved, and the
// re-check keeps a stale or unverifiable revocation from being read as loss.
// A session with no binding at all (mid-CREATE before bind, or a deleted
// session) resolves to a generic error — incomplete creation and deliberate
// deletion are not runtime loss. A revoked binding can appear during CREATE
// too (the reaper revokes a bound-but-unfinished create whose sandbox dies);
// that still reports loss because the bound compute is confirmed gone.
func (d *E2BDialer) resolveMissingBinding(ctx context.Context, sessionID string, miss error) error {
	res, err := d.bindings.GetExecutionBindingResolution(ctx, sessionID)
	if err != nil {
		// The session row itself is absent (deleted or never existed): the
		// original not-found stands.
		if errors.Is(err, database.ErrNotFound) {
			return fmt.Errorf("resolve E2B session %s binding: %w", sessionID, miss)
		}
		// A failed resolution read is a dependency outage, not a missing
		// binding — surface the new error so the caller can retry rather than
		// acting on the stale not-found.
		return fmt.Errorf("resolve E2B session %s binding state: %w", sessionID, err)
	}
	// A live session whose most recent binding was revoked is the loss
	// reaper's confirmed-loss record: no other writer revokes a binding while
	// the session stays non-deleted (delete revokes inside its tombstone
	// transaction). Confirm the bound sandbox is really gone before surfacing
	// ErrRuntimeLost; a revoked binding with a still-present or unverifiable
	// sandbox is not proof of loss.
	if res.RevokedAt != nil && res.Provider == database.ExecutionProviderE2B &&
		res.Installation != "" && res.ExternalID != "" {
		gone, gerr := d.bindingSandboxGone(ctx, res)
		if gerr != nil {
			// Provider unreachable or lookup failed: transient, not loss and
			// not a missing binding — report it so the caller can retry.
			return fmt.Errorf("verify E2B session %s bound sandbox %s: %w", sessionID, res.ExternalID, gerr)
		}
		if gone {
			return fmt.Errorf("session %s bound sandbox %s is gone: %w", sessionID, res.ExternalID, ErrRuntimeLost)
		}
	}
	return fmt.Errorf("resolve E2B session %s binding: %w", sessionID, miss)
}

// bindingSandboxGone reports whether the bound external sandbox a revoked
// binding recorded is confirmed absent by the provider. A definitive 404 is
// loss; a resolved (present) sandbox is not. Any other inspector or lookup
// error is returned so the caller treats the check as unverifiable rather than
// collapsing it into a not-found or a loss verdict.
func (d *E2BDialer) bindingSandboxGone(ctx context.Context, res *database.ExecutionBindingResolution) (bool, error) {
	if d.inspectors == nil {
		return false, fmt.Errorf("no E2B inspector is configured for installation %q", res.Installation)
	}
	inspector, err := d.inspectors(res.Installation)
	if err != nil {
		return false, fmt.Errorf("resolve inspector for installation %q: %w", res.Installation, err)
	}
	if inspector == nil {
		return false, fmt.Errorf("no E2B inspector for installation %q", res.Installation)
	}
	_, gerr := inspector.GetSandbox(ctx, res.ExternalID)
	if e2b.IsNotFound(gerr) {
		return true, nil
	}
	if gerr != nil {
		return false, gerr
	}
	return false, nil // sandbox present — not gone
}

// e2bTarget is the resolved, server-owned destination for one E2B session.
type e2bTarget struct {
	proxyURL  string
	sandboxID string
}

// e2bRoutingInterceptor attaches caller auth forwarding and the server-owned
// E2b routing headers to every call. Unlike the Substrate interceptor it never
// emits ate-target-actor; the proxy instead reads the sandbox id and fixed
// port set here from the durable binding.
type e2bRoutingInterceptor struct {
	a2aclient.PassthroughInterceptor
	authenticator auth.AuthProvider
	session       *apiv1alpha1.Session
	target        *e2bTarget
}

func (u *e2bRoutingInterceptor) Before(ctx context.Context, req *a2aclient.Request) (context.Context, any, error) {
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, req.BaseURL, nil)
	if err != nil {
		return ctx, nil, err
	}
	if session, ok := auth.AuthSessionFrom(ctx); ok {
		principal := auth.Principal{Agent: auth.Agent{ID: u.session.GetId()}}
		if err := u.authenticator.UpstreamAuth(httpRequest, session, principal); err != nil {
			return ctx, nil, err
		}
	}
	for key, values := range httpRequest.Header {
		for _, value := range values {
			// Routing and Substrate-target headers are never forwarded: the
			// proxy's sandbox selection is owned entirely by this dialer, so a
			// header an authenticator emits cannot redirect or relabel the hop.
			if routingHeader(key) {
				continue
			}
			req.ServiceParams.Append(key, value)
		}
	}
	// Any routing key already present in ServiceParams — set case-insensitively
	// by an earlier interceptor, SDK metadata or a caller — is removed before
	// the authoritative binding-derived values are written, so a forged or
	// stale value cannot survive alongside the generated one.
	for key := range req.ServiceParams {
		if routingHeader(key) {
			delete(req.ServiceParams, key)
		}
	}
	req.ServiceParams[e2bSandboxIDHeader] = []string{u.target.sandboxID}
	req.ServiceParams[e2bSandboxPortHeader] = []string{E2BIngressPort}
	return ctx, nil, nil
}

// routingHeader reports whether key is a runtime-routing header this dialer
// owns and must never accept from caller or authenticator output. The match is
// case-insensitive because HTTP header keys and ServiceParams keys are.
func routingHeader(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case e2bSandboxIDHeader, e2bSandboxPortHeader, "Ate-Target-Actor":
		return true
	}
	return false
}

var _ runtimeDialer = (*E2BDialer)(nil)
