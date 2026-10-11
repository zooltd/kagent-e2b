package a2agateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/stretchr/testify/require"
)

// fakeE2BBindings is a static E2BBindingReader for tests.
type fakeE2BBindings struct {
	binding *database.ExecutionInstanceBinding
	err     error
	gotID   string
	// resolution is returned by GetExecutionBindingResolution; resolutionErr
	// overrides it. Nil both means "session not found" (ErrNotFound).
	resolution    *database.ExecutionBindingResolution
	resolutionErr error
}

func (f *fakeE2BBindings) GetLiveExecutionBinding(_ context.Context, sessionID string) (*database.ExecutionInstanceBinding, error) {
	f.gotID = sessionID
	if f.err != nil {
		return nil, f.err
	}
	return f.binding, nil
}

func (f *fakeE2BBindings) GetExecutionBindingResolution(_ context.Context, _ string) (*database.ExecutionBindingResolution, error) {
	if f.resolutionErr != nil {
		return nil, f.resolutionErr
	}
	if f.resolution == nil {
		return nil, database.ErrNotFound
	}
	return f.resolution, nil
}

func e2bSession(id string) *apiv1alpha1.Session {
	return &apiv1alpha1.Session{Id: id, A2AAuthority: database.E2BSessionAuthority(id)}
}

func e2bBinding(installation, externalID string) *database.ExecutionInstanceBinding {
	return &database.ExecutionInstanceBinding{
		Provider:     database.ExecutionProviderE2B,
		Installation: installation,
		ExternalID:   externalID,
		Generation:   1,
	}
}

// capturedRequest records the headers the client proxy received.
type capturedRequest struct {
	mu      sync.Mutex
	headers http.Header
	got     bool
}

func (c *capturedRequest) record(h http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = true
	c.headers = h.Clone()
}

func (c *capturedRequest) get() http.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.headers
}

// e2bProxyServer fakes the E2B client proxy + harness: it captures request
// headers and answers the A2A JSON-RPC GetTask call with a minimal task.
func e2bProxyServer(t *testing.T, cap *capturedRequest) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.record(r.Header)
		w.Header().Set("Content-Type", "application/json")
		// Minimal JSON-RPC result for any method; the client only needs a
		// decodable success envelope for the dial-path assertion.
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":"x","result":{"id":"t1","status":{"state":"TASK_STATE_SUBMITTED"}}}`)
	}))
}

func TestE2BDialerSendsServerOwnedRoutingHeaders(t *testing.T) {
	cap := &capturedRequest{}
	proxy := e2bProxyServer(t, cap)
	defer proxy.Close()

	bindings := &fakeE2BBindings{binding: e2bBinding("embed-lab", "sandbox-abc123")}
	installations, err := E2BInstallations(`{"embed-lab":{"clientProxy":"` + proxy.URL + `"}}`)
	require.NoError(t, err)
	dialer, err := NewE2BDialer(bindings, installations, runtimeTestAuth{}, nil)
	require.NoError(t, err)

	ctx := auth.AuthSessionTo(t.Context(), auth.ControlPlaneSession{})
	client, err := dialer.Dial(ctx, e2bSession("session-1"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Destroy()) })

	_, err = client.GetTask(ctx, &a2atype.GetTaskRequest{ID: "t1"})
	require.NoError(t, err)

	h := cap.get()
	require.NotNil(t, h)
	// Server-owned routing headers come from the binding, not the caller.
	require.Equal(t, "sandbox-abc123", h.Get(e2bSandboxIDHeader))
	require.Equal(t, E2BIngressPort, h.Get(e2bSandboxPortHeader))
	// Caller auth is forwarded; routing headers an authenticator emits are
	// stripped so they can never redirect or relabel the E2B hop.
	require.Equal(t, "Bearer runtime-test", h.Get("Authorization"))
	require.Empty(t, h.Values("ate-target-actor"))
	// The binding lookup used the session's own id, not an authority-supplied id.
	require.Equal(t, "session-1", bindings.gotID)
}

func TestE2BDialerForgedCallerHeadersCannotRedirect(t *testing.T) {
	cap := &capturedRequest{}
	proxy := e2bProxyServer(t, cap)
	defer proxy.Close()

	// An authenticator that injects forged routing headers, as a compromised or
	// malicious caller might attempt, must not override the binding's target.
	// runtimeTestAuth already emits ate-target-actor and an Authorization that
	// still flows; here we confirm only routing state is dropped.
	forged := runtimeTestAuth{}
	bindings := &fakeE2BBindings{binding: e2bBinding("embed-lab", "sandbox-real")}
	installations, err := E2BInstallations(`{"embed-lab":{"clientProxy":"` + proxy.URL + `"}}`)
	require.NoError(t, err)
	dialer, err := NewE2BDialer(bindings, installations, forged, nil)
	require.NoError(t, err)

	ctx := auth.AuthSessionTo(t.Context(), auth.ControlPlaneSession{})
	client, err := dialer.Dial(ctx, e2bSession("session-1"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Destroy()) })
	_, err = client.GetTask(ctx, &a2atype.GetTaskRequest{ID: "t1"})
	require.NoError(t, err)
	got := cap.get()
	require.Equal(t, "sandbox-real", got.Get(e2bSandboxIDHeader))
	require.Empty(t, got.Values("ate-target-actor"))
}

func TestE2BDialerRejectsNonE2BSession(t *testing.T) {
	dialer, err := NewE2BDialer(&fakeE2BBindings{}, nil, runtimeTestAuth{}, nil)
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), &apiv1alpha1.Session{Id: "s", A2AAuthority: "substrate/actor"})
	require.ErrorContains(t, err, "not an E2B session")
}

func TestE2BDialerRejectsAuthorityMismatch(t *testing.T) {
	dialer, err := NewE2BDialer(&fakeE2BBindings{binding: e2bBinding("i", "x")}, nil, runtimeTestAuth{}, nil)
	require.NoError(t, err)
	// authority claims a different session id than the session being routed.
	s := &apiv1alpha1.Session{Id: "real-session", A2AAuthority: database.E2BSessionAuthority("other-session")}
	_, err = dialer.Dial(t.Context(), s)
	require.ErrorContains(t, err, "does not match session")
}

func TestE2BDialerMissingBindingFailsClosed(t *testing.T) {
	dialer, err := NewE2BDialer(&fakeE2BBindings{err: errors.New("no rows")}, nil, runtimeTestAuth{}, nil)
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.ErrorContains(t, err, "resolve E2B session")
}

func TestE2BDialerRevokedBindingFailsClosed(t *testing.T) {
	// A revoked binding returns notFound from the store. When the resolution
	// shows the session is still live and the reaper-revoked binding's sandbox
	// is confirmed gone, Dial must report ErrRuntimeLost — the explicit
	// runtime-loss signal the plan requires — not an opaque resolution error.
	revoked := time.Now()
	resolution := &database.ExecutionBindingResolution{
		State:        "RUNTIME_STATE_READY",
		Provider:     database.ExecutionProviderE2B,
		Installation: "embed-lab",
		ExternalID:   "sandbox-dead",
		RevokedAt:    &revoked,
	}
	inspector := &fakeE2BInspector{err: &e2b.Error{Status: http.StatusNotFound, Message: "gone"}}
	bindings := &fakeE2BBindings{err: database.ErrNotFound, resolution: resolution}
	dialer, err := NewE2BDialer(bindings, nil, runtimeTestAuth{}, nil,
		func(string) (E2BSandboxInspector, error) { return inspector, nil })
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.ErrorIs(t, err, ErrRuntimeLost, "reaper-revoked binding on a live session is confirmed runtime loss")
	require.Equal(t, "sandbox-dead", inspector.got)
}

// A revoked binding whose recorded sandbox still resolves on the provider is
// not confirmed loss — the revocation is stale or raced a provider recovery.
// The dial fails closed but must not report ErrRuntimeLost.
func TestE2BDialerRevokedBindingLiveSandboxNotLoss(t *testing.T) {
	revoked := time.Now()
	resolution := &database.ExecutionBindingResolution{
		State:        "RUNTIME_STATE_READY",
		Provider:     database.ExecutionProviderE2B,
		Installation: "embed-lab",
		ExternalID:   "sandbox-live",
		RevokedAt:    &revoked,
	}
	inspector := &fakeE2BInspector{} // sandbox resolves — present, not gone
	bindings := &fakeE2BBindings{err: database.ErrNotFound, resolution: resolution}
	dialer, err := NewE2BDialer(bindings, nil, runtimeTestAuth{}, nil,
		func(string) (E2BSandboxInspector, error) { return inspector, nil })
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrRuntimeLost, "a live sandbox under a revoked binding is not confirmed loss")
}

// A session mid-CREATE has no live binding yet; the miss is not runtime loss
// even though resolution returns the session's CREATING state with no revoked
// binding row.
func TestE2BDialerMissingBindingCreatingNotLoss(t *testing.T) {
	resolution := &database.ExecutionBindingResolution{State: "RUNTIME_STATE_CREATING"}
	bindings := &fakeE2BBindings{err: database.ErrNotFound, resolution: resolution}
	dialer, err := NewE2BDialer(bindings, nil, runtimeTestAuth{}, nil)
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrRuntimeLost, "an in-flight create is not runtime loss")
}

// A deleted session resolves to not-found on the resolution read too, so the
// miss stays a generic resolution error — deletion is not runtime loss.
func TestE2BDialerMissingBindingDeletedSessionNotLoss(t *testing.T) {
	bindings := &fakeE2BBindings{err: database.ErrNotFound, resolutionErr: database.ErrNotFound}
	dialer, err := NewE2BDialer(bindings, nil, runtimeTestAuth{}, nil)
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrRuntimeLost, "a deleted session is not runtime loss")
}

// A failed resolution read is a dependency outage, not a missing binding. The
// outage must surface as its own error — neither the original not-found nor
// ErrRuntimeLost — so the caller can retry rather than act on stale state.
func TestE2BDialerResolutionOutageIsTransient(t *testing.T) {
	resErr := errors.New("connection refused")
	bindings := &fakeE2BBindings{err: database.ErrNotFound, resolutionErr: resErr}
	dialer, err := NewE2BDialer(bindings, nil, runtimeTestAuth{}, nil)
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.Error(t, err)
	require.ErrorContains(t, err, "binding state", "a resolution outage must be distinguishable from a missing binding")
	require.NotErrorIs(t, err, ErrRuntimeLost)
	require.NotErrorIs(t, err, database.ErrNotFound)
}

// A revoked binding whose bound sandbox cannot be verified (provider timeout,
// 5xx, unknown installation) is unverifiable, not loss and not a missing
// binding. The inspector failure propagates so the caller retries rather than
// silently degrading to a not-found.
func TestE2BDialerRevokedBindingUnverifiableIsTransient(t *testing.T) {
	revoked := time.Now()
	resolution := &database.ExecutionBindingResolution{
		State:        "RUNTIME_STATE_READY",
		Provider:     database.ExecutionProviderE2B,
		Installation: "embed-lab",
		ExternalID:   "sandbox-unverifiable",
		RevokedAt:    &revoked,
	}
	// Inspector returns a 500 — a provider outage, not proof the sandbox is gone.
	inspector := &fakeE2BInspector{err: &e2b.Error{Status: http.StatusInternalServerError, Message: "upstream down"}}
	bindings := &fakeE2BBindings{err: database.ErrNotFound, resolution: resolution}
	dialer, err := NewE2BDialer(bindings, nil, runtimeTestAuth{}, nil,
		func(string) (E2BSandboxInspector, error) { return inspector, nil })
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.Error(t, err)
	require.ErrorContains(t, err, "verify", "an unverifiable bound sandbox must surface the provider error")
	require.NotErrorIs(t, err, ErrRuntimeLost)
	require.NotErrorIs(t, err, database.ErrNotFound)
}

func TestE2BDialerUnknownInstallationFailsClosed(t *testing.T) {
	bindings := &fakeE2BBindings{binding: e2bBinding("unknown-install", "sandbox-1")}
	installations, err := E2BInstallations(`{"other":{"clientProxy":"http://x:1"}}`)
	require.NoError(t, err)
	dialer, err := NewE2BDialer(bindings, installations, runtimeTestAuth{}, nil)
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.ErrorContains(t, err, "not configured")
}

func TestE2BDialerNonE2BBindingProviderRejected(t *testing.T) {
	bindings := &fakeE2BBindings{binding: &database.ExecutionInstanceBinding{
		Provider: database.ExecutionProviderSubstrate, Installation: "i", ExternalID: "x", Generation: 1,
	}}
	dialer, err := NewE2BDialer(bindings, nil, runtimeTestAuth{}, nil)
	require.NoError(t, err)
	_, err = dialer.Dial(t.Context(), e2bSession("s"))
	require.ErrorContains(t, err, "not an E2B binding")
}

func TestProviderDialerRoutesByAuthority(t *testing.T) {
	cap := &capturedRequest{}
	proxy := e2bProxyServer(t, cap)
	defer proxy.Close()

	installations, err := E2BInstallations(`{"embed-lab":{"clientProxy":"` + proxy.URL + `"}}`)
	require.NoError(t, err)
	e2b, err := NewE2BDialer(&fakeE2BBindings{binding: e2bBinding("embed-lab", "sb")}, installations, runtimeTestAuth{}, nil)
	require.NoError(t, err)

	// Substrate dialer pointing at an unreachable router; an E2B session must
	// not touch it.
	sub, err := NewRuntimeDialer("http://127.0.0.1:1", runtimeTestAuth{})
	require.NoError(t, err)
	d := NewProviderDialer(sub, e2b)

	ctx, cancel := context.WithTimeout(auth.AuthSessionTo(t.Context(), auth.ControlPlaneSession{}), 10*time.Second)
	defer cancel()
	client, err := d.Dial(ctx, e2bSession("session-1"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Destroy()) })
	_, err = client.GetTask(ctx, &a2atype.GetTaskRequest{ID: "t1"})
	require.NoError(t, err)
	require.Equal(t, "sb", cap.get().Get(e2bSandboxIDHeader))
}

func TestProviderDialerE2BWithoutConfigFailsClosed(t *testing.T) {
	sub, err := NewRuntimeDialer("http://127.0.0.1:1", runtimeTestAuth{})
	require.NoError(t, err)
	d := NewProviderDialer(sub, nil)
	_, err = d.Dial(t.Context(), e2bSession("s"))
	require.ErrorContains(t, err, "no E2B installation is configured")
}

func TestE2BInstallationsValidation(t *testing.T) {
	_, err := E2BInstallations(`not json`)
	require.Error(t, err)
	_, err = E2BInstallations(`{"x":{"clientProxy":"ftp://bad"}}`)
	require.ErrorContains(t, err, "http or https")
	_, err = E2BInstallations(`{"x":{"clientProxy":"http://h?q=1"}}`)
	require.ErrorContains(t, err, "must not contain")
	m, err := E2BInstallations("")
	require.NoError(t, err)
	require.Nil(t, m)
}

// TestE2BInstallationsHarnessContract asserts a configured harness launch must
// declare its full non-root contract — user and cwd are required alongside the
// command and file paths, so a configured launch cannot silently run as root
// @ the envd default home.
func TestE2BInstallationsHarnessContract(t *testing.T) {
	// One workload selecting image "img" carries the harness contract.
	workload := func(harness string) string {
		return fmt.Sprintf(`{"embed-lab":{"clientProxy":"http://x:1","workloads":[{"image":"img","templateId":"t","buildId":"b","harness":%s}]}}`, harness)
	}
	// missing user/cwd -> rejected
	_, err := E2BInstallations(workload(`{"command":"nohup h &","configPath":"/c","credentialPath":"/cr","activatePath":"/a"}`))
	require.ErrorContains(t, err, "user and cwd")
	// user only -> still rejected
	_, err = E2BInstallations(workload(`{"command":"nohup h &","configPath":"/c","credentialPath":"/cr","activatePath":"/a","user":"kagent"}`))
	require.ErrorContains(t, err, "user and cwd")
	// full contract -> accepted
	m, err := E2BInstallations(workload(`{"command":"nohup h &","configPath":"/c","credentialPath":"/cr","activatePath":"/a","user":"kagent","cwd":"/data/workspace","prepareCommand":"sysctl -w x=80"}`))
	require.NoError(t, err)
	require.Equal(t, "kagent", m["embed-lab"].Workloads[0].Harness.User)
	require.Equal(t, "/data/workspace", m["embed-lab"].Workloads[0].Harness.Cwd)
	require.Equal(t, "sysctl -w x=80", m["embed-lab"].Workloads[0].Harness.PrepareCommand)
}

// TestE2BInstallationsCallbackCABundle asserts the callback CA bundle and its
// guest destination are configured together and validated as PEM: a caPath
// without a bundle (or vice versa) is rejected, and a non-PEM bundle is
// rejected before provisioning rather than surfacing as a callback TLS error.
func TestE2BInstallationsCallbackCABundle(t *testing.T) {
	harness := `{"command":"nohup h &","configPath":"/c","credentialPath":"/cr","activatePath":"/a","user":"kagent","cwd":"/data/workspace"%s}`
	inst := func(extra string, harnessJSON string) string {
		return fmt.Sprintf(`{"embed-lab":{"clientProxy":"http://x:1"%s,"workloads":[{"image":"img","templateId":"t","buildId":"b","harness":%s}]}}`, extra, harnessJSON)
	}
	validCert := "-----BEGIN CERTIFICATE-----\nMIIC1zCCAb+gAwIBAgIBATANBgkqhkiG9w0BAQsFADANMQswCQYDVQQDEwJjYTAe\nFw0yNjEwMDQxNDExMTVaFw0yNjEwMDQxNTExMTVaMA0xCzAJBgNVBAMTAmNhMIIB\nIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnisNXQPQTF3mVXtdCllPd2au\nU7BL1KTUjV9MxuNFEsp4h2PV5ghl9lHTc7xGkWR1DVchVvs6GyLiVYBpy78etScb\n3Sc5Ru5yf/Xf4UNCyXphXywbhzK+qtdFAsJjtXjO+YOBwWVOJCeJ7jMzvMfJqmXo\nre0NXqWAheNWA3pzDxavlCBl9gULbicEAi+69H+UMWmo6jaPh6S+rbqBoUze3UZd\n2EyzAF1jY0NMOHINu8i88Ros/TOqStWFKFr0f8lyXk3xdpeoWXxPrlxdtdOiCwwV\n9Fgqz75H0OJitK77rjIEUoNNQVNaBP8PzQnA8Pu6lDdU0Mihovq7+ka7LiY3DQID\nAQABo0IwQDAOBgNVHQ8BAf8EBAMCAgQwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4E\nFgQUZiOEGWf8MrgnMdjbMBRReswNajEwDQYJKoZIhvcNAQELBQADggEBACPxso+u\nwzNoBFBIwbE/EJBs8xeBadQacgaUTeT9tq1eZkzGxepoHNA6eTF2riVPVNzpTrxV\nnF64H1RGmIS4A8FcOWtQMBwngT1xIlbIvRFqSWPqBDrxcBXATu7Yxjoqn/zfrY7u\nEDi0YKiF5pSny2kLZb1YgShQHXc01VgOR34Akt6y8qaqPrEDSZUZJeJBCzhqVbEk\n8Srl3GpwwkBNT/dRlHHfn+dsVVwqmmgbghREas1jFy0KIB9ywMae+BWVVlTWsqgv\nCeW0RzQ/nKkYd4ME7aV2/ry9PmrBSZxC3qpoLrq22/o1nVrRFyCmxGF4J9CSsd0V\nCbKx8NuF2FAoo6g=\n-----END CERTIFICATE-----\n"
	// caPath set but no bundle -> rejected (nowhere to get content).
	_, err := E2BInstallations(inst("", fmt.Sprintf(harness, `,"caPath":"/run/kagent/ca.pem"`)))
	require.ErrorContains(t, err, "configured together")
	// bundle set but no caPath -> rejected (no destination).
	_, err = E2BInstallations(inst(fmt.Sprintf(`,"callbackCABundle":%q`, validCert), fmt.Sprintf(harness, "")))
	require.ErrorContains(t, err, "configured together")
	// malformed bundle -> rejected before provisioning.
	_, err = E2BInstallations(inst(`,"callbackCABundle":"not-pem"`, fmt.Sprintf(harness, `,"caPath":"/run/kagent/ca.pem"`)))
	require.ErrorContains(t, err, "callbackCABundle")
	// a bundle carrying private-key material -> rejected: non-certificate
	// blocks must not be provisioned into the guest.
	keyPEM := "-----BEGIN PRIVATE KEY-----\nMII=\n-----END PRIVATE KEY-----\n"
	_, err = E2BInstallations(inst(fmt.Sprintf(`,"callbackCABundle":%q`, validCert+keyPEM), fmt.Sprintf(harness, `,"caPath":"/run/kagent/ca.pem"`)))
	require.ErrorContains(t, err, "non-certificate")
	// valid bundle + caPath -> accepted and both fields carried through.
	m, err := E2BInstallations(inst(fmt.Sprintf(`,"callbackCABundle":%q`, validCert), fmt.Sprintf(harness, `,"caPath":"/run/kagent/ca.pem"`)))
	require.NoError(t, err)
	require.Equal(t, validCert, m["embed-lab"].CallbackCABundle)
	require.Equal(t, "/run/kagent/ca.pem", m["embed-lab"].Workloads[0].Harness.CAPath)
	// neither set -> accepted (system trust).
	_, err = E2BInstallations(inst("", fmt.Sprintf(harness, "")))
	require.NoError(t, err)
}

// TestE2BInstallationsCallbackURL asserts the callback endpoint is a strict
// scheme://authority URL: the ADK dials the value verbatim as KAGENT_API_URL,
// so a path, query, fragment or userinfo — anything beyond scheme and
// authority — is rejected at configuration validation rather than dropped or
// breaking the gRPC dial downstream. A bare host:port carries no scheme and is
// rejected the same way; only http(s)://host[:port] is accepted.
func TestE2BInstallationsCallbackURL(t *testing.T) {
	inst := func(callback string) string {
		return fmt.Sprintf(`{"embed-lab":{"clientProxy":"http://x:1","callback":%q}}`, callback)
	}
	// bare host:port -> no scheme -> rejected.
	_, err := E2BInstallations(inst("198.51.100.1:18443"))
	require.ErrorContains(t, err, "http(s)://host[:port]")
	// non-http(s) scheme -> rejected.
	_, err = E2BInstallations(inst("ftp://198.51.100.1:18443"))
	require.ErrorContains(t, err, "http(s)://host[:port]")
	// scheme://authority + path/query/fragment/userinfo -> rejected: the ADK
	// requires scheme-and-authority only.
	for _, bad := range []string{
		"https://198.51.100.1:18443/callback",
		"https://198.51.100.1:18443?x=1",
		"https://198.51.100.1:18443#frag",
		"https://user@198.51.100.1:18443",
	} {
		_, err = E2BInstallations(inst(bad))
		require.ErrorContains(t, err, "scheme://authority only", "callback %q", bad)
	}
	// clean scheme://authority -> accepted for both http and https.
	for _, good := range []string{"http://198.51.100.1:18443", "https://cb.internal:443"} {
		m, err := E2BInstallations(inst(good))
		require.NoError(t, err, "callback %q", good)
		require.Equal(t, good, m["embed-lab"].CallbackAddress)
	}
}

// TestE2BInstallationsBrokerModelURL asserts the broker endpoint is only
// required to be an absolute http(s) URL when a workload's harness declares
// modelURLEnv — the variable the broker address is exported into as the model
// base_url. Without modelURLEnv the broker need only be a valid host[:port]
// (it is an egress allowlist entry, not a dialed URL); with it, a bare
// host:port or non-http scheme is rejected because the model client would be
// handed a value it cannot dial.
func TestE2BInstallationsBrokerModelURL(t *testing.T) {
	inst := func(broker string, modelURLEnv bool) string {
		harness := `{"command":"nohup h &","configPath":"/c","credentialPath":"/cr","activatePath":"/a","user":"kagent","cwd":"/data/workspace"`
		if modelURLEnv {
			harness += `,"modelURLEnv":"ANTHROPIC_BASE_URL"`
		}
		harness += `}`
		return fmt.Sprintf(`{"embed-lab":{"clientProxy":"http://x:1","broker":%q,"workloads":[{"image":"img","templateId":"t","buildId":"b","harness":%s}]}}`, broker, harness)
	}
	// No modelURLEnv: a bare host:port broker is a valid egress entry.
	_, err := E2BInstallations(inst("198.51.100.1:19099", false))
	require.NoError(t, err)
	// modelURLEnv set: bare host:port is rejected — it would be exported as a
	// model base URL the client cannot dial.
	_, err = E2BInstallations(inst("198.51.100.1:19099", true))
	require.ErrorContains(t, err, "modelURLEnv")
	// modelURLEnv set: non-http(s) scheme rejected too.
	_, err = E2BInstallations(inst("ftp://198.51.100.1:19099", true))
	require.ErrorContains(t, err, "modelURLEnv")
	// modelURLEnv set: absolute http(s) URL accepted.
	for _, good := range []string{"http://198.51.100.1:19099", "https://broker.internal:443"} {
		m, err := E2BInstallations(inst(good, true))
		require.NoError(t, err, "broker %q", good)
		require.Equal(t, "ANTHROPIC_BASE_URL", m["embed-lab"].Workloads[0].Harness.ModelURLEnv)
	}
}

func TestE2BDialerDoesNotFollowRedirects(t *testing.T) {
	// A 307 from the proxy/sandbox must not replay the request — carrying the
	// routing headers and caller Authorization — onto a redirect-supplied host.
	redirected := &capturedRequest{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.record(r.Header)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer proxy.Close()

	bindings := &fakeE2BBindings{binding: e2bBinding("embed-lab", "sandbox-abc")}
	installations, err := E2BInstallations(`{"embed-lab":{"clientProxy":"` + proxy.URL + `"}}`)
	require.NoError(t, err)
	dialer, err := NewE2BDialer(bindings, installations, runtimeTestAuth{}, nil)
	require.NoError(t, err)

	ctx := auth.AuthSessionTo(t.Context(), auth.ControlPlaneSession{})
	client, err := dialer.Dial(ctx, e2bSession("session-1"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Destroy()) })
	// The call errors on the unhandled 3xx, but the redirect target is the
	// assertion: it must never receive the request.
	_, _ = client.GetTask(ctx, &a2atype.GetTaskRequest{ID: "t1"})
	require.False(t, redirected.got, "request followed redirect to another host with routing headers")
}

func TestE2BRoutingInterceptorStripsPreexistingRoutingParams(t *testing.T) {
	target := &e2bTarget{proxyURL: "http://proxy", sandboxID: "sandbox-real"}
	u := &e2bRoutingInterceptor{authenticator: runtimeTestAuth{}, session: e2bSession("s"), target: target}
	req := &a2aclient.Request{
		BaseURL: "http://proxy",
		ServiceParams: a2aclient.ServiceParams{
			// Stale/forged routing values under different casings must all be
			// dropped before the authoritative binding values are written.
			"e2b-sandbox-id":   {"forged-lowercase"},
			"E2B-SANDBOX-PORT": {"9999"},
			"ate-target-actor": {"team/other"},
		},
	}
	_, _, err := u.Before(auth.AuthSessionTo(t.Context(), auth.ControlPlaneSession{}), req)
	require.NoError(t, err)
	require.Equal(t, []string{"sandbox-real"}, req.ServiceParams[e2bSandboxIDHeader])
	require.Equal(t, []string{E2BIngressPort}, req.ServiceParams[e2bSandboxPortHeader])
	require.Empty(t, req.ServiceParams["e2b-sandbox-id"])
	require.Empty(t, req.ServiceParams["E2B-SANDBOX-PORT"])
	require.Empty(t, req.ServiceParams["ate-target-actor"])
}

// fakeE2BInspector reports a bound sandbox's liveness for the runtime-loss
// pre-flight.
type fakeE2BInspector struct {
	err error
	got string
}

func (f *fakeE2BInspector) GetSandbox(_ context.Context, id string) (*e2b.Sandbox, error) {
	f.got = id
	if f.err != nil {
		return nil, f.err
	}
	return &e2b.Sandbox{SandboxID: id}, nil
}

// A killed bound sandbox must surface as ErrRuntimeLost — explicit runtime
// loss while history stays readable — not an opaque proxy failure. A paused
// sandbox still resolves (the proxy auto-resumes) and is not loss.
func TestE2BDialerKilledSandboxIsRuntimeLost(t *testing.T) {
	inspector := &fakeE2BInspector{err: &e2b.Error{Status: http.StatusNotFound, Message: "gone"}}
	bindings := &fakeE2BBindings{binding: e2bBinding("embed-lab", "sandbox-dead")}
	installations, err := E2BInstallations(`{"embed-lab":{"clientProxy":"http://127.0.0.1:1"}}`)
	require.NoError(t, err)
	dialer, err := NewE2BDialer(bindings, installations, runtimeTestAuth{}, nil,
		func(string) (E2BSandboxInspector, error) { return inspector, nil })
	require.NoError(t, err)

	_, err = dialer.Dial(t.Context(), e2bSession("session-1"))
	require.ErrorIs(t, err, ErrRuntimeLost, "a killed sandbox must be explicit runtime loss")
	require.Equal(t, "sandbox-dead", inspector.got, "pre-flight checked the bound external id")
}

// With no inspector configured the dialer cannot pre-flight liveness; it dials
// and lets the proxy surface the outcome (auto-resume for paused).
func TestE2BDialerLiveSandboxPreFlightPasses(t *testing.T) {
	cap := &capturedRequest{}
	proxy := e2bProxyServer(t, cap)
	defer proxy.Close()
	inspector := &fakeE2BInspector{} // resolves: sandbox exists (paused counts as live)
	bindings := &fakeE2BBindings{binding: e2bBinding("embed-lab", "sandbox-live")}
	installations, err := E2BInstallations(`{"embed-lab":{"clientProxy":"` + proxy.URL + `"}}`)
	require.NoError(t, err)
	dialer, err := NewE2BDialer(bindings, installations, runtimeTestAuth{}, nil,
		func(string) (E2BSandboxInspector, error) { return inspector, nil })
	require.NoError(t, err)

	ctx := auth.AuthSessionTo(t.Context(), auth.ControlPlaneSession{})
	client, err := dialer.Dial(ctx, e2bSession("session-1"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Destroy()) })
}
