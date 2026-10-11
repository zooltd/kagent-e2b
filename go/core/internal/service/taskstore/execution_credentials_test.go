package taskstore

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/stretchr/testify/require"
)

const testSecret = "0123456789abcdef0123456789abcdef"

func execAuth(t *testing.T) *ExecutionAuthenticator {
	a, err := NewExecutionAuthenticator([]byte(testSecret))
	require.NoError(t, err)
	return a
}

func bearer(token string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+token)
	return h
}

func TestExecutionAuthenticatorRejects(t *testing.T) {
	a := execAuth(t)
	sessionID := uuid.NewString()
	good, err := a.SignExecutionCredential(sessionID, "embed-lab", "sandbox-1", 1, time.Now().Add(time.Hour))
	require.NoError(t, err)

	for _, test := range []struct {
		name    string
		headers http.Header
	}{
		{"missing credential", http.Header{}},
		{"insecure header only", func() http.Header {
			h := http.Header{}
			h.Set(apia2a.InsecureRuntimeIdentityHeader, "team-a/session-"+sessionID+"/uid")
			return h
		}()},
		{"mixed signed + insecure", func() http.Header {
			h := bearer(good)
			h.Set(apia2a.InsecureRuntimeIdentityHeader, "team-a/session-"+sessionID+"/uid")
			return h
		}()},
		{"not bearer", http.Header{"Authorization": []string{good}}},
		{"empty bearer", http.Header{"Authorization": []string{"Bearer "}}},
		{"forged signature", bearer(good[:len(good)-2] + "xx")},
		{"unsigned alg none", bearer(func() string {
			tok, _ := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{"iss": ExecutionIssuer, "aud": ExecutionAudience, "sub": sessionID, "ins": "embed-lab", "eid": "sandbox-1", "gen": 1, "exp": time.Now().Add(time.Hour).Unix()}).SignedString(jwt.UnsafeAllowNoneSignatureType)
			return tok
		}())},
		{"expired", bearer(func() string {
			tok, _ := a.SignExecutionCredential(sessionID, "embed-lab", "sandbox-1", 1, time.Now().Add(-time.Hour))
			return tok
		}())},
		{"wrong issuer", bearer(func() string {
			tok, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, ExecutionClaims{RegisteredClaims: jwt.RegisteredClaims{Issuer: "evil", Audience: jwt.ClaimStrings{ExecutionAudience}, Subject: sessionID, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}, Installation: "embed-lab", ExternalID: "sandbox-1", Generation: 1}).SignedString([]byte(testSecret))
			return tok
		}())},
		{"wrong audience", bearer(func() string {
			tok, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, ExecutionClaims{RegisteredClaims: jwt.RegisteredClaims{Issuer: ExecutionIssuer, Audience: jwt.ClaimStrings{"other"}, Subject: sessionID, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}, Installation: "embed-lab", ExternalID: "sandbox-1", Generation: 1}).SignedString([]byte(testSecret))
			return tok
		}())},
		{"missing generation", bearer(func() string {
			tok, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, ExecutionClaims{RegisteredClaims: jwt.RegisteredClaims{Issuer: ExecutionIssuer, Audience: jwt.ClaimStrings{ExecutionAudience}, Subject: sessionID, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}, Installation: "embed-lab", ExternalID: "sandbox-1"}).SignedString([]byte(testSecret))
			return tok
		}())},
		{"missing installation", bearer(func() string {
			tok, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, ExecutionClaims{RegisteredClaims: jwt.RegisteredClaims{Issuer: ExecutionIssuer, Audience: jwt.ClaimStrings{ExecutionAudience}, Subject: sessionID, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}, ExternalID: "sandbox-1", Generation: 1}).SignedString([]byte(testSecret))
			return tok
		}())},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := a.Authenticate(context.Background(), test.headers, url.Values{})
			require.Error(t, err)
		})
	}
}

func TestExecutionAuthenticatorAccepts(t *testing.T) {
	a := execAuth(t)
	sessionID := uuid.NewString()
	token, err := a.SignExecutionCredential(sessionID, "embed-lab", "sandbox-1", 3, time.Now().Add(time.Hour))
	require.NoError(t, err)
	session, err := a.Authenticate(context.Background(), bearer(token), url.Values{})
	require.NoError(t, err)
	require.Equal(t, e2bSession{sessionID: sessionID, installation: "embed-lab", externalID: "sandbox-1", generation: 3}, session)
}

// fakeStore implements Store for dispatch tests.
type fakeStore struct {
	binding *database.ExecutionInstanceBinding
	session *apiv1alpha1.Session
	bindErr error
	sessErr error
}

func (f *fakeStore) SettleSessionTask(context.Context, string, string, int64, *database.ExecutionInstanceBinding) error {
	return nil
}
func (f *fakeStore) GetSessionForRuntime(context.Context, string, string) (*apiv1alpha1.Session, error) {
	return f.session, f.sessErr
}
func (f *fakeStore) GetSessionForExecution(context.Context, string) (*apiv1alpha1.Session, error) {
	return f.session, f.sessErr
}
func (f *fakeStore) GetLiveExecutionBinding(context.Context, string) (*database.ExecutionInstanceBinding, error) {
	return f.binding, f.bindErr
}
func (f *fakeStore) CreateRuntimeTask(context.Context, string, []byte, *a2a.Task, string, *database.ExecutionInstanceBinding) (int64, error) {
	return 0, nil
}
func (f *fakeStore) GetVersionedSessionTask(context.Context, string, string) (*a2a.Task, int64, error) {
	return nil, 0, nil
}
func (f *fakeStore) UpdateSessionTask(context.Context, string, int64, []byte, *a2a.Task, a2a.Event, string, *database.ExecutionInstanceBinding) (int64, error) {
	return 0, nil
}
func (f *fakeStore) ListSessionTasks(context.Context, string, string, a2a.TaskState, *time.Time, int, *int) ([]*a2a.Task, int, error) {
	return nil, 0, nil
}

func sessCtx(s auth.Session) context.Context {
	return auth.AuthSessionTo(context.Background(), s)
}

// The trust model is fixed at server configuration. A signed-only endpoint
// rejects insecure and mixed requests outright; a Substrate endpoint accepts
// the legacy identity header and never consults a bearer credential.
func TestConfiguredRuntimeAuthenticator(t *testing.T) {
	sessionID := uuid.NewString()
	exec := execAuth(t)
	good, err := exec.SignExecutionCredential(sessionID, "embed-lab", "sandbox-1", 1, time.Now().Add(time.Hour))
	require.NoError(t, err)

	signed := NewSignedRuntimeAuthenticator(exec)
	substrate := NewRuntimeAuthenticator()

	insecure := http.Header{}
	insecure.Set(apia2a.InsecureRuntimeIdentityHeader, "team-a/session-"+sessionID+"/uid")

	// Signed-only endpoint: a valid credential authenticates; unsigned and
	// mixed requests are rejected — the insecure header is never a fallback.
	_, err = signed.Authenticate(context.Background(), bearer(good), url.Values{})
	require.NoError(t, err)
	_, err = signed.Authenticate(context.Background(), insecure, url.Values{})
	require.Error(t, err)
	mixed := bearer(good)
	mixed.Set(apia2a.InsecureRuntimeIdentityHeader, "team-a/session-"+sessionID+"/uid")
	_, err = signed.Authenticate(context.Background(), mixed, url.Values{})
	require.Error(t, err)

	// Substrate endpoint: the legacy identity header authenticates; a bare
	// bearer credential is meaningless and rejected.
	s, err := substrate.Authenticate(context.Background(), insecure, url.Values{})
	require.NoError(t, err)
	require.Equal(t, runtimeSession{sessionID: sessionID, atespace: "team-a", actorUID: "uid"}, s)
	_, err = substrate.Authenticate(context.Background(), bearer(good), url.Values{})
	require.Error(t, err)
}

func TestE2BSessionDispatch(t *testing.T) {
	sessionID := uuid.NewString()
	binding := &database.ExecutionInstanceBinding{
		Provider: database.ExecutionProviderE2B, Installation: "embed-lab",
		ExternalID: "sandbox-1", Generation: 2,
	}
	for _, test := range []struct {
		name    string
		ident   e2bSession
		binding *database.ExecutionInstanceBinding
		bindErr error
		wantErr bool
	}{
		{"match", e2bSession{sessionID, "embed-lab", "sandbox-1", 2}, binding, nil, false},
		{"wrong session", e2bSession{uuid.NewString(), "embed-lab", "sandbox-1", 2}, binding, nil, true},
		{"wrong installation", e2bSession{sessionID, "other-lab", "sandbox-1", 2}, binding, nil, true},
		{"wrong external id", e2bSession{sessionID, "embed-lab", "sandbox-2", 2}, binding, nil, true},
		{"stale generation", e2bSession{sessionID, "embed-lab", "sandbox-1", 1}, binding, nil, true},
		{"no binding", e2bSession{sessionID, "embed-lab", "sandbox-1", 2}, nil, database.ErrNotFound, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			svc := NewService(&fakeStore{binding: test.binding, session: &apiv1alpha1.Session{Id: sessionID}, bindErr: test.bindErr})
			_, _, err := svc.session(sessCtx(test.ident), sessionID)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
