package taskstore

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
)

// Signed runtime credentials for the E2B callback path. Unlike the temporary
// Substrate header, these are issued and verified by the control plane: the
// caller presents a Bearer JWT carrying the session, external compute and
// generation it is bound to. Verification is fixed to HMAC-SHA-256, a fixed
// issuer and audience, an expiry, and the binding claims the durable
// execution_instance_binding row is checked against.
//
// Mixed transport is rejected here: a request carrying both a signed
// credential and the insecure Substrate identity header is never allowed to
// fall back to the legacy authenticator.

const (
	// ExecutionIssuer identifies kagent-issued runtime credentials.
	ExecutionIssuer = "kagent.dev/execution"
	// ExecutionAudience scopes credentials to the private runtime callback.
	ExecutionAudience = "kagent.dev/runtime-taskstore"
	// ExecutionBindingHeader carries the binding's external compute id.
	ExecutionBindingHeader = "x-kagent-execution-external-id"
)

// ExecutionClaims are the verified claims of a signed E2B runtime credential.
type ExecutionClaims struct {
	jwt.RegisteredClaims
	// Installation is the E2B deployment the bound compute belongs to.
	Installation string `json:"ins"`
	// ExternalID is the bound external compute identity (e.g. E2B sandbox id).
	ExternalID string `json:"eid"`
	// Generation is the instance generation the credential was issued under.
	Generation int64 `json:"gen"`
}

// ExecutionAuthenticator verifies signed E2B runtime credentials. It produces
// an executionSession, a distinct identity type from Substrate's
// runtimeSession, so the service can dispatch provider-specific checks.
type ExecutionAuthenticator struct {
	// secret is the HMAC key the control plane signs credentials with.
	secret []byte
}

var _ auth.AuthProvider = (*ExecutionAuthenticator)(nil)

// NewExecutionAuthenticator returns an authenticator that verifies credentials
// signed with secret. secret must be a non-empty high-entropy key.
func NewExecutionAuthenticator(secret []byte) (*ExecutionAuthenticator, error) {
	if len(secret) < 16 {
		return nil, fmt.Errorf("execution credential secret too short")
	}
	return &ExecutionAuthenticator{secret: secret}, nil
}

// SignExecutionCredential issues a credential for a session's bound compute.
// It is used by the credential broker/issuance path, not by the runtime. The
// installation must match the binding row the credential will be checked
// against.
func (a *ExecutionAuthenticator) SignExecutionCredential(sessionID, installation, externalID string, generation int64, expiresAt time.Time) (string, error) {
	claims := ExecutionClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    ExecutionIssuer,
			Audience:  jwt.ClaimStrings{ExecutionAudience},
			Subject:   sessionID,
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		Installation: installation,
		ExternalID:   externalID,
		Generation:   generation,
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(a.secret)
}

// e2bSession is the verified E2B runtime identity. Distinct from
// runtimeSession so service.session can dispatch on provider.
type e2bSession struct {
	sessionID    string
	installation string
	externalID   string
	generation   int64
}

func (s e2bSession) Principal() auth.Principal {
	return auth.Principal{Agent: auth.Agent{ID: s.sessionID}}
}

// Authenticate verifies a signed credential and rejects mixed identity.
func (a *ExecutionAuthenticator) Authenticate(_ context.Context, headers http.Header, _ url.Values) (auth.Session, error) {
	if len(headers.Values(apia2a.InsecureRuntimeIdentityHeader)) != 0 {
		return nil, fmt.Errorf("insecure identity header is not accepted on the signed runtime path")
	}
	authz := headers.Values("Authorization")
	if len(authz) != 1 {
		return nil, fmt.Errorf("one bearer credential is required")
	}
	tokenString, ok := strings.CutPrefix(authz[0], "Bearer ")
	if !ok || strings.TrimSpace(tokenString) == "" {
		return nil, fmt.Errorf("a signed runtime credential is required")
	}
	var claims ExecutionClaims
	token, err := jwt.ParseWithClaims(tokenString, &claims, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return a.secret, nil
	},
		jwt.WithIssuer(ExecutionIssuer),
		jwt.WithAudience(ExecutionAudience),
		jwt.WithExpirationRequired(),
	)
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid runtime credential")
	}
	if claims.Subject == "" || claims.Installation == "" || claims.ExternalID == "" || claims.Generation <= 0 {
		return nil, fmt.Errorf("incomplete runtime credential claims")
	}
	return e2bSession{sessionID: claims.Subject, installation: claims.Installation, externalID: claims.ExternalID, generation: claims.Generation}, nil
}

func (*ExecutionAuthenticator) UpstreamAuth(*http.Request, auth.Session, auth.Principal) error {
	return fmt.Errorf("runtime authentication cannot forward public credentials")
}

// RuntimeAuthenticator binds the trust model for the private runtime callback
// to server configuration. Construct it with NewSignedRuntimeAuthenticator
// for a signed-only E2B endpoint, or NewRuntimeAuthenticator for the legacy
// Substrate-identity endpoint. The choice is fixed per server: a signed-only
// endpoint never consults the insecure Substrate header, so unsigned or mixed
// requests cannot fall back to legacy auth.
type RuntimeAuthenticator struct {
	execution *ExecutionAuthenticator
	substrate *Authenticator
}

var _ auth.AuthProvider = (*RuntimeAuthenticator)(nil)

// NewSignedRuntimeAuthenticator returns a runtime authenticator that accepts
// only signed E2B credentials. The insecure Substrate identity header is
// rejected outright; there is no unsigned fallback on this endpoint.
func NewSignedRuntimeAuthenticator(execution *ExecutionAuthenticator) *RuntimeAuthenticator {
	return &RuntimeAuthenticator{execution: execution}
}

// NewRuntimeAuthenticator returns the runtime authenticator for a Substrate
// deployment: legacy insecure identity only, no signed-credential path.
func NewRuntimeAuthenticator() *RuntimeAuthenticator {
	return &RuntimeAuthenticator{substrate: &Authenticator{}}
}

func (a *RuntimeAuthenticator) Authenticate(ctx context.Context, headers http.Header, query url.Values) (auth.Session, error) {
	// A signed-only endpoint has no Substrate authenticator: every request is
	// verified as a signed credential and the insecure header is never a
	// fallback.
	if a.execution != nil {
		return a.execution.Authenticate(ctx, headers, query)
	}
	return a.substrate.Authenticate(ctx, headers, query)
}

func (*RuntimeAuthenticator) UpstreamAuth(*http.Request, auth.Session, auth.Principal) error {
	return fmt.Errorf("runtime authentication cannot forward public credentials")
}
