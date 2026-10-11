// Package e2b is the minimal REST client for a self-deployed E2B Embed
// control-plane API. It implements only what the kagent session-create path
// needs: creating a sandbox under an egress policy, deleting it, reading its
// live state, and updating the egress rules on a running sandbox.
//
// The client is intentionally narrow. It is not a port of the e2b SDK: it
// covers exactly the operations the provider lifecycle calls, so the request
// and response shapes stay explicit and auditable. The Embed API is the same
// OpenAPI surface the e2b JS SDK serializes to.
package e2b

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DenyAllTraffic is the denyOut token that blocks every outbound destination.
// When allowOut uses domain names the API requires this token (serializes to
// "0.0.0.0/0") be present or create is rejected.
const DenyAllTraffic = "0.0.0.0/0"

// Config holds connection settings for an E2B installation's control-plane API.
type Config struct {
	// APIURL is the base URL of the installation's API, e.g.
	// http://e2b-api.internal:3000. It is operator configuration, private to
	// control-plane clients.
	APIURL string
	// APIKey authenticates control-plane calls as the X-API-KEY header.
	APIKey string
	// HTTPClient, when nil, uses a default client with CallTimeout.
	HTTPClient  *http.Client
	CallTimeout time.Duration
}

// Client is a thin REST wrapper over the Embed API.
type Client struct {
	cfg Config
	hc  *http.Client
}

// ErrCreateRedirect rejects a redirect on the non-idempotent create path. A
// 307/308 would re-POST the create body to a second host — an automatic retry
// of an operation the plan requires never be retried. The caller reconciles
// the ambiguous outcome; it does not follow.
var ErrCreateRedirect = errors.New("e2b: refusing to follow a redirect on a non-idempotent request")

// New validates the config and returns a client for one installation.
func New(cfg Config) (*Client, error) {
	if cfg.APIURL == "" {
		return nil, fmt.Errorf("e2b: api url is required")
	}
	if _, err := url.Parse(cfg.APIURL); err != nil {
		return nil, fmt.Errorf("e2b: parse api url: %w", err)
	}
	hc := cfg.HTTPClient
	if hc == nil {
		timeout := cfg.CallTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		hc = &http.Client{
			Timeout: timeout,
			// A redirect on a non-idempotent call (create/delete) re-issues the
			// body to a second host — the same blind retry the plan forbids.
			// GET (list/get) is idempotent and may follow; mutating methods
			// stop at the first redirect so the caller reconciles instead.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				switch via[0].Method {
				case http.MethodGet:
					if len(via) >= 10 {
						return errors.New("e2b: too many redirects")
					}
					return nil
				default:
					return ErrCreateRedirect
				}
			},
		}
	}
	return &Client{cfg: cfg, hc: hc}, nil
}

// NetworkConfig is the per-sandbox egress firewall. allowOut lists permitted
// destinations (CIDR, IP, or domain); denyOut lists denied CIDRs/IPs. The
// enforced default-deny baseline is denyOut:[DenyAllTraffic] plus an explicit
// allowOut for the broker, the callback endpoint and any required infra.
type NetworkConfig struct {
	AllowOut []string `json:"allowOut,omitempty"`
	DenyOut  []string `json:"denyOut,omitempty"`
}

// CreateRequest is the NewSandbox body the create call sends.
type CreateRequest struct {
	TemplateID string `json:"templateID"`
	// Timeout bounds the sandbox's lifetime in seconds. With AutoPause the
	// expiry pauses instead of killing — the timeout-to-pause the plan wants so
	// idle compute is preserved rather than lost to a hard kill.
	Timeout  int32             `json:"timeout,omitempty"`
	Network  *NetworkConfig    `json:"network,omitempty"`
	EnvVars  map[string]string `json:"envVars,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Secure   *bool             `json:"secure,omitempty"`
	// AutoPause makes Timeout pause rather than kill on expiry. AutoPauseMemory
	// retains the memory image across that pause so a filesystem-only resume
	// distinction is unnecessary for the pinned pin.
	AutoPause       *bool `json:"autoPause,omitempty"`
	AutoPauseMemory *bool `json:"autoPauseMemory,omitempty"`
	// AutoResume lets the proxy wake a paused sandbox on ingress — the wake
	// path the dialer relies on. The API expects {enabled: bool}.
	AutoResume *AutoResumeConfig `json:"autoResume,omitempty"`
}

// AutoResumeConfig wraps the proxy auto-resume toggle the API expects.
type AutoResumeConfig struct {
	Enabled bool `json:"enabled"`
}

// Sandbox is the create/get response's identifying fields.
type Sandbox struct {
	SandboxID          string `json:"sandboxID"`
	TemplateID         string `json:"templateID"`
	ClientID           string `json:"clientID"`
	Domain             string `json:"domain"`
	EnvdAccessToken    string `json:"envdAccessToken"`
	TrafficAccessToken string `json:"trafficAccessToken"`
	EnvdVersion        string `json:"envdVersion"`
}

// apiError is the Embed error envelope.
type apiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error is a non-2xx API response with its status code preserved so callers can
// distinguish "not found" (orphan/reconcile) from transient failures.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("e2b: http %d: %s", e.Status, e.Message) }

// IsNotFound reports whether the error is a 404, the signal the reconciler uses
// to decide a recorded sandbox is gone.
func IsNotFound(err error) bool {
	var ae *Error
	return errorsAs(err, &ae) && ae.Status == http.StatusNotFound
}

// IsDefinitiveCreateRejection reports whether a CreateSandbox error is a
// definitive non-creation: the API understood and refused the request, so no
// sandbox was placed and a reconcile can never find one. It is an explicit,
// provider-supported rejection list — NOT every 4xx. Auth (401, 403),
// validation (400, 422) and missing-resource (404) failures are definitive;
// a 409 conflict or any 5xx/timeout/connection outcome stays ambiguous and is
// reconciled, never re-created.
func IsDefinitiveCreateRejection(err error) bool {
	var ae *Error
	if !errorsAs(err, &ae) {
		return false
	}
	switch ae.Status {
	case http.StatusBadRequest, // 400 malformed/invalid create
		http.StatusUnauthorized,        // 401 bad/absent credential
		http.StatusForbidden,           // 403 credential not permitted
		http.StatusNotFound,            // 404 template/build not found
		http.StatusUnprocessableEntity: // 422 semantically refused
		return true
	}
	return false
}

func errorsAs(err error, target **Error) bool {
	for err != nil {
		if ae, ok := err.(*Error); ok {
			*target = ae
			return true
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
			continue
		}
		break
	}
	return false
}

// do issues one JSON request and decodes a 2xx response into out.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	_, err := c.doFull(ctx, method, path, body, out)
	return err
}

// doFull issues one JSON request, decodes a 2xx response into out, and returns
// the response headers so paginated endpoints can read the x-next-token
// continuation cursor.
func (c *Client) doFull(ctx context.Context, method, path string, body, out any) (http.Header, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("e2b: marshal %s %s: %w", method, path, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(c.cfg.APIURL, "/")+path, rdr)
	if err != nil {
		return nil, fmt.Errorf("e2b: build %s %s: %w", method, path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("X-API-KEY", c.cfg.APIKey)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("e2b: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := string(data)
		var ae apiError
		if json.Unmarshal(data, &ae) == nil && ae.Message != "" {
			msg = ae.Message
		}
		return nil, &Error{Status: resp.StatusCode, Message: msg}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return nil, fmt.Errorf("e2b: decode %s %s: %w", method, path, err)
		}
	}
	return resp.Header, nil
}

// CreateSandbox creates a sandbox from a template under the supplied egress
// policy and returns its identifying fields. The caller binds the returned
// SandboxID to the session only through the durable create-completion step;
// a create whose response is lost is an unknown outcome, not a retry.
func (c *Client) CreateSandbox(ctx context.Context, req CreateRequest) (*Sandbox, error) {
	var sb Sandbox
	if err := c.do(ctx, http.MethodPost, "/sandboxes", req, &sb); err != nil {
		return nil, err
	}
	return &sb, nil
}

// DeleteSandbox removes a sandbox. A 404 means it is already gone.
func (c *Client) DeleteSandbox(ctx context.Context, sandboxID string) error {
	return c.do(ctx, http.MethodDelete, "/sandboxes/"+sandboxID, nil, nil)
}

// ListedSandbox is the summary record the list endpoint returns for one
// running sandbox. Metadata carries the create-path correlation tags the
// reconciler uses to find a sandbox whose create response was lost.
type ListedSandbox struct {
	SandboxID  string            `json:"sandboxID"`
	TemplateID string            `json:"templateID"`
	State      string            `json:"state"`
	StartedAt  string            `json:"startedAt"`
	EndAt      string            `json:"endAt"`
	Metadata   map[string]string `json:"metadata"`
}

// Sandbox lifecycle states the list endpoint reports. A paused sandbox retains
// its memory image and binding; a renewer must not wake it just to refresh a
// credential — only a state of SandboxStateRunning is a refresh target in a
// background sweep.
const (
	SandboxStateRunning = "running"
	SandboxStatePaused  = "paused"
)

// ResumeLifetimeSeconds is the lifetime a resumed sandbox is given on connect.
// It is also the renewal floor an omitted configured timeout resolves to: an
// installation left at the provider default still keeps an admitted task's
// bound sandbox ahead of its pause bound, never silently under it.
const ResumeLifetimeSeconds int32 = 300

// ListSandboxes returns every running AND paused sandbox matching a metadata
// filter across all pages. The API returns both states by default and pages
// via x-next-token; the reconciler must see the full set, so this follows the
// continuation cursor until it is empty rather than reading only the first
// page. The filter is encoded as the API expects ("k1=v1&k2=v2"); pass nil for
// all sandboxes. A truly empty result distinguishes a definitively absent
// create from a lost-but-created one.
func (c *Client) ListSandboxes(ctx context.Context, metadata map[string]string) ([]ListedSandbox, error) {
	var meta string
	if len(metadata) > 0 {
		q := url.Values{}
		for k, v := range metadata {
			q.Set(k, v)
		}
		meta = q.Encode()
	}
	var all []ListedSandbox
	nextToken := ""
	for {
		q := url.Values{}
		if meta != "" {
			// The API expects the metadata filter as a single query param
			// whose value is a "k1=v1&k2=v2" string (each key/value already
			// encoded once by the producer). Set it as a param value so the
			// outer encoding is applied exactly once.
			q.Set("metadata", meta)
		}
		if nextToken != "" {
			q.Set("nextToken", nextToken)
		}
		path := "/v2/sandboxes"
		if len(q) > 0 {
			path += "?" + q.Encode()
		}
		var page []ListedSandbox
		hdr, err := c.doFull(ctx, http.MethodGet, path, nil, &page)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		nextToken = hdr.Get("x-next-token")
		if nextToken == "" {
			return all, nil
		}
	}
}

// GetSandbox returns the sandbox's current identifying record.
func (c *Client) GetSandbox(ctx context.Context, sandboxID string) (*Sandbox, error) {
	var sb Sandbox
	if err := c.do(ctx, http.MethodGet, "/sandboxes/"+sandboxID, nil, &sb); err != nil {
		return nil, err
	}
	return &sb, nil
}

// BuildStatus is the API's per-build readiness record.
type BuildStatus struct {
	BuildID    string `json:"buildID"`
	TemplateID string `json:"templateID"`
	Status     string `json:"status"`
}

// GetTemplateBuildStatus fetches one build's status directly — not the
// paginated template record — so a build that fell off page one is still
// resolved. It returns the record so the caller can verify the returned
// template and build IDs match the requested artifact before trusting the
// status; a 404 means the build does not exist.
func (c *Client) GetTemplateBuildStatus(ctx context.Context, templateID, buildID string) (*BuildStatus, error) {
	var s BuildStatus
	if err := c.do(ctx, http.MethodGet,
		"/templates/"+url.PathEscape(templateID)+"/builds/"+url.PathEscape(buildID)+"/status", nil, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// UpdateNetwork replaces the egress rules on a running sandbox. Omitting a
// field clears it; the create path sets the policy at creation so this is for
// post-create revision changes.
func (c *Client) UpdateNetwork(ctx context.Context, sandboxID string, net NetworkConfig) error {
	return c.do(ctx, http.MethodPost, "/sandboxes/"+sandboxID+"/network", net, nil)
}

// PauseSandbox pauses a running sandbox. The create path pauses the bound
// sandbox after the bootstrap probe so creation completes READY-and-paused,
// matching the plan's external lifecycle result.
func (c *Client) PauseSandbox(ctx context.Context, sandboxID string) error {
	return c.do(ctx, http.MethodPost, "/sandboxes/"+sandboxID+"/pause", nil, nil)
}

// ResumeSandbox wakes a paused sandbox so it can serve again. It calls the
// provider's connect endpoint — the same call a client makes to reach a
// paused sandbox — which resumes memory-preserved compute and returns its
// identifying record. A 404 means the sandbox is already gone; the caller
// treats that as runtime loss, not a retryable transient.
func (c *Client) ResumeSandbox(ctx context.Context, sandboxID string) (*Sandbox, error) {
	var sb Sandbox
	if err := c.do(ctx, http.MethodPost, "/sandboxes/"+sandboxID+"/connect", map[string]int32{"timeout": ResumeLifetimeSeconds}, &sb); err != nil {
		return nil, err
	}
	return &sb, nil
}

// SetSandboxTimeout renews (or shortens) a running sandbox's lifetime. The
// plan requires renewing the timeout during admitted tasks — including waiting
// native turns that remain physically running — so a task's compute is not
// killed or paused mid-turn. timeoutSeconds is the new lifetime in seconds;
// a 404 means the sandbox is already gone.
func (c *Client) SetSandboxTimeout(ctx context.Context, sandboxID string, timeoutSeconds int32) error {
	return c.do(ctx, http.MethodPost, "/sandboxes/"+sandboxID+"/timeout", map[string]int32{"timeout": timeoutSeconds}, nil)
}

// Snapshot is the persistent image record a sandbox snapshot returns. Its ID
// is usable as a create templateID, producing a new sandbox with the source's
// filesystem but an independent identity — the supported filesystem-only
// snapshot path the plan allows for DATA-equivalent fork.
type Snapshot struct {
	SnapshotID string   `json:"snapshotID"`
	Names      []string `json:"names"`
}

// CreateSnapshot captures a sandbox as a persistent image that survives the
// sandbox's deletion. On Embed this is filesystem state, not live memory —
// verified live: create-from-snapshot yields an independent sandbox with the
// source's filesystem and a fresh identity. A 404 means the sandbox is gone.
func (c *Client) CreateSnapshot(ctx context.Context, sandboxID, name string) (*Snapshot, error) {
	var snap Snapshot
	body := map[string]string{}
	if name != "" {
		body["name"] = name
	}
	if err := c.do(ctx, http.MethodPost, "/sandboxes/"+sandboxID+"/snapshots", body, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// DeleteSnapshot removes a persistent snapshot image. The delete target is
// the templates collection, keyed by the snapshot ID. A 404 means it is
// already gone. snapshotID may carry a "/" or ":" (e.g. "team/name:tag"), so
// it is path-escaped.
func (c *Client) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	return c.do(ctx, http.MethodDelete, "/templates/"+url.PathEscape(snapshotID), nil, nil)
}
