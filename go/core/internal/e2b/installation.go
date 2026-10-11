package e2b

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// E2BIngressPort is the fixed port the E2B client proxy forwards to inside the
// sandbox, where the harness serves A2A JSON-RPC. It is a deployment constant,
// never caller- or binding-supplied.
const E2BIngressPort = "80"

// E2BInstallation is one configured E2B deployment's client-proxy endpoint,
// keyed by the immutable installation identity stored in execution bindings.
// The endpoint is operator configuration, not durable state; bindings keep the
// identity so sessions stay resolvable if the endpoint value later changes.
type E2BInstallation struct {
	// ClientProxy is the installation's sandbox-traffic proxy base URL, e.g.
	// http://e2b-proxy.internal:3002. It must be private to trusted
	// control-plane clients; the E2b-Sandbox-* routing headers it trusts are
	// not authentication, so public or guest reachability of this endpoint is a
	// network-boundary defect the deployment, not this dialer, must prevent.
	ClientProxy string `json:"clientProxy"`

	// API is the installation's control-plane API base URL the session-create
	// path calls, e.g. http://e2b-api.internal:3000. It is optional at parse
	// time so existing dial-only configs still load; the create path fails a
	// revision whose installation has no API endpoint.
	API string `json:"api,omitempty"`

	// APIKey authenticates the create-path's control-plane calls as the
	// X-API-KEY header. The whole installations map is operator-private
	// config, so the key lives here rather than in durable state; it is
	// optional at parse time and required by create.
	APIKey string `json:"apiKey,omitempty"`

	// BrokerAddress is the model/MCP credential broker's host[:port] that
	// sandboxes' model base_url resolves to and that the egress allowlist
	// permits. The broker holds the real credentials outside the guest and
	// injects them server-side; it must be reachable from the sandbox on a
	// deployment-private address. Optional at parse time; the create path
	// requires it.
	BrokerAddress string `json:"broker,omitempty"`

	// CallbackAddress is the control-plane TaskStore/gRPC endpoint the harness
	// calls back on. It must be a full http(s)://host[:port] URL — the value is
	// exported to the guest verbatim as KAGENT_API_URL, and its host is added to
	// the egress allowlist so the enforced default-deny policy does not block
	// callbacks. A bare host:port is rejected: the scheme decides the harness's
	// transport, so it must be configured, never guessed.
	CallbackAddress string `json:"callback,omitempty"`

	// CallbackCABundle is the PEM-encoded CA bundle the harness trusts for the
	// private callback endpoint's TLS. The operator supplies the CA that signs
	// CallbackAddress's serving certificate; trust is never derived from the
	// controller's serving cert, and no TLS private key or verification-disable
	// is ever configured here. It is written to the guest at a workload's
	// Harness.CAPath before activation, so both must be configured together.
	// With neither set the harness relies on system trust. Optional at parse
	// time; required whenever a Harness.CAPath is set, and validated as PEM.
	CallbackCABundle string `json:"callbackCABundle,omitempty"`

	// Workloads maps a compiled Harness workload — pinned image plus ordered
	// command and args — to the concrete E2B artifact that runs it. Each entry
	// is an exact selector; a compiled revision whose image/command/args does
	// not match exactly one entry fails preparation. The mapping is operator
	// config: the control plane does not invent which build runs an image.
	Workloads []E2BWorkload `json:"workloads,omitempty"`
}

// E2BWorkload binds one pinned runtime workload to its prepared E2B artifact
// and the guest launch contract that artifact satisfies. Image is the
// digest-pinned OCI reference the Harness workload declares; Command and Args
// are its optional overrides — nil, null and empty arrays all normalize to
// "no override" so a managed harness with no command/args matches on image
// alone. TemplateID and BuildID identify the prepared artifact the E2B
// control plane instantiates; Harness is the complete launch contract for
// that artifact (no merging with any installation-level contract).
type E2BWorkload struct {
	// Image is the pinned workload image reference (name@sha256:digest) the
	// compiled revision carries. Matching is exact string equality.
	Image string `json:"image"`
	// Command and Args must exactly equal the Harness workload's overrides in
	// order; absent/null/empty on both sides are equivalent.
	Command []string `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`
	// TemplateID is the external template the create path instantiates.
	TemplateID string `json:"templateId"`
	// BuildID is the concrete template build the revision binds to.
	BuildID string `json:"buildId"`
	// Harness is the guest launch contract this artifact satisfies — the
	// detached supervisor launch and the file contract paths. It is complete
	// per-workload: no field is inherited from the installation.
	Harness *E2BHarnessSpec `json:"harness,omitempty"`
}

// E2BHarnessSpec is the guest-side launch contract a machine template
// satisfies. All paths live under the contract root the supervisor reads.
type E2BHarnessSpec struct {
	// Command is the detached launch line run as `/bin/bash -lc <Command>` —
	// e.g. `nohup /usr/local/bin/kagent-codex >>/tmp/harness.log 2>&1 &`. It
	// must return (detach) so the bootstrap's Start call completes.
	Command string `json:"command"`
	// ConfigPath is where the compiled harness config JSON is written.
	ConfigPath string `json:"configPath"`
	// AgentCardPath is where the per-revision Agent Card JSON is written;
	// the launch command exports it into KAGENT_AGENT_CARD_JSON. Optional;
	// an empty path means no card file is written and the harness must read
	// the card some other way.
	AgentCardPath string `json:"agentCardPath,omitempty"`
	// CredentialPath is where the fresh instance credential is written.
	CredentialPath string `json:"credentialPath"`
	// CAPath is where the callback CA bundle is written; optional.
	CAPath string `json:"caPath,omitempty"`
	// ActivatePath is the commit-point marker written last; its presence is
	// what authorizes the supervisor to launch on a filesystem-only resume.
	ActivatePath string `json:"activatePath"`
	// User is the guest account the harness runs as (envd's username param).
	// Bundled harnesses run non-root — e.g. the kagent user (uid 65532) the
	// template creates — so the production launch is not the envd default
	// user (root). The instance credential is also written as this user, so
	// it is not readable by other guest accounts.
	User string `json:"user,omitempty"`
	// Cwd is the working directory the harness lands in (processConfig.cwd),
	// matching the upstream image's /data/workspace rather than the envd
	// default user's home.
	Cwd string `json:"cwd,omitempty"`
	// PrepareCommand is the privileged guest-side step run as the envd
	// default user (root) before the file contract is written — e.g.
	// `sysctl -w net.ipv4.ip_unprivileged_port_start=80` so a non-root
	// harness may bind the ingress port. It is a trusted installation
	// setting, not agent input, and must exit 0 for activation to proceed.
	PrepareCommand string `json:"prepareCommand,omitempty"`

	// Env names the harness-specific process variables this launch contract
	// owns and exports into the guest at start — wiring the harness reads but
	// a revision may not control. These are contract-owned: a same-named
	// revision environment entry never overrides them.
	Env map[string]string `json:"env,omitempty"`

	// ModelURLEnv names the environment variable the harness reads its model
	// base-url from (e.g. ANTHROPIC_BASE_URL for Claude). When set it is
	// exported with the installation's BrokerAddress so the guest's model
	// traffic reaches the credential broker, never a revision-supplied
	// endpoint. Contract-owned, like Env.
	ModelURLEnv string `json:"modelURLEnv,omitempty"`
}

// E2BInstallations parses the operator-supplied installation map: a JSON object
// of installation ID to endpoint config. Unknown IDs are ignored at parse time;
// lookup fails closed for any installation a binding references but config does
// not define.
func E2BInstallations(raw string) (map[string]E2BInstallation, error) {
	if raw == "" {
		return nil, nil
	}
	var parsed map[string]E2BInstallation
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("parse E2B installations: %w", err)
	}
	for id, inst := range parsed {
		if id == "" {
			return nil, fmt.Errorf("E2B installation id must not be empty")
		}
		if _, err := ParseE2BProxy(inst.ClientProxy); err != nil {
			return nil, fmt.Errorf("E2B installation %q: %w", id, err)
		}
		// Optional create-path endpoints are validated only when present; an
		// installation configured for dialing but not creation is valid config.
		if inst.API != "" {
			if _, err := ParseE2BProxy(inst.API); err != nil {
				return nil, fmt.Errorf("E2B installation %q api: %w", id, err)
			}
		}
		if inst.BrokerAddress != "" {
			if _, err := E2BHostPort(inst.BrokerAddress); err != nil {
				return nil, fmt.Errorf("E2B installation %q broker: %w", id, err)
			}
		}
		if inst.CallbackAddress != "" {
			u, err := url.Parse(inst.CallbackAddress)
			if err != nil || u.Scheme == "" || u.Host == "" ||
				(u.Scheme != "http" && u.Scheme != "https") {
				return nil, fmt.Errorf("E2B installation %q callback must be an http(s)://host[:port] URL", id)
			}
			// The ADK dials the value as scheme://authority only; a path, query
			// or fragment is silently dropped or rejected downstream, so the
			// configured callback must carry none.
			if u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
				return nil, fmt.Errorf("E2B installation %q callback must be scheme://authority only (no path, query, fragment or userinfo)", id)
			}
		}
		// Each workload selector must be unambiguous: image required, template
		// and build identified, and no two entries may select the same
		// image+command+args (which would make artifact resolution ambiguous).
		seen := map[string]bool{}
		anyCAPath := false
		for wi, w := range inst.Workloads {
			if w.Image == "" || w.TemplateID == "" || w.BuildID == "" {
				return nil, fmt.Errorf("E2B installation %q workloads[%d]: image, templateId and buildId are required", id, wi)
			}
			// nil/null/empty command and args all mean "no override" — normalize
			// to empty slices before encoding so equivalent entries dedupe, then
			// encode as a JSON tuple so a value ending in one field cannot
			// collide with a split across the boundary (command=[a,b],args=[c]
			// vs command=[a],args=[b,c]).
			keyBytes, _ := json.Marshal([3]any{w.Image, emptySlice(w.Command), emptySlice(w.Args)})
			key := string(keyBytes)
			if seen[key] {
				return nil, fmt.Errorf("E2B installation %q workloads[%d]: duplicate selector for image %q", id, wi, w.Image)
			}
			seen[key] = true
			h := w.Harness
			if h == nil {
				return nil, fmt.Errorf("E2B installation %q workloads[%d]: harness launch contract is required", id, wi)
			}
			if h.Command == "" || h.ConfigPath == "" || h.CredentialPath == "" || h.ActivatePath == "" {
				return nil, fmt.Errorf("E2B installation %q workloads[%d] harness: command, configPath, credentialPath and activatePath are required", id, wi)
			}
			// A configured launch must declare its non-root runtime contract:
			// omitting user or cwd would silently launch as the envd default
			// (root @ its home), which is never acceptable for a bundled
			// harness. The contract must be explicit, not defaulted.
			if h.User == "" || h.Cwd == "" {
				return nil, fmt.Errorf("E2B installation %q workloads[%d] harness: user and cwd are required for a configured launch", id, wi)
			}
			if h.CAPath != "" {
				anyCAPath = true
			}
			// When the contract declares the harness's model base-url variable,
			// the broker value it exports must be an absolute http(s) URL — a
			// bare host:port would be handed to the model client verbatim and
			// break the request, not merely fail the allowlist.
			if h.ModelURLEnv != "" {
				u, err := url.Parse(inst.BrokerAddress)
				if inst.BrokerAddress == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
					return nil, fmt.Errorf("E2B installation %q workloads[%d]: modelURLEnv requires the installation broker to be an http(s)://host[:port] URL", id, wi)
				}
			}
		}
		// The callback CA bundle and its guest destination travel together: a
		// caPath without bundle content has nothing to write, and a bundle with
		// no caPath has nowhere to land. With neither set the harness uses
		// system trust.
		if (inst.CallbackCABundle == "") == anyCAPath {
			return nil, fmt.Errorf("E2B installation %q: callbackCABundle and a workload harness.caPath must be configured together", id)
		}
		if inst.CallbackCABundle != "" {
			if err := validatePEMCertificates(inst.CallbackCABundle); err != nil {
				return nil, fmt.Errorf("E2B installation %q callbackCABundle: %w", id, err)
			}
		}
	}
	return parsed, nil
}

// ResolveWorkload returns the single workload entry matching a compiled
// revision's pinned image and ordered command/args. Matching is exact: image
// string equality plus elementwise command and args equality with nil, null
// and empty all equivalent. Zero or several matches fail preparation — the
// operator config must select exactly one artifact.
func (inst E2BInstallation) ResolveWorkload(image string, command, args []string) (*E2BWorkload, error) {
	var match *E2BWorkload
	for i := range inst.Workloads {
		w := &inst.Workloads[i]
		if w.Image != image || !stringSlicesEqual(w.Command, command) || !stringSlicesEqual(w.Args, args) {
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("E2B workload selector for image %q matches multiple entries", image)
		}
		match = w
	}
	if match == nil {
		return nil, fmt.Errorf("no E2B workload mapping matches image %q (command=%v args=%v)", image, command, args)
	}
	return match, nil
}

// stringSlicesEqual treats nil, null and empty slices as equal (no override).
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// emptySlice normalizes nil/null to an empty slice so "no override" encodes
// identically regardless of how the JSON supplied it.
func emptySlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// validatePEMCertificates rejects a callback CA bundle that decodes to no
// CERTIFICATE block — a malformed bundle would write a useless file the
// harness then fails TLS verification against, surfacing as an opaque
// callback error rather than a configuration error.
func validatePEMCertificates(bundle string) error {
	rest := []byte(bundle)
	found := false
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		// The bundle is copied verbatim into the guest, so any non-certificate
		// block — above all private-key material — must be rejected, not
		// skipped: a combined cert+key PEM would otherwise provision a private
		// key into the sandbox.
		if block.Type != "CERTIFICATE" {
			return fmt.Errorf("unexpected non-certificate PEM block %q", block.Type)
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return fmt.Errorf("invalid certificate in bundle: %w", err)
		}
		found = true
	}
	if !found {
		return errors.New("no PEM CERTIFICATE block")
	}
	return nil
}

// E2BHostPort validates a host[:port] or scheme://host[:port] endpoint used by
// the egress allowlist. It returns the bare hostname the E2B allowlist matches.
func E2BHostPort(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", fmt.Errorf("endpoint must not be empty")
	}
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil {
			return "", fmt.Errorf("parse endpoint %q: %w", addr, err)
		}
		if u.Hostname() == "" {
			return "", fmt.Errorf("endpoint %q has no host", addr)
		}
		return strings.TrimSuffix(strings.ToLower(u.Hostname()), "."), nil
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return strings.TrimSuffix(strings.ToLower(host), "."), nil
	}
	if strings.Trim(addr, "[]") == "" {
		return "", fmt.Errorf("endpoint %q has no host", addr)
	}
	return strings.TrimSuffix(strings.ToLower(strings.Trim(addr, "[]")), "."), nil
}

// ParseE2BProxy validates a client-proxy base URL. Only http/https are allowed;
// https uses TLS with system trust. The URL must have no userinfo, query or
// fragment; its path (if any) is preserved as a path prefix for the JSON-RPC
// endpoint.
func ParseE2BProxy(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse client proxy URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("client proxy URL %q must use http or https", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("client proxy URL %q must include a host", raw)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("client proxy URL %q must not contain userinfo, query or fragment", raw)
	}
	return u.String(), nil
}
