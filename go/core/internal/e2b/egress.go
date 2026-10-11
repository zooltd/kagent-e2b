package e2b

import (
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
)

// EgressInput is everything the enforced sandbox egress policy is computed
// from. The broker and callback endpoints are control-plane infrastructure the
// plan always allows; RevisionDestinations are the agent's declared model/MCP
// upstreams.
type EgressInput struct {
	// BrokerAddress is the credential broker's host[:port] the sandbox's model
	// base_url resolves to. Required: the plan's baseline keeps model creds out
	// of the guest by routing all model traffic through the broker.
	BrokerAddress string
	// CallbackAddress is the control-plane gRPC/TaskStore endpoint host[:port]
	// the harness calls back on. Required for the runtime to function.
	CallbackAddress string
	// RevisionDestinations are the revision's declared egress origins
	// (scheme://host[:port]). Their host portions are appended to the allowlist;
	// the port is carried in the base_url the broker/harness dials, not here,
	// because the E2B allowlist matches host/CIDR only.
	RevisionDestinations []string
}

// BuildEgressNetworkConfig computes the enforced default-deny sandbox egress
// policy: denyOut is always ALL_TRAFFIC and allowOut is exactly the broker,
// the callback endpoint and the declared revision destinations. The result is
// the network config the create call sends; it is deterministic and sorted so
// two revisions with the same effective policy produce identical configs.
//
// This is a pure function: it performs no I/O and returns an error only when a
// required endpoint is missing or unparseable — preparation must fail rather
// than produce a sandbox with a silently-wider allowlist.
func BuildEgressNetworkConfig(in EgressInput) (*NetworkConfig, error) {
	if strings.TrimSpace(in.BrokerAddress) == "" {
		return nil, fmt.Errorf("e2b egress: broker address is required")
	}
	if strings.TrimSpace(in.CallbackAddress) == "" {
		return nil, fmt.Errorf("e2b egress: callback address is required")
	}
	allow := map[string]struct{}{}
	addHost := func(addr, what string) error {
		host, err := hostOf(addr)
		if err != nil {
			return fmt.Errorf("e2b egress: %s %q: %w", what, addr, err)
		}
		if host == "" {
			return fmt.Errorf("e2b egress: %s %q has no host", what, addr)
		}
		allow[host] = struct{}{}
		return nil
	}
	if err := addHost(in.BrokerAddress, "broker"); err != nil {
		return nil, err
	}
	if err := addHost(in.CallbackAddress, "callback"); err != nil {
		return nil, err
	}
	for _, dest := range in.RevisionDestinations {
		if err := addHost(dest, "destination"); err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(allow))
	for h := range allow {
		out = append(out, h)
	}
	slices.Sort(out)
	return &NetworkConfig{AllowOut: out, DenyOut: []string{DenyAllTraffic}}, nil
}

// hostOf extracts the hostname (no port, no scheme) from an endpoint that may
// be a bare host, host:port, or a scheme://host[:port] URL. IPv6 literals are
// returned without brackets so they match E2B's address entries.
func hostOf(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", nil
	}
	// Try URL form first when a scheme is present.
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil {
			return "", err
		}
		return strings.TrimSuffix(strings.ToLower(u.Hostname()), "."), nil
	}
	// host or host:port
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return strings.TrimSuffix(strings.ToLower(host), "."), nil
	}
	// bare hostname/IP (possibly IPv6 without port)
	return strings.TrimSuffix(strings.ToLower(strings.Trim(addr, "[]")), "."), nil
}
