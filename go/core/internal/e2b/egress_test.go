package e2b

import (
	"testing"
)

func TestBuildEgressNetworkConfigDefaultDeny(t *testing.T) {
	got, err := BuildEgressNetworkConfig(EgressInput{
		BrokerAddress:        "198.51.100.1:19001",
		CallbackAddress:      "https://198.51.100.1:18443",
		RevisionDestinations: []string{"https://api.openai.com:443", "http://mcp.internal:8080"},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(got.DenyOut) != 1 || got.DenyOut[0] != DenyAllTraffic {
		t.Fatalf("denyOut = %v, want default-deny", got.DenyOut)
	}
	want := []string{"198.51.100.1", "api.openai.com", "mcp.internal"}
	if len(got.AllowOut) != len(want) {
		t.Fatalf("allowOut = %v, want %v", got.AllowOut, want)
	}
	for i, w := range want {
		if got.AllowOut[i] != w {
			t.Fatalf("allowOut[%d] = %q, want %q (sorted)", i, got.AllowOut[i], w)
		}
	}
}

func TestBuildEgressNetworkConfigRequiresBrokerAndCallback(t *testing.T) {
	if _, err := BuildEgressNetworkConfig(EgressInput{CallbackAddress: "x:1"}); err == nil {
		t.Fatal("expected error for missing broker")
	}
	if _, err := BuildEgressNetworkConfig(EgressInput{BrokerAddress: "x:1"}); err == nil {
		t.Fatal("expected error for missing callback")
	}
}

func TestBuildEgressNetworkConfigDedupesAndStripsPort(t *testing.T) {
	got, err := BuildEgressNetworkConfig(EgressInput{
		BrokerAddress:   "broker.svc:9000",
		CallbackAddress: "broker.svc:18443",
		// same host as broker via a declared destination -> single allow entry
		RevisionDestinations: []string{"broker.svc:9000"},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(got.AllowOut) != 1 || got.AllowOut[0] != "broker.svc" {
		t.Fatalf("allowOut = %v", got.AllowOut)
	}
}

func TestBuildEgressNetworkConfigRejectsBadDestination(t *testing.T) {
	if _, err := BuildEgressNetworkConfig(EgressInput{
		BrokerAddress: "b:1", CallbackAddress: "c:2",
		RevisionDestinations: []string{"://bad"},
	}); err == nil {
		t.Fatal("expected error for malformed destination")
	}
}
