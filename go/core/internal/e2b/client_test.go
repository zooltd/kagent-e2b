package e2b

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateSandboxSendsEgressPolicyAndAPIKey(t *testing.T) {
	var gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sandboxes" || r.Method != http.MethodPost {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("X-API-KEY")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sandboxID": "sb-1", "templateID": "tpl", "clientID": "c1",
			"envdVersion": "0.1", "envdAccessToken": "envd", "trafficAccessToken": "traffic",
		})
	}))
	defer srv.Close()

	c, err := New(Config{APIURL: srv.URL, APIKey: "k-secret"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sb, err := c.CreateSandbox(context.Background(), CreateRequest{
		TemplateID: "tpl", Timeout: 300,
		Network: &NetworkConfig{AllowOut: []string{"198.51.100.1"}, DenyOut: []string{DenyAllTraffic}},
		EnvVars: map[string]string{"A": "1"}, Metadata: map[string]string{"op": "x"},
	})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if gotAuth != "k-secret" {
		t.Fatalf("api key header = %q", gotAuth)
	}
	net := gotBody["network"].(map[string]any)
	if net["denyOut"].([]any)[0] != DenyAllTraffic {
		t.Fatalf("denyOut = %v", net["denyOut"])
	}
	if sb.SandboxID != "sb-1" || sb.EnvdAccessToken != "envd" {
		t.Fatalf("sandbox = %+v", sb)
	}
}

func TestErrorStatusPreservedForNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 404, "message": "sandbox not found"})
	}))
	defer srv.Close()
	c, _ := New(Config{APIURL: srv.URL})
	err := c.DeleteSandbox(context.Background(), "gone")
	if !IsNotFound(err) {
		t.Fatalf("expected not-found, got %v", err)
	}
	var ae *Error
	if !errorsAs(err, &ae) || ae.Status != 404 {
		t.Fatalf("status not preserved: %v", err)
	}
}

func TestNewRequiresAPIURL(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty api url")
	}
}

// ListSandboxes must follow the x-next-token continuation cursor across all
// pages — a lost create reconciles only if every page is read, and a paused
// sandbox beyond page 1 is still a candidate.
func TestListSandboxesFollowsPaginationCursor(t *testing.T) {
	var pages []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/sandboxes" || r.Method != http.MethodGet {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		token := r.URL.Query().Get("nextToken")
		pages = append(pages, token)
		w.Header().Set("Content-Type", "application/json")
		switch token {
		case "":
			w.Header().Set("x-next-token", "p2")
			_ = json.NewEncoder(w).Encode([]map[string]any{{"sandboxID": "sb-1", "state": "running"}})
		case "p2":
			w.Header().Set("x-next-token", "p3")
			_ = json.NewEncoder(w).Encode([]map[string]any{{"sandboxID": "sb-2", "state": "paused"}})
		case "p3":
			// No x-next-token: last page.
			_ = json.NewEncoder(w).Encode([]map[string]any{{"sandboxID": "sb-3", "state": "running"}})
		}
	}))
	defer srv.Close()

	c, err := New(Config{APIURL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	list, err := c.ListSandboxes(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListSandboxes: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected all 3 pages, got %d: %+v", len(list), list)
	}
	var seenPaused bool
	for _, sb := range list {
		if sb.State == "paused" {
			seenPaused = true
		}
	}
	if !seenPaused {
		t.Fatalf("paused sandbox beyond page 1 was not returned: %+v", list)
	}
	if len(pages) != 3 || pages[0] != "" || pages[1] != "p2" || pages[2] != "p3" {
		t.Fatalf("cursor sequence = %v", pages)
	}
}

// A metadata filter must be forwarded to the API so the reconcile query is
// scoped to this operation's tag, not the whole installation.
func TestListSandboxesSendsMetadataFilter(t *testing.T) {
	var gotMeta string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMeta = r.URL.Query().Get("metadata")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer srv.Close()

	c, _ := New(Config{APIURL: srv.URL})
	_, err := c.ListSandboxes(context.Background(), map[string]string{"kagent.dev/operation": "op-1"})
	if err != nil {
		t.Fatalf("ListSandboxes: %v", err)
	}
	// The server decodes the outer param once, then parses the "k1=v1" sub-
	// filter where each key/value is itself encoded — the key's "/" arrives
	// still-encoded so the server unescapes it to "kagent.dev/operation".
	if gotMeta != "kagent.dev%2Foperation=op-1" {
		t.Fatalf("metadata filter = %q", gotMeta)
	}
}

// A redirect on the non-idempotent create must be refused, not followed: a
// 307/308 re-POSTs the body to a second host, which is the blind retry the
// plan forbids. The caller reconciles the ambiguous outcome instead.
func TestCreateSandboxRefusesRedirect(t *testing.T) {
	var redirected bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sandboxID": "sb-x"})
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	c, err := New(Config{APIURL: srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = c.CreateSandbox(context.Background(), CreateRequest{TemplateID: "tpl"})
	if err == nil {
		t.Fatalf("create followed a redirect on a non-idempotent request")
	}
	if redirected {
		t.Fatalf("redirected create re-issued the POST to a second host")
	}
}

// GET (list/get) is idempotent and may follow redirects — only mutating
// methods are refused.
func TestGetSandboxMayFollowRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sandboxID": "sb-1", "state": "running"})
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	c, _ := New(Config{APIURL: srv.URL})
	sb, err := c.GetSandbox(context.Background(), "sb-1")
	if err != nil {
		t.Fatalf("idempotent GET should follow a redirect: %v", err)
	}
	if sb.SandboxID != "sb-1" {
		t.Fatalf("sandbox = %+v", sb)
	}
}
