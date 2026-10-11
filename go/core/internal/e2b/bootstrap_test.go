package e2b

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// envdSpy captures the ordered file writes the bootstrapper issues so the test
// can assert config-before-credential-before-activate ordering.
type envdSpy struct {
	mu     sync.Mutex
	writes []string // paths in write order
	bodies map[string]string
	// runs records each process.Process/Start call's Authorization header and
	// whether it arrived before the first /files write — the order assertion
	// the privileged prepare step depends on.
	runs           []string
	prepareFailing bool
	// cmds records each /process.Process/Start call's command line so tests can
	// assert the credential refresh's atomic mv/install arguments.
	cmds []string
}

func (s *envdSpy) handler() http.Handler {
	s.bodies = map[string]string{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/files" && r.Method == http.MethodPost {
			p := r.URL.Query().Get("path")
			b, _ := io.ReadAll(r.Body)
			s.mu.Lock()
			s.writes = append(s.writes, p)
			s.bodies[p] = string(b)
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == "/process.Process/Start" && r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			s.mu.Lock()
			marker := r.Header.Get("Authorization")
			if len(s.writes) == 0 {
				marker += "|pre-write"
			}
			s.runs = append(s.runs, marker)
			var parsed struct {
				Process struct {
					Args []string `json:"args"`
				} `json:"process"`
			}
			// Connect envelope: 1 flag byte + 4-byte big-endian length precede the
			// JSON message, then an end-of-stream trailer follows. Decode only the
			// first frame's payload for the process args.
			if len(body) > 5 {
				n := binary.BigEndian.Uint32(body[1:5])
				if int(n) <= len(body)-5 {
					body = body[5 : 5+n]
				}
			}
			if json.Unmarshal(body, &parsed) == nil && len(parsed.Process.Args) > 0 {
				s.cmds = append(s.cmds, strings.Join(parsed.Process.Args, " "))
			}
			s.mu.Unlock()
			status := "exit status 0"
			if s.prepareFailing {
				status = "exit status 7"
			}
			var out bytes.Buffer
			out.Write(commandFrame(0, `{"event":{"start":{"pid":1}}}`))
			out.Write(commandFrame(0, `{"event":{"end":{"exited":true,"status":"`+status+`"}}}`))
			out.Write(commandFrame(2, `{}`))
			w.WriteHeader(http.StatusOK)
			w.Write(out.Bytes())
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
}

func TestBootstrapWritesConfigCredentialThenActivates(t *testing.T) {
	envdSpy := &envdSpy{}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	var paused []string
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pause") {
			paused = append(paused, r.URL.Path)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiSrv.Close()

	envd, err := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "", nil)
	if err != nil {
		t.Fatalf("envd: %v", err)
	}
	lifecycle, err := New(Config{APIURL: apiSrv.URL})
	if err != nil {
		t.Fatalf("lifecycle: %v", err)
	}
	var signedArgs []string
	signer := func(sessionID, installation, externalID string, generation int64, exp time.Time) (string, error) {
		signedArgs = []string{sessionID, installation, externalID, "cred-token"}
		return "cred-token", nil
	}
	boot, err := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Lifecycle: lifecycle, Signer: signer,
		Files: BootstrapFiles{
			ConfigPath:     "/run/kagent/config.json",
			CredentialPath: "/run/kagent/credential", ActivatePath: "/run/kagent/activate",
		},
	})
	if err != nil {
		t.Fatalf("bootstrapper: %v", err)
	}
	if err := boot.Bootstrap(context.Background(), "embed-lab", "sess-1", "sb-1", 1, []byte("CFG"), nil, nil); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	want := []string{"/run/kagent/config.json", "/run/kagent/credential", "/run/kagent/activate"}
	if len(envdSpy.writes) != len(want) {
		t.Fatalf("writes = %v", envdSpy.writes)
	}
	for i, w := range want {
		if envdSpy.writes[i] != w {
			t.Fatalf("write[%d] = %q, want %q", i, envdSpy.writes[i], w)
		}
	}
	if envdSpy.bodies["/run/kagent/credential"] != "cred-token" {
		t.Fatalf("credential body = %q", envdSpy.bodies["/run/kagent/credential"])
	}
	if envdSpy.bodies["/run/kagent/config.json"] != "CFG" {
		t.Fatalf("config body = %q", envdSpy.bodies["/run/kagent/config.json"])
	}
	if len(paused) != 1 || !strings.Contains(paused[0], "sb-1") {
		t.Fatalf("pause = %v", paused)
	}
	if len(signedArgs) != 4 || signedArgs[0] != "sess-1" || signedArgs[1] != "embed-lab" || signedArgs[2] != "sb-1" {
		t.Fatalf("signer args = %v", signedArgs)
	}
}

// The agent card is per-revision data the harness's KAGENT_AGENT_CARD_JSON
// launch contract reads; it must land at AgentCardPath (when configured)
// before the credential and activation signal so the guest never launches
// without its card.
func TestBootstrapWritesAgentCardBeforeActivate(t *testing.T) {
	envdSpy := &envdSpy{}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	envd, err := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "", nil)
	if err != nil {
		t.Fatalf("envd: %v", err)
	}
	signer := func(s, i, e string, g int64, exp time.Time) (string, error) { return "tok", nil }
	boot, err := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Signer: signer,
		Files: BootstrapFiles{
			ConfigPath: "/run/kagent/config.json", AgentCardPath: "/run/kagent/agent-card.json",
			CredentialPath: "/run/kagent/credential", ActivatePath: "/run/kagent/activate",
		},
	})
	if err != nil {
		t.Fatalf("bootstrapper: %v", err)
	}
	if err := boot.Bootstrap(context.Background(), "inst", "s", "sb-1", 1, []byte("CFG"), []byte("CARD"), nil); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if envdSpy.bodies["/run/kagent/agent-card.json"] != "CARD" {
		t.Fatalf("agent card body = %q", envdSpy.bodies["/run/kagent/agent-card.json"])
	}
	cardIdx, actIdx := -1, -1
	for i, w := range envdSpy.writes {
		if w == "/run/kagent/agent-card.json" {
			cardIdx = i
		}
		if w == "/run/kagent/activate" {
			actIdx = i
		}
	}
	if cardIdx < 0 || actIdx < 0 || cardIdx > actIdx {
		t.Fatalf("card write index %d must precede activate %d in %v", cardIdx, actIdx, envdSpy.writes)
	}
}

func TestBootstrapCredentialWrittenBeforeActivate(t *testing.T) {
	envdSpy := &envdSpy{}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	envd, _ := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "", nil)
	signer := func(s, i, e string, g int64, exp time.Time) (string, error) { return "tok", nil }
	boot, _ := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Signer: signer,
		Files: BootstrapFiles{ConfigPath: "/c", CredentialPath: "/cred", ActivatePath: "/act"},
	})
	// Lifecycle nil -> skip pause; ReadyProbe nil -> skip readiness.
	if err := boot.Bootstrap(context.Background(), "inst", "s", "sb-1", 1, []byte("x"), nil, nil); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	credIdx, actIdx := -1, -1
	for i, w := range envdSpy.writes {
		if w == "/cred" {
			credIdx = i
		}
		if w == "/act" {
			actIdx = i
		}
	}
	if credIdx < 0 || actIdx < 0 || credIdx > actIdx {
		t.Fatalf("credential must be written before activate: %v", envdSpy.writes)
	}
}

// TestBootstrapWritesCABundleBeforeActivate asserts the configured callback CA
// bundle is written after the credential and before the activation commit
// point — the documented contract order the supervisor relies on, so a guest
// is never activated with a credential but no trust anchor.
func TestBootstrapWritesCABundleBeforeActivate(t *testing.T) {
	envdSpy := &envdSpy{}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	envd, _ := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "", nil)
	signer := func(s, i, e string, g int64, exp time.Time) (string, error) { return "tok", nil }
	boot, _ := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Signer: signer,
		Files: BootstrapFiles{
			ConfigPath: "/c", CredentialPath: "/cred",
			CAPath: "/run/kagent/ca.pem", CAPEM: []byte("CAPEM"),
			ActivatePath: "/act",
		},
	})
	if err := boot.Bootstrap(context.Background(), "inst", "s", "sb-1", 1, []byte("x"), nil, nil); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	credIdx, caIdx, actIdx := -1, -1, -1
	for i, w := range envdSpy.writes {
		switch w {
		case "/cred":
			credIdx = i
		case "/run/kagent/ca.pem":
			caIdx = i
		case "/act":
			actIdx = i
		}
	}
	if credIdx < 0 || caIdx < 0 || actIdx < 0 || !(credIdx < caIdx && caIdx < actIdx) {
		t.Fatalf("want credential -> ca.pem -> activate order, got %v", envdSpy.writes)
	}
	if envdSpy.bodies["/run/kagent/ca.pem"] != "CAPEM" {
		t.Fatalf("ca body = %q", envdSpy.bodies["/run/kagent/ca.pem"])
	}
}

// TestBootstrapNoCABundleWritesNoCAFile asserts that with no configured CA
// destination no ca.pem write is issued — the harness then relies on system
// trust rather than an empty file.
func TestBootstrapNoCABundleWritesNoCAFile(t *testing.T) {
	envdSpy := &envdSpy{}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	envd, _ := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "", nil)
	signer := func(s, i, e string, g int64, exp time.Time) (string, error) { return "tok", nil }
	boot, _ := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Signer: signer,
		Files: BootstrapFiles{ConfigPath: "/c", CredentialPath: "/cred", ActivatePath: "/act"},
	})
	if err := boot.Bootstrap(context.Background(), "inst", "s", "sb-1", 1, []byte("x"), nil, nil); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	for _, w := range envdSpy.writes {
		if w == "/run/kagent/ca.pem" {
			t.Fatalf("unexpected CA write: %v", envdSpy.writes)
		}
	}
}

// TestBootstrapPrepareRunsPrivilegedBeforeFiles asserts the prepare step
// executes as the envd default user (no Authorization user = root) before any
// /files write — the ordering the non-root launch contract needs: the
// privileged sysctl/port-floor drop precedes trusting the guest with a
// credential.
func TestBootstrapPrepareRunsPrivilegedBeforeFiles(t *testing.T) {
	envdSpy := &envdSpy{}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	envd, _ := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "kagent", nil)
	signer := func(s, i, e string, g int64, exp time.Time) (string, error) { return "tok", nil }
	boot, err := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Signer: signer,
		Files:   BootstrapFiles{ConfigPath: "/c", CredentialPath: "/cred", ActivatePath: "/act"},
		Prepare: &BootstrapPrepare{Cmd: "sysctl -w net.ipv4.ip_unprivileged_port_start=80"},
	})
	if err != nil {
		t.Fatalf("bootstrapper: %v", err)
	}
	if err := boot.Bootstrap(context.Background(), "inst", "s", "sb-1", 1, []byte("x"), nil, nil); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if len(envdSpy.runs) != 1 {
		t.Fatalf("prepare runs = %v, want exactly 1", envdSpy.runs)
	}
	// No Authorization user (root) AND it ran before the first file write.
	if envdSpy.runs[0] != "|pre-write" {
		t.Fatalf("prepare marker = %q, want privileged pre-write run", envdSpy.runs[0])
	}
}

// TestBootstrapPrepareFailureAborts asserts a non-zero prepare exit aborts the
// bootstrap before the file contract is written — the guest is never trusted
// with a credential when privileged setup failed.
func TestBootstrapPrepareFailureAborts(t *testing.T) {
	envdSpy := &envdSpy{prepareFailing: true}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	envd, _ := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "kagent", nil)
	signer := func(s, i, e string, g int64, exp time.Time) (string, error) { return "tok", nil }
	boot, _ := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Signer: signer,
		Files:   BootstrapFiles{ConfigPath: "/c", CredentialPath: "/cred", ActivatePath: "/act"},
		Prepare: &BootstrapPrepare{Cmd: "false"},
	})
	err := boot.Bootstrap(context.Background(), "inst", "s", "sb-1", 1, []byte("x"), nil, nil)
	if err == nil {
		t.Fatal("expected prepare failure")
	}
	if len(envdSpy.writes) != 0 {
		t.Fatalf("no files may be written after a failed prepare: %v", envdSpy.writes)
	}
}

var _ = url.Values{}

// RefreshCredential must stage a unique temp file in the credential's own
// directory and rename it over the live path atomically (mv), preserving the
// session/installation/externalID/generation identity — never rewriting the
// live file in place or re-running the launch.
func TestRefreshCredentialStagesThenRenamesAtomically(t *testing.T) {
	envdSpy := &envdSpy{}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	envd, err := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "kagent", nil)
	if err != nil {
		t.Fatalf("envd: %v", err)
	}
	var signSession, signInst, signExt string
	var signGen int64
	signer := func(sessionID, installation, externalID string, generation int64, exp time.Time) (string, error) {
		signSession, signInst, signExt, signGen = sessionID, installation, externalID, generation
		return "fresh-cred", nil
	}
	boot, err := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Signer: signer,
		Files: BootstrapFiles{ConfigPath: "/run/kagent/config.json", CredentialPath: "/run/kagent/credential"},
	})
	if err != nil {
		t.Fatalf("bootstrapper: %v", err)
	}
	if err := boot.RefreshCredential(context.Background(), "embed-lab", "sess-1", "sb-1", 7); err != nil {
		t.Fatalf("RefreshCredential: %v", err)
	}
	// Identity is preserved verbatim — same session/install/external/generation.
	if signSession != "sess-1" || signInst != "embed-lab" || signExt != "sb-1" || signGen != 7 {
		t.Fatalf("signer identity = %q/%q/%q gen %d, want sess-1/embed-lab/sb-1 gen 7", signSession, signInst, signExt, signGen)
	}
	// Exactly one file write: a unique temp sibling of the credential path —
	// the live credential path is never written directly.
	if len(envdSpy.writes) != 1 {
		t.Fatalf("writes = %v, want one staged temp write", envdSpy.writes)
	}
	tmp := envdSpy.writes[0]
	if !strings.HasPrefix(tmp, "/run/kagent/credential.") || !strings.HasSuffix(tmp, ".next") {
		t.Fatalf("staged path = %q, want /run/kagent/credential.<n>.next", tmp)
	}
	if envdSpy.bodies[tmp] != "fresh-cred" {
		t.Fatalf("staged body = %q", envdSpy.bodies[tmp])
	}
	// One process run performs chown+chmod+mv over the live path.
	if len(envdSpy.cmds) != 1 {
		t.Fatalf("process cmds = %v, want one install/rename command", envdSpy.cmds)
	}
	cmd := envdSpy.cmds[0]
	// The replace runs as the contract owner (the harness user "kagent"), not
	// as envd's default root: the contract dir is workload-owned, so a root
	// chown/chmod on the staged path could follow a substituted symlink onto
	// another guest file. The run-as user is the Basic-auth username envd
	// resolves to a uid.
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("kagent:"))
	if len(envdSpy.runs) != 1 || !strings.HasPrefix(envdSpy.runs[0], wantAuth) {
		t.Fatalf("refresh run-as = %v, want Basic auth user kagent (non-root)", envdSpy.runs)
	}
	// No privileged chown appears: the staged file is already owned by the
	// harness user (WriteFile writes as envd's username), so only chmod+mv are
	// needed and both run unprivileged.
	if strings.Contains(cmd, "chown") {
		t.Fatalf("refresh must not chown as root on a workload-owned path: %q", cmd)
	}
	if !strings.Contains(cmd, "mv -f") || !strings.Contains(cmd, "/run/kagent/credential") {
		t.Fatalf("rename command = %q, want an mv -f over the live credential path", cmd)
	}
	// Each staged/live path arg is single-quoted; no bare path or install-copy
	// appears — the replace is a rename, not a copy.
	if strings.Contains(cmd, "install ") || strings.Contains(cmd, " "+tmp+" ") {
		t.Fatalf("command args must be shell-quoted renames: %q", cmd)
	}
}

// The refresh replace must run entirely as the harness user with no root
// chown: if a workload substituted the staged path with a symlink, an
// unprivileged chmod/mv cannot escalate to modify another file's owner or
// mode, and mv replaces the symlink itself rather than following it. This
// guards the non-root contract the prepare step establishes.
func TestRefreshCredentialIsUnprivilegedAgainstSymlinkSwap(t *testing.T) {
	envdSpy := &envdSpy{}
	envdSrv := httptest.NewServer(envdSpy.handler())
	defer envdSrv.Close()
	envd, err := NewEnvdClient(envdSrv.URL, "sb-1", "tok", "kagent", nil)
	if err != nil {
		t.Fatalf("envd: %v", err)
	}
	boot, err := NewBootstrapper(BootstrapperConfig{
		Envd: envd, Signer: func(_, _, _ string, _ int64, _ time.Time) (string, error) { return "c", nil },
		Files:   BootstrapFiles{ConfigPath: "/run/kagent/config.json", CredentialPath: "/run/kagent/credential"},
		Command: &BootstrapCommand{Cmd: "run", User: "kagent", Cwd: "/home/kagent"},
	})
	if err != nil {
		t.Fatalf("bootstrapper: %v", err)
	}
	if err := boot.RefreshCredential(context.Background(), "embed-lab", "sess-1", "sb-1", 1); err != nil {
		t.Fatalf("RefreshCredential: %v", err)
	}
	if len(envdSpy.cmds) != 1 {
		t.Fatalf("process cmds = %v", envdSpy.cmds)
	}
	cmd := envdSpy.cmds[0]
	// mv -f on a symlink replaces the link; combined with the non-root user
	// (asserted below) a substituted symlink cannot redirect an ownership or
	// permission change onto a target the workload does not own.
	if !strings.Contains(cmd, "chmod 0600") || !strings.Contains(cmd, "mv -f") {
		t.Fatalf("refresh must chmod-then-mv unprivileged, got %q", cmd)
	}
	if strings.Contains(cmd, "chown") || strings.Contains(cmd, "sudo") {
		t.Fatalf("refresh must not invoke privileged ops on the staged path: %q", cmd)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("kagent:"))
	if !strings.HasPrefix(envdSpy.runs[0], wantAuth) {
		t.Fatalf("refresh must run as harness user, got auth %q", envdSpy.runs[0])
	}
}
