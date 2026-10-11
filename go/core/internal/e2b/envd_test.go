package e2b

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
)

func TestEnvdSignatureFormat(t *testing.T) {
	c, err := NewEnvdClient("http://proxy:3002", "sb-1", "tok", "user1", nil)
	if err != nil {
		t.Fatalf("NewEnvdClient: %v", err)
	}
	sig := c.signature("/run/kagent/config.json", "write", 0)
	if len(sig) < 4 || sig[:3] != "v1_" {
		t.Fatalf("signature = %q, want v1_<base64>", sig)
	}
	// Same inputs -> same signature; different token -> different signature.
	c2, _ := NewEnvdClient("http://proxy:3002", "sb-1", "tok2", "user1", nil)
	if c.signature("/p", "write", 0) == c2.signature("/p", "write", 0) {
		t.Fatal("signature must depend on the access token")
	}
	// Expiring signature differs from non-expiring.
	if c.signature("/p", "write", 0) == c.signature("/p", "write", 1700000000) {
		t.Fatal("expiring signature must differ from non-expiring")
	}
}

func TestEnvdWriteFileRoutesAndSigns(t *testing.T) {
	var gotPath, gotSandbox, gotPort, gotSig, gotBody, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Query().Get("path")
		gotSig = r.URL.Query().Get("signature")
		gotSandbox = r.Header.Get("E2b-Sandbox-Id")
		gotPort = r.Header.Get("E2b-Sandbox-Port")
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c, err := NewEnvdClient(srv.URL, "sb-1", "tok", "", nil)
	if err != nil {
		t.Fatalf("NewEnvdClient: %v", err)
	}
	if err := c.WriteFile(context.Background(), "/run/kagent/config.json", []byte("{}"), 0); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if gotSandbox != "sb-1" || gotPort != EnvdPort {
		t.Fatalf("routing headers = %q/%q", gotSandbox, gotPort)
	}
	if gotPath != "/run/kagent/config.json" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotSig == "" || gotSig[:3] != "v1_" {
		t.Fatalf("signature = %q", gotSig)
	}
	if gotBody != "{}" || gotCT != "application/octet-stream" {
		t.Fatalf("body=%q ct=%q", gotBody, gotCT)
	}
}

func TestEnvdWriteFileErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("denied"))
	}))
	defer srv.Close()
	c, _ := NewEnvdClient(srv.URL, "sb-1", "tok", "", nil)
	err := c.WriteFile(context.Background(), "/x", []byte("y"), 0)
	var ae *Error
	if err == nil || !errorsAs(err, &ae) || ae.Status != 403 {
		t.Fatalf("expected 403 Error, got %v", err)
	}
}

var _ = url.Values{}

func TestEnvdStartCommandSendsRequestAndReadsStart(t *testing.T) {
	var gotCT, gotSandbox, gotPort, gotToken, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		gotSandbox = r.Header.Get("E2b-Sandbox-Id")
		gotPort = r.Header.Get("E2b-Sandbox-Port")
		gotToken = r.Header.Get("X-Access-Token")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		// One Connect-protocol envelope: flag 0x00 + 4-byte length + start event.
		payload := []byte(`{"event":{"start":{"pid":1234}}}`)
		frame := append([]byte{0, 0, 0, 0, byte(len(payload))}, payload...)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(frame)
	}))
	defer srv.Close()
	c, err := NewEnvdClient(srv.URL, "sb-9", "tok", "", nil)
	if err != nil {
		t.Fatalf("NewEnvdClient: %v", err)
	}
	if err := c.StartCommand(context.Background(), "nohup /usr/local/bin/kagent-codex &", map[string]string{"K": "v"}, CommandRun{}); err != nil {
		t.Fatalf("StartCommand: %v", err)
	}
	if gotCT != "application/connect+json" {
		t.Fatalf("Content-Type = %q", gotCT)
	}
	if gotSandbox != "sb-9" || gotPort != EnvdPort {
		t.Fatalf("routing headers sandbox=%q port=%q", gotSandbox, gotPort)
	}
	if gotToken != "tok" {
		t.Fatalf("X-Access-Token = %q", gotToken)
	}
	if gotBody == "" || !strings.Contains(gotBody, "kagent-codex") || !strings.Contains(gotBody, "/bin/bash") {
		t.Fatalf("body missing cmd: %q", gotBody)
	}
	if !strings.Contains(gotBody, `"envs":{"K":"v"}`) || !strings.Contains(gotBody, `"stdin":false`) {
		t.Fatalf("body missing envs/stdin: %q", gotBody)
	}
}

func TestEnvdStartCommandTrailerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := []byte(`{"error":{"code":"permission_denied","message":"cannot exec"}}`)
		frame := append([]byte{2, 0, 0, 0, byte(len(payload))}, payload...)
		w.WriteHeader(http.StatusOK)
		w.Write(frame)
	}))
	defer srv.Close()
	c, _ := NewEnvdClient(srv.URL, "sb-1", "tok", "", nil)
	if err := c.StartCommand(context.Background(), "true", nil, CommandRun{}); err == nil {
		t.Fatal("expected trailer error")
	}
}

// commandFrame builds one Connect-protocol envelope: a data frame (flag 0x00)
// or the end-of-stream trailer (flag 0x02). The 4-byte big-endian length
// follows the flag, exactly as the live envd emits.
func commandFrame(flag byte, payload string) []byte {
	var b bytes.Buffer
	b.WriteByte(flag)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(payload)))
	b.Write(n[:])
	b.WriteString(payload)
	return b.Bytes()
}

// TestRunCommandReadsDataAndEnd asserts the real wire shape captured live: the
// data event's output oneof is flattened ({"data":{"stdout":"<base64>"}}), and
// the end event reports {"exited":true,"status":"exit status N"} with no
// numeric exitCode field.
func TestRunCommandReadsDataAndEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var out bytes.Buffer
		out.Write(commandFrame(0, `{"event":{"start":{"pid":7}}}`))
		// "aGVsbG8K" = base64("hello\n"); "YmFkCg==" = base64("bad\n").
		out.Write(commandFrame(0, `{"event":{"data":{"stdout":"aGVsbG8K"}}}`))
		out.Write(commandFrame(0, `{"event":{"data":{"stderr":"YmFkCg=="}}}`))
		out.Write(commandFrame(0, `{"event":{"end":{"exited":true,"status":"exit status 3"}}}`))
		out.Write(commandFrame(2, `{}`))
		w.WriteHeader(http.StatusOK)
		w.Write(out.Bytes())
	}))
	defer srv.Close()
	c, _ := NewEnvdClient(srv.URL, "sb-1", "tok", "", nil)
	res, err := c.RunCommand(context.Background(), "echo hi", nil, CommandRun{})
	if err != nil {
		t.Fatalf("RunCommand: %v", err)
	}
	if res.Stdout != "hello\n" || res.Stderr != "bad\n" || res.ExitCode != 3 {
		t.Fatalf("result = %+v", res)
	}
}

// TestEnvdCommandCarriesUserAndCwd asserts the non-root launch contract lands
// on the wire: the run-as user travels as the Authorization: Basic header (the
// same mechanism the SDK's commands.run(user) uses — processConfig has no user
// field) and the working directory as processConfig.cwd — so a bundled harness
// runs as uid 65532 @ /data/workspace, not the envd default root @ /home/user.
func TestEnvdCommandCarriesUserAndCwd(t *testing.T) {
	var gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		payload := []byte(`{"event":{"start":{"pid":9}}}`)
		frame := append([]byte{0, 0, 0, 0, byte(len(payload))}, payload...)
		w.WriteHeader(http.StatusOK)
		w.Write(frame)
	}))
	defer srv.Close()
	c, _ := NewEnvdClient(srv.URL, "sb-1", "tok", "", nil)
	if err := c.StartCommand(context.Background(), "nohup /h &", nil, CommandRun{User: "kagent", Cwd: "/data/workspace"}); err != nil {
		t.Fatalf("StartCommand: %v", err)
	}
	if gotAuth != "Basic a2FnZW50Og==" { // base64("kagent:")
		t.Fatalf("Authorization = %q, want Basic kagent", gotAuth)
	}
	if !strings.Contains(gotBody, `"cwd":"/data/workspace"`) {
		t.Fatalf("body missing cwd: %q", gotBody)
	}
}

// TestEnvdCommandDefaultRunCarriesNoUser asserts an empty CommandRun sends no
// Authorization user and no cwd — the envd default user (root) and home apply,
// which is what the privileged prepare step relies on.
func TestEnvdCommandDefaultRunCarriesNoUser(t *testing.T) {
	var gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		payload := []byte(`{"event":{"start":{"pid":9}}}`)
		frame := append([]byte{0, 0, 0, 0, byte(len(payload))}, payload...)
		w.WriteHeader(http.StatusOK)
		w.Write(frame)
	}))
	defer srv.Close()
	c, _ := NewEnvdClient(srv.URL, "sb-1", "tok", "", nil)
	if err := c.StartCommand(context.Background(), "sysctl -w x=1", nil, CommandRun{}); err != nil {
		t.Fatalf("StartCommand: %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want none", gotAuth)
	}
	if strings.Contains(gotBody, "cwd") {
		t.Fatalf("body unexpectedly carries cwd: %q", gotBody)
	}
}

func TestExitCodeFromStatus(t *testing.T) {
	for status, want := range map[string]int{
		"exit status 0":   0,
		"exit status 42":  42,
		"signal: killed":  1, // non-exit status is a failure, not success
		"":                1,
		"not a status":    1,
		"exit status abc": 1,
	} {
		if got := exitCodeFromStatus(status); got != want {
			t.Errorf("exitCodeFromStatus(%q) = %d, want %d", status, got, want)
		}
	}
}

// TestExportDataChunksAcrossReadCap drives ExportData against a fake envd that
// answers any ".partNN" read from an in-memory map, the way the live 8MiB read
// cap forces the archive into split chunks. The parts reassemble in order; the
// export must run tar+split and list the parts via RunCommand.
func TestExportDataChunksAcrossReadCap(t *testing.T) {
	// Parts are returned in the order ls lists them; each carries a slice of
	// the archive. Reads are keyed by whatever part path the ls printed.
	var archive bytes.Buffer
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/process.Process/Start":
			b, _ := io.ReadAll(r.Body)
			bs := string(b)
			if strings.Contains(bs, "split") {
				// tar+split "succeeds".
				w.Write(commandFrame(0, `{"event":{"end":{"exited":true,"status":"exit status 0"}}}`))
				return
			}
			// ls prints the part paths plus the guest's size/sha trailers the
			// export verifies. "hello-world" is 11 bytes; sha256 of it below.
			sum := sha256.Sum256([]byte("hello-world"))
			var out bytes.Buffer
			listing := fmt.Sprintf("P0\nP1\nSIZE=11\nSHA=%x\n", sum)
			out.Write(commandFrame(0, `{"event":{"data":{"stdout":"`+
				base64.StdEncoding.EncodeToString([]byte(listing))+`"}}}`))
			out.Write(commandFrame(0, `{"event":{"end":{"exited":true,"status":"exit status 0"}}}`))
			w.Write(out.Bytes())
		case "/files":
			p := r.URL.Query().Get("path")
			// The fake returns fixed slices by part basename, ignoring the real
			// path — the test asserts ordering and reassembly, not naming.
			switch path.Base(p) {
			case "part00", "P0":
				w.Write([]byte("hello-"))
			case "part01", "P1":
				w.Write([]byte("world"))
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	c, _ := NewEnvdClient(srv.URL, "sb-1", "tok", "", nil)
	got, err := c.ExportData(context.Background(), "/data")
	if err != nil {
		t.Fatalf("ExportData: %v", err)
	}
	if string(got) != "hello-world" {
		t.Fatalf("reassembled archive = %q", got)
	}
	_ = archive
}

// TestExportDataAbortsOverBound drives an export whose guest-reported size
// exceeds ExportDataLimit: the export must fail during collection — before
// buffering the archive — rather than landing an oversized blob for the SQL
// bound to reject later.
func TestExportDataAbortsOverBound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/process.Process/Start" {
			b, _ := io.ReadAll(r.Body)
			bs := string(b)
			if strings.Contains(bs, "split") {
				w.Write(commandFrame(0, `{"event":{"end":{"exited":true,"status":"exit status 0"}}}`))
				return
			}
			// Report an archive larger than the bound so the export aborts.
			listing := fmt.Sprintf("P0\nSIZE=%d\n", ExportDataLimit+1)
			var out bytes.Buffer
			out.Write(commandFrame(0, `{"event":{"data":{"stdout":"`+
				base64.StdEncoding.EncodeToString([]byte(listing))+`"}}}`))
			out.Write(commandFrame(0, `{"event":{"end":{"exited":true,"status":"exit status 0"}}}`))
			w.Write(out.Bytes())
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c, _ := NewEnvdClient(srv.URL, "sb-1", "tok", "", nil)
	if _, err := c.ExportData(context.Background(), "/data"); err == nil ||
		!strings.Contains(err.Error(), "bound") {
		t.Fatalf("an over-bound export was not aborted during collection: %v", err)
	}
}

// TestRestoreDataRejectsOverBound asserts a compressed archive over the limit
// is refused before it ever reaches the guest.
func TestRestoreDataRejectsOverBound(t *testing.T) {
	c, _ := NewEnvdClient("http://unused", "sb-1", "tok", "", nil)
	if err := c.RestoreData(context.Background(), "/data", make([]byte, ExportDataLimit+1)); err == nil ||
		!strings.Contains(err.Error(), "bound") {
		t.Fatalf("an over-bound restore was not refused: %v", err)
	}
}
