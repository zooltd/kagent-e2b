package e2b

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// EnvdPort is the envd listen port inside every sandbox. File and command
// operations are sent through the installation's client proxy with the
// E2b-Sandbox-* routing headers, the same ingress mechanism the gateway uses.
const EnvdPort = "49983"

// EnvdClient performs sandbox-local filesystem operations through the envd
// HTTP API. It is the bootstrap-probe primitive the E2B create path uses to
// write the harness config and instance credential into a sandbox before the
// supervisor launches the harness. Each call carries the file signature the
// envd access token authorizes.
type EnvdClient struct {
	// ProxyURL is the installation's client-proxy base URL; requests are routed
	// to the bound sandbox via E2b-Sandbox-Id/E2b-Sandbox-Port headers.
	ProxyURL    string
	SandboxID   string
	AccessToken string
	User        string
	HTTPClient  *http.Client
}

// NewEnvdClient builds an envd client for one bound sandbox. AccessToken is
// the per-sandbox envdAccessToken the create response returns; user defaults
// to the sandbox's default user when empty.
func NewEnvdClient(proxyURL, sandboxID, accessToken, user string, hc *http.Client) (*EnvdClient, error) {
	if proxyURL == "" {
		return nil, fmt.Errorf("e2b envd: proxy url is required")
	}
	if sandboxID == "" {
		return nil, fmt.Errorf("e2b envd: sandbox id is required")
	}
	if accessToken == "" {
		return nil, fmt.Errorf("e2b envd: access token is required")
	}
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return &EnvdClient{ProxyURL: strings.TrimSuffix(proxyURL, "/"), SandboxID: sandboxID, AccessToken: accessToken, User: user, HTTPClient: hc}, nil
}

// signature computes the envd file-operation token: a base64-encoded
// SHA256 of path:operation:user:accessToken[:expiration], prefixed "v1_".
// expiration is the unix-second deadline; 0 means a non-expiring signature.
func (c *EnvdClient) signature(path, operation string, expiration int64) string {
	raw := fmt.Sprintf("%s:%s:%s:%s", path, operation, c.User, c.AccessToken)
	if expiration != 0 {
		raw += ":" + strconv.FormatInt(expiration, 10)
	}
	sum := sha256.Sum256([]byte(raw))
	// envd expects standard base64 (the SDK's btoa), not base64url, with
	// padding stripped.
	return "v1_" + strings.TrimRight(base64.StdEncoding.EncodeToString(sum[:]), "=")
}

func (c *EnvdClient) route(req *http.Request) {
	req.Header.Set("E2b-Sandbox-Id", c.SandboxID)
	req.Header.Set("E2b-Sandbox-Port", EnvdPort)
}

// do issues one request through the client proxy to the bound sandbox's envd.
func (c *EnvdClient) do(ctx context.Context, method, path string, q url.Values, body io.Reader, contentType string) ([]byte, int, error) {
	u := c.ProxyURL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, 0, fmt.Errorf("e2b envd: build %s %s: %w", method, path, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	c.route(req)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("e2b envd: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return data, resp.StatusCode, nil
}

// WriteFile writes content to path inside the sandbox, creating parent
// directories. The write signature authorizes the operation; expiration is the
// optional unix-second deadline (0 = non-expiring). Returns an error on a
// non-2xx envd response.
func (c *EnvdClient) WriteFile(ctx context.Context, path string, content []byte, expiration int64) error {
	q := url.Values{}
	q.Set("path", path)
	q.Set("signature", c.signature(path, "write", expiration))
	if expiration != 0 {
		q.Set("signature_expiration", strconv.FormatInt(expiration, 10))
	}
	if c.User != "" {
		q.Set("username", c.User)
	}
	data, code, err := c.do(ctx, http.MethodPost, "/files", q, bytes.NewReader(content), "application/octet-stream")
	if err != nil {
		return err
	}
	if code < 200 || code >= 300 {
		return &Error{Status: code, Message: "envd write " + path + ": " + string(data)}
	}
	return nil
}

// ReadFile returns the content of path inside the sandbox.
func (c *EnvdClient) ReadFile(ctx context.Context, path string, expiration int64) ([]byte, error) {
	q := url.Values{}
	q.Set("path", path)
	q.Set("signature", c.signature(path, "read", expiration))
	if expiration != 0 {
		q.Set("signature_expiration", strconv.FormatInt(expiration, 10))
	}
	if c.User != "" {
		q.Set("username", c.User)
	}
	data, code, err := c.do(ctx, http.MethodGet, "/files", q, nil, "")
	if err != nil {
		return nil, err
	}
	if code < 200 || code >= 300 {
		return nil, &Error{Status: code, Message: "envd read " + path + ": " + string(data)}
	}
	return data, nil
}

// processStartRequest is the Connect-protocol JSON body for
// process.Process/Start. It launches through /bin/bash -lc exactly as
// sandbox.commands.run does; stdin is closed so the process is detached.
type processStartRequest struct {
	Process processConfig `json:"process"`
	Stdin   bool          `json:"stdin"`
}

type processConfig struct {
	Cmd  string            `json:"cmd"`
	Args []string          `json:"args"`
	Envs map[string]string `json:"envs,omitempty"`
	Cwd  string            `json:"cwd,omitempty"`
}

// processEvent is one Connect-protocol envelope message: either the start
// event (carries the pid) or the end-of-stream trailer.
type processEvent struct {
	Event struct {
		Start *struct {
			PID int `json:"pid"`
		} `json:"start"`
		// The data event's output oneof is flattened onto the event itself in
		// Connect JSON: {"data":{"stdout":"<base64>"}} — not nested under an
		// "output" key. stdout/stderr are protobuf bytes fields → base64 → []byte.
		Data *struct {
			Stdout []byte `json:"stdout"`
			Stderr []byte `json:"stderr"`
		} `json:"data"`
		// The end event reports {"exited":true,"status":"exit status 0"}: there
		// is no numeric exitCode field, so the code is parsed from status.
		End *struct {
			Exited bool   `json:"exited"`
			Status string `json:"status"`
		} `json:"end"`
	} `json:"event"`
	// End-of-stream trailer message.
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// CommandRun is the launch identity a process carries: the run-as user envd
// applies (sent as the Authorization: Basic <user>: header — the same
// mechanism the SDK's commands.run(user) uses, verified live to spawn the
// process as that uid) and the working directory (processConfig.cwd, field 4,
// verified live). An empty User runs as the envd connection's default user
// (root); an empty Cwd uses that user's home.
type CommandRun struct {
	User string
	Cwd  string
}

// startProcessRequest builds the enveloped process.Process/Start request for
// one command. The run-as user travels as HTTP Basic auth — envd resolves the
// Authorization username to a guest uid before spawning; processConfig has no
// user field (the proto carries only cmd/args/envs/cwd, confirmed against the
// generated descriptor and by live probe). cwd goes in processConfig so the
// harness lands in its intended working directory rather than the envd
// default user's home.
func (c *EnvdClient) startProcessRequest(ctx context.Context, cmd string, envs map[string]string, run CommandRun) (*http.Request, error) {
	msg, err := json.Marshal(processStartRequest{
		Process: processConfig{Cmd: "/bin/bash", Args: []string{"-l", "-c", cmd}, Envs: envs, Cwd: run.Cwd},
		Stdin:   false,
	})
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	body.WriteByte(0x00)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(msg)))
	body.Write(n[:])
	body.Write(msg)
	body.WriteByte(0x02)
	binary.BigEndian.PutUint32(n[:], 2)
	body.Write(n[:])
	body.WriteString("{}")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.ProxyURL+"/process.Process/Start", &body)
	if err != nil {
		return nil, err
	}
	// The run-as user is HTTP Basic auth (username, empty password) — envd's
	// own mechanism, the same one the SDK's commands.run(user) sends. Empty
	// leaves the envd default (root), which the privileged prepare step uses.
	if run.User != "" {
		req.SetBasicAuth(run.User, "")
	}
	// Server-streaming Connect protocol: application/connect+json, not plain
	// application/json (which envd answers 415 to).
	req.Header.Set("Content-Type", "application/connect+json")
	req.Header.Set("Connect-Protocol-Version", "1")
	if c.AccessToken != "" {
		req.Header.Set("X-Access-Token", c.AccessToken)
	}
	c.route(req)
	return req, nil
}

// StartCommand launches a detached command in the bound sandbox through the
// envd process Connect-RPC. It is the bootstrap's activation step: the
// supervisor is started detached (the caller embeds `nohup … &` in cmd) and
// this returns once the process has started — it does not wait for exit, so a
// long-running harness launch does not hold the call. envs are the process
// environment the supervisor launches with; run carries the run-as user and
// working directory.
//
// Wire: Connect-protocol server stream, JSON messages, 5-byte envelopes
// (1 flag byte + 4-byte big-endian length) on BOTH request and response.
// Request = one data envelope (flag 0x00) + one end-stream envelope (flag
// 0x02, {}). Response flag 0x00 = data, 0x02 = end-of-stream trailer carrying
// a JSON error (absent on clean close).
func (c *EnvdClient) StartCommand(ctx context.Context, cmd string, envs map[string]string, run CommandRun) error {
	req, err := c.startProcessRequest(ctx, cmd, envs, run)
	if err != nil {
		return err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &Error{Status: resp.StatusCode, Message: "envd start command: " + string(data)}
	}
	return readProcessStart(resp.Body)
}

// CommandResult is the completed outcome of a synchronous RunCommand: the
// process's collected stdout and stderr and its exit code. envd reports these
// on the process stream's end event.
type CommandResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// RunCommand executes a command to completion in the bound sandbox and returns
// its captured output and exit code. Unlike StartCommand — which detaches for
// a long-running harness — RunCommand drains the process stream until the end
// event, so callers get a synchronous result. This is the transport the /data
// export uses to run `tar` in the guest and wait for the archive.
//
// The command runs through /bin/bash -lc; a non-zero exit is reported in the
// result, not as a Go error, so the caller decides whether it is a failure.
// A stream or protocol failure (envd unreachable, malformed frame) is a Go error.
func (c *EnvdClient) RunCommand(ctx context.Context, cmd string, envs map[string]string, run CommandRun) (*CommandResult, error) {
	req, err := c.startProcessRequest(ctx, cmd, envs, run)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &Error{Status: resp.StatusCode, Message: "envd run command: " + string(data)}
	}
	return readProcessResult(resp.Body)
}

// ExportDataLimit bounds the compressed archive ExportData will return. The
// checkpoint archive is stored in PostgreSQL bytea, so a checkpoint is a
// bounded /data, not uncontrolled growth: the limit is enforced during
// collection — before the caller buffers the whole archive — not only at the
// SQL CHECK afterward. 64 MiB compressed covers a real workspace; a source
// over the bound fails the export rather than truncating or ballooning.
const ExportDataLimit = 64 << 20

// RestoreDataExpandedLimit bounds the uncompressed size RestoreData will
// unpack. A gzip bomb or a corrupt/over-large archive must not fill the
// sandbox disk; the expanded size is measured guest-side before extraction
// and the restore aborts over the bound. Generous headroom over the
// compressed limit since real workspaces compress well.
const RestoreDataExpandedLimit = 512 << 20

// ExportData packs the guest's checkpointable workspace (dir) into a gzip tar
// inside the sandbox and returns the archive bytes. envd has no bulk/directory
// export and caps one file read at envdFileReadCap, so the control plane runs a
// guest `tar`, splits the archive into sub-cap chunks with `split`, and reads
// each chunk in order through signed file reads — keeping envd stock. dir must
// be absolute; the archive preserves the dir's basename as its top entry so
// RestoreData can unpack it back over the same path.
func (c *EnvdClient) ExportData(ctx context.Context, dir string) ([]byte, error) {
	if !strings.HasPrefix(dir, "/") {
		return nil, fmt.Errorf("export dir must be absolute: %q", dir)
	}
	stage := "/tmp/kagent-export-" + uuid.NewString() + ".tgz"
	parent, base := path.Dir(dir), path.Base(dir)
	// Pack, then split beneath the read cap so no single file read truncates.
	// chunkSize is under envdFileReadCap with headroom for the signed read.
	res, err := c.RunCommand(ctx,
		"tar -czf "+stage+" -C "+parent+" "+base+
			" && split -b 4m -d "+stage+" "+stage+".part", nil, CommandRun{})
	if err != nil {
		return nil, fmt.Errorf("export tar: %w", err)
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf("export tar exit %d: %s", res.ExitCode, res.Stderr)
	}
	// List the parts in order plus the whole-archive size and checksum, so the
	// reassembled bytes are verified — a truncated or reordered read is an
	// error, not a corrupt archive. split -d names them stage.part00, part01, …
	parts, err := c.RunCommand(ctx,
		"ls "+stage+".part* 2>/dev/null | sort; "+
			"echo SIZE=$(wc -c < "+stage+"); echo SHA=$(sha256sum "+stage+" | cut -d' ' -f1)", nil, CommandRun{})
	if err != nil {
		return nil, fmt.Errorf("list export parts: %w", err)
	}
	if parts.ExitCode != 0 || strings.TrimSpace(parts.Stdout) == "" {
		return nil, fmt.Errorf("export produced no parts: %s", parts.Stderr)
	}
	defer func() {
		// Best-effort cleanup of the staged archive and parts; the sandbox may
		// already be gone, so an error here is not the export's failure.
		rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = c.RunCommand(rctx, "rm -f "+stage+" "+stage+".part*", nil, CommandRun{})
	}()
	var out bytes.Buffer
	var wantSHA, wantSize string
	// Fail fast on the declared whole-archive size before reading a single
	// part: a source over the bound aborts without buffering it. This is the
	// during-collection enforcement the storage bound requires — the SQL
	// CHECK alone would let an oversized export land before being rejected.
	for part := range strings.FieldsSeq(parts.Stdout) {
		if v, ok := strings.CutPrefix(part, "SIZE="); ok {
			wantSize = v
		}
		if v, ok := strings.CutPrefix(part, "SHA="); ok {
			wantSHA = v
		}
	}
	if wantSize != "" {
		if n, err := strconv.Atoi(wantSize); err == nil && n > ExportDataLimit {
			return nil, fmt.Errorf("export archive %d bytes exceeds the %d-byte checkpoint bound", n, ExportDataLimit)
		}
	}
	for part := range strings.FieldsSeq(parts.Stdout) {
		if strings.HasPrefix(part, "SIZE=") {
			continue
		}
		if strings.HasPrefix(part, "SHA=") {
			continue
		}
		data, err := c.ReadFile(ctx, part, 0)
		if err != nil {
			return nil, fmt.Errorf("read export part %s: %w", part, err)
		}
		// Stop accumulating the moment the buffer crosses the bound; a missing
		// SIZE= trailer cannot let an unbounded archive through.
		if out.Len()+len(data) > ExportDataLimit {
			return nil, fmt.Errorf("export exceeds the %d-byte checkpoint bound during collection", ExportDataLimit)
		}
		out.Write(data)
	}
	got := out.Bytes()
	// Verify byte count and digest against the guest-side values so a silent
	// truncation or missing part surfaces as an error instead of a bad archive.
	if wantSize != "" && strconv.Itoa(len(got)) != wantSize {
		return nil, fmt.Errorf("export reassembled %d bytes, guest archive is %s", len(got), wantSize)
	}
	if wantSHA != "" {
		sum := sha256.Sum256(got)
		if hex.EncodeToString(sum[:]) != wantSHA {
			return nil, fmt.Errorf("export archive checksum mismatch")
		}
	}
	return got, nil
}

// RestoreData unpacks a previously-exported gzip tar back over dir inside the
// sandbox. The archive was created with the dir's basename as its top entry,
// so extracting at the parent restores the tree in place. The destination's
// parent must exist; the command creates it. This is the restore half of the
// /data-export fork path: a fresh sandbox gets the source's workspace then a
// new-generation credential — never a memory clone.
func (c *EnvdClient) RestoreData(ctx context.Context, dir string, archive []byte) error {
	if !strings.HasPrefix(dir, "/") {
		return fmt.Errorf("restore dir must be absolute: %q", dir)
	}
	if len(archive) > ExportDataLimit {
		return fmt.Errorf("restore archive %d bytes exceeds the %d-byte checkpoint bound", len(archive), ExportDataLimit)
	}
	stage := "/tmp/kagent-restore-" + uuid.NewString() + ".tgz"
	// Only reads are capped at envdFileReadCap; writes land whole (verified
	// live: a 20MiB WriteFile round-trips). One write stages the archive.
	if err := c.WriteFile(ctx, stage, archive, 0); err != nil {
		return fmt.Errorf("write restore archive: %w", err)
	}
	parent := path.Dir(dir)
	// Measure the expanded size before extracting so an over-large or hostile
	// archive cannot fill the sandbox disk. Only then unpack in place.
	res, err := c.RunCommand(ctx,
		"EXPANDED=$(gzip -dc "+stage+" | wc -c) && "+
			"[ \"$EXPANDED\" -le "+strconv.Itoa(RestoreDataExpandedLimit)+" ] "+
			"|| { echo 'expanded size '$EXPANDED' exceeds limit'; exit 3; } && "+
			"mkdir -p "+parent+" && tar -xzf "+stage+" -C "+parent+" && rm -f "+stage, nil, CommandRun{})
	if err != nil {
		return fmt.Errorf("restore tar: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("restore tar exit %d: %s", res.ExitCode, res.Stderr)
	}
	return nil
}

// readProcessResult consumes the process stream to the end event, collecting
// stdout/stderr data frames and the terminal exit code. A non-zero exit is a
// result, not a Go error; a missing end event or protocol failure is.
func readProcessResult(r io.Reader) (*CommandResult, error) {
	res := &CommandResult{}
	var stdout, stderr strings.Builder
	hdr := make([]byte, 5)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return nil, fmt.Errorf("envd command stream: %w", err)
		}
		n := binary.BigEndian.Uint32(hdr[1:])
		if n > 1<<22 {
			return nil, fmt.Errorf("envd command stream: oversized frame %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("envd command stream payload: %w", err)
		}
		var ev processEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, fmt.Errorf("envd command stream decode: %w", err)
		}
		if ev.Error != nil {
			return nil, fmt.Errorf("envd command failed: %s: %s", ev.Error.Code, ev.Error.Message)
		}
		if d := ev.Event.Data; d != nil {
			if len(d.Stdout) > 0 {
				stdout.Write(d.Stdout)
			}
			if len(d.Stderr) > 0 {
				stderr.Write(d.Stderr)
			}
			continue
		}
		if end := ev.Event.End; end != nil {
			res.Stdout, res.Stderr = stdout.String(), stderr.String()
			res.ExitCode = exitCodeFromStatus(end.Status)
			return res, nil
		}
	}
}

// exitCodeFromStatus parses the process end event's status string, which is
// the Go-style "exit status N" (or "exit status 0" on success). A parse
// failure or a non-exit status maps to 1 so a crashed/killed command does not
// read as success.
func exitCodeFromStatus(status string) int {
	const prefix = "exit status "
	if rest, ok := strings.CutPrefix(status, prefix); ok {
		if code, err := strconv.Atoi(strings.TrimSpace(rest)); err == nil {
			return code
		}
	}
	return 1
}

// readProcessStart consumes the start stream until the first start event or
// the end-of-stream trailer. It returns once the process is running; any
// later stream frames are the caller's to drain (here none — the body is
// already closed by the caller).
func readProcessStart(r io.Reader) error {
	hdr := make([]byte, 5)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return fmt.Errorf("envd start stream: %w", err)
		}
		n := binary.BigEndian.Uint32(hdr[1:])
		if n > 1<<22 {
			return fmt.Errorf("envd start stream: oversized frame %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return fmt.Errorf("envd start stream payload: %w", err)
		}
		var ev processEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			return fmt.Errorf("envd start stream decode: %w", err)
		}
		if ev.Event.Start != nil {
			return nil // process started; detached, so nothing further to read
		}
		if ev.Error != nil {
			return fmt.Errorf("envd start command failed: %s: %s", ev.Error.Code, ev.Error.Message)
		}
	}
}
