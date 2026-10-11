package e2b

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"
)

// shellesc single-quotes one shell argument for the /bin/bash -lc commands the
// credential refresh issues, so a contract path or owner containing whitespace
// or metacharacters cannot break out of the command.
func shellesc(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// BootstrapCredentialSigner issues the fresh instance credential the bound
// sandbox receives. It is satisfied by
// taskstore.ExecutionAuthenticator.SignExecutionCredential in production; the
// bootstrapper never sees the signing key, only the issued token.
type BootstrapCredentialSigner func(sessionID, installation, externalID string, generation int64, expiresAt time.Time) (string, error)

// BootstrapFiles are the guest paths the supervisor contract reads. The
// config payload is per-revision and passed to Bootstrap at call time; the
// signal and ready-probe steps are pluggable because their mechanism is the
// template/supervisor contract, not this client.
type BootstrapFiles struct {
	ConfigPath     string
	CredentialPath string
	// AgentCardPath is where the per-revision Agent Card JSON is written.
	// The card is revision-scoped data the harness's KAGENT_AGENT_CARD_JSON
	// launch contract reads, so it lands as a contract file alongside config
	// rather than in the operator's static Command. Optional; empty means no
	// card file is written.
	AgentCardPath string
	// IdentityPath is where the runtime session identity is written —
	// "session-<sessionID>" the signed TaskStore reads to attribute persisted
	// tasks. Written before the activate commit point so a guest never
	// activates unable to name its session.
	IdentityPath string
	// CAPath is where the callback CA bundle is written; CAPEM is its PEM
	// content. Both must be set together; neither means the harness relies on
	// system trust for the callback endpoint and no CA file is written.
	CAPath          string
	CAPEM           []byte
	ActivatePayload []byte // when non-nil, written last to signal the supervisor
	ActivatePath    string
}

// Bootstrapper activates a bound sandbox: it writes the revision config and a
// fresh instance credential through envd's files API, signals the supervisor,
// probes readiness, and pauses the sandbox so create completes
// READY-and-paused. It is the envd-backed implementation of the activation
// boundary the session workflow calls after the instance binding commits, so
// only a bound guest ever receives a credential.
type Bootstrapper struct {
	envd          *EnvdClient
	lifecycle     *Client
	sign          BootstrapCredentialSigner
	files         BootstrapFiles
	prepare       *BootstrapPrepare
	command       *BootstrapCommand
	readyProbe    func(ctx context.Context) error
	credentialTTL time.Duration
}

// BootstrapCommand is the detached launch the activation step issues after
// the file contract is written. Cmd runs through the envd process RPC as
// `/bin/bash -lc <Cmd>`; it should detach (e.g. `nohup <harness> &`) so the
// Start call returns once the process is started rather than waiting on a
// long-running harness. Envs are the process environment — config,
// credential path, callback and CA paths the harness reads at start.
type BootstrapCommand struct {
	Cmd string
	// Envs are the contract-owned process environment the launch exports — the
	// E2B wiring (callback/control-plane endpoint, credential path, CA path)
	// plus the contract file paths. They are operator/trusted values that
	// override any same-named revision environment at launch, so a guest's
	// callback and credential wiring can never be redirected by agent config.
	Envs map[string]string
	// User is the guest account the harness runs as (envd's username param).
	// Bundled harnesses run non-root — uid 65532 kagent — so the production
	// launch is not the envd default user (root). Cwd is the working dir the
	// harness lands in (processConfig.cwd) — the upstream contract's
	// /data/workspace, not the envd default home. Both come from the trusted
	// installation harness spec, never agent input.
	User string
	Cwd  string
}

// BootstrapPrepare is the privileged guest-side step run before the harness
// launch, separate from it: it executes as the envd default user (root) so
// it can adjust guest state a non-root harness cannot — e.g. dropping
// net.ipv4.ip_unprivileged_port_start so a non-root process may bind the
// ingress port, or pre-creating the contract directory mode 0700 owned by the
// harness user so a credential written into it is not readable by other guest
// users. It runs synchronously (RunCommand) and must exit 0 before activation
// proceeds.
type BootstrapPrepare struct {
	Cmd string
	// SecureDirs are directories the bootstrap creates mode 0700 owned by the
	// harness user before the file contract is written — the mechanism that
	// makes a credential file inside unreadable to other guest users, not
	// merely owned by the harness user. Ownership alone is not permission
	// enforcement; the 0700 directory is.
	SecureDirs []string
}

// BootstrapperConfig wires the dependencies the bootstrap probe needs. Signer
// issues the instance credential; Envd and Lifecycle reach the bound sandbox
// through the installation's proxy and control-plane API.
type BootstrapperConfig struct {
	Envd      *EnvdClient
	Lifecycle *Client
	Signer    BootstrapCredentialSigner
	Files     BootstrapFiles
	// Prepare is the optional privileged pre-launch step; it runs as the
	// envd default user before the file contract is written and must exit 0.
	Prepare *BootstrapPrepare
	// Command launches the supervisor/harness detached after the file
	// contract is written. Nil skips the process start — a pre-launch
	// milestone that writes the contract but leaves the guest inert.
	Command    *BootstrapCommand
	ReadyProbe func(ctx context.Context) error
	// CredentialTTL bounds the issued credential's lifetime; default 2h.
	CredentialTTL time.Duration
}

// NewBootstrapper builds the activation probe for one bound sandbox.
func NewBootstrapper(cfg BootstrapperConfig) (*Bootstrapper, error) {
	if cfg.Envd == nil {
		return nil, fmt.Errorf("e2b bootstrap: envd client is required")
	}
	if cfg.Signer == nil {
		return nil, fmt.Errorf("e2b bootstrap: credential signer is required")
	}
	if cfg.Files.ConfigPath == "" || cfg.Files.CredentialPath == "" {
		return nil, fmt.Errorf("e2b bootstrap: config and credential guest paths are required")
	}
	ttl := cfg.CredentialTTL
	if ttl <= 0 {
		ttl = 2 * time.Hour
	}
	return &Bootstrapper{envd: cfg.Envd, lifecycle: cfg.Lifecycle, sign: cfg.Signer, files: cfg.Files, prepare: cfg.Prepare, command: cfg.Command, readyProbe: cfg.ReadyProbe, credentialTTL: ttl}, nil
}

// contractOwner is the guest account the contract directory and its files
// belong to — the harness user when a launch is configured, else the envd
// client's file-owner user. Secure directories are created owned by it so a
// credential file inside stays private to that account.
func (b *Bootstrapper) contractOwner() string {
	if b.command != nil && b.command.User != "" {
		return b.command.User
	}
	return b.envd.User
}

// launchEnvs merges the revision's persisted non-secret environment under the
// launch contract's own Envs. Contract-owned values — the callback/broker
// endpoints, contract file paths and the runtime credential location — always
// win over revision-supplied values, so an authored environment cannot hijack
// the control-plane callback or inject credentials the broker is meant to hold.
func (b *Bootstrapper) launchEnvs(revision map[string]string) map[string]string {
	if len(revision) == 0 && len(b.command.Envs) == 0 {
		return nil
	}
	out := make(map[string]string, len(revision)+len(b.command.Envs))
	maps.Copy(out, revision)
	// Contract-owned keys override revision entries; the binding environment is
	// a base layer, never the authority for E2B-owned wiring.
	maps.Copy(out, b.command.Envs)
	return out
}

// Bootstrap runs the activation sequence against the bound sandbox. The
// credential is issued fresh for this binding and written with the config
// before the activation signal, so the supervisor never sees a credential
// without its config. A nil ReadyProbe skips readiness; a nil Lifecycle skips
// the final pause (a pre-pause milestone).
func (b *Bootstrapper) Bootstrap(ctx context.Context, installation, sessionID, externalID string, generation int64, configPayload, agentCardPayload []byte, environment map[string]string) error {
	expiry := time.Now().Add(b.credentialTTL)
	credential, err := b.sign(sessionID, installation, externalID, generation, expiry)
	if err != nil {
		return fmt.Errorf("issue E2B instance credential: %w", err)
	}
	// Privileged preparation runs before the file contract, as the envd
	// default user (root): it adjusts guest state a non-root harness cannot
	// (e.g. the unprivileged-port floor for a non-root ingress bind). It must
	// exit 0 before the guest is trusted with a credential.
	if b.prepare != nil {
		res, err := b.envd.RunCommand(ctx, b.prepare.Cmd, nil, CommandRun{})
		if err != nil {
			return fmt.Errorf("prepare guest: %w", err)
		}
		if res.ExitCode != 0 {
			return fmt.Errorf("prepare guest exit %d: %s", res.ExitCode, res.Stderr)
		}
		// Restrictive contract directories before any write: mode 0700 owned by
		// the harness user, so a credential written inside is not readable by
		// other guest users — enforcement, not just ownership.
		for _, dir := range b.prepare.SecureDirs {
			cmd := fmt.Sprintf("install -d -m 0700 -o %s %s", b.contractOwner(), dir)
			r, err := b.envd.RunCommand(ctx, cmd, nil, CommandRun{})
			if err != nil {
				return fmt.Errorf("secure contract dir %s: %w", dir, err)
			}
			if r.ExitCode != 0 {
				return fmt.Errorf("secure contract dir %s exit %d: %s", dir, r.ExitCode, r.Stderr)
			}
		}
	}
	if err := b.envd.WriteFile(ctx, b.files.ConfigPath, configPayload, 0); err != nil {
		return fmt.Errorf("write harness config: %w", err)
	}
	if b.files.AgentCardPath != "" {
		if err := b.envd.WriteFile(ctx, b.files.AgentCardPath, agentCardPayload, 0); err != nil {
			return fmt.Errorf("write agent card: %w", err)
		}
	}
	if err := b.envd.WriteFile(ctx, b.files.CredentialPath, []byte(credential), 0); err != nil {
		return fmt.Errorf("write instance credential: %w", err)
	}
	// The callback CA bundle lands after the credential and before the
	// activation commit point, matching the documented contract order. With no
	// configured bundle the harness uses system trust and no file is written.
	if b.files.CAPath != "" {
		if err := b.envd.WriteFile(ctx, b.files.CAPath, b.files.CAPEM, 0); err != nil {
			return fmt.Errorf("write callback CA bundle: %w", err)
		}
	}
	// The runtime session identity lands before the activate commit point so
	// the harness's signed TaskStore can attribute persisted tasks to this
	// session — "session-<sessionID>" matching the ADK's identity contract.
	if b.files.IdentityPath != "" {
		if err := b.envd.WriteFile(ctx, b.files.IdentityPath, []byte("session-"+sessionID), 0); err != nil {
			return fmt.Errorf("write runtime identity: %w", err)
		}
	}
	// Signal the supervisor only after config+credential exist, so a guest is
	// never activated without both. The activate payload is the supervisor's
	// contract; writing it last is the commit point.
	if b.files.ActivatePath != "" {
		payload := b.files.ActivatePayload
		if payload == nil {
			payload = []byte(sessionID)
		}
		if err := b.envd.WriteFile(ctx, b.files.ActivatePath, payload, 0); err != nil {
			return fmt.Errorf("signal supervisor: %w", err)
		}
	}
	// Launch the supervisor/harness detached once the file contract is fully
	// written: config, credential and the activation marker all exist before
	// the process starts, so the guest never runs without its identity. A nil
	// command leaves the guest inert (pre-launch milestone).
	if b.command != nil {
		if err := b.envd.StartCommand(ctx, b.command.Cmd, b.launchEnvs(environment), CommandRun{User: b.command.User, Cwd: b.command.Cwd}); err != nil {
			return fmt.Errorf("launch supervisor: %w", err)
		}
	}
	if b.readyProbe != nil {
		if err := b.readyProbe(ctx); err != nil {
			return fmt.Errorf("readiness probe: %w", err)
		}
	}
	if b.lifecycle != nil {
		if err := b.lifecycle.PauseSandbox(ctx, externalID); err != nil {
			return fmt.Errorf("pause sandbox after bootstrap: %w", err)
		}
	}
	return nil
}

// RefreshCredential issues and installs a fresh runtime credential on an
// already-bound, already-running sandbox without touching its binding,
// compute, or launch state. It preserves the session, installation, external
// id and generation so the renewed credential carries the same identity the
// taskstore verifies; only the expiry advances. The write is atomic: the new
// credential lands in a sibling temp file in the protected contract directory
// and is installed over the live path with a single rename, so a concurrently
// reading ADK never sees an empty or partial JWT. A failed refresh leaves the
// previous complete credential in place.
func (b *Bootstrapper) RefreshCredential(ctx context.Context, installation, sessionID, externalID string, generation int64) error {
	expiry := time.Now().Add(b.credentialTTL)
	credential, err := b.sign(sessionID, installation, externalID, generation, expiry)
	if err != nil {
		return fmt.Errorf("issue E2B instance credential: %w", err)
	}
	// Stage in the credential's own directory so the rename stays on one
	// filesystem. A per-refresh unique name keeps concurrent controller
	// replicas from clobbering each other's staged file.
	tmp := fmt.Sprintf("%s.%d.next", b.files.CredentialPath, time.Now().UnixNano())
	if err := b.envd.WriteFile(ctx, tmp, []byte(credential), 0); err != nil {
		return fmt.Errorf("stage refreshed credential: %w", err)
	}
	// Restrict the staged file, then rename it over the live path in one
	// atomic step — a concurrent reader sees either the old or the new
	// complete JWT, never a partial copy. mv is a same-directory rename; on
	// failure the staged file is removed and the live credential is untouched.
	// The whole replace runs as the contract owner (the same user WriteFile
	// wrote the staged file as), never as root: the contract directory is
	// workload-owned, so a root chown/chmod on the staged path could follow a
	// substituted symlink onto another file. Running unprivileged on a path in
	// a directory the harness user owns removes that privilege escalation.
	owner := b.contractOwner()
	cmd := fmt.Sprintf(
		"chmod 0600 %s && mv -f %s %s || { rm -f %s; exit 1; }",
		shellesc(tmp),
		shellesc(tmp), shellesc(b.files.CredentialPath), shellesc(tmp))
	res, err := b.envd.RunCommand(ctx, cmd, nil, CommandRun{User: owner})
	if err != nil {
		return fmt.Errorf("install refreshed credential: %w", err)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("install refreshed credential exit %d: %s", res.ExitCode, res.Stderr)
	}
	return nil
}
