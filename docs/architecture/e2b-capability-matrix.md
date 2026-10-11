# E2B Capability Matrix

What this fork's E2B execution provider actually does today, per surface, with
the evidence behind each claim. This is the P4 exit record: every row is
either backed by a named test/witness or explicitly marked unsupported — never
inferred from Substrate parity and never extrapolated to E2B Cloud.

Legend — **proven**: backed by a named Go test or the live witness on the
pinned Embed stack. **rejected**: returns an explicit error rather than
silently degrading or fabricating a Substrate artifact. **absent**: no code
path; the operation is rejected before reaching any provider call.

## Session lifecycle

| Operation | E2B | Evidence |
| --- | --- | --- |
| Create (prepare revision → provision → READY) | proven | `BindE2BSessionCreate` commits the binding while CREATE stays open, then `activateBoundSandbox`, then `FinishE2BSessionCreate` -> READY; a create-attempt record prevents a retry re-issuing CreateSandbox. workflow tests in `workflow_e2b_test.go` |
| Delete | proven | `e2bDelete` → `FinishE2BSessionDelete`; tolerates already-gone sandbox |
| Suspend (explicit) | proven | `e2bSuspend` → `FinishSessionOperation` → SUSPENDED, keeps live binding |
| Resume (explicit) | proven | `e2bResume` → `POST /sandboxes/{id}/connect`; 404 = runtime loss, never recreates |
| Idle quiescence (pause) | proven | `FinishE2BSessionQuiescence` pauses the bound sandbox beneath a still-READY session; `TestE2BIdleSessionPausesWithoutCheckpoint` |
| Second turn after quiescence | proven | paused sandbox auto-resumes on client-proxy ingress; no Resume op needed |
| Checkpoint / fork | **rejected** | `FailedPrecondition` — idle quiescence is paused-without-checkpoint; no fabricated snapshot. E2B's `createSnapshot`/`/fork` are **memory clones** (proven live: a heartbeat kept ticking on the fork), which the plan does not treat as DATA-equivalent; a fresh-identity fork needs the `/data`-export path |

## Deployment (controller-driven)

| Surface | E2B | Evidence |
| --- | --- | --- |
| Ordinary `Agent`/`Harness` → prepared revision → READY session | proven | E2B-only `app.Run` on a clean cluster (kagent CRDs only, no Substrate CRDs/services): reconciler compiled the revision, resolved the workload→artifact mapping, verified `GetTemplateBuildStatus`, recorded `execution_revision_binding` + `runtime_revision` (`provider=e2b`), Agent `Ready`; `SessionService.Create` → `e2bCreate` → bind → bootstrap → `READY`. Live witness on the Lima Embed |
| E2B-only controller startup without Substrate | proven | every KRT collection synced with no Substrate CRDs; the `WorkerPools` input is an empty static collection (`workerPoolCollection`), not an informer that would block on a resource that does not exist. `collections_test.go` + live boot |
| Revision preparation | proven | `newAgentReconciliations` E2B branch digests via `Revision.E2BDigest` and calls `RecordE2BRuntimeRevision`; artifact readiness gated on `GetTemplateBuildStatus`, unverifiable/unready → pending, never promoted |
| Paused-sandbox resume on next turn | proven | second `SendMessage` woke the same bound sandbox (`autoResume`, `onTimeout=pause`) — no replacement compute, generation unchanged |

## Pause semantics

| Pause trigger | Memory | Workspace | Evidence |
| --- | --- | --- | --- |
| Idle quiescence, `INPUT_REQUIRED`, `AUTH_REQUIRED` | preserved (`autoPauseMemory`) | preserved | live witness SB1: paused sandbox resumed same-id with heartbeat still ticking |
| Explicit Suspend op | preserved | preserved | `e2bSuspend` uses the memory-preserving pause |
| Filesystem-only pause | rootfs retained, process lost | `pause({memory:false})` cold-boots the **same** sandbox; produces no forkable image |
| Forkable snapshot image | **absent** | `createSnapshot`/`/fork` capture full memory (live witness: heartbeat survived on the fork); no fresh-identity filesystem-only fork primitive on this Embed build |
| `/data` export / restore transport | proven | guest `tar` via `RunCommand` + signed file read/write, stock envd; live `datawitness` restored a `/data` tree byte-for-byte into a fresh sandbox |

`autoPauseMemory: true` is set on create. A genuine filesystem-only resume for
checkpoint/fork is P4 follow-up and correctly out of scope for pause.

## Compute identity and loss

| Behavior | E2B | Evidence |
| --- | --- | --- |
| Runtime authority | `e2b://session/<session-uuid>` | exposes neither sandbox ID nor installation; resolves through the private binding |
| `session.actor_uid` | NULL | E2B revisions hold NULL ActorTemplate fields by constraint |
| Runtime loss detection | proven | 404 on resume/connect = loss; `LossReaper` revokes the binding and kills its credential |
| Orphaned-sandbox sweep | proven | `OrphanReaper` deletes unbound sandboxes |
| Lost-create-response adoption | proven | `LateBindingReconciler` adopts a sandbox whose create response was lost to a still-open operation via `FinishE2BSessionCreate` |
| Timeout renewal | proven | `TimeoutRenewer` extends the sandbox lifetime bound during running tasks |
| Silent recreation after loss | **rejected** | a gone sandbox is reported as runtime loss; never replaced under the same session |

## Harness activation

| Step | E2B | Evidence |
| --- | --- | --- |
| Config/credential/CA/activate file writes | proven | live witness: envd files API on `:49983` with `envdAccessToken` |
| Detached harness launch | proven | live witness: Connect-RPC `process.Process/Start`, `nohup` detached |
| Readiness probe | proven | `e2bReadyProbe` polls `/readyz:80` through the client proxy routing headers |
| Bootstrap ordering (activate last = commit point) | proven | `Bootstrapper` writes config → credential → ca → activate |

## Model/MCP credential broker

| Placement | E2B | Evidence |
| --- | --- | --- |
| Per-installation, co-located with Embed | proven | a sandbox reaches only its Embed host's network; `brokerAddress` is a VM-private alias the broker listens on |
| Control-plane sidecar | **rejected** | unreachable from the guest |

## Unsupported in this release

| Surface | State |
| --- | --- |
| Standalone `Sandbox` resources | rejected — `FailedPrecondition` |
| Guest / PTY / file operations | rejected — `FailedPrecondition` |
| Cross-provider session migration | absent — provider is fixed at revision preparation |
| Gateway dialing of `e2b://` from arbitrary callers | absent — E2B ingress goes through the client-proxy path only |
| Uninterrupted Embed rolling upgrade | absent — launcher shutdown ends running compute; see lifecycle-aware upgrades |
| E2B Cloud conformance | untested — no Cloud/Embed capability inference; a separate installation binding + conformance lane is P4 |

## Harness matrix status

| Harness | E2B |
| --- | --- |
| kagent | proven end-to-end via the supervisor contract |
| Codex | runtime witnessed on template `f7q55eor8yys7st64ia5`: bounded public ingress (`ProviderDialer` → `E2BDialer` → client proxy) → A2A → signed gRPC callback → persisted terminal task; forged routing headers and revoked bindings rejected. Real model turn pending |
| Claude | runtime/A2A/callback integration witnessed on template `ktxkakuh5t3yaj4alu80`; a bounded real-model turn completed via OpenRouter (`claude-sonnet-4-5`), including public incremental SSE delivery before completion, a persisted cancellation outcome, and a completed turn after an observed pause on the same bound sandbox |
| Python (kagent ADK) | runtime witnessed on template `lnueoiduh0j8xq5okdji` build `f66d2e9c-376a-4ec1-afc5-04b51e7c842e` through the ordinary controller path: reconciliation → verified artifact → create → bootstrap (`kagent-adk static`, uid 65532, `/data/workspace`, `/readyz:80`) → signed gRPC callback → two same-sandbox turns → persisted `TASK_STATE_FAILED` (dead model) → delete. `KAgentTaskStore` honors `KAGENT_RUNTIME_CREDENTIAL_PATH` (signed-bearer mode, insecure header never sent); callback CA trusted via `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH`. Real model turn pending |
| BYO | untested — no BYO template/witness exercised; capability is a per-harness launch contract, not a blanket claim |

Native approval continuations require tested memory/recovery guarantees;
unsupported harness/provider combinations are reported explicitly rather than
assumed equivalent to Substrate.

## Tested launch contracts

Each proven harness runs under a workload launch contract recorded on the
revision binding (`E2BHarnessSpec`). The contract owns the guest wiring: the
bootstrapper writes config/credential/CA/activate files and merges the
contract env over the compiled revision env before launching the `command`
via `/bin/bash -l -c`. Common shape across all three:

| Field | Value |
| --- | --- |
| `user` / `cwd` | `kagent` (uid 65532) / `/data/workspace` |
| `credentialPath` | `/run/kagent/credential.jwt` (signed E2B instance credential) |
| `caPath` | `/run/kagent/ca.pem` (callback CA bundle) |
| `activatePath` | `/run/kagent/activate` (written last = commit point) |
| `prepareCommand` | `sysctl -w net.ipv4.ip_unprivileged_port_start=80` |
| `modelURLEnv` | model base-url var sourced from the installation broker |

| Harness | `configPath` / `agentCardPath` | Entrypoint | Contract env additions |
| --- | --- | --- | --- |
| kagent (Claude) | `/run/kagent/config.json` / `/run/kagent/agent-card.json` | `kagent-claude` | `KAGENT_CONFIG_JSON`, `KAGENT_AGENT_CARD_JSON`, `KAGENT_RUNTIME_CREDENTIAL_PATH`, `SSL_CERT_FILE` (Go), `ANTHROPIC_API_KEY`, `ANTHROPIC_MAX_RETRIES` |
| Codex | `/run/kagent/*` | `kagent-codex` | same shape; Go CA env |
| Python (kagent ADK) | `/config/config.json` / `/config/agent-card.json` | `kagent-adk static --filepath /config --host 0.0.0.0 --port 80` | `KAGENT_CONFIG_JSON`, `KAGENT_AGENT_CARD_JSON`, `KAGENT_RUNTIME_CREDENTIAL_PATH`, `KAGENT_PORT=8082` (gRPC vs HTTP:80 split), `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` (Python gRPC CA trust, not `SSL_CERT_FILE`) |

The model base-url variable is `ANTHROPIC_BASE_URL` for the Anthropic-backed
witnesses; it resolves to the installation broker (`modelURLEnv`).

`KAGENT_API_URL` (signed callback) and `KAGENT_NAME`/`KAGENT_NAMESPACE`/
`KAGENT_GATEWAY_URL` arrive from the compiled revision env; the contract
overrides `KAGENT_API_URL`. `KAGENT_GATEWAY_URL` remains an in-cluster value
with no consumer on the TaskStore path — recorded untested, not a proven path.
