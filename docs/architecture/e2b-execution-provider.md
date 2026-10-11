# E2B Execution Provider

This fork can run sessions on a self-hosted E2B (Embed) installation instead of
Substrate Actors. Substrate remains the default; E2B is selectable per
deployment and per prepared revision. This document covers installation
surface, the E2B-only mode, and lifecycle-aware upgrades. The general session
lifecycle is in [runtime-and-lifecycle.md](runtime-and-lifecycle.md).

## Model

- **Execution provider**: `substrate` (default) or `e2b`, recorded on each
  `runtime_revision`. A session's provider comes from its prepared revision,
  never from ambient state.
- **E2B installation**: one named external deployment (Embed today; Cloud or
  another verified API-compatible installation later). Installation identity
  is durable in bindings; endpoint values are operator config.
- **Bindings**: `execution_revision_binding` ties an E2B revision to the
  concrete template/build it runs. `execution_instance_binding` ties a live
  session to its bound sandbox for one generation. Both commit atomically
  with the state transitions they authorize; READY is never visible without
  its instance binding.
- **Logical authority**: an E2B session's runtime authority is
  `e2b://session/<session-uuid>`. It exposes neither the sandbox ID nor
  installation details; those resolve through the private binding.
  `session.actor_uid` stays NULL for E2B sessions.

## Configuration

The controller reads three variables, exposed through `controller.execution`
in `helm/kagent`:

| Value | Environment | Purpose |
| --- | --- | --- |
| `execution.provider` | `KAGENT_EXECUTION_PROVIDER` | Empty/`substrate` (default) or `e2b` for E2B-only. |
| `execution.signingKeyFile` | `KAGENT_EXECUTION_SIGNING_KEY_FILE` | HMAC key file signing E2B runtime credentials; mount from a Secret. |
| `execution.e2b.installations` | `KAGENT_E2B_INSTALLATIONS` | JSON map of installation identity to endpoints. |
| `execution.e2b.sandboxTimeout` | `KAGENT_E2B_SANDBOX_TIMEOUT` | Created sandbox lifetime; timeout-to-pause bound, renewed during running tasks. |

One installation entry:

```json
{"embed-prod": {
  "clientProxy": "http://e2b-proxy.e2b.svc:3002",
  "api": "http://e2b-api.e2b.svc:3000",
  "apiKey": "<control-plane key>",
  "broker": "http://kagent-modelbroker.kagent.svc:8080",
  "callback": "https://kagent-controller.kagent.svc:8083",
  "callbackCABundle": "<PEM for the callback TLS endpoint>",
  "workloads": [{
    "image": "registry.example.com/agent@sha256:<digest>",
    "templateId": "<e2b template id>",
    "buildId": "<e2b ready build id>",
    "harness": {
      "command": "export KAGENT_CONFIG_JSON=\"$(cat /run/kagent/config.json)\" KAGENT_AGENT_CARD_JSON=\"$(cat /run/kagent/agent-card.json)\" KAGENT_RUNTIME_CREDENTIAL_PATH=/run/kagent/credential.jwt SSL_CERT_FILE=/run/kagent/ca.pem; nohup /usr/local/bin/kagent-claude >>/tmp/harness.log 2>&1 &",
      "configPath": "/run/kagent/config.json",
      "agentCardPath": "/run/kagent/agent-card.json",
      "credentialPath": "/run/kagent/credential.jwt",
      "caPath": "/run/kagent/ca.pem",
      "activatePath": "/run/kagent/activate",
      "user": "kagent",
      "cwd": "/data/workspace",
      "prepareCommand": "sysctl -w net.ipv4.ip_unprivileged_port_start=80",
      "modelURLEnv": "ANTHROPIC_BASE_URL"}}]}}
```

- `clientProxy` forwards sandbox traffic and trusts `E2b-Sandbox-*` routing
  headers set server-side by the gateway. Those headers are not
  authentication: this endpoint, like `api`, must be private to trusted
  control-plane clients. Public or guest reachability is a network-boundary
  defect.
- `workloads` maps a compiled harness workload (pinned `image` plus ordered
  `command`/`args`) to the concrete E2B artifact (`templateId`/`buildId`) that
  runs it. Matching is exact; a managed harness with no `command`/`args`
  selects on `image` alone.
- `broker` is the model/MCP credential broker reachable from sandboxes;
  the real credentials live outside the guest. The broker is a
  **per-installation** process: a sandbox can only reach addresses on its
  Embed host's network, so the broker runs co-located with the Embed install
  (e.g. on a VM-private alias), never as a control-plane sidecar the guest
  cannot route to. `callback` is where the sandbox reaches the controller's
  private runtime callback; `callbackCABundle` is the PEM the harness trusts
  for that TLS endpoint.
- `harness` is the machine template's supervisor contract the bootstrap probe
  drives: `command` (detached `nohup` launch), `configPath`, `credentialPath`,
  `caPath`, `activatePath` under `/run/kagent`. See
  [e2b-supervisor-contract.md](e2b-supervisor-contract.md). A create on an
  installation with no configured `harness` (or no `execution.signingKeyFile`)
  fails in preparation before any sandbox is provisioned — READY is never
  reachable without the activation step, so there is no READY-without-harness
  milestone.
- `apiKey` is operator config (the chart renders the map into a ConfigMap;
  use `controller.envFrom` with a Secret if the deployment prefers).

## E2B-only mode

`execution.provider: e2b` installs kagent without Substrate: no ate-api dial,
no Substrate clients, reconcilers, runtime-revision GC, or the
Substrate/guest dialers. Standalone `Sandbox` resources and guest/PTY/file
operations return `FailedPrecondition` in this release; their E2B port is a
separate capability milestone. Reads stay honest — existing objects report
their real state rather than silently degrading. A non-E2B session dialed in
this mode fails explicitly instead of routing nowhere.

Mixed deployments leave `provider` empty and add the installations map:
Substrate stays default and E2B-prepared revisions route through the map —
**but a mixed deployment is not a supported target in this release.** A
signing key switches the single runtime-callback endpoint to signed-only
auth, which the insecure Substrate identity header can never satisfy; setting
`execution.signingKeyFile` while `provider` is empty/Substrate is rejected at
startup. Since an E2B session cannot reach READY without a signing key (the
activation probe has no credential to write), a deployment that needs E2B
must run `execution.provider: e2b`. Running with no signing key at all
leaves the callback endpoint on insecure-header auth — acceptable only for
evaluating the slice, never for a real E2B deployment.

## Limitations in this release

- Checkpoint and fork return `FailedPrecondition` on E2B sessions: idle
  quiescence is a confirmed paused-without-checkpoint outcome, never a
  fabricated Substrate snapshot.
- Sessions cannot be migrated between providers; the provider is fixed at
  revision preparation.
- The gateway does not dial `e2b://` authorities from arbitrary callers;
  E2B ingress goes through the client-proxy path above.
- Successful model execution is proven only for the bounded Claude/OpenRouter
  lane: a completed real-model turn with public incremental SSE delivery
  before completion, a persisted cancellation outcome, and a completed turn
  after an observed pause on the same bound sandbox. That evidence is
  harness-scoped — it covers Claude on the recorded template, not Codex,
  Python, or BYO, whose real-model turns remain unproven. The UI is excluded
  from this claim and the run used local network bridges between the host,
  the Lima Embed VM, and the cluster, not a general deployment topology.
- `KAGENT_GATEWAY_URL` reachability is untested: it compiles to the in-cluster
  controller address, which the sandbox cannot route; no consumer reads it on
  the TaskStore/callback path.
- Helm installation conformance passed on an isolated release: the chart-
  installed controller reached Ready, ran the E2B lifecycle (ordinary
  Agent → create → bootstrap → signed-callback task → delete). The controller
  reached Ready after two startup retries caused by PostgreSQL connection
  refusal. A bounded restart witness then restarted the controller mid-life:
  the same session kept its bound sandbox (unchanged `external_id` +
  generation), persisted a second distinct terminal task through the new
  pod's signed callback, and deleted cleanly. This establishes restart
  recovery only — not version-upgrade or general maintenance safety. All of
  this ran behind witness-local network bridges, so it is not a claim about
  a general multi-cluster or HA install.
- E2B Cloud conformance is out of scope; capability is per-installation, not
  inferred across Embed deployments.

## Lifecycle-aware upgrades

Restart and loss behavior are now **witnessed**; *version-upgrade* safety is
the part that is not. A bounded witness restarted the controller mid-life on
the same image: a paused bound session kept its `external_id` + generation,
persisted a second distinct terminal task on the new pod's signed callback,
and deleted cleanly. A separate provider-deletion witness proved the loss
path: deleting the bound sandbox through the provider API (bypassing the
control plane) led the production loss reaper to revoke the binding, after
which ordinary dispatch returned the explicit runtime-loss signal
("session runtime lost: e2b runtime compute lost"), prior task history stayed
readable, and no replacement compute appeared. These ran behind witness-local
bridges and say nothing about multi-cluster or HA installs.

**What is unproven:** a version upgrade — a new controller image or a new
Embed build — has not been exercised, and in-flight work across any restart
was not covered. The fork does not promise uninterrupted rolling upgrades.
Workspace survival across an Embed rebuild is **not established** — there is
no retained checkpoint in this release, and whether a rebuild preserves a
paused sandbox's workspace has not been verified either way. Whether a
version upgrade is disruptive to running or paused compute is likewise not
established; only the same-image restart and the provider-deletion loss paths
above are witnessed. Until the drain / upgrade / reconcile sequence is
exercised end to end, treat any Embed or
control-plane restart as potentially disruptive to in-flight work.

A bounded maintenance witness has now exercised the operator-led sequence end
to end for a **settled, paused** session. Using the `admission_barrier` plus
the read-only drain report (`kagent db admissions close` / `report` /
`status` / `open`), an operator: closed admissions; observed the drain report
reach its GO condition (no non-terminal tasks, open operations, dispatch
reservations, unsettled events, or pending quiescence — only the expected
retained binding, provider-confirmed `paused`); performed a verified
same-image `rollout restart` (old pod terminated, replacement Ready on the
same image digest); reconciled while still closed (binding `external_id` +
generation unchanged, provider inventory still showed the single paused
sandbox → classified **surviving**); then reopened and persisted a second
distinct terminal task on the **same** sandbox through the signed callback,
and deleted cleanly. This is the close → settle → restart → reconcile →
reopen → continuation sequence. It remains scoped to a paused, drained
session — it does not cover active or waiting tasks, a version upgrade, or
workspace durability across an Embed rebuild, and it ran behind witness-local
bridges.

- **Keep the callback and credential-renewal paths available during a drain.**
  Scaling the controller to zero removes the TaskStore callback service and
  the timeout/credential renewal workers that active tasks and paused
  sandboxes depend on; it must not be used as the admission barrier. Likewise,
  making the installation's `api` endpoint unreachable is not a demonstrated
  admission barrier and severs the reconcile/reaper paths.
- **Drain or settle sessions** — let running tasks finish, or let idle
  sessions reach their timeout-to-pause bound, while the control plane stays
  up. Workspace durability across the maintenance action is not established
  (no retained checkpoint exists in this release), so delete sessions whose
  state must not be claimed durable rather than assuming a paused sandbox
  survives a rebuild.
- **Upgrade Embed** using the commit-pinned upstream package plus minimal
  overlays; do not copy the Embed stack into this repository, and keep its
  databases separate from kagent's.
- **Reconcile** — after restart, the loss reaper revokes bindings whose
  sandboxes are gone (their credentials die with them), and the late-binding
  reconciler adopts any sandbox whose create response was lost to a still-open
  operation. Orphan cleanup is ownership-gated, not a sweep of every unbound
  sandbox: `E2BOrphanDeletionGate` serializes the decision on the owning
  session's row lock and refuses a sandbox that is bound, belongs to a
  still-open CREATE, is foreign, or is named by a different recorded attempt,
  so only durably-unclaimed compute is reaped.
- **Reopen admissions** and re-prepare revisions. A surviving PostgreSQL
  Session row proves control-plane history survival only — not that native
  workspace state survived. Sessions whose sandboxes are gone report runtime
  loss through accurate status, not silent recreation.

## Upstream sync

The patch lane stays narrow: provider discrimination on `runtime_revision`,
fork-owned `execution_*` tables and reconcilers, the E2B branches in session
workflow and the gateway dialer, and the `execution.provider` gate. Upstream
`000001` migrations, Substrate defaults, and the standalone Sandbox surface
are unchanged so upstream merges stay textual.
