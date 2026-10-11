# E2B Supervisor Contract

The guest-side contract the control plane drives a bound E2B sandbox through.
It is the template/supervisor agreement, not Go API: the bootstrap probe in
`go/core/internal/e2b/bootstrap.go` implements the control-plane half; this
document fixes the guest half so a machine template can be built to satisfy
it. The live witness (`e2b-broker-witness`) already proved every step end to
end against the pinned Embed stack.

## Inert supervisor

The machine template ships a harness plus an **inert supervisor**: a guest
entrypoint that mounts the runtime filesystem, waits for the activation file,
and does **not** read credentials, contact TaskStore, or launch the harness
until the control plane explicitly activates it. On a memory resume the same
process is still alive; on a filesystem-only resume it must re-read identity
from files, not from the (unreplayed) template start command.

## Guest file contract

All activation state lives under `/run/kagent`, written through envd's files
API (port 49983) with the bound sandbox's `envdAccessToken`. The bootstrapper
writes in this order, last write = the commit point:

| Path | Contents | Writer |
| --- | --- | --- |
| `/run/kagent/config` | Compiled harness config JSON (`KAGENT_CONFIG_JSON` payload) | bootstrapper, first |
| `/run/kagent/credential` | Fresh instance credential, generation-scoped, short TTL | bootstrapper, after config |
| `/run/kagent/ca.pem` | CA bundle the harness trusts for the callback TLS endpoint | bootstrapper, optional per install |
| `/run/kagent/identity/name` | Runtime session identity `session-<uuid>` the signed TaskStore reads to attribute persisted tasks | bootstrapper, before activate |
| `/run/kagent/activate` | Session UUID — the activation signal | bootstrapper, **last** |

The supervisor polls (or watches) for `/run/kagent/activate`. Its presence is
the commit point: the supervisor never starts the harness on a guest that has
a credential but no activation file, and a filesystem-only resume re-reads
the file rather than assuming the earlier boot already activated.

## Activation sequence (proven by the witness)

1. Create returns bound. The sequence is split so a failed activation is
   recoverable by retry: the revision + artifact binding commits at
   preparation, the create intent/provider create runs, the instance binding
   commits while CREATE stays open, bootstrap runs, and only then does
   `FinishE2BSessionCreate` commit the final READY. The guest gets nothing
   before the binding commits.
2. Write `/run/kagent/config`, `/run/kagent/credential`,
   `/run/kagent/ca.pem`, `/run/kagent/identity/name`, then
   `/run/kagent/activate` — in that order.
3. Supervisor launches the harness detached with its environment:
   `nohup <harness> <args> </dev/null >/tmp/harness.log 2>&1 &`, envs carrying
   `KAGENT_CONFIG_JSON` (from `config`), `KAGENT_AGENT_CARD_JSON`,
   `KAGENT_API_URL` (callback), `KAGENT_RUNTIME_CREDENTIAL_PATH=/run/kagent/credential`,
   and the CA variable the harness's TLS stack reads (`SSL_CERT_FILE` for Go;
   `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` for the Python gRPC runtime). The
   witness used exactly this shape.
4. Readiness is the harness's `/readyz` on port **80**, probed through the
   client proxy (`E2b-Sandbox-Id` + `E2b-Sandbox-Port: 80`) — the same ingress
   mechanism the gateway dials on. Probe to 200 within the bounded window.
5. `PauseSandbox` so create completes logically READY and physically paused,
   matching the external lifecycle result.

## Status

Steps 1–5 are implemented and witnessed live on the ordinary controller path:
`EnvdClient.WriteFile` writes the contract, `EnvdClient.StartCommand` issues
the detached launch through the envd `process.Process/Start` Connect-RPC
(`/bin/bash -lc "nohup … &"`), `e2bReadyProbe` confirms `/readyz:80`, and
`PauseSandbox` completes READY-and-paused. `BootstrapperConfig.Command` is the
launch step; `ReadyProbe` reuses the dialer's `/readyz` path. Three harnesses
(kagent/Claude, Codex, Python ADK) have run this contract end-to-end on their
own templates; see the tested launch contracts in
[e2b-capability-matrix.md](e2b-capability-matrix.md#tested-launch-contracts).

## Filesystem-only resume

On a filesystem-only resume the supervisor/harness starts before dial and
cannot rely on the template start command replaying. The supervisor therefore
re-derives all identity from `/run/kagent/*` files on every boot: the
credential path is stable, the generation is in the credential, and a fresh
credential is written before any resume that would mint a new generation.
A same-VM memory resume keeps its live identity; expiring credentials refresh
through the per-call reload path without switching generation under a live
process.

## Fork: `/data` export, not memory clone

E2B's own fork primitives do not produce a DATA-equivalent boundary. Verified
live on the pinned Embed stack: `POST /sandboxes/{id}/snapshots` and
`POST /sandboxes/{id}/fork` both capture **full memory** — a fork seeded with
a live heartbeat process kept the heartbeat ticking on the child. The only
filesystem-only variant is `pause({memory:false})`, which cold-boots the
**same** sandbox on resume and yields no separate forkable image. So this
Embed build has no fresh-identity filesystem-only fork primitive, and a memory
clone is not a substitute for upstream's fresh-identity DATA restore.

The DATA-equivalent fork is therefore a clean harness boot plus a retained
`/data` export:

1. Export the source sandbox's `/data` tree (harness workspace state).
2. Boot a **fresh** sandbox from the base machine template — never the
   snapshot image, which would carry memory.
3. Restore `/data` into the fresh guest.
4. Drive the normal contract: fresh `/run/kagent/config`, a **new-generation**
   credential, `ca.pem`, then `activate` last.

The supervisor treats a `/data`-restored boot identically to a filesystem-only
resume: identity comes from `/run/kagent/*` files written at boot, not from
any replayed template start.

The transport is proven and needs no envd extension: `EnvdClient.ExportData`
runs a guest `tar` through `RunCommand` and pulls the archive with a signed
file read, and `RestoreData` writes the archive back and extracts it. Verified
live (`datawitness`): a `/data` tree restored byte-for-byte into a fresh
sandbox.

One transport bound matters: envd caps a single file **read** at 8MiB (writes
are uncapped). `ExportData` therefore splits the archive into sub-cap chunks
and reads each part in order; `RestoreData` uses a single `WriteFile`. A
checkpoint `/data` archive is tens of MB for a real workspace, so the durable
store carries a bounded (not unbounded) bytea payload.

What remains is the checkpoint service path that sequences
pause → export → provision-fresh → restore → fresh-credential → READY, and a
harness that treats a `/data`-restored boot as a valid fresh-identity start.
