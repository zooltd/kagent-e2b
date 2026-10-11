# E2B Checkpoint / Fork Design

How E2B sessions get a DATA-equivalent checkpoint and fork, settled on live
evidence against the pinned Embed stack. This is the design record the
implementation slice builds against; every negative claim below is witnessed,
not assumed. Status: **DEFERRED.** The provider-discriminated `session_checkpoint` schema
+ archive child landed (migration 000003) with all integrity guards proven,
but E2B checkpoint creation stays disabled — no reliable writer-freeze
boundary exists on the pinned Embed stack (see below).

## What E2B cannot do (witnessed)

- **No filesystem-only fork image.** `POST /sandboxes/{id}/snapshots` and
  `POST /sandboxes/{id}/fork` both capture **full memory** — a fork seeded
  with a live heartbeat kept the heartbeat ticking on the child (`forkwitness`).
- **No durable filesystem-only artifact.** `pause({memory:false})` cold-boots
  the *same* sandbox on resume but makes it **unsnappable** — `createSnapshot`
  on a memory-false-paused sandbox returns 404. So there is no way to capture
  a filesystem-only image that outlives the sandbox.
- **Memory clone is not DATA-equivalent.** Per the plan, a full memory clone
  is not a substitute for upstream's fresh-identity DATA restore: it carries
  the source's live process, open connections, and in-memory credential, which
  is exactly the state a fork must *not* share.

## The fork path

```
pause source (quiesce writes)
  -> ExportData(source, /data)          # guest tar -> chunked signed read
  -> create FRESH sandbox from base template   # never the snapshot image
  -> RestoreData(fresh, /data archive)  # write + extract
  -> Bootstrap(fresh)                   # new-generation credential + config + activate
  -> READY
```

- `/data` is the harness's durable root (`/data/{adapter,codex,workspace}` in
  the machine template). A `/data`-restored boot is a valid fresh-identity
  start; the harness re-derives identity from `/run/kagent/*`, never from a
  replayed template start.
- The credential minted at `Bootstrap` is a **new generation** — the fork owns
  its own identity, not the source's. Fresh context/task IDs come from the
  existing `ForkSession` replay semantics.
- envd needs **no extension**: `tar` runs via `RunCommand`, and file
  read/write moves the archive. The only bound is the 8MiB read cap, handled
  by chunked reads (see below).

## Transport (proven)

- `EnvdClient.RunCommand` — synchronous envd exec draining the Connect stream
  to the end event (stdout/stderr/exit). Wire detail that matters: the data
  oneof is *flattened* onto the event (`{"data":{"stdout":"<base64>"}}`), and
  the end event is `{"exited":true,"status":"exit status N"}`.
- `EnvdClient.ExportData(dir)` — guest `tar` + `split` into sub-cap parts,
  ordered signed reads, reassembled and verified by size + sha256.
- `EnvdClient.RestoreData(dir, archive)` — single `WriteFile` (writes are
  uncapped) + `tar -xzf` at the parent.
- `datawitness` PASS live: 20MB `/data` → 13.7MB archive → restored
  byte-for-byte into a fresh sandbox with a new identity.

## Storage

A checkpoint's durable artifact is the exported `/data` archive. There is no
durable fs-only image to point at (above) and no object store in this design,
so the archive lives in **PostgreSQL bytea with a size bound**. ~0.68
compression on random payload (20MB → 13.7MB); a real workspace compresses
better. The bound is a design input — a bounded `/data`, not unbounded state.

## Checkpoint record (decision, revised)

A provider-discriminated `session_checkpoint` row, **not** a fork-owned
`execution_checkpoint` table. The earlier separate-table lean was reversed on
review — and the reversal is the load-bearing correctness call, not a style
preference:

- Every admission guard that freezes a session during a checkpoint keys on
  `session_checkpoint.state = 'CREATING'` — task writes
  (`storeSessionTaskEvent`), dispatch (`ReserveSessionDispatch`), and
  quiescence claims (`ClaimSessionQuiescence`). A separate table inserts
  reservations none of those guards read, letting a checkpoint run
  concurrently with the writes it must freeze.
- `session.pinned_checkpoint_id` already references `session_checkpoint`, so
  an E2B fork pins through the same column and shares fork, authorization and
  admission semantics.

Schema (migration 000003):

- `session_checkpoint.provider` ∈ `substrate|e2b`, default `substrate`.
- The Substrate snapshot columns (`snapshot_atespace/uri/content_scope`,
  `tag_uid`) relax to NULL, with per-provider CHECK constraints: a
  `substrate` row requires them non-NULL, an `e2b` row requires them all NULL
  — so neither provider can masquerade as the other at the column level.
- `session_checkpoint_archive` is a child table, one row per checkpoint,
  holding the E2B-only fields: `archive BYTEA` (NULL while CREATING, ≤64MiB),
  `archive_sha256` (32B), `installation`. A list or get never drags the blob.
- A `READY`-gate trigger enforces "no READY without its artifact" for both
  providers: e2b requires a committed archive+digest, substrate its snapshot
  tag — committed in the same transaction as the state transition.
- `source_session_id` stays provenance, **not** an ownership cascade: a
  durable checkpoint survives its source session's hard delete (idle-timeout
  removes the session row), matching Substrate semantics.

## Consistency boundary (blocking — DEFFERED on review)

A checkpoint is only honest if the exported `/data` is a stable boundary.
Process-scanning freeze attempts are **heuristics, not ownership
boundaries**, and were rejected on review:

- `pause → export` cannot work: a paused sandbox has no compute to run the
  guest `tar`; resuming restarts the writers.
- `envd-pgid` freeze misses `setsid`/re-parented writers (verified live — a
  setsid'd process escapes envd's process group to its own).
- `deny-by-essential` freeze is still a name/pid heuristic: a workload named
  `sshd` escapes the daemon exclusion, a process can spawn after
  enumeration, and re-enumerated thaw can resume foreign-stopped procs.

The only reliable boundary is a **supervisor-owned cgroup** the control plane
freezes via `cgroup.freeze` with confirmed completion, envd and the export
outside that group — a template/supervisor contract, not a control-plane
scan. Until it exists, `e2bCheckpointCreationEnabled` stays `false`,
`ReserveE2BCheckpoint` returns an explicit unsupported `FailedPrecondition`,
and a `substrate`-only `ForkSession` rejects E2B rows. The schema is dormant
but correct; it is not deleted.

## Storage and bounds

PostgreSQL bytea for the bounded archive; no object store, no durable
fs-only image (witnessed absent on Embed). `ExportData` enforces **64MiB
compressed during collection** — aborting before the caller buffers an
oversized archive, so the column CHECK is the last line of defense, not the
first. `RestoreData` refuses an over-bound blob before the write and measures
expanded size guest-side (≤512MiB) before extraction, bounding a hostile or
corrupt archive.

## Remaining work

1. Demonstrate a consistent stopped-writer export boundary, then enable
   `e2bCheckpointCreationEnabled` and wire the create/fork service path.
2. Harness conformance: confirm a `/data`-restored boot reaches READY for
   kagent/Codex (the matrix lanes).
