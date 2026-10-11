# E2B Upstream Sync Checklist

The seams that must be inspected on every upstream merge, even when Git merges
cleanly. These are the places the fork deliberately diverges from upstream;
a text-clean merge can still be a semantic conflict. Keep the patch lane
narrow so merges stay textual where possible.

## Merge-sensitive seams

| Seam | What the fork changes | Check on each sync |
| --- | --- | --- |
| Revision digest | `provider` is part of a revision's identity | upstream digest inputs must not silently drop or absorb `provider` |
| Session admission and claims | E2B sessions route through provider, not actor lookup | `actor_uid` NULL for E2B must not break a shared claim path |
| TaskStore identity | `e2b://session/<uuid>` authority | upstream TaskStore changes must not assume an actor host |
| Checkpoint / history boundaries | E2B has no retained DATA checkpoint | fork must not fabricate a Substrate snapshot upstream expects |
| Migrations | fork-owned `execution` track, own ledger + lock | never a reserved version in upstream's `schema_migrations` |
| Controller startup | `execution.provider` gate, E2B reconcilers | E2B-only mode must not require Substrate clients |
| CRD validation | E2B is config-driven; no E2B field is published | confirm no E2B field leaked into a published CRD (`controller-manifests` drift check covers the generated surface) |
| Gateway dialer | `RuntimeDialer` parses Substrate actor hosts only | E2B ingress stays off the Substrate dial path |

## Migration rules (hard constraints)

- Extension migrations live in `pkg/migrations/execution/` with their own
  `execution_schema_migrations` tracking table and advisory lock, applied via
  `app.Options.ExtraMigrations` after built-in sources.
- Never put a high reserved version (e.g. `900001`) into upstream's ledger —
  later lower-numbered upstream migrations become out-of-order.
- Extension migrations own the new `execution_*` tables and their documented
  adjustments to core constraints (`runtime_revision` provider/NULL columns).
  Core overlap stays a reviewed compatibility boundary; a separate ledger does
  not eliminate it.
- Do not rewrite `000001` or enable global allow-missing as a workaround.
- Test both real paths on each sync that touches either track:
  1. fresh upstream → extension
  2. existing extension → new upstream → next extension
  using the actual runner, including crash/restart and inline-SQL preparation.
- If a core change needs a prerequisite bridge, deploy/test the bridge before
  the core upgrade; otherwise fail the sync.

## Rollback posture

E2B objects are unpublished and fork-owned, so there is no deployed-release
compatibility floor for them. The execution track's Down refuses to roll back
while E2B rows remain (they have no Substrate identity to restore into), and
restores the pre-provider shape when none do. This is a deliberate
clean-install posture, not a gap: see `execution_down_test.go`.

## Generated code

Regenerate CRDs, protobufs and clients from merged source definitions on every
sync. Generated outputs are never the source of truth — do not hand-resolve
conflicts in them.

## Compatibility lane

The seams in the table above are exercised by the repository's existing checks
rather than a parallel suite: `go test -race ./...` with envtest assets
(covering every package below, including the real-Postgres migration and
backend-contract tests via testcontainers), `make generate` for generated
output completeness, and `make controller-manifests` plus a git-diff gate on a
clean checkout for CRD/RBAC drift. No live Embed deployment, Kubernetes
cluster, or model credentials are required.

What the lane proves:

- Both real migration-track upgrade paths plus crash/restart, the execution
  track's own ledger + advisory lock, and inline-SQL preparation against the
  migrated schema (`core/pkg/migrations`, `core/internal/database`). The
  core↔vector cross-track ownership rule is enforced by `TestNoCrossTrackDDL`;
  the execution track is intentionally exempt because it ALTERs the
  upstream-owned `runtime_revision` table (the provider column) as the
  documented compatibility boundary.
- The shared backend contract on the E2B seam: revision digest carries
  `provider`, NULL `actor_*` handling, session create/suspend/resume/delete,
  quiescence, credential renewal and reclaim sweeps, signed TaskStore
  credential accept/reject, `ProviderDialer` authority routing, and the
  fail-closed `DisabledActorClient` + Sandbox/guest/PTY/file surface.
- Generated-output completeness on the CRD/RBAC/deepcopy seam — including a
  generated file that regeneration re-adds as untracked, which a
  clean-checkout git-diff gate cannot see (git diff does catch tracked-file
  edits and deletions; the blind spot is new untracked additions).

What it does not prove:

- Compatibility with any specific future upstream release. The upgrade tests
  drive the real runner against synthetic next-version fixtures — ordering
  and locking machinery, not a particular upstream delta. A text-clean merge
  can still be a semantic conflict; the seam table above still requires review
  on every sync.
- E2B-only controller startup (no ate-api dial). `app.Run` wires the provider
  gate and disabled-actor stub inline; there is no unit test for it in
  `pkg/app`, and the property is established by the bounded live witnesses,
  not this lane.
- Live Embed conformance, gateway reachability, or model-execution success.
  Those stay with the bounded live witnesses and the capability matrix.

## Compatibility record

A merge candidate records the tested compatibility line alongside the change:
kagent commit, Substrate pin, Embed package commit + component pins, SDK
version, harness images, template builds, and accepted capabilities. Upgrade
kagent and E2B in separate changes unless a documented compatibility
dependency requires otherwise. Cloud and Embed have separate acceptance
lanes; one passing does not qualify the other.
