package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/stretchr/testify/require"
)

type fakeOrphanStore struct {
	bound   map[string]bool // installation+"/"+external -> has live binding
	openOps []uuid.UUID     // operation ids with a still-open CREATE
	// attempts maps operation id -> this DB's create attempt; the fake gate
	// uses it to decide deletion the same way the real serialized gate does.
	attempts map[string]database.E2BCreateAttempt
	err      error
}

func (s *fakeOrphanStore) ListOpenE2BCreateOperations(context.Context) ([]database.OpenE2BCreate, error) {
	if s.err != nil {
		return nil, s.err
	}
	opens := make([]database.OpenE2BCreate, 0, len(s.openOps))
	for _, id := range s.openOps {
		opens = append(opens, database.OpenE2BCreate{OperationID: id})
	}
	return opens, nil
}

// E2BOrphanDeletionGate mimics the serialized deletion fence: it returns false
// (do not delete) for a foreign attempt, a still-open CREATE, a mismatched
// installation/external, or a live binding claiming the external id; true only
// when the sandbox is provably unclaimed and un-adoptable.
func (s *fakeOrphanStore) E2BOrphanDeletionGate(_ context.Context, operationID uuid.UUID, installation, external string) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	a, ok := s.attempts[operationID.String()]
	if !ok {
		return false, nil // foreign tag
	}
	if a.Installation != installation {
		return false, nil // wrong installation
	}
	if a.ExternalID != nil && *a.ExternalID != "" && *a.ExternalID != external {
		return false, nil // attempt names another sandbox
	}
	if s.bound[installation+"/"+external] {
		return false, nil // live binding claims it
	}
	for _, op := range s.openOps {
		if op == operationID {
			return false, nil // still an open CREATE
		}
	}
	return true, nil
}

type fakeOrphanLister struct {
	sandboxes []e2b.ListedSandbox
	deleted   []string
	listErr   error
}

func (l *fakeOrphanLister) ListSandboxes(context.Context, map[string]string) ([]e2b.ListedSandbox, error) {
	if l.listErr != nil {
		return nil, l.listErr
	}
	return l.sandboxes, nil
}

func (l *fakeOrphanLister) DeleteSandbox(_ context.Context, id string) error {
	l.deleted = append(l.deleted, id)
	return nil
}

// owned returns the operation tag the create path stamps, marking a sandbox as
// this deployment's. The reaper only deletes owned sandboxes.
func owned(op string) map[string]string {
	return map[string]string{"kagent.dev/operation": op}
}

func TestOrphanReaperKeepsBoundDeletesOwnedUnbound(t *testing.T) {
	op := uuid.NewString() // not an open CREATE
	store := &fakeOrphanStore{
		bound:    map[string]bool{"embed/sb-kept": true},
		attempts: map[string]database.E2BCreateAttempt{op: {OperationID: uuid.MustParse(op), Installation: "embed"}},
	}
	old := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	lister := &fakeOrphanLister{sandboxes: []e2b.ListedSandbox{
		{SandboxID: "sb-kept", StartedAt: old, Metadata: owned(op)},
		{SandboxID: "sb-orphan", StartedAt: old, Metadata: owned(op)},
	}}
	reaper := NewE2BOrphanReaper(store, []string{"embed"},
		func(string) (E2BOrphanLister, error) { return lister, nil })
	require.NoError(t, reaper.sweepInstallation(context.Background(), "embed", map[string]struct{}{}))
	require.Equal(t, []string{"sb-orphan"}, lister.deleted, "only the owned unbound sandbox is reaped")
}

func TestOrphanReaperNeverDeletesForeignOrCoTenantSandbox(t *testing.T) {
	// A sandbox with no ownership tag was placed by another client; one with a
	// tag that resolves to no create attempt in this DB belongs to a co-tenant
	// deployment. Neither is ever this controller's to delete, however old.
	cotenant := uuid.New() // tag that resolves to NO local attempt
	mismatchInst := uuid.New()
	mismatchExt := uuid.New()
	otherExt := "sb-other"
	store := &fakeOrphanStore{
		bound: map[string]bool{},
		attempts: map[string]database.E2BCreateAttempt{
			mismatchInst.String(): {OperationID: mismatchInst, Installation: "other-install"},               // wrong installation
			mismatchExt.String():  {OperationID: mismatchExt, Installation: "embed", ExternalID: &otherExt}, // names another sandbox
		},
	}
	old := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	lister := &fakeOrphanLister{sandboxes: []e2b.ListedSandbox{
		{SandboxID: "sb-foreign", StartedAt: old}, // no tags
		{SandboxID: "sb-other-key", StartedAt: old, Metadata: map[string]string{"other": "x"}},
		{SandboxID: "sb-cotenant", StartedAt: old, Metadata: owned(cotenant.String())}, // tag not in this DB
		{SandboxID: "sb-badtag", StartedAt: old, Metadata: owned("not-a-uuid")},
		{SandboxID: "sb-wronginst", StartedAt: old, Metadata: owned(mismatchInst.String())}, // attempt in another install
		{SandboxID: "sb-wrongext", StartedAt: old, Metadata: owned(mismatchExt.String())},   // attempt names another sandbox
	}}
	reaper := NewE2BOrphanReaper(store, []string{"embed"},
		func(string) (E2BOrphanLister, error) { return lister, nil })
	require.NoError(t, reaper.sweepInstallation(context.Background(), "embed", map[string]struct{}{}))
	require.Empty(t, lister.deleted, "unowned or foreign-tagged sandboxes are never reaped")
}

func TestOrphanReaperLeavesOpenCreateCandidate(t *testing.T) {
	// A sandbox whose owning operation is still an open CREATE can be adopted by
	// the late-binding sweep; the reaper must not delete it even past the claim
	// timeout, because adoption may commit between the binding lookup and delete.
	op := uuid.New()
	store := &fakeOrphanStore{bound: map[string]bool{}, openOps: []uuid.UUID{op},
		attempts: map[string]database.E2BCreateAttempt{op.String(): {OperationID: op, Installation: "embed"}}}
	old := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	lister := &fakeOrphanLister{sandboxes: []e2b.ListedSandbox{
		{SandboxID: "sb-adoptable", StartedAt: old, Metadata: owned(op.String())},
	}}
	reaper := NewE2BOrphanReaper(store, []string{"embed"},
		func(string) (E2BOrphanLister, error) { return lister, nil })
	openOps := map[string]struct{}{op.String(): {}}
	require.NoError(t, reaper.sweepInstallation(context.Background(), "embed", openOps))
	require.Empty(t, lister.deleted, "a sandbox an open create can adopt is left for binding")
}

func TestOrphanReaperSkipsYoungUnboundSandbox(t *testing.T) {
	// A sandbox younger than the operation timeout may still be committing its
	// binding; it must not be reaped even though no live binding claims it yet.
	op := uuid.NewString()
	store := &fakeOrphanStore{bound: map[string]bool{}, attempts: map[string]database.E2BCreateAttempt{op: {OperationID: uuid.MustParse(op), Installation: "embed"}}}
	young := time.Now().Add(-5 * time.Second).UTC().Format(time.RFC3339)
	old := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	lister := &fakeOrphanLister{sandboxes: []e2b.ListedSandbox{
		{SandboxID: "sb-young", StartedAt: young, Metadata: owned(op)},
		{SandboxID: "sb-old", StartedAt: old, Metadata: owned(op)},
	}}
	reaper := NewE2BOrphanReaper(store, []string{"embed"},
		func(string) (E2BOrphanLister, error) { return lister, nil })
	require.NoError(t, reaper.sweepInstallation(context.Background(), "embed", map[string]struct{}{}))
	require.Equal(t, []string{"sb-old"}, lister.deleted, "in-flight create must not be reaped")
}

func TestOrphanReaperSkipsMalformedStartedAt(t *testing.T) {
	// A missing or unparsed StartedAt cannot establish that a create is no
	// longer in flight; the conservative outcome is to leave the sandbox.
	op := uuid.NewString()
	store := &fakeOrphanStore{bound: map[string]bool{}, attempts: map[string]database.E2BCreateAttempt{op: {OperationID: uuid.MustParse(op), Installation: "embed"}}}
	lister := &fakeOrphanLister{sandboxes: []e2b.ListedSandbox{
		{SandboxID: "sb-nostart", Metadata: owned(op)},
		{SandboxID: "sb-badstart", StartedAt: "not-a-time", Metadata: owned(op)},
	}}
	reaper := NewE2BOrphanReaper(store, []string{"embed"},
		func(string) (E2BOrphanLister, error) { return lister, nil })
	require.NoError(t, reaper.sweepInstallation(context.Background(), "embed", map[string]struct{}{}))
	require.Empty(t, lister.deleted, "unparsable age must not be reaped")
}

func TestOrphanReaperSkipsOnBindingLookupError(t *testing.T) {
	op := uuid.NewString()
	store := &fakeOrphanStore{err: errors.New("db down"), attempts: map[string]database.E2BCreateAttempt{op: {OperationID: uuid.MustParse(op), Installation: "embed"}}}
	old := time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	lister := &fakeOrphanLister{sandboxes: []e2b.ListedSandbox{{SandboxID: "sb-1", StartedAt: old, Metadata: owned(op)}}}
	reaper := NewE2BOrphanReaper(store, []string{"embed"},
		func(string) (E2BOrphanLister, error) { return lister, nil })
	require.NoError(t, reaper.sweepInstallation(context.Background(), "embed", map[string]struct{}{}))
	require.Empty(t, lister.deleted, "indeterminate claim state must not delete")
}

func TestOrphanReaperEmptyInstallationsNoop(t *testing.T) {
	reaper := NewE2BOrphanReaper(&fakeOrphanStore{}, nil,
		func(string) (E2BOrphanLister, error) { return &fakeOrphanLister{}, nil })
	// Start blocks on ctx.Done with no installations; cancel quickly.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, reaper.Start(ctx))
}
