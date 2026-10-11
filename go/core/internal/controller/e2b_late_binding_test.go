package controller

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/stretchr/testify/require"
)

// fakeLateStore drives the reconcile sweep: it reports the open operations and
// the artifact binding, and records claim/bind calls.
type fakeLateStore struct {
	opens   []database.OpenE2BCreate
	binding *database.ExecutionRevisionBinding
	claimed bool
	bound   *database.E2BCreateBinding
	claimOK bool
}

func (s *fakeLateStore) ListOpenE2BCreateOperations(context.Context) ([]database.OpenE2BCreate, error) {
	return s.opens, nil
}
func (s *fakeLateStore) GetE2BRevisionBinding(context.Context, string) (*database.ExecutionRevisionBinding, error) {
	return s.binding, nil
}
func (s *fakeLateStore) ClaimSessionOperation(context.Context, string, uuid.UUID, uuid.UUID) (bool, error) {
	s.claimed = true
	return s.claimOK, nil
}
func (s *fakeLateStore) ReleaseRuntimeOperation(context.Context, string, uuid.UUID, uuid.UUID) error {
	return nil
}
func (s *fakeLateStore) BindE2BSessionCreate(_ context.Context, _ string, _ uuid.UUID, _ uuid.UUID, b database.E2BCreateBinding) error {
	s.bound = &b
	return nil
}

type fakeLateLister struct {
	listed  []e2b.ListedSandbox
	gotMeta map[string]string
}

func (l *fakeLateLister) ListSandboxes(_ context.Context, m map[string]string) ([]e2b.ListedSandbox, error) {
	l.gotMeta = m
	return l.listed, nil
}
func (l *fakeLateLister) DeleteSandbox(context.Context, string) error { return nil }

func lateLookup(l *fakeLateLister) E2BOrphanLookup {
	return func(string) (E2BOrphanLister, error) { return l, nil }
}

// A single verified match binds to the open operation — the late-create case.
func TestLateBindingBindsSingleMatch(t *testing.T) {
	opID := uuid.New()
	store := &fakeLateStore{
		opens:   []database.OpenE2BCreate{{SessionID: uuid.New(), OperationID: opID, Revision: "rev-1"}},
		binding: &database.ExecutionRevisionBinding{Installation: "embed-lab", ExternalID: "tpl-abc"},
		claimOK: true,
	}
	lister := &fakeLateLister{listed: []e2b.ListedSandbox{{SandboxID: "sb-late", TemplateID: "tpl-abc"}}}
	r := NewE2BLateBindingReconciler(store, []string{"embed-lab"}, lateLookup(lister))
	r.sweep(context.Background())

	require.True(t, store.claimed, "sweep claims the open operation")
	require.NotNil(t, store.bound, "single verified match binds")
	require.Equal(t, "sb-late", store.bound.ExternalID)
	require.Equal(t, "embed-lab", store.bound.Installation)
	require.Equal(t, opID.String(), lister.gotMeta["kagent.dev/operation"], "list is scoped to the operation tag")
}

// Zero matches is not proof of absence — no claim, no bind, stays pending.
func TestLateBindingZeroMatchesStaysPending(t *testing.T) {
	store := &fakeLateStore{
		opens:   []database.OpenE2BCreate{{SessionID: uuid.New(), OperationID: uuid.New(), Revision: "rev-1"}},
		binding: &database.ExecutionRevisionBinding{Installation: "embed-lab", ExternalID: "tpl-abc"},
	}
	lister := &fakeLateLister{}
	r := NewE2BLateBindingReconciler(store, []string{"embed-lab"}, lateLookup(lister))
	r.sweep(context.Background())

	require.False(t, store.claimed, "zero matches does not claim")
	require.Nil(t, store.bound)
}

// Multiple matches is the orphan reaper's quarantine case — the binding sweep
// must not pick one arbitrarily.
func TestLateBindingMultipleMatchesNoBind(t *testing.T) {
	store := &fakeLateStore{
		opens:   []database.OpenE2BCreate{{SessionID: uuid.New(), OperationID: uuid.New(), Revision: "rev-1"}},
		binding: &database.ExecutionRevisionBinding{Installation: "embed-lab", ExternalID: "tpl-abc"},
	}
	lister := &fakeLateLister{listed: []e2b.ListedSandbox{
		{SandboxID: "sb-a", TemplateID: "tpl-abc"},
		{SandboxID: "sb-b", TemplateID: "tpl-abc"},
	}}
	r := NewE2BLateBindingReconciler(store, []string{"embed-lab"}, lateLookup(lister))
	r.sweep(context.Background())

	require.False(t, store.claimed, "multiple matches does not claim or bind")
	require.Nil(t, store.bound)
}

// A template mismatch is not a verified candidate even under the operation tag.
func TestLateBindingTemplateMismatchIgnored(t *testing.T) {
	store := &fakeLateStore{
		opens:   []database.OpenE2BCreate{{SessionID: uuid.New(), OperationID: uuid.New(), Revision: "rev-1"}},
		binding: &database.ExecutionRevisionBinding{Installation: "embed-lab", ExternalID: "tpl-abc"},
	}
	lister := &fakeLateLister{listed: []e2b.ListedSandbox{{SandboxID: "sb-x", TemplateID: "other-tpl"}}}
	r := NewE2BLateBindingReconciler(store, []string{"embed-lab"}, lateLookup(lister))
	r.sweep(context.Background())

	require.Nil(t, store.bound)
}
