package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/stretchr/testify/require"
)

type fakeCredBindingStore struct {
	bindings []database.E2BCredentialBinding
	err      error
}

func (s *fakeCredBindingStore) ListE2BCredentialBindings(context.Context) ([]database.E2BCredentialBinding, error) {
	return s.bindings, s.err
}

type fakeCredRefresh struct {
	called []string
	fail   map[string]error
	result map[string]bool
}

func (f *fakeCredRefresh) RefreshRunning(_ context.Context, sessionID string) (bool, error) {
	f.called = append(f.called, sessionID)
	if f.fail != nil && f.fail[sessionID] != nil {
		return false, f.fail[sessionID]
	}
	if f.result != nil {
		return f.result[sessionID], nil
	}
	return true, nil
}

func TestCredentialRenewerRefreshesEachLiveBinding(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	store := &fakeCredBindingStore{bindings: []database.E2BCredentialBinding{
		{SessionID: a, Installation: "embed", ExternalID: "sb-1", Generation: 1},
		{SessionID: b, Installation: "embed", ExternalID: "sb-2", Generation: 1},
	}}
	refresh := &fakeCredRefresh{}
	r := NewE2BCredentialRenewer(store, refresh)
	r.sweep(context.Background())
	require.ElementsMatch(t, []string{a.String(), b.String()}, refresh.called,
		"every live binding is offered to the refresher; RefreshRunning gates paused")
}

func TestCredentialRenewerContinuesPastFailure(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	store := &fakeCredBindingStore{bindings: []database.E2BCredentialBinding{
		{SessionID: a, ExternalID: "sb-1"}, {SessionID: b, ExternalID: "sb-2"},
	}}
	refresh := &fakeCredRefresh{fail: map[string]error{a.String(): errors.New("envd unreachable")}}
	r := NewE2BCredentialRenewer(store, refresh)
	r.sweep(context.Background())
	require.ElementsMatch(t, []string{a.String(), b.String()}, refresh.called,
		"one session's failure does not block the rest")
}

func TestCredentialRenewerListErrorAbortsSweep(t *testing.T) {
	store := &fakeCredBindingStore{err: errors.New("db down")}
	refresh := &fakeCredRefresh{}
	r := NewE2BCredentialRenewer(store, refresh)
	r.sweep(context.Background())
	require.Empty(t, refresh.called, "a list failure aborts the sweep rather than refreshing blind")
}

func TestCredentialRenewerNilRefresherNoop(t *testing.T) {
	r := NewE2BCredentialRenewer(&fakeCredBindingStore{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, r.Start(ctx))
}
