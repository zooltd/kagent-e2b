package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/stretchr/testify/require"
)

// fakeLossStore reports live bindings and records revocations.
type fakeLossStore struct {
	bindings []database.ExecutionInstanceBinding
	revoked  []string
	listErr  error
}

func (s *fakeLossStore) ListLiveE2BExecutionBindings(context.Context) ([]database.ExecutionInstanceBinding, error) {
	return s.bindings, s.listErr
}
func (s *fakeLossStore) RevokeExecutionBinding(_ context.Context, sessionID string) error {
	s.revoked = append(s.revoked, sessionID)
	return nil
}

// fakeLossInspector answers GetSandbox per sandbox id; a nil entry in the map
// means present, an error entry means that error.
type fakeLossInspector struct {
	errs map[string]error
	got  []string
}

func (i *fakeLossInspector) GetSandbox(_ context.Context, sandboxID string) (*e2b.Sandbox, error) {
	i.got = append(i.got, sandboxID)
	if err, ok := i.errs[sandboxID]; ok {
		return nil, err
	}
	return &e2b.Sandbox{SandboxID: sandboxID}, nil
}

func lossLookup(i E2BLossInspector, err error) E2BLossInspectorLookup {
	return func(string) (E2BLossInspector, error) { return i, err }
}

func liveBinding(sessionID uuid.UUID, installation, externalID string) database.ExecutionInstanceBinding {
	return database.ExecutionInstanceBinding{
		SessionID: sessionID, Provider: database.ExecutionProviderE2B,
		Installation: installation, ExternalID: externalID, Generation: 1,
	}
}

// A bound sandbox that 404s revokes its binding — the live credential dies.
func TestLossReaperRevokesGoneSandbox(t *testing.T) {
	sessionID := uuid.New()
	store := &fakeLossStore{bindings: []database.ExecutionInstanceBinding{
		liveBinding(sessionID, "embed-lab", "sb-gone"),
	}}
	inspector := &fakeLossInspector{errs: map[string]error{
		"sb-gone": &e2b.Error{Status: 404, Message: "not found"},
	}}
	r := NewE2BLossReaper(store, lossLookup(inspector, nil))
	r.sweep(context.Background())

	require.Equal(t, []string{sessionID.String()}, store.revoked)
}

// A present sandbox — running or paused — keeps its binding live.
func TestLossReaperKeepsPresentSandbox(t *testing.T) {
	store := &fakeLossStore{bindings: []database.ExecutionInstanceBinding{
		liveBinding(uuid.New(), "embed-lab", "sb-alive"),
	}}
	inspector := &fakeLossInspector{}
	r := NewE2BLossReaper(store, lossLookup(inspector, nil))
	r.sweep(context.Background())

	require.Empty(t, store.revoked)
	require.Equal(t, []string{"sb-alive"}, inspector.got, "the bound sandbox was still verified")
}

// A transient provider failure is not proof of loss — never revoke on
// uncertainty.
func TestLossReaperTransientErrorKeepsBinding(t *testing.T) {
	store := &fakeLossStore{bindings: []database.ExecutionInstanceBinding{
		liveBinding(uuid.New(), "embed-lab", "sb-flaky"),
	}}
	inspector := &fakeLossInspector{errs: map[string]error{
		"sb-flaky": errors.New("connection refused"),
	}}
	r := NewE2BLossReaper(store, lossLookup(inspector, nil))
	r.sweep(context.Background())

	require.Empty(t, store.revoked)
}

// An unresolvable installation fails closed: the binding stays live.
func TestLossReaperUnconfiguredInstallationKeepsBinding(t *testing.T) {
	store := &fakeLossStore{bindings: []database.ExecutionInstanceBinding{
		liveBinding(uuid.New(), "no-such-install", "sb-x"),
	}}
	r := NewE2BLossReaper(store, lossLookup(nil, errors.New("not configured")))
	r.sweep(context.Background())

	require.Empty(t, store.revoked)
}

// One lost binding does not block verification of the rest.
func TestLossReaperMixedBindings(t *testing.T) {
	gone := uuid.New()
	alive := uuid.New()
	store := &fakeLossStore{bindings: []database.ExecutionInstanceBinding{
		liveBinding(gone, "embed-lab", "sb-gone"),
		liveBinding(alive, "embed-lab", "sb-alive"),
	}}
	inspector := &fakeLossInspector{errs: map[string]error{
		"sb-gone": &e2b.Error{Status: 404, Message: "not found"},
	}}
	r := NewE2BLossReaper(store, lossLookup(inspector, nil))
	r.sweep(context.Background())

	require.Equal(t, []string{gone.String()}, store.revoked)
	require.ElementsMatch(t, []string{"sb-gone", "sb-alive"}, inspector.got)
}
