package controller

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/stretchr/testify/require"
)

type fakeRenewStore struct {
	running []database.RunningE2BTask
	err     error
}

func (s *fakeRenewStore) ListE2BSessionsWithRunningTasks(context.Context) ([]database.RunningE2BTask, error) {
	return s.running, s.err
}

type fakeTimeoutSetter struct {
	set    map[string]int32
	gotIDs []string
	err    error
}

func (f *fakeTimeoutSetter) SetSandboxTimeout(_ context.Context, id string, secs int32) error {
	f.gotIDs = append(f.gotIDs, id)
	if f.err != nil {
		return f.err
	}
	if f.set == nil {
		f.set = map[string]int32{}
	}
	f.set[id] = secs
	return nil
}

func TestTimeoutRenewerRenewsRunningTask(t *testing.T) {
	setter := &fakeTimeoutSetter{}
	store := &fakeRenewStore{running: []database.RunningE2BTask{
		{SessionID: uuid.New(), Installation: "embed-lab", ExternalID: "sb-1"},
		{SessionID: uuid.New(), Installation: "embed-lab", ExternalID: "sb-2"},
	}}
	r := NewE2BTimeoutRenewer(store, func(installation string) (E2BTimeoutSetter, int32, error) {
		return setter, 3600, nil
	})
	r.sweep(context.Background())
	require.ElementsMatch(t, []string{"sb-1", "sb-2"}, setter.gotIDs, "each running-task sandbox is renewed")
	require.Equal(t, int32(3600), setter.set["sb-1"])
}

func TestTimeoutRenewerSkipsGoneSandbox(t *testing.T) {
	setter := &fakeTimeoutSetter{err: &e2b.Error{Status: 404, Message: "gone"}}
	store := &fakeRenewStore{running: []database.RunningE2BTask{
		{SessionID: uuid.New(), Installation: "embed-lab", ExternalID: "sb-dead"},
	}}
	r := NewE2BTimeoutRenewer(store, func(string) (E2BTimeoutSetter, int32, error) { return setter, 60, nil })
	// A 404 must not bubble up as an error — the sandbox's loss is reported by
	// the binding/dialer path, not the renewer.
	r.sweep(context.Background())
	require.Equal(t, []string{"sb-dead"}, setter.gotIDs)
}

func TestTimeoutRenewerNoSetterNoop(t *testing.T) {
	store := &fakeRenewStore{running: []database.RunningE2BTask{
		{SessionID: uuid.New(), Installation: "unknown", ExternalID: "sb-x"},
	}}
	r := NewE2BTimeoutRenewer(store, func(string) (E2BTimeoutSetter, int32, error) { return nil, 0, nil })
	r.sweep(context.Background()) // nil setter / zero lifetime is a no-op, not an error
}
