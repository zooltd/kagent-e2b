package database

import (
	"context"
	"testing"

	a2a "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdmissionReportSnapshot seeds one session of each durable-work kind and
// verifies the report inventories them with the right classifications. It is a
// database observation only — no provider state is implied.
func TestAdmissionReportSnapshot(t *testing.T) {
	client := NewClient(setupTestDB(t))
	ctx := context.Background()
	leaveAdmissionsOpen(t, client)

	// A ready Substrate session carrying a WORKING (physically running) task.
	sessionFixture(t, client, ctx, "team-a", "sub-rev", "assistant", "kagent")
	subReady, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.NoError(t, err)
	subReady, err = markSessionReady(ctx, client, subReady.Id, "agent.example")
	require.NoError(t, err)
	working := newSessionTask(uuid.NewString(), "initial")
	working.ContextID = subReady.ContextId
	_, err = client.CreateRuntimeTask(ctx, subReady.Id, taskMutationHash("req"), working, "", nil)
	require.NoError(t, err)

	// A ready Substrate session holding an outstanding dispatch reservation.
	dispatched, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.NoError(t, err)
	dispatched, err = markSessionReady(ctx, client, dispatched.Id, "agent.example")
	require.NoError(t, err)
	dispatchID := uuid.New()
	require.NoError(t, client.ReserveSessionDispatch(ctx, dispatched.Id, dispatchID, "msg-1"))

	// A ready session with a waiting (input-required) task whose settle left a
	// pending quiescence boundary that a worker then claimed but has not
	// finished. Claiming without Finish keeps quiescence_pending set with an
	// executor — the "claimed but unfinished" drain state.
	waiting, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.NoError(t, err)
	waiting, err = markSessionReady(ctx, client, waiting.Id, "agent.example")
	require.NoError(t, err)
	waitingTask := newSessionTask(uuid.NewString(), "ask")
	waitingTask.ContextID = waiting.ContextId
	waitingTask.Status = a2a.TaskStatus{State: a2a.TaskStateInputRequired, Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("which?"))}
	wversion, err := client.CreateRuntimeTask(ctx, waiting.Id, taskMutationHash("ask"), waitingTask, "", nil)
	require.NoError(t, err)
	require.NoError(t, client.SettleSessionTask(ctx, waiting.Id, string(waitingTask.ID), wversion, nil))
	qwork, err := client.ClaimSessionQuiescence(ctx)
	require.NoError(t, err)
	require.Equal(t, waiting.Id, qwork.Session.Id)
	// Deliberately do NOT FinishSessionQuiescence: the boundary stays pending
	// and claimed, which is the drain-relevant state.

	// A bound Substrate session mid-lifecycle operation: CREATE finished (so
	// actor_uid is set), then a SUSPEND began and was claimed but not finished.
	// Created before the E2B revision below so it resolves to the Substrate
	// revision for this agent.
	subBoundOp, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.NoError(t, err)
	subBoundOp, err = markSessionReady(ctx, client, subBoundOp.Id, "agent.example")
	require.NoError(t, err)
	suspendOp, err := client.BeginSessionOperation(ctx, subBoundOp.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	require.NoError(t, err)
	subExecutor := uuid.New()
	_, err = client.ClaimSessionOperation(ctx, subBoundOp.Id, suspendOp.ID, subExecutor)
	require.NoError(t, err)
	// Suspend left unfinished: the open operation is bound via actor_uid.

	// An E2B session still inside an open, claimed CREATE with an issued
	// create-attempt but no binding yet — the reconcile inventory must keep it.
	e2bRevisionFixture(t, client, ctx, "team-a", "e2b-rev", "assistant", "embed-lab")
	e2bOpen := e2bSessionFixture(t, client, ctx, "e2b-rev")
	e2bOp, err := client.BeginSessionOperation(ctx, e2bOpen.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	e2bExecutor := uuid.New()
	_, err = client.ClaimSessionOperation(ctx, e2bOpen.Id, e2bOp.ID, e2bExecutor)
	require.NoError(t, err)
	_, _, err = client.RecordE2BCreateAttempt(ctx, e2bOpen.Id, e2bOp.ID, e2bExecutor, "embed-lab")
	require.NoError(t, err)

	// A READY E2B session with a retained live binding but no running task —
	// idle-but-bound, NOT paused/proven safe.
	e2bBound := e2bSessionFixture(t, client, ctx, "e2b-rev")
	bop, err := client.BeginSessionOperation(ctx, e2bBound.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	bexecutor := uuid.New()
	_, err = client.ClaimSessionOperation(ctx, e2bBound.Id, bop.ID, bexecutor)
	require.NoError(t, err)
	require.NoError(t, client.BindE2BSessionCreate(ctx, e2bBound.Id, bop.ID, bexecutor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sb-bound", Generation: 1,
	}))
	_, err = client.FinishE2BSessionCreate(ctx, e2bBound.Id, bop.ID, bexecutor)
	require.NoError(t, err)

	// Two E2B-bound sessions carrying waiting turns — one INPUT_REQUIRED, one
	// AUTH_REQUIRED. Both are non-terminal and active (waiting is not idle).
	e2bWait := func(state a2a.TaskState, external string) *apiv1alpha1.Session {
		s := e2bSessionFixture(t, client, ctx, "e2b-rev")
		op, err := client.BeginSessionOperation(ctx, s.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
		require.NoError(t, err)
		ex := uuid.New()
		_, err = client.ClaimSessionOperation(ctx, s.Id, op.ID, ex)
		require.NoError(t, err)
		require.NoError(t, client.BindE2BSessionCreate(ctx, s.Id, op.ID, ex, E2BCreateBinding{
			Installation: "embed-lab", ExternalID: external, Generation: 1,
		}))
		_, err = client.FinishE2BSessionCreate(ctx, s.Id, op.ID, ex)
		require.NoError(t, err)
		task := newSessionTask(uuid.NewString(), "ask-"+external)
		task.ContextID = s.ContextId
		// Create as WORKING, then settle to the waiting boundary. saveRuntimeTask
		// runs the full publish+settle path so the projection lands on the
		// waiting state (a first-save boundary would stay SUBMITTED/WORKING).
		task.Status = a2a.TaskStatus{State: state, Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("need input"))}
		require.NoError(t, saveRuntimeTask(t, client, s.Id, task, task, nil))
		return s
	}
	e2bInput := e2bWait(a2a.TaskStateInputRequired, "sb-input")
	e2bAuth := e2bWait(a2a.TaskStateAuthRequired, "sb-auth")

	report, err := client.AdmissionReportSnapshot(ctx)
	require.NoError(t, err)
	assert.False(t, report.AdmissionsClosed)
	assert.False(t, report.Truncated)

	subID, _ := uuid.Parse(subReady.Id)
	waitID, _ := uuid.Parse(waiting.Id)
	dispID, _ := uuid.Parse(dispatched.Id)
	e2bOpenID, _ := uuid.Parse(e2bOpen.Id)
	e2bBoundID, _ := uuid.Parse(e2bBound.Id)
	subBoundOpID, _ := uuid.Parse(subBoundOp.Id)
	e2bInputID, _ := uuid.Parse(e2bInput.Id)
	e2bAuthID, _ := uuid.Parse(e2bAuth.Id)

	var sawWorking, sawWaiting, sawInputE2B, sawAuthE2B bool
	for _, task := range report.NonTerminalTasks {
		if task.SessionID == subID {
			sawWorking = true
			assert.False(t, task.Waiting, "WORKING is running, not waiting")
			assert.Equal(t, "substrate", task.Provider)
		}
		if task.SessionID == waitID {
			sawWaiting = true
			assert.True(t, task.Waiting, "INPUT_REQUIRED is a waiting native turn")
			assert.Equal(t, "TASK_STATE_INPUT_REQUIRED", task.State)
		}
		if task.SessionID == e2bInputID {
			sawInputE2B = true
			assert.True(t, task.Waiting)
			assert.Equal(t, "TASK_STATE_INPUT_REQUIRED", task.State)
			assert.Equal(t, "e2b", task.Provider)
		}
		if task.SessionID == e2bAuthID {
			sawAuthE2B = true
			assert.True(t, task.Waiting)
			assert.Equal(t, "TASK_STATE_AUTH_REQUIRED", task.State)
			assert.Equal(t, "e2b", task.Provider)
		}
	}
	assert.True(t, sawWorking, "running WORKING task must be reported")
	assert.True(t, sawWaiting, "waiting INPUT_REQUIRED task must be reported")
	assert.True(t, sawInputE2B, "E2B INPUT_REQUIRED task must be reported as waiting")
	assert.True(t, sawAuthE2B, "E2B AUTH_REQUIRED task must be reported as waiting")

	var sawOpen bool
	var sawSubBound bool
	for _, op := range report.OpenOperations {
		require.NotEqual(t, e2bBoundID, op.SessionID, "finished CREATE must not be an open operation")
		if op.SessionID == e2bOpenID {
			sawOpen = true
			assert.Equal(t, "RUNTIME_OPERATION_CREATE", op.Operation)
			assert.Equal(t, "e2b", op.Provider)
			assert.False(t, op.Bound, "no live binding yet")
			require.NotNil(t, op.CreateAttemptID, "issued create-attempt must correlate")
			assert.NotNil(t, op.ExecutorExpiresAt, "claim expiry must be reported")
		}
		if op.SessionID == subBoundOpID {
			sawSubBound = true
			assert.Equal(t, "substrate", op.Provider)
			assert.True(t, op.Bound, "a Substrate session with actor_uid is bound")
			assert.Equal(t, "RUNTIME_OPERATION_SUSPEND", op.Operation)
		}
	}
	assert.True(t, sawOpen, "open unbound CREATE must be reported")
	assert.True(t, sawSubBound, "bound Substrate open operation must be reported")

	var sawDispatch bool
	for _, d := range report.DispatchReservations {
		if d.SessionID == dispID {
			sawDispatch = true
			assert.Equal(t, dispatchID, d.DispatchID)
			assert.False(t, d.DispatchExpiresAt.IsZero())
		}
	}
	assert.True(t, sawDispatch, "outstanding dispatch reservation must be reported")

	var sawQuiescence bool
	for _, q := range report.PendingQuiescence {
		if q.SessionID == waitID {
			sawQuiescence = true
			assert.True(t, q.Claimed, "the claimed-but-unfinished pause must read as claimed")
			require.NotNil(t, q.ExecutorID, "a claimed quiescence names its worker")
		}
	}
	assert.True(t, sawQuiescence, "pending quiescence boundary must be reported")

	// The same claimed quiescence is also unsettled work for the drain.
	var sawUnsettled bool
	for _, u := range report.UnsettledEvents {
		if u.SessionID == waitID {
			sawUnsettled = true
			assert.Equal(t, "quiescence_claimed", u.Kind)
		}
	}
	assert.True(t, sawUnsettled, "claimed-but-unfinished quiescence must surface as unsettled")

	// Retained identities cover BOTH providers: the E2B session reports its
	// installation/external id; ready Substrate sessions report their actor_uid.
	var sawBound bool
	sawSubstrate := map[string]bool{}
	for _, b := range report.RetainedBindings {
		if b.SessionID == e2bBoundID {
			sawBound = true
			assert.Equal(t, "e2b", b.Provider)
			assert.Equal(t, "embed-lab", b.Installation)
			assert.Equal(t, "sb-bound", b.ExternalID)
			require.NotNil(t, b.Generation)
			assert.Empty(t, b.ActorUID, "an E2B binding must not fabricate an actor_uid")
			assert.False(t, b.ActiveTask, "idle-but-bound must not read as active")
		}
		for _, subID := range []uuid.UUID{subID, dispID, waitID, subBoundOpID} {
			if b.SessionID == subID {
				sawSubstrate[subID.String()] = true
				assert.Equal(t, "substrate", b.Provider)
				assert.NotEmpty(t, b.ActorUID, "Substrate retained identity is its actor_uid")
				assert.Empty(t, b.Installation, "Substrate must not carry E2B identity fields")
			}
		}
		// E2B waiting turns are active: a session holding INPUT_REQUIRED/
		// AUTH_REQUIRED still has a live runtime identity.
		if b.SessionID == e2bInputID || b.SessionID == e2bAuthID {
			assert.True(t, b.ActiveTask, "an E2B waiting turn is not idle")
		}
	}
	assert.True(t, sawBound, "retained E2B binding must be reported")
	assert.Len(t, sawSubstrate, 4, "every ready Substrate session reports its actor_uid binding")
	// Waiting turn counts as active (the session still holds compute identity).
	for _, b := range report.RetainedBindings {
		if b.SessionID == waitID {
			assert.True(t, b.ActiveTask, "a waiting INPUT_REQUIRED turn is not idle")
		}
		if b.SessionID == subID {
			assert.True(t, b.ActiveTask, "a WORKING task is active")
		}
	}
}

// TestAdmissionReportSnapshotAfterClose verifies the report still reflects the
// barrier state and continues to inventory work while admissions are closed —
// the report observes the barrier; it does not gate on it.
func TestAdmissionReportSnapshotAfterClose(t *testing.T) {
	client := NewClient(setupTestDB(t))
	ctx := context.Background()
	leaveAdmissionsOpen(t, client)
	sessionFixture(t, client, ctx, "team-a", "sub-rev", "assistant", "kagent")

	closed, err := client.SetAdmissionsClosed(ctx, true, "drain")
	require.NoError(t, err)
	require.True(t, closed)
	t.Cleanup(func() {
		_, _ = client.SetAdmissionsClosed(context.Background(), false, "")
	})

	report, err := client.AdmissionReportSnapshot(ctx)
	require.NoError(t, err)
	assert.True(t, report.AdmissionsClosed, "report must reflect the closed barrier")
}

// TestAdmissionReportSnapshotMissingBarrierRow proves a broken schema (no
// barrier row) fails the report rather than producing a clean "open" result.
func TestAdmissionReportSnapshotMissingBarrierRow(t *testing.T) {
	client := NewClient(setupTestDB(t))
	ctx := context.Background()
	_, err := client.db.Exec(ctx, `DELETE FROM admission_barrier WHERE id = 1`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.db.Exec(context.Background(),
			`INSERT INTO admission_barrier (id, closed) VALUES (1, false) ON CONFLICT (id) DO NOTHING`)
	})
	_, err = client.AdmissionReportSnapshot(ctx)
	require.Error(t, err, "a missing barrier row must fail the report, not read as open")
}
