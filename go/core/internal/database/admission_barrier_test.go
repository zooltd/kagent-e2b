package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a2aproject/a2a-go/v2/a2a"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
)

// leaveAdmissionsOpen restores the global barrier after a test closes it, so a
// failure mid-test cannot leak a closed barrier into other tests sharing the
// database. Register it in any test that calls SetAdmissionsClosed(true, ...).
// The reopen is bounded so a stuck barrier lock cannot hang cleanup forever.
func leaveAdmissionsOpen(t *testing.T, client *Client) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.SetAdmissionsClosed(ctx, false, "")
	})
}

// TestAdmissionsClosedRejectsNewSession proves a closed barrier rejects the
// durable session-insert admission — for BOTH providers — while leaving the
// store readable. Rejection happens inside insertSession's transaction, so no
// partial session row is left behind.
func TestAdmissionsClosedRejectsNewSession(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	leaveAdmissionsOpen(t, client)
	sessionFixture(t, client, ctx, "team-a", "sub-rev", "assistant", "kagent")
	e2bRevisionFixture(t, client, ctx, "team-a", "e2b-rev", "e2b-agent", "embed-lab")

	closed, err := client.SetAdmissionsClosed(ctx, true, "drain")
	require.NoError(t, err)
	require.True(t, closed)

	// Substrate create
	_, _, err = client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.ErrorIs(t, err, ErrAdmissionClosed)
	// E2B create
	_, _, err = client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "e2b-agent", "kagent", ""), uuid.NewString())
	require.ErrorIs(t, err, ErrAdmissionClosed)

	// No partial rows were written.
	var n int
	require.NoError(t, client.db.QueryRow(ctx, `SELECT count(*) FROM session_record`).Scan(&n))
	assert.Equal(t, 0, n, "closed admission must not leave a partial session row")
}

// TestAdmissionsClosedBlocksNewDispatchButNotRetry proves the dispatch barrier
// gates a NEW turn while the already-accepted-message retry still resolves —
// the recovery-vs-admission distinction the drain depends on.
func TestAdmissionsClosedBlocksNewDispatchButNotRetry(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	leaveAdmissionsOpen(t, client)
	sessionFixture(t, client, ctx, "team-a", "sub-rev", "assistant", "kagent")
	created, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), "req")
	require.NoError(t, err)
	ready, err := markSessionReady(ctx, client, created.GetId(), "actor.example")
	require.NoError(t, err)

	// Persist a task whose history message carries the accepted id, so the
	// retry short-circuit (session_task_event.message_id) fires.
	accepted := newSessionTask(uuid.NewString(), "msg-accepted")
	accepted.ContextID = ready.ContextId
	_, err = client.CreateRuntimeTask(ctx, ready.Id, taskMutationHash("accepted"), accepted, "", nil)
	require.NoError(t, err)
	// Return the task to a non-active state so a NEW dispatch is not blocked by
	// the active-task conflict check instead of the barrier. InputRequired is a
	// settle boundary but needs no snapshot, unlike a terminal state.
	accepted.Status.State = a2a.TaskStateInputRequired
	require.NoError(t, saveRuntimeTask(t, client, ready.Id, accepted, accepted, nil))

	closed, err := client.SetAdmissionsClosed(ctx, true, "drain")
	require.NoError(t, err)
	require.True(t, closed)

	// A NEW dispatch is refused.
	err = client.ReserveSessionDispatch(ctx, ready.Id, uuid.New(), "msg-new")
	require.ErrorIs(t, err, ErrAdmissionClosed)

	// A retry of the already-accepted message short-circuits BEFORE the barrier
	// check — recovery of accepted work is not blocked by closure.
	err = client.ReserveSessionDispatch(ctx, ready.Id, uuid.New(), "msg-accepted")
	require.ErrorIs(t, err, ErrMessageAccepted, "retry of an accepted message must not be blocked by closure")
}

// TestAdmissionsClosedBlocksNewResume proves RESUME (new work) honors the
// barrier while the not-admitting retry path (needed==false) is untouched.
func TestAdmissionsClosedBlocksNewResume(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	leaveAdmissionsOpen(t, client)
	sessionFixture(t, client, ctx, "team-a", "sub-rev", "assistant", "kagent")
	created, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), "req")
	require.NoError(t, err)
	ready, err := markSessionReady(ctx, client, created.GetId(), "actor.example")
	require.NoError(t, err)

	// Drive the session to SUSPENDED so RESUME is the admitted operation.
	suspend, err := client.BeginSessionOperation(ctx, ready.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
	require.NoError(t, err)
	ex := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, ready.Id, suspend.ID, ex)
	require.NoError(t, err)
	require.True(t, claimed)
	_, err = client.FinishSessionOperation(ctx, ready.Id, suspend.ID, ex, "", "", "")
	require.NoError(t, err)

	closed, err := client.SetAdmissionsClosed(ctx, true, "drain")
	require.NoError(t, err)
	require.True(t, closed)

	// New RESUME is refused by the barrier.
	_, err = client.BeginSessionOperation(ctx, ready.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
	require.ErrorIs(t, err, ErrAdmissionClosed)

	// DELETE must remain open during drain — settlement is not blocked.
	_, err = client.BeginSessionOperation(ctx, ready.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.NoError(t, err, "delete of an existing session must not be blocked by closure")
}

// TestAdmissionsReopenRestoresAdmission proves reopening the barrier restores
// new admission and that the flag persists across a fresh store instance.
func TestAdmissionsReopenRestoresAdmission(t *testing.T) {
	db := setupTestDB(t)
	client := NewClient(db, "public")
	ctx := context.Background()
	leaveAdmissionsOpen(t, client)
	sessionFixture(t, client, ctx, "team-a", "sub-rev", "assistant", "kagent")

	_, err := client.SetAdmissionsClosed(ctx, true, "drain")
	require.NoError(t, err)
	_, _, err = client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), "req")
	require.ErrorIs(t, err, ErrAdmissionClosed)

	// Reopen, then create succeeds.
	closed, err := client.SetAdmissionsClosed(ctx, false, "")
	require.NoError(t, err)
	require.False(t, closed)
	_, created, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), "req2")
	require.NoError(t, err)
	require.True(t, created)
}

// TestAdmissionsClosedPersistsAcrossStoreInstance proves the flag is durable
// state, not process-local: a fresh Client over the same database sees the
// closed barrier and enforces it.
func TestAdmissionsClosedPersistsAcrossStoreInstance(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	first := NewClient(db, "public")
	leaveAdmissionsOpen(t, first)
	_, err := first.SetAdmissionsClosed(ctx, true, "drain")
	require.NoError(t, err)

	second := NewClient(db, "public") // fresh handle over the same pool — no shared memory
	closed, err := second.AdmissionsClosed(ctx)
	require.NoError(t, err)
	require.True(t, closed, "closed state must persist across store instances")
}

// TestAdmissionsClosedRejectsFork proves a checkpoint fork — which inserts its
// session via insertSessionRecords directly, not insertSession — still honors
// the barrier. It would otherwise be a silent new-admission bypass.
func TestAdmissionsClosedRejectsFork(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	leaveAdmissionsOpen(t, client)
	sessionFixture(t, client, ctx, "team-a", "sub-rev", "assistant", "kagent")

	created, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), "req")
	require.NoError(t, err)
	source, err := markSessionReady(ctx, client, created.GetId(), "actor.example")
	require.NoError(t, err)

	// Completed task supplies the checkpoint's terminal boundary.
	task := newSessionTask("task-1", "message-1")
	task.ContextID = source.GetContextId()
	require.NoError(t, saveRuntimeTask(t, client, source.Id, task, task,
		&SessionTaskSnapshot{Atespace: "team-a", URI: "s3://snapshots/snap-1", ContentScope: "DATA"}))
	task.Status.State = a2a.TaskStateCompleted
	require.NoError(t, saveRuntimeTask(t, client, source.Id, task, task,
		&SessionTaskSnapshot{Atespace: "team-a", URI: "s3://snapshots/snap-1", ContentScope: "DATA"}))

	checkpoint, _, err := client.ReserveSessionCheckpoint(ctx, &apiv1alpha1.Checkpoint{
		Id: uuid.NewString(), SessionId: source.GetId(), HeadTaskId: "task-1",
	}, "alice", "cp-request")
	require.NoError(t, err)
	_, err = client.FinalizeSessionCheckpoint(ctx, checkpoint.GetId(), "tag-uid", "s3://tags/cp", "")
	require.NoError(t, err)

	closed, err := client.SetAdmissionsClosed(ctx, true, "drain")
	require.NoError(t, err)
	require.True(t, closed)

	// The fork admits a brand-new session; it must be refused while closed.
	_, _, err = client.ForkSession(ctx, checkpoint.GetId(), "alice", "fork-request", uuid.NewString())
	require.ErrorIs(t, err, ErrAdmissionClosed)

	var n int
	require.NoError(t, client.db.QueryRow(ctx, `SELECT count(*) FROM session_record`).Scan(&n))
	assert.Equal(t, 1, n, "closed admission must not leave a partial fork session row")
}

// waitForBlockedQuery polls pg_stat_activity until a backend running a query
// matching queryLike is observed waiting on another pid's lock — explicit lock
// observation, not a sleep. Returns true once the wait is seen.
func waitForBlockedQuery(t *testing.T, db *pgxpool.Pool, ctx context.Context, queryLike string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting bool
		err := db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND query LIKE $1
				  AND cardinality(pg_blocking_pids(pid)) > 0)
		`, queryLike).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond,
		"expected a backend blocked on %q", queryLike)
}

// TestAdmissionsCloseSerializesWithAdmission proves the shared barrier row lock
// serializes close and admit in BOTH directions, with the wait observed in
// pg_stat_activity rather than assumed.
//
// Ordering A: a transaction that has already taken the barrier FOR UPDATE lock
// (what an admitting transaction does in requireAdmissionsOpen) blocks a
// concurrent close until it commits — a close can never commit underneath an
// in-flight admission.
//
// Ordering B: a transaction holding the lock and closing (uncommitted) blocks a
// concurrent admission; once the close commits, the queued admission observes
// closed and is refused with no partial writes.
func TestAdmissionsCloseSerializesWithAdmission(t *testing.T) {
	db := setupTestDB(t)
	client := NewClient(db, "public")
	// Bound every database call and channel wait so a stuck lock fails the test
	// instead of hanging the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	leaveAdmissionsOpen(t, client)
	sessionFixture(t, client, ctx, "team-a", "sub-rev", "assistant", "kagent")

	// ---- Ordering A: admission's barrier hold blocks close until it commits.
	holder, err := db.Begin(ctx)
	require.NoError(t, err)
	// Register the rollback AFTER leaveAdmissionsOpen so it runs first (LIFO):
	// if a later assertion FailNows, the holder still releases the barrier lock
	// before the reopen cleanup tries to take it.
	t.Cleanup(func() { _ = holder.Rollback(context.Background()) })
	var locked bool
	require.NoError(t, holder.QueryRow(ctx,
		`SELECT closed FROM admission_barrier WHERE id = 1 FOR UPDATE`).Scan(&locked))
	require.False(t, locked)

	closeDone := make(chan error, 1)
	go func() {
		closed, err := client.SetAdmissionsClosed(ctx, true, "drain")
		if err == nil && !closed {
			err = fmt.Errorf("close returned false")
		}
		closeDone <- err
	}()
	// The closer's UPDATE must be observed waiting on the holder's lock.
	waitForBlockedQuery(t, db, ctx, "%UPDATE admission_barrier%")
	require.NoError(t, holder.Commit(ctx))
	select {
	case err := <-closeDone:
		require.NoError(t, err, "close must complete once the admission's lock releases")
	case <-ctx.Done():
		t.Fatal("close did not complete after the barrier lock released")
	}
	_, err = client.SetAdmissionsClosed(ctx, false, "") // reopen for ordering B
	require.NoError(t, err)

	// ---- Ordering B: an uncommitted close blocks admission, which then fails.
	holder, err = db.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Rollback(context.Background()) })
	require.NoError(t, holder.QueryRow(ctx,
		`SELECT closed FROM admission_barrier WHERE id = 1 FOR UPDATE`).Scan(&locked))
	_, err = holder.Exec(ctx,
		`UPDATE admission_barrier SET closed = true, closed_at = now(), reason = 'drain' WHERE id = 1`)
	require.NoError(t, err)

	admitted := make(chan error, 1)
	go func() {
		_, _, err := client.CreateSession(ctx,
			newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
		admitted <- err
	}()
	// The admission's requireAdmissionsOpen SELECT ... FOR UPDATE must be
	// observed waiting on the uncommitted close.
	waitForBlockedQuery(t, db, ctx, "%SELECT closed FROM admission_barrier%FOR UPDATE%")
	require.NoError(t, holder.Commit(ctx))

	select {
	case err := <-admitted:
		require.ErrorIs(t, err, ErrAdmissionClosed,
			"admission queued behind a closing commit must observe the closed barrier")
	case <-ctx.Done():
		t.Fatal("admission did not resolve after the close committed")
	}
	var n int
	require.NoError(t, client.db.QueryRow(ctx, `SELECT count(*) FROM session_record`).Scan(&n))
	assert.Equal(t, 0, n, "a refused admission must leave no partial session row")
}

// TestAdmissionsClosedMissingRowIsError proves the observation read does not
// report "open" when the singleton row is absent. The migration creates the
// table and its row together, so a missing row means a broken/unmigrated
// schema — surfacing the error is safer than a false "open" that a drain
// check would trust.
func TestAdmissionsClosedMissingRowIsError(t *testing.T) {
	db := setupTestDB(t)
	client := NewClient(db, "public")
	ctx := context.Background()

	// Remove and restore the singleton row around the read.
	_, err := db.Exec(ctx, `DELETE FROM admission_barrier WHERE id = 1`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(),
			`INSERT INTO admission_barrier (id, closed) VALUES (1, false) ON CONFLICT (id) DO NOTHING`)
	})

	closed, err := client.AdmissionsClosed(ctx)
	require.Error(t, err, "a missing barrier row must surface an error, not report open")
	assert.False(t, closed)
}

// TestCheckAdmissionsOpenSerializesWithClose proves the transactional admission
// decision used by runtime-backed subscriptions takes the same barrier lock as
// durable admissions: an uncommitted close blocks it, and once that close
// commits the check observes closed and is refused. This is the serialization
// the maintenance subscription contract relies on — the lock is held only for
// the decision, released before any dial.
func TestCheckAdmissionsOpenSerializesWithClose(t *testing.T) {
	db := setupTestDB(t)
	client := NewClient(db, "public")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	leaveAdmissionsOpen(t, client)

	// An open barrier admits immediately.
	require.NoError(t, client.CheckAdmissionsOpen(ctx))

	// Hold the barrier FOR UPDATE and close it, uncommitted.
	holder, err := db.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Rollback(context.Background()) })
	var locked bool
	require.NoError(t, holder.QueryRow(ctx,
		`SELECT closed FROM admission_barrier WHERE id = 1 FOR UPDATE`).Scan(&locked))
	_, err = holder.Exec(ctx,
		`UPDATE admission_barrier SET closed = true, closed_at = now(), reason = 'drain' WHERE id = 1`)
	require.NoError(t, err)

	checkDone := make(chan error, 1)
	go func() { checkDone <- client.CheckAdmissionsOpen(ctx) }()
	// The check's SELECT ... FOR UPDATE must be observed waiting on the close.
	waitForBlockedQuery(t, db, ctx, "%SELECT closed FROM admission_barrier%FOR UPDATE%")
	require.NoError(t, holder.Commit(ctx))

	select {
	case err := <-checkDone:
		require.ErrorIs(t, err, ErrAdmissionClosed,
			"an admission decision queued behind a closing commit must observe the closed barrier")
	case <-ctx.Done():
		t.Fatal("CheckAdmissionsOpen did not resolve after the close committed")
	}
}
