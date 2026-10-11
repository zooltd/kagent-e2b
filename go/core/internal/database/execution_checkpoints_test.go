package database

import (
	"context"
	"errors"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
)

// readyE2BSession drives a session to READY bound to an E2B sandbox, then
// settles one terminal task so a checkpointable boundary exists. Returns the
// READY session, the head task ID, and the installation it is bound to.
func readyE2BSession(t *testing.T, client *Client, ctx context.Context) (*apiv1alpha1.Session, string) {
	t.Helper()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sandbox-1", Generation: 1,
	}))
	created, err := client.FinishE2BSessionCreate(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, created.State)

	task := &a2a.Task{ID: "task-1", ContextID: created.ContextId,
		Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}}
	version, err := client.CreateRuntimeTask(ctx, created.Id, taskMutationHash("task-1"), task, "", nil)
	require.NoError(t, err)
	require.NoError(t, client.SettleSessionTask(ctx, created.Id, string(task.ID), version, nil))
	// Clear the idle-quiescence marker the settle left so requireSettledRuntime
	// sees a fully-settled boundary — an E2B finish leaves snapshot NULL.
	work, err := client.ClaimSessionQuiescence(ctx)
	require.NoError(t, err)
	require.NoError(t, client.FinishE2BSessionQuiescence(ctx, work))
	return created, string(task.ID)
}

// An E2B checkpoint reserves a CREATING boundary on an E2B-bound session,
// returns the installation its archive must come from, then commits the
// archive + digest to READY and serves the fork artifact.
func TestE2BCheckpointReserveFinalizeGetArtifact(t *testing.T) {
	enableE2BCheckpointCreation(t)
	client := NewClient(setupTestDB(t))
	ctx := context.Background()
	session, headTask := readyE2BSession(t, client, ctx)

	checkpoint := &apiv1alpha1.Checkpoint{Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: headTask}
	reserved, installation, err := client.ReserveE2BCheckpoint(ctx, checkpoint, "alice", "req-1")
	require.NoError(t, err)
	assert.Equal(t, "embed-lab", installation)
	assert.Equal(t, apiv1alpha1.CheckpointState_CHECKPOINT_STATE_CREATING, reserved.State)
	assert.Equal(t, headTask, reserved.HeadTaskId)

	// A repeated owner/requestID returns the same reservation.
	again, againInst, err := client.ReserveE2BCheckpoint(ctx, checkpoint, "alice", "req-1")
	require.NoError(t, err)
	assert.Equal(t, reserved.Id, again.Id)
	assert.Equal(t, "embed-lab", againInst)

	// A different task for the same request ID conflicts.
	clash := &apiv1alpha1.Checkpoint{Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: "other-task"}
	_, _, err = client.ReserveE2BCheckpoint(ctx, clash, "alice", "req-1")
	require.ErrorIs(t, err, ErrIdempotencyConflict)

	// Committing the archive + its real digest promotes it READY.
	archive := []byte("gzip-archive")
	sha := sha256Sum(archive)
	finalized, err := client.FinalizeE2BCheckpoint(ctx, reserved.Id, archive, sha, "")
	require.NoError(t, err)
	assert.Equal(t, apiv1alpha1.CheckpointState_CHECKPOINT_STATE_READY, finalized.State)

	// The fork artifact carries the archive, digest, and installation.
	art, err := client.GetE2BCheckpointArtifact(ctx, reserved.Id, "alice")
	require.NoError(t, err)
	assert.Equal(t, archive, art.Archive)
	assert.Equal(t, sha, art.ArchiveSHA256)
	assert.Equal(t, "embed-lab", art.Installation)

	// Get on a READY checkpoint succeeds; a wrong owner does not.
	if _, err := client.GetE2BCheckpoint(ctx, reserved.Id, "alice"); err != nil {
		t.Fatalf("GetE2BCheckpoint: %v", err)
	}
	if _, err := client.GetE2BCheckpoint(ctx, reserved.Id, "bob"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unowned checkpoint returned %v", err)
	}
}

// A Substrate session has no E2B binding: the reserve must reject it rather
// than route a checkpoint through the E2B archive path.
func TestE2BCheckpointRejectsSubstrateSession(t *testing.T) {
	enableE2BCheckpointCreation(t)
	client := NewClient(setupTestDB(t))
	ctx := context.Background()
	sessionFixture(t, client, ctx, "team-a", "sub-rev-"+uuid.NewString()[:8], "assistant", "kagent")
	insertReadySessionFixture(t, client.db, "11111111-1111-4111-8111-111111111111", "request-1", []byte{})

	checkpoint := &apiv1alpha1.Checkpoint{Id: uuid.NewString(), SessionId: "11111111-1111-4111-8111-111111111111", HeadTaskId: "task-1"}
	_, _, err := client.ReserveE2BCheckpoint(ctx, checkpoint, "alice", "req-1")
	require.Error(t, err, "a Substrate session must not reserve an E2B checkpoint")
}

// Finalize requires archive + digest together; a READY checkpoint cannot be
// re-finalized to a different artifact.
func TestE2BCheckpointFinalizeRules(t *testing.T) {
	enableE2BCheckpointCreation(t)
	client := NewClient(setupTestDB(t))
	ctx := context.Background()
	session, headTask := readyE2BSession(t, client, ctx)
	checkpoint := &apiv1alpha1.Checkpoint{Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: headTask}
	reserved, _, err := client.ReserveE2BCheckpoint(ctx, checkpoint, "alice", "req-1")
	require.NoError(t, err)

	if _, err := client.FinalizeE2BCheckpoint(ctx, reserved.Id, nil, nil, ""); err == nil {
		t.Fatal("finalize with neither archive nor failure was allowed")
	}
	// A failure carrying an artifact is malformed, not coerced to success.
	if _, err := client.FinalizeE2BCheckpoint(ctx, reserved.Id, []byte("blob"), sha256Sum([]byte("blob")), "export failed"); err == nil {
		t.Fatal("finalize with both archive and failure was allowed")
	}
	// A success carrying a digest that does not match its bytes is rejected.
	if _, err := client.FinalizeE2BCheckpoint(ctx, reserved.Id, []byte("blob"), taskMutationHash("not-the-digest"), ""); err == nil {
		t.Fatal("finalize with a mismatched digest was allowed")
	}
	// Failure path records FAILED.
	failed, err := client.FinalizeE2BCheckpoint(ctx, reserved.Id, nil, nil, "export timed out")
	require.NoError(t, err)
	assert.Equal(t, apiv1alpha1.CheckpointState_CHECKPOINT_STATE_FAILED, failed.State)
	// A FAILED checkpoint yields no artifact.
	_, err = client.GetE2BCheckpointArtifact(ctx, reserved.Id, "alice")
	require.Error(t, err)
}

// enableE2BCheckpointCreation flips the feature flag that gates E2B checkpoint
// reservation so the store mechanics are exercised. It restores the gate on
// test cleanup so the disabled-by-default contract keeps holding for other
// tests in the package.
func enableE2BCheckpointCreation(t *testing.T) {
	t.Helper()
	e2bCheckpointCreationEnabled = true
	t.Cleanup(func() { e2bCheckpointCreationEnabled = false })
}

// E2B checkpoint creation is disabled by default: the reservation fails closed
// until a consistent stopped-writer boundary exists, so a session is never
// frozen for an export whose correctness is undemonstrated.
func TestE2BCheckpointCreationDisabledByDefault(t *testing.T) {
	client := NewClient(setupTestDB(t))
	ctx := context.Background()
	session, headTask := readyE2BSession(t, client, ctx)

	checkpoint := &apiv1alpha1.Checkpoint{Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: headTask}
	_, _, err := client.ReserveE2BCheckpoint(ctx, checkpoint, "alice", "req-1")
	require.ErrorIs(t, err, ErrFailedPrecondition)
}

// A concurrent archive deletion that COMMITS cannot slip past a concurrent
// READY promotion. Transaction A deletes the archive while its parent is
// CREATING and commits; transaction B then promotes the same checkpoint. The
// archive guard serializes the delete on the parent's row lock and the gate
// re-reads the settled archive, so once the delete wins the promote must see
// no artifact and fail — the pair can never leave READY without an archive.
func TestE2BCheckpointDeleteCommittedBlocksPromote(t *testing.T) {
	enableE2BCheckpointCreation(t)
	client := NewClient(setupTestDB(t))
	ctx := context.Background()
	session, headTask := readyE2BSession(t, client, ctx)
	checkpoint := &apiv1alpha1.Checkpoint{Id: uuid.NewString(), SessionId: session.Id, HeadTaskId: headTask}
	reserved, _, err := client.ReserveE2BCheckpoint(ctx, checkpoint, "alice", "req-1")
	require.NoError(t, err)

	archive := []byte("gzip-archive")
	sha := sha256Sum(archive)

	// Stage the archive, then commit its deletion before the promote runs:
	// the delete is settled state the gate must see, not an in-flight write.
	_, err = client.db.Exec(ctx, `
		UPDATE session_checkpoint_archive SET archive = $2, archive_sha256 = $3
		WHERE checkpoint_id = $1`, reserved.Id, archive, sha)
	require.NoError(t, err)
	_, err = client.db.Exec(ctx, `
		DELETE FROM session_checkpoint_archive WHERE checkpoint_id = $1`, reserved.Id)
	require.NoError(t, err)

	// Promoting now must fail: the committed delete left the checkpoint with
	// no artifact, so the READY gate rejects rather than promote past it.
	_, err = client.FinalizeE2BCheckpoint(ctx, reserved.Id, archive, sha, "")
	require.Error(t, err, "promotion over a committed archive delete must fail")
	// The checkpoint is still CREATING, not READY-without-archive.
	got, err := client.GetE2BCheckpoint(ctx, reserved.Id, "alice")
	require.Error(t, err, "a checkpoint whose archive was deleted must not be READY")
	_ = got
}
