package database

import (
	"context"
	"fmt"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
)

// SessionQuiescence grants one worker permission to pause an idle session or
// suspend it and record its snapshot. Task publication has already completed.
type SessionQuiescence struct {
	Session    *apiv1alpha1.Session
	TaskID     string
	State      a2a.TaskState
	Version    int64
	ExecutorID uuid.UUID
}

// SessionQuiescenceOutcome is the typed result a provider reports for an idle
// boundary: whether a resumable checkpoint was retained or the runtime is
// merely paused with no durable continuation. The store uses it to decide
// whether a snapshot is required and whether checkpoint/fork stays legal.
type SessionQuiescenceOutcome int

const (
	// QuiescenceCheckpointRetained means the provider left a durable
	// checkpoint boundary (Substrate's external snapshot). A snapshot record
	// must accompany the finish.
	QuiescenceCheckpointRetained SessionQuiescenceOutcome = iota
	// QuiescencePausedWithoutCheckpoint means the provider confirmed a pause
	// but retained no resumable checkpoint. Snapshot columns stay NULL and
	// checkpoint/fork returns FailedPrecondition — there is no durable
	// continuation to fork from. This is the E2B filesystem-only-pause result.
	QuiescencePausedWithoutCheckpoint
)

// ClaimSessionQuiescence claims an idle session at its latest settled task
// version. A new turn supersedes unclaimed idle work; a claim blocks task writes,
// checkpoints, and explicit lifecycle operations until it finishes. Missing work
// returns ErrNotFound. Uncertain issued work is never reassigned after a timeout.
func (c *Client) ClaimSessionQuiescence(ctx context.Context) (*SessionQuiescence, error) {
	var result *SessionQuiescence
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		type candidate struct {
			SessionID string
			TaskID    string
			Sequence  int64
		}
		row, err := queryOne(ctx, tx, `
			SELECT i.id::text AS session_id, e.task_id, e.sequence
			FROM session_task_event e JOIN session_record i ON i.history_id = e.history_id
			WHERE e.published AND e.quiescence_pending = TRUE
			  AND e.quiescence_executor_id IS NULL
			  AND i.state = 'RUNTIME_STATE_READY'
			  AND i.operation = 'RUNTIME_OPERATION_NONE'
			  AND (i.dispatch_expires_at IS NULL OR i.dispatch_expires_at <= clock_timestamp())
			  AND NOT EXISTS (SELECT 1 FROM session_checkpoint WHERE source_session_id = i.id AND state = 'CREATING')
			ORDER BY e.sequence LIMIT 1 FOR UPDATE OF i SKIP LOCKED
		`, pgx.RowToStructByName[candidate])
		if err != nil {
			return notFoundOr(err)
		}
		session, err := readSession(ctx, tx, row.SessionID)
		if err != nil {
			return err
		}
		if session.State != "RUNTIME_STATE_READY" || session.Operation != "RUNTIME_OPERATION_NONE" {
			return ErrNotFound
		}
		stored, err := readSessionTask(ctx, tx, session.HistoryID, row.TaskID)
		if err != nil {
			return err
		}
		task, err := unmarshalSessionTask(stored.Data)
		if err != nil {
			return err
		}
		value, err := toSession(session)
		if err != nil {
			return err
		}
		result = &SessionQuiescence{Session: value, TaskID: row.TaskID, State: task.Status.State, Version: row.Sequence, ExecutorID: uuid.New()}
		// Recheck after locking: the candidate query's snapshot may predate a
		// new turn that committed just before we acquired the session lock.
		tag, err := tx.Exec(ctx, `
			UPDATE session_task_event SET quiescence_executor_id = $2
			WHERE sequence = $1 AND published AND quiescence_pending
			  AND quiescence_executor_id IS NULL
			  AND NOT EXISTS (SELECT 1 FROM session WHERE id = $3 AND dispatch_expires_at > clock_timestamp())
			  AND NOT EXISTS (SELECT 1 FROM session_checkpoint WHERE source_session_id = $3 AND state = 'CREATING')
		`, result.Version, result.ExecutorID, session.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrNotFound
		}
		return nil
	})
	return result, err
}

// FinishSessionQuiescence records a claimed pause/suspend outcome and releases
// the session for new execution. Terminal tasks require the matching snapshot.
// Retries are harmless, and a stale claim cannot release another operation.
// Task state and history remain readable throughout lifecycle work.
func (c *Client) FinishSessionQuiescence(ctx context.Context, work *SessionQuiescence, snapshot *SessionTaskSnapshot) error {
	if work == nil || work.ExecutorID == uuid.Nil {
		return fmt.Errorf("claimed task boundary is required")
	}
	if work.State.Terminal() && (snapshot == nil || snapshot.URI == "" || snapshot.Atespace == "" || snapshot.ContentScope == "") {
		return fmt.Errorf("terminal task quiescence requires a runtime snapshot")
	}
	return c.withTx(ctx, func(tx pgx.Tx) error {
		session, err := lockSession(ctx, tx, work.Session.Id)
		if err != nil {
			return notFoundOr(err)
		}
		if session.State == "RUNTIME_STATE_DELETED" {
			return ErrNotFound
		}
		pending, err := queryOne(ctx, tx, `
			SELECT quiescence_pending FROM session_task_event
			WHERE sequence = $1 AND history_id = $2 AND task_id = $3 AND quiescence_executor_id = $4
		`, pgx.RowTo[bool], work.Version, session.HistoryID, work.TaskID, work.ExecutorID)
		if err != nil {
			return notFoundOr(err)
		}
		if !pending {
			return nil
		}
		stored, err := readSessionTask(ctx, tx, session.HistoryID, work.TaskID)
		if err != nil {
			return err
		}
		if stored.State != string(work.State) {
			return ErrConflict
		}
		if snapshot != nil {
			if err := execSQL(ctx, tx, `
				UPDATE session_task SET snapshot_atespace = $3, snapshot_uri = $4,
				    snapshot_content_scope = $5, history_sequence = $6
				WHERE history_id = $1 AND id = $2
			`, session.HistoryID, work.TaskID, snapshot.Atespace, snapshot.URI, snapshot.ContentScope, work.Version); err != nil {
				return err
			}
			if err := execSQL(ctx, tx, `
				UPDATE session_task_event SET snapshot_atespace = $2, snapshot_uri = $3, snapshot_content_scope = $4
				WHERE sequence = $1
			`, work.Version, snapshot.Atespace, snapshot.URI, snapshot.ContentScope); err != nil {
				return err
			}
		}
		return execSQL(ctx, tx, `
			UPDATE session_task_event SET quiescence_pending = FALSE WHERE sequence = $1
		`, work.Version)
	})
}

// FinishE2BSessionQuiescence records an E2B idle boundary as a typed outcome
// rather than a Substrate snapshot. An E2B pause retains no resumable
// checkpoint, so the outcome is always QuiescencePausedWithoutCheckpoint:
// snapshot columns are left NULL and the idle claim is cleared atomically in
// one transaction. Terminal tasks are allowed — there is no retained
// checkpoint to require — and the absence of a session_task_snapshot row is
// what makes a later checkpoint/fork return FailedPrecondition. The claim is
// verified against the same executor binding the Substrate finish uses.
func (c *Client) FinishE2BSessionQuiescence(ctx context.Context, work *SessionQuiescence) error {
	if work == nil || work.ExecutorID == uuid.Nil {
		return fmt.Errorf("claimed task boundary is required")
	}
	return c.withTx(ctx, func(tx pgx.Tx) error {
		session, err := lockSession(ctx, tx, work.Session.Id)
		if err != nil {
			return notFoundOr(err)
		}
		if session.State == "RUNTIME_STATE_DELETED" {
			return ErrNotFound
		}
		pending, err := queryOne(ctx, tx, `
			SELECT quiescence_pending FROM session_task_event
			WHERE sequence = $1 AND history_id = $2 AND task_id = $3 AND quiescence_executor_id = $4
		`, pgx.RowTo[bool], work.Version, session.HistoryID, work.TaskID, work.ExecutorID)
		if err != nil {
			return notFoundOr(err)
		}
		if !pending {
			return nil
		}
		stored, err := readSessionTask(ctx, tx, session.HistoryID, work.TaskID)
		if err != nil {
			return err
		}
		if stored.State != string(work.State) {
			return ErrConflict
		}
		// Confirmed pause with no retained checkpoint: snapshot columns stay
		// NULL and the idle claim clears in the same statement.
		return execSQL(ctx, tx, `
			UPDATE session_task_event SET quiescence_pending = FALSE,
			    snapshot_atespace = NULL, snapshot_uri = NULL, snapshot_content_scope = NULL
			WHERE sequence = $1
		`, work.Version)
	})
}
