package database

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file holds the E2B half of the shared session_checkpoint record. Per
// the provider-discriminated schema (migration 000003), an E2B checkpoint is
// a session_checkpoint row with provider='e2b' whose artifact is an exported
// /data archive in session_checkpoint_archive, not an ate-api snapshot. It
// reuses the shared row so task-admission, lifecycle and quiescence guards —
// all of which key on session_checkpoint.state = 'CREATING' — apply to E2B
// checkpoints exactly as they do to Substrate ones.

// E2BCheckpointArtifact is what a fork needs beyond the Checkpoint proto: the
// exported /data archive, its digest, and the installation it restores onto.
// Returned only on the fork read — never to a list or get.
type E2BCheckpointArtifact struct {
	Archive       []byte
	ArchiveSHA256 []byte
	Installation  string
}

// e2bCheckpointCreationEnabled gates E2B checkpoint reservation. It is false —
// and a var, not a const, so the gate is a runtime feature flag rather than
// dead code — until a consistent stopped-writer boundary is demonstrated.
// The reservation would otherwise freeze admission while the source still runs
// writers, and a guest tar over a live workspace captures an inconsistent
// /data; the copy witness proved transport, not a stable boundary. Pause-then-
// export cannot work either: a paused sandbox has no compute to run the tar,
// and resuming to export restarts the writers. Reserve fails closed until a
// consistent boundary exists, then this flips true.
var e2bCheckpointCreationEnabled = false

// e2bCheckpointArchiveBound is the compressed /data archive size a checkpoint
// retains, matching the envd client's during-collection ExportDataLimit and
// the session_checkpoint_archive column CHECK. 64 MiB.
const e2bCheckpointArchiveBound = 64 << 20

// sha256Sum returns the 32-byte digest FinalizeE2BCheckpoint verifies the
// caller's archive against, so a stored sha always proves the bytes present.
func sha256Sum(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

// ReserveE2BCheckpoint captures the terminal task named by HeadTaskId on an
// E2B-bound session, provided it is still the latest boundary, and reserves
// the checkpoint as a CREATING session_checkpoint row — blocking new task
// writes and lifecycle while the caller exports /data. The session must be
// READY with a live E2B instance binding; a Substrate session has none and is
// rejected, not silently routed. A repeated owner/requestID returns the same
// reservation for the same source and task, or ErrIdempotencyConflict
// otherwise. Returns the reserved checkpoint and the installation the archive
// must come from.
func (c *Client) ReserveE2BCheckpoint(ctx context.Context, checkpoint *apiv1alpha1.Checkpoint, userID, requestID string) (*apiv1alpha1.Checkpoint, string, error) {
	if !e2bCheckpointCreationEnabled {
		return nil, "", fmt.Errorf("E2B checkpoint creation is not yet supported: %w", ErrFailedPrecondition)
	}
	if checkpoint == nil {
		return nil, "", fmt.Errorf("missing checkpoint")
	}
	sourceID, err := uuid.Parse(checkpoint.GetSessionId())
	if err != nil {
		return nil, "", fmt.Errorf("invalid source session ID: %w", err)
	}
	var result *apiv1alpha1.Checkpoint
	var installation string
	err = c.withTx(ctx, func(tx pgx.Tx) error {
		existing, err := readCheckpointRequest(ctx, tx, userID, requestID)
		if err == nil {
			if existing.Provider != ExecutionProviderE2B || existing.SourceSessionID != sourceID || existing.HeadTaskID != checkpoint.HeadTaskId {
				return ErrIdempotencyConflict
			}
			var err error
			installation, err = readCheckpointArchiveInstallation(ctx, tx, existing.ID)
			if err != nil {
				return err
			}
			result, err = toSessionCheckpoint(existing)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("get E2B checkpoint by request: %w", err)
		}

		session, err := lockSession(ctx, tx, checkpoint.GetSessionId())
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && (session.UserID != userID || session.State == apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED.String())) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock Session %s: %w", checkpoint.GetSessionId(), err)
		}
		if _, err := toSession(session); err != nil {
			return err
		}
		if session.State != apiv1alpha1.RuntimeState_RUNTIME_STATE_READY.String() || session.Operation != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE.String() {
			return fmt.Errorf("session %s cannot checkpoint in state %s with operation %s: %w", checkpoint.GetSessionId(), session.State, session.Operation, ErrConflict)
		}
		// The source must be an E2B session: its live instance binding carries
		// the installation the /data archive comes from. A Substrate session
		// has no E2B binding and is rejected, not silently routed.
		binding, err := liveExecutionBindingForUpdate(ctx, tx, checkpoint.GetSessionId())
		if err != nil {
			return fmt.Errorf("get session execution binding: %w", err)
		}
		if binding == nil || binding.Provider != ExecutionProviderE2B {
			return fmt.Errorf("session %s is not bound to an E2B execution: %w", checkpoint.GetSessionId(), ErrFailedPrecondition)
		}
		installation = binding.Installation

		type head struct {
			TaskID   string
			State    string
			Sequence int64
		}
		current, err := queryOne(ctx, tx, `
			SELECT e.task_id, t.state, e.sequence FROM session_task_event e
			JOIN session_task t ON t.history_id = e.history_id AND t.id = e.task_id
			WHERE e.history_id = $1 ORDER BY e.sequence DESC LIMIT 1
		`, pgx.RowToStructByName[head], session.HistoryID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrFailedPrecondition
		}
		if err != nil {
			return err
		}
		if current.TaskID != checkpoint.HeadTaskId {
			return ErrCheckpointAdvanced
		}
		if !a2a.TaskState(current.State).Terminal() {
			return fmt.Errorf("checkpoint requires a terminal task: %w", ErrFailedPrecondition)
		}
		pending, err := queryOne(ctx, tx, `
            SELECT EXISTS (SELECT 1 FROM session_task WHERE history_id = $1
                AND state NOT IN ('TASK_STATE_COMPLETED', 'TASK_STATE_FAILED', 'TASK_STATE_CANCELED', 'TASK_STATE_REJECTED'))
        `, pgx.RowTo[bool], session.HistoryID)
		if err != nil {
			return err
		}
		if pending {
			return fmt.Errorf("checkpoint requires all tasks to be terminal: %w", ErrFailedPrecondition)
		}
		if err := requireSettledRuntime(ctx, tx, session.HistoryID, ""); err != nil {
			if errors.Is(err, ErrFailedPrecondition) {
				return ErrSnapshotPending
			}
			return err
		}
		if session.PreparedRevision != nil {
			if _, err := getAvailableRuntimeRevisionForUpdate(ctx, tx, *session.PreparedRevision); err != nil {
				return err
			}
		}
		// The E2B boundary is the terminal task's own event sequence — the
		// immutable A2A cutoff. Unlike the Substrate reserve there is no prior
		// snapshot boundary to validate: the artifact is the /data archive
		// committed at finalize. history_sequence is NOT set on the
		// session_task for E2B (FinishE2BSessionQuiescence leaves snapshot and
		// sequence NULL by design), so the boundary comes from current.
		if _, err := readSessionTask(ctx, tx, session.HistoryID, current.TaskID); err != nil {
			return fmt.Errorf("get latest Session task boundary: %w", err)
		}
		boundarySequence := current.Sequence

		value := proto.Clone(checkpoint).(*apiv1alpha1.Checkpoint)
		value.HeadTaskId = current.TaskID
		value.HistorySequence = uint64(boundarySequence)
		value.State = apiv1alpha1.CheckpointState_CHECKPOINT_STATE_CREATING
		value.CreatedAt = timestamppb.Now()
		value.Failure = nil
		value.Name = defaultCheckpointName(sourceID, current.TaskID)
		data, err := proto.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode execution checkpoint: %w", err)
		}
		row, err := queryOne(ctx, tx, `
			INSERT INTO session_checkpoint (id, source_session_id, user_id, request_id, head_task_id,
			    history_sequence, snapshot_atespace, snapshot_uri, snapshot_content_scope, tag_uid,
			    provider, source_history_id, prepared_revision, data, state)
			VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, NULL, NULL, 'e2b', $7, $8, $9, 'CREATING')
			ON CONFLICT DO NOTHING
			RETURNING id, source_session_id, user_id, request_id, head_task_id, history_sequence, snapshot_atespace,
			    snapshot_uri, snapshot_content_scope, tag_uid, provider, state, data, source_history_id, prepared_revision
		`,
			pgx.RowToStructByName[sessionCheckpointRow], checkpoint.GetId(),
			sourceID, userID, requestID, current.TaskID, boundarySequence,
			session.HistoryID, session.PreparedRevision, data,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			existing, existingErr := readCheckpointRequest(ctx, tx, userID, requestID)
			if existingErr == nil {
				if existing.Provider != ExecutionProviderE2B || existing.SourceSessionID != sourceID {
					return ErrIdempotencyConflict
				}
				var err error
				installation, err = readCheckpointArchiveInstallation(ctx, tx, existing.ID)
				if err != nil {
					return err
				}
				result, existingErr = toSessionCheckpoint(existing)
				return existingErr
			}
			if errors.Is(existingErr, pgx.ErrNoRows) {
				return fmt.Errorf("session %s has a checkpoint being created: %w", checkpoint.GetSessionId(), ErrConflict)
			}
			return fmt.Errorf("get conflicting E2B checkpoint request: %w", existingErr)
		}
		if err != nil {
			return fmt.Errorf("insert E2B checkpoint: %w", err)
		}
		// Reserve the archive row that finalize commits into. It names the
		// installation now — from the live binding — so the fork restores onto
		// the installation the checkpoint was taken against, even if the
		// source's sandbox is gone by then.
		if err := execSQL(ctx, tx, `
			INSERT INTO session_checkpoint_archive (checkpoint_id, installation)
			VALUES ($1, $2)
		`, row.ID, installation); err != nil {
			return fmt.Errorf("reserve E2B checkpoint archive: %w", err)
		}
		result, err = toSessionCheckpoint(row)
		return err
	})
	if err != nil {
		return nil, "", err
	}
	return result, installation, nil
}

// FinalizeE2BCheckpoint commits the exported /data archive and digest, then
// promotes the checkpoint READY — in the same transaction, so a checkpoint is
// never READY without its archive — or records a failure. An identical
// terminal retry returns the existing checkpoint; a conflicting terminal
// result returns ErrNotFound. The supplied digest must match the archive —
// the store recomputes it rather than trusting the caller — and a failure
// must carry no artifact.
func (c *Client) FinalizeE2BCheckpoint(ctx context.Context, id string, archive, sha256 []byte, failure string) (*apiv1alpha1.Checkpoint, error) {
	// Exactly one terminal outcome, disjointly: a success carries archive +
	// digest and no failure; a failure carries a message and no artifact.
	// Any other combination — an artifact with a failure, a digest with no
	// archive, or nothing at all — is malformed and rejected, not coerced.
	success := len(archive) > 0 && len(sha256) > 0 && failure == ""
	failed := len(archive) == 0 && len(sha256) == 0 && failure != ""
	if !success && !failed {
		return nil, fmt.Errorf("finalize E2B checkpoint requires archive+digest with no failure, or a failure with no artifact")
	}
	// The store enforces the same 64 MiB compressed bound the export applies
	// during collection, so an over-bound artifact fails here with a clear
	// error instead of the column CHECK's generic violation.
	if len(archive) > e2bCheckpointArchiveBound {
		return nil, fmt.Errorf("E2B checkpoint archive %d bytes exceeds the %d-byte bound: %w", len(archive), e2bCheckpointArchiveBound, ErrFailedPrecondition)
	}
	// Verify the caller's digest against the bytes: the archive is the source
	// of truth, and a digest that does not match it must never be stored —
	// a wrong-but-consistent sha would let a corrupt archive verify later.
	if success {
		sum := sha256Sum(archive)
		if !bytes.Equal(sum, sha256) {
			return nil, fmt.Errorf("E2B checkpoint archive digest does not match its bytes")
		}
	}
	var result *apiv1alpha1.Checkpoint
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockCheckpoint(ctx, tx, id, true, "", nil)
		if err != nil {
			return notFoundOr(err)
		}
		if row.Provider != ExecutionProviderE2B {
			return fmt.Errorf("checkpoint %s is not an E2B checkpoint: %w", id, ErrNotFound)
		}
		result, err = toSessionCheckpoint(row)
		if err != nil {
			return err
		}
		if row.State != "CREATING" {
			if row.State == "READY" && success {
				// An idempotent retry must carry the SAME artifact: compare
				// the committed archive/digest, not just that some success
				// call arrived. A different archive is a conflicting result.
				committed, aerr := queryOne(ctx, tx, `
					SELECT archive, archive_sha256 FROM session_checkpoint_archive
					WHERE checkpoint_id = $1
				`, pgx.RowToStructByName[struct {
					Archive       []byte `db:"archive"`
					ArchiveSHA256 []byte `db:"archive_sha256"`
				}], row.ID)
				if aerr != nil {
					return aerr
				}
				if bytes.Equal(committed.Archive, archive) && bytes.Equal(committed.ArchiveSHA256, sha256) {
					return nil
				}
				return ErrNotFound
			}
			if row.State == "FAILED" && failed && result.GetFailure().GetMessage() == failure {
				return nil
			}
			return ErrNotFound
		}
		result.State = apiv1alpha1.CheckpointState_CHECKPOINT_STATE_READY
		if failure != "" {
			result.State = apiv1alpha1.CheckpointState_CHECKPOINT_STATE_FAILED
			result.Failure = &apiv1alpha1.Failure{Reason: "ExportFailed", Message: failure}
		}
		data, err := proto.Marshal(result)
		if err != nil {
			return fmt.Errorf("encode execution checkpoint: %w", err)
		}
		// Commit the artifact first, then promote: the archive row and the
		// state transition commit in the same transaction so READY is never
		// visible without its archive. The ready-gate trigger verifies the
		// child row carries archive+sha before the state update lands.
		if failure == "" {
			tag, err := tx.Exec(ctx, `
				UPDATE session_checkpoint_archive
				SET archive = $2, archive_sha256 = $3
				WHERE checkpoint_id = $1
			`, row.ID, archive, sha256)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("E2B checkpoint %s has no reserved archive: %w", id, ErrNotFound)
			}
		}
		tag, err := tx.Exec(ctx, `
			UPDATE session_checkpoint
			SET state = CASE WHEN $2 THEN 'READY' ELSE 'FAILED' END,
			    data = $3
			WHERE id = $1 AND state = 'CREATING'
		`, row.ID, failure == "", data)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrNotFound
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("finalize E2B checkpoint: %w", err)
	}
	return result, nil
}

// GetE2BCheckpoint returns an owned READY E2B checkpoint.
func (c *Client) GetE2BCheckpoint(ctx context.Context, id, userID string) (*apiv1alpha1.Checkpoint, error) {
	row, err := readCheckpoint(ctx, c.db, id, userID, new("READY"))
	if err != nil {
		return nil, fmt.Errorf("get E2B checkpoint: %w", notFoundOr(err))
	}
	if row.Provider != ExecutionProviderE2B {
		return nil, fmt.Errorf("get E2B checkpoint %s: %w", id, notFoundOr(pgx.ErrNoRows))
	}
	return toSessionCheckpoint(row)
}

// GetE2BCheckpointArtifact returns an owned READY checkpoint's /data archive,
// digest, and installation — the payload a fork restores onto a fresh sandbox.
func (c *Client) GetE2BCheckpointArtifact(ctx context.Context, id, userID string) (*E2BCheckpointArtifact, error) {
	row, err := readCheckpoint(ctx, c.db, id, userID, new("READY"))
	if err != nil {
		return nil, fmt.Errorf("get E2B checkpoint artifact: %w", notFoundOr(err))
	}
	if row.Provider != ExecutionProviderE2B {
		return nil, fmt.Errorf("get E2B checkpoint artifact %s: %w", id, notFoundOr(pgx.ErrNoRows))
	}
	type artifact struct {
		Archive       []byte `db:"archive"`
		ArchiveSHA256 []byte `db:"archive_sha256"`
		Installation  string `db:"installation"`
	}
	a, err := queryOne(ctx, c.db, `
		SELECT archive, archive_sha256, installation FROM session_checkpoint_archive
		WHERE checkpoint_id = $1
	`, pgx.RowToStructByName[artifact], row.ID)
	if err != nil {
		return nil, fmt.Errorf("get E2B checkpoint artifact %s: %w", id, notFoundOr(err))
	}
	if len(a.Archive) == 0 || len(a.ArchiveSHA256) == 0 {
		return nil, fmt.Errorf("E2B checkpoint %s has no retained archive: %w", id, ErrFailedPrecondition)
	}
	return &E2BCheckpointArtifact{Archive: a.Archive, ArchiveSHA256: a.ArchiveSHA256, Installation: a.Installation}, nil
}

// readCheckpointArchiveInstallation returns the reserved archive's
// installation for a checkpoint inside the caller's transaction.
func readCheckpointArchiveInstallation(ctx context.Context, db dbExecutor, checkpointID uuid.UUID) (string, error) {
	installation, err := queryOne(ctx, db, `
		SELECT installation FROM session_checkpoint_archive WHERE checkpoint_id = $1
	`, pgx.RowTo[string], checkpointID)
	if err != nil {
		return "", fmt.Errorf("get E2B checkpoint archive: %w", err)
	}
	return installation, nil
}

// liveExecutionBindingForUpdate returns the session's unrevoked instance
// binding inside the caller's transaction, or nil if none is live. It is the
// tx-scoped counterpart to GetLiveExecutionBinding, used to prove a checkpoint
// source is E2B-bound and to name the installation its /data archive comes
// from — returning nil, not ErrNotFound, when no binding exists.
func liveExecutionBindingForUpdate(ctx context.Context, db dbExecutor, sessionID string) (*ExecutionInstanceBinding, error) {
	row, err := queryOne(ctx, db, `
		SELECT id, session_id, provider, installation, external_id, generation, created_at, revoked_at
		FROM execution_instance_binding
		WHERE session_id = $1 AND revoked_at IS NULL
	`, pgx.RowToStructByName[ExecutionInstanceBinding], sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}
