package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
)

type sessionDeletionReason string

const (
	sessionDeletionUserRequested sessionDeletionReason = "user_requested"
	sessionDeletionIdleTimeout   sessionDeletionReason = "idle_timeout"
)

// beginSessionDeletion attaches the reason to ordinary lifecycle admission.
// The caller holds the session lock and has checked any reason-specific policy.
func beginSessionDeletion(ctx context.Context, tx pgx.Tx, row sessionRow, reason sessionDeletionReason) (*SessionOperation, error) {
	operation, err := beginSessionOperation(ctx, tx, row, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	if errors.Is(err, ErrConflict) {
		// The conflict is a pending operation the delete could not supersede.
		// The only supersedable-lost case is an E2B CREATE that committed its
		// binding and then had the sandbox definitively lost (provider delete ->
		// reaper revoked the binding): the retained executor claim no longer
		// protects live issued work, so the create may be superseded. Verify it
		// under this same lock before relaxing the retained-claim invariant.
		lost, lerr := lostBoundE2BCreateBlockingDelete(ctx, tx, row)
		if lerr != nil {
			return nil, lerr
		}
		if lost {
			row, err = lockSession(ctx, tx, row.ID.String())
			if err != nil {
				return nil, err
			}
			operation, err = beginSessionOperation(ctx, tx, row, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
		}
	}
	if err != nil {
		return nil, err
	}
	if err := execSQL(ctx, tx, `UPDATE session SET deletion_reason = COALESCE(deletion_reason, $2) WHERE id = $1`, row.ID, string(reason)); err != nil {
		return nil, err
	}
	return operation, nil
}

// lostBoundE2BCreateBlockingDelete reports whether the session's open CREATE is
// an E2B create whose bound sandbox is definitively lost, and — when it is —
// clears the stale retained executor claim so a delete may supersede it. It runs
// entirely inside the session-lock transaction; the row lock is the mutual-
// exclusion point every binder/claimer already contends on.
//
// All four conditions must hold; if any fails it returns false and the
// caller's ErrConflict stands, preserving the retained-claim protection:
//   - the open operation is CREATE on an E2B-prepared revision,
//   - its executor lease has ENDED (executor_expires_at <= now), not merely
//     that an executor id is retained — an in-flight attempt stays protected,
//   - the session's latest execution binding is REVOKED and correlates to this
//     create's recorded attempt (session + installation + known external id),
//   - no live (unrevoked) binding remains.
//
// Clearing executor_id here is safe only because the revoked binding is
// durable proof the create's compute is gone: there is no live issued work a
// superseding delete could discard. The attempt and binding rows are retained
// for audit; only the claim is released.
func lostBoundE2BCreateBlockingDelete(ctx context.Context, tx pgx.Tx, row sessionRow) (bool, error) {
	// Fast exit: only a pending CREATE with a retained executor id can match.
	// Both are already loaded on the locked row.
	if row.OperationID == nil || row.ExecutorID == nil ||
		row.Operation != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE.String() {
		return false, nil
	}
	// $5 is the provider discriminator: a NULL prepared_revision LEFT JOINs to
	// NULL provider, so "NULL = 'e2b'" is NULL and the row never qualifies —
	// non-E2B (Substrate) creates stay protected automatically.
	ok, err := queryOne(ctx, tx, `
		SELECT (r.provider = $5)
		   AND i.executor_expires_at <= clock_timestamp()
		   AND b.external_id IS NOT NULL AND b.revoked_at IS NOT NULL
		   AND a.operation_id IS NOT NULL
		   AND NOT EXISTS (
		       SELECT 1 FROM execution_instance_binding lb
		       WHERE lb.session_id = i.id AND lb.revoked_at IS NULL)
		FROM runtime_instance i
		LEFT JOIN runtime_revision r ON r.revision = i.prepared_revision
		LEFT JOIN LATERAL (
		    SELECT installation, external_id, revoked_at
		    FROM execution_instance_binding x
		    WHERE x.session_id = i.id
		    ORDER BY x.generation DESC
		    LIMIT 1
		) b ON true
		LEFT JOIN execution_create_attempt a
		       ON a.operation_id = i.operation_id
		      AND a.session_id = i.id
		      AND a.installation = b.installation
		      AND a.external_id IS NOT NULL
		      AND a.external_id = b.external_id
		WHERE i.id = $1 AND i.kind = $2 AND i.operation_id = $3
		  AND i.operation = $4 AND i.executor_id IS NOT NULL
	`, pgx.RowTo[bool],
		row.ID, runtimeKindAgent, *row.OperationID,
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE.String(),
		ExecutionProviderE2B)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check lost E2B create blocking delete: %w", err)
	}
	if !ok {
		return false, nil
	}
	// The bound compute is definitively lost: release the stale claim in place
	// so the normal DELETE admission can supersede the dead CREATE. Match the
	// operation_id so a superseding generation cannot be cleared here.
	tag, err := tx.Exec(ctx, `
		UPDATE runtime_instance
		   SET executor_id = NULL, executor_expires_at = NULL
		WHERE id = $1 AND kind = $2 AND operation_id = $3
		  AND operation = $4 AND executor_expires_at <= clock_timestamp()
	`, row.ID, runtimeKindAgent, *row.OperationID,
		apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE.String())
	if err != nil {
		return false, fmt.Errorf("release lost E2B create claim: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// finishSessionDeletion releases expired creation receipts. Explicit deletion
// retains its tombstone so the original request cannot recreate compute.
func finishSessionDeletion(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	return execSQL(ctx, tx, `
		DELETE FROM runtime_instance r USING session s
		WHERE r.id = $1 AND s.id = r.id AND s.deletion_reason = 'idle_timeout'
	`, id)
}
