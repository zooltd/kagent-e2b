package database

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// AdmissionReport is one coherent read-only snapshot of durable agent-session
// work, taken for maintenance/drain auditing. It is a database observation
// ONLY — it is not a drain-completion or provider-compute check. Empty results
// mean no listed durable work was observed, not that every stream or runtime
// is inactive. No locks are taken for admission; nothing is written.
type AdmissionReport struct {
	// GeneratedAt is the snapshot time (the transaction's view of now()).
	GeneratedAt          time.Time          `json:"generatedAt"`
	AdmissionsClosed     bool               `json:"admissionsClosed"`
	NonTerminalTasks     []ReportTask       `json:"nonTerminalTasks"`
	OpenOperations       []ReportOperation  `json:"openOperations"`
	DispatchReservations []ReportDispatch   `json:"dispatchReservations"`
	UnsettledEvents      []ReportUnsettled  `json:"unsettledEvents"`
	PendingQuiescence    []ReportQuiescence `json:"pendingQuiescence"`
	RetainedBindings     []ReportBinding    `json:"retainedBindings"`
	// Truncated marks sections that hit the per-section row cap. A capped
	// section is an incomplete inventory and must not read as exhaustive.
	Truncated bool `json:"truncated"`
}

// ReportTask is one non-terminal task on an agent session. State is reported
// verbatim from durable storage; INPUT_REQUIRED/AUTH_REQUIRED are waiting
// native turns, distinct from actively-progressing SUBMITTED/WORKING. The
// persisted state records what the session was last told, not whether provider
// compute is currently alive.
type ReportTask struct {
	SessionID   uuid.UUID  `db:"session_id" json:"sessionId"`
	TaskID      string     `db:"task_id" json:"taskId"`
	State       string     `db:"state" json:"state"`
	Provider    string     `db:"provider" json:"provider"`
	Waiting     bool       `db:"waiting" json:"waiting"`
	StatusStamp *time.Time `db:"status_timestamp" json:"statusTimestamp,omitempty"`
}

// ReportOperation is one open lifecycle operation on an agent session,
// including a CREATE that already has a binding but has not finished bootstrap.
// ExecutorID is the current claim, not proof work is live; ExecutorExpiresAt is
// the claim's expiry. CreateAttempt carries the E2B create correlation when a
// "create was issued" marker exists for this operation.
type ReportOperation struct {
	SessionID         uuid.UUID  `db:"session_id" json:"sessionId"`
	Operation         string     `db:"operation" json:"operation"`
	OperationID       uuid.UUID  `db:"operation_id" json:"operationId"`
	State             string     `db:"state" json:"state"`
	Provider          string     `db:"provider" json:"provider"`
	ExecutorID        *uuid.UUID `db:"executor_id" json:"executorId,omitempty"`
	ExecutorExpiresAt *time.Time `db:"executor_expires_at" json:"executorExpiresAt,omitempty"`
	CreateAttemptID   *uuid.UUID `db:"create_attempt_id" json:"createAttemptId,omitempty"`
	CreateOutcome     *string    `db:"create_outcome" json:"createOutcome,omitempty"`
	CreateExternalID  *string    `db:"create_external_id" json:"createExternalId,omitempty"`
	Bound             bool       `db:"bound" json:"bound"`
}

// ReportDispatch is one outstanding dispatch reservation. Expiry is reported
// verbatim; expiry alone does not prove the message did or did not execute.
type ReportDispatch struct {
	SessionID         uuid.UUID `db:"session_id" json:"sessionId"`
	DispatchID        uuid.UUID `db:"dispatch_id" json:"dispatchId"`
	DispatchExpiresAt time.Time `db:"dispatch_expires_at" json:"dispatchExpiresAt"`
}

// ReportUnsettled is one session_task_event row whose publication or runtime
// cleanup is not finished — either an unpublished boundary (native cleanup may
// still be in flight) or a claimed-but-unfinished quiescence. A terminal task
// state alone does not prove cleanup finished.
type ReportUnsettled struct {
	SessionID  uuid.UUID  `db:"session_id" json:"sessionId"`
	TaskID     string     `db:"task_id" json:"taskId"`
	Sequence   int64      `db:"sequence" json:"sequence"`
	Kind       string     `db:"kind" json:"kind"`
	ExecutorID *uuid.UUID `db:"executor_id" json:"executorId,omitempty"`
}

// ReportQuiescence is one pending-quiescence boundary that is published but
// not yet finished — distinguishing a still-unclaimed idle pause from one a
// worker already claimed. Claimed means a worker owns the pause; Unclaimed
// means it is waiting for a worker.
type ReportQuiescence struct {
	SessionID  uuid.UUID  `db:"session_id" json:"sessionId"`
	TaskID     string     `db:"task_id" json:"taskId"`
	Sequence   int64      `db:"sequence" json:"sequence"`
	Claimed    bool       `db:"claimed" json:"claimed"`
	ExecutorID *uuid.UUID `db:"executor_id" json:"executorId,omitempty"`
}

// ReportBinding is one retained runtime identity for a session generation —
// the durable association between a session and its external compute. For E2B
// it is installation + external sandbox id + generation; for Substrate it is
// the session's actor_uid (installation/external fields stay empty). A session
// here with no non-terminal task is idle-but-bound: it retains a runtime
// identity, NOT proof the compute is paused or safe to upgrade.
type ReportBinding struct {
	SessionID uuid.UUID `db:"session_id" json:"sessionId"`
	Provider  string    `db:"provider" json:"provider"`
	// Installation/ExternalID/Generation are E2B identity fields; they are
	// empty for Substrate, whose identity is ActorUID. No E2B-shaped value is
	// fabricated for a Substrate row.
	Installation string     `db:"installation" json:"installation,omitempty"`
	ExternalID   string     `db:"external_id" json:"externalId,omitempty"`
	Generation   *int64     `db:"generation" json:"generation,omitempty"`
	ActorUID     string     `db:"actor_uid" json:"actorUid,omitempty"`
	CreatedAt    *time.Time `db:"created_at" json:"createdAt,omitempty"`
	// ActiveTask is true when the bound session has any non-terminal task
	// (including a waiting INPUT_REQUIRED/AUTH_REQUIRED turn); false marks an
	// idle-but-bound session.
	ActiveTask bool `db:"active_task" json:"activeTask"`
}

// reportRowCap bounds each section so one pathological table cannot produce an
// unbounded dump. Overflow sets Truncated rather than silently dropping rows.
const reportRowCap = 500

// AdmissionReportSnapshot reads one repeatable-read snapshot of the durable
// agent-session work a drain must account for. It is read-only and takes no
// admission lock: it never blocks or is blocked by a close. Every section is
// drawn from the same transaction, so the report is internally consistent.
// Provider, runtime, Kubernetes, or envd state is NOT consulted; this is a
// database observation and says nothing about whether compute is paused.
func (c *Client) AdmissionReportSnapshot(ctx context.Context) (*AdmissionReport, error) {
	report := &AdmissionReport{}
	err := pgx.BeginTxFunc(ctx, c.db, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		// Barrier state first; a missing row is a broken schema, not "open".
		closed, err := queryOne(ctx, tx, `
			SELECT closed FROM admission_barrier WHERE id = $1
		`, pgx.RowTo[bool], admissionBarrierID)
		if err != nil {
			return err
		}
		report.AdmissionsClosed = closed
		report.GeneratedAt = time.Now()

		// Non-terminal tasks with the session's provider, distinguishing
		// waiting (input/auth) turns from progressing ones.
		tasks, err := queryMany(ctx, tx, `
			SELECT i.id AS session_id, t.id AS task_id, t.state, t.status_timestamp,
			       r.provider,
			       (t.state IN ('TASK_STATE_INPUT_REQUIRED','TASK_STATE_AUTH_REQUIRED')) AS waiting
			FROM session_task t
			JOIN session_record i ON i.history_id = t.history_id
			LEFT JOIN runtime_revision r ON r.revision = i.prepared_revision
			WHERE i.state <> 'RUNTIME_STATE_DELETED'
			  AND t.state NOT IN (
			      'TASK_STATE_COMPLETED','TASK_STATE_CANCELED',
			      'TASK_STATE_FAILED','TASK_STATE_REJECTED')
			ORDER BY i.id, t.id
			LIMIT $1
		`, pgx.RowToStructByName[ReportTask], reportRowCap+1)
		if err != nil {
			return err
		}
		report.NonTerminalTasks = capReport(tasks, &report.Truncated)

		// Open lifecycle operations (any non-NONE operation), joined to the
		// E2B create-attempt correlation and whether a live binding exists.
		ops, err := queryMany(ctx, tx, `
			SELECT i.id AS session_id, i.operation, i.operation_id, i.state,
			       ri.executor_id, ri.executor_expires_at,
			       COALESCE(r.provider, 'substrate') AS provider,
			       a.id AS create_attempt_id, a.outcome AS create_outcome, a.external_id AS create_external_id,
			       -- "Bound" is provider-specific retained identity: an E2B
			       -- session's live execution_instance_binding, or a Substrate
			       -- session's non-NULL actor_uid. Either is the durable claim
			       -- that this operation's runtime identity already exists.
			       (CASE WHEN COALESCE(r.provider,'substrate') = 'e2b'
			             THEN EXISTS (SELECT 1 FROM execution_instance_binding b
			                          WHERE b.session_id = i.id AND b.revoked_at IS NULL)
			             ELSE i.actor_uid IS NOT NULL END) AS bound
			FROM session_record i
			JOIN runtime_instance ri ON ri.id = i.id
			LEFT JOIN runtime_revision r ON r.revision = i.prepared_revision
			LEFT JOIN execution_create_attempt a ON a.operation_id = i.operation_id
			WHERE i.operation <> 'RUNTIME_OPERATION_NONE'
			ORDER BY i.id
			LIMIT $1
		`, pgx.RowToStructByName[ReportOperation], reportRowCap+1)
		if err != nil {
			return err
		}
		report.OpenOperations = capReport(ops, &report.Truncated)

		// Outstanding dispatch reservations.
		dispatches, err := queryMany(ctx, tx, `
			SELECT i.id AS session_id, i.dispatch_id, i.dispatch_expires_at
			FROM session_record i
			WHERE i.dispatch_id IS NOT NULL
			ORDER BY i.dispatch_expires_at
			LIMIT $1
		`, pgx.RowToStructByName[ReportDispatch], reportRowCap+1)
		if err != nil {
			return err
		}
		report.DispatchReservations = capReport(dispatches, &report.Truncated)

		// Unsettled publication and unfinished claimed quiescence.
		unsettled, err := queryMany(ctx, tx, `
			SELECT i.id AS session_id, e.task_id, e.sequence,
			       CASE WHEN NOT e.published THEN 'unpublished' ELSE 'quiescence_claimed' END AS kind,
			       e.quiescence_executor_id AS executor_id
			FROM session_task_event e
			JOIN session_record i ON i.history_id = e.history_id
			WHERE NOT e.published OR (e.quiescence_pending AND e.quiescence_executor_id IS NOT NULL)
			ORDER BY e.history_id, e.sequence
			LIMIT $1
		`, pgx.RowToStructByName[ReportUnsettled], reportRowCap+1)
		if err != nil {
			return err
		}
		report.UnsettledEvents = capReport(unsettled, &report.Truncated)

		// Pending quiescence: published pause boundaries not yet finished.
		quiescence, err := queryMany(ctx, tx, `
			SELECT i.id AS session_id, e.task_id, e.sequence,
			       (e.quiescence_executor_id IS NOT NULL) AS claimed,
			       e.quiescence_executor_id AS executor_id
			FROM session_task_event e
			JOIN session_record i ON i.history_id = e.history_id
			WHERE e.published AND e.quiescence_pending
			ORDER BY e.history_id, e.sequence
			LIMIT $1
		`, pgx.RowToStructByName[ReportQuiescence], reportRowCap+1)
		if err != nil {
			return err
		}
		report.PendingQuiescence = capReport(quiescence, &report.Truncated)

		// Retained runtime identities — each provider's durable association for
		// a live session generation. E2B rows come from unrevoked
		// execution_instance_binding; Substrate rows are non-deleted sessions
		// carrying a non-NULL actor_uid. active_task marks whether the session
		// has any non-terminal task, including a waiting INPUT_REQUIRED/
		// AUTH_REQUIRED turn (waiting is not idle — the session still holds the
		// compute identity a task occupies).
		bindings, err := queryMany(ctx, tx, `
			SELECT session_id, provider, installation, external_id, generation,
			       actor_uid, created_at,
			       EXISTS (
			           SELECT 1 FROM session_task t
			           JOIN session_record i2 ON i2.history_id = t.history_id
			           WHERE i2.id = session_id
			             AND t.state NOT IN (
			                 'TASK_STATE_COMPLETED','TASK_STATE_CANCELED',
			                 'TASK_STATE_FAILED','TASK_STATE_REJECTED')
			       ) AS active_task
			FROM (
			    SELECT b.session_id, b.provider, b.installation, b.external_id,
			           b.generation, '' AS actor_uid, b.created_at
			    FROM execution_instance_binding b
			    WHERE b.revoked_at IS NULL
			    UNION ALL
			    SELECT i.id AS session_id, 'substrate' AS provider,
			           '' AS installation, '' AS external_id, NULL AS generation,
			           i.actor_uid, NULL::timestamptz AS created_at
			    FROM session_record i
			    WHERE i.actor_uid IS NOT NULL AND i.state <> 'RUNTIME_STATE_DELETED'
			) identities
			ORDER BY session_id
			LIMIT $1
		`, pgx.RowToStructByName[ReportBinding], reportRowCap+1)
		if err != nil {
			return err
		}
		report.RetainedBindings = capReport(bindings, &report.Truncated)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

// capReport enforces the per-section row cap. The query fetched cap+1 rows; a
// full page means the section is truncated, and the extra row is dropped so the
// returned slice is exactly the cap. Truncation is reported, never silent.
func capReport[T any](rows []T, truncated *bool) []T {
	if len(rows) > reportRowCap {
		*truncated = true
		return rows[:reportRowCap]
	}
	return rows
}
