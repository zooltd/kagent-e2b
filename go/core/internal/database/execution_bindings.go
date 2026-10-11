// Package database owns the durable execution-provider bindings used by the
// E2B runtime authenticator. A binding ties one session generation to concrete
// external compute; Substrate sessions keep their actor_uid binding and do not
// use this table.
package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// jsonEqual compares two JSONB payloads semantically: the column normalizes
// formatting (key order, spacing), so byte equality would falsely reject an
// identical re-record. Decoded comparison treats equal documents as equal.
func jsonEqual(a, b []byte) bool {
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return string(a) == string(b)
	}
	return reflect.DeepEqual(av, bv)
}

// ExecutionInstanceBinding is the durable association between a session and the
// external compute it runs on for one generation.
type ExecutionInstanceBinding struct {
	ID           uuid.UUID  `db:"id"`
	SessionID    uuid.UUID  `db:"session_id"`
	Provider     string     `db:"provider"`
	Installation string     `db:"installation"`
	ExternalID   string     `db:"external_id"`
	Generation   int64      `db:"generation"`
	CreatedAt    time.Time  `db:"created_at"`
	RevokedAt    *time.Time `db:"revoked_at"`
}

// ExecutionProviderE2B is the only provider binding supported in this slice.
const ExecutionProviderE2B = "e2b"

// ExecutionProviderSubstrate is the default execution provider; it keeps the
// ActorTemplate-based revision identity and the actor_uid session binding.
const ExecutionProviderSubstrate = "substrate"

// OpenE2BCreate is one E2B session whose CREATE operation is still open and
// has no live binding: the durable intent the reconcile sweep tries to match
// to a late-arriving provider sandbox.
type OpenE2BCreate struct {
	SessionID   uuid.UUID `db:"id"`
	OperationID uuid.UUID `db:"operation_id"`
	// Revision is the prepared revision the create was admitted against; its
	// artifact binding is loaded separately for the install/template check.
	Revision string `db:"prepared_revision"`
}

// ListOpenE2BCreateOperations returns every E2B session still inside an open
// CREATE operation with no live instance binding. These are the sessions a
// late create response can still bind to: the operation tag is durable intent,
// the missing binding is what makes the sandbox unreconciled rather than
// already-placed. Executor ownership is irrelevant here — the sweep binds on
// operation/generation, not on whichever attempt last claimed.
func (c *Client) ListOpenE2BCreateOperations(ctx context.Context) ([]OpenE2BCreate, error) {
	return queryMany(ctx, c.db, `
		SELECT i.id, i.operation_id, i.prepared_revision
		FROM session_record i
		JOIN runtime_revision r ON r.revision = i.prepared_revision
		WHERE i.operation = 'RUNTIME_OPERATION_CREATE'
		  AND i.operation_id IS NOT NULL
		  AND r.provider = 'e2b'
		  AND NOT EXISTS (
		      SELECT 1 FROM execution_instance_binding b
		      WHERE b.session_id = i.id AND b.revoked_at IS NULL
		  )
	`, pgx.RowToStructByName[OpenE2BCreate])
}

// RunningE2BTask identifies one bound E2B session with at least one physically
// running task — the set whose compute the timeout renewer must keep alive.
// INPUT_REQUIRED and AUTH_REQUIRED count as running: a native turn waiting on
// input is still executing on the sandbox, so its compute must not time out.
type RunningE2BTask struct {
	SessionID    uuid.UUID `db:"session_id"`
	Installation string    `db:"installation"`
	ExternalID   string    `db:"external_id"`
}

// ListE2BSessionsWithRunningTasks returns every live E2B-bound session that has
// at least one physically running task — the renewal set. A task is running
// until it reaches a terminal state; INPUT_REQUIRED/AUTH_REQUIRED are waiting
// native turns on still-running compute, not idle, so they are included.
func (c *Client) ListE2BSessionsWithRunningTasks(ctx context.Context) ([]RunningE2BTask, error) {
	return queryMany(ctx, c.db, `
		SELECT DISTINCT b.session_id, b.installation, b.external_id
		FROM execution_instance_binding b
		JOIN session_task t ON t.history_id = (
		    SELECT i.history_id FROM session_record i WHERE i.id = b.session_id
		)
		WHERE b.provider = 'e2b'
		  AND b.revoked_at IS NULL
		  AND t.state NOT IN (
		      'TASK_STATE_COMPLETED', 'TASK_STATE_CANCELED',
		      'TASK_STATE_FAILED', 'TASK_STATE_REJECTED'
		  )
	`, pgx.RowToStructByName[RunningE2BTask])
}

// ListLiveE2BExecutionBindings returns every unrevoked E2B execution binding —
// the set whose bound compute the loss reaper verifies is still present. Each
// binding's live credential dies when its row is revoked, so this is the
// revocation set, not the running-task set.
func (c *Client) ListLiveE2BExecutionBindings(ctx context.Context) ([]ExecutionInstanceBinding, error) {
	return queryMany(ctx, c.db, `
		SELECT id, session_id, provider, installation, external_id, generation, created_at, revoked_at
		FROM execution_instance_binding
		WHERE provider = 'e2b' AND revoked_at IS NULL
	`, pgx.RowToStructByName[ExecutionInstanceBinding])
}

// E2BCredentialBinding is one live E2B session's bound compute plus the
// frozen launch-contract credential path its renewer rewrites. generation is
// the binding's instance generation the renewed credential must preserve so a
// stale token cannot outlive a rebind.
type E2BCredentialBinding struct {
	SessionID    uuid.UUID `db:"session_id"`
	Installation string    `db:"installation"`
	ExternalID   string    `db:"external_id"`
	Generation   int64     `db:"generation"`
	// Revision is the session's prepared revision the binding was created
	// under; it keys the frozen launch contract.
	Revision string `db:"prepared_revision"`
	// LaunchContract is the bound revision's frozen launch contract JSON; the
	// renewer decodes it for the credential path rather than reading live
	// install config that may have moved since.
	LaunchContract []byte `db:"launch_contract"`
}

// ListE2BCredentialBindings returns every live E2B binding joined to its
// prepared revision's launch contract — the set whose runtime credential the
// renewer keeps ahead of expiry. Only sessions whose prepared revision is an
// E2B revision with a recorded binding are returned; a binding with no
// revision binding cannot carry a credential path and is skipped by the
// renewer rather than erroring the whole sweep.
func (c *Client) ListE2BCredentialBindings(ctx context.Context) ([]E2BCredentialBinding, error) {
	return queryMany(ctx, c.db, `
		SELECT b.session_id, b.installation, b.external_id, b.generation, i.prepared_revision, rb.launch_contract
		FROM execution_instance_binding b
		JOIN session_record i ON i.id = b.session_id
		JOIN execution_revision_binding rb ON rb.revision = i.prepared_revision
		WHERE b.provider = 'e2b' AND b.revoked_at IS NULL
		  AND i.state <> 'RUNTIME_STATE_DELETED'
	`, pgx.RowToStructByName[E2BCredentialBinding])
}

// GetE2BSessionPreparedRevision returns the session's prepared revision, the
// key under which its frozen launch contract — including the credential path
// the refresher rewrites — is recorded. It reads by session id alone because
// an E2B session has no actor_uid to key on.
func (c *Client) GetE2BSessionPreparedRevision(ctx context.Context, sessionID string) (string, error) {
	sid, err := uuid.Parse(sessionID)
	if err != nil {
		return "", fmt.Errorf("invalid session id %q: %w", sessionID, err)
	}
	rev, err := queryOne(ctx, c.db, `
		SELECT i.prepared_revision
		FROM session_record i
		WHERE i.id = $1 AND i.state <> 'RUNTIME_STATE_DELETED'
		  AND i.prepared_revision IS NOT NULL
	`, pgx.RowTo[string], sid)
	if err != nil {
		return "", notFoundOr(err)
	}
	return rev, nil
}

// ExecutionRevisionBinding is the durable association between an E2B revision
// and the concrete external build it runs: the installation plus the external
// template and build identities reported by the provider.
type ExecutionRevisionBinding struct {
	ID           uuid.UUID `db:"id"`
	Revision     string    `db:"revision"`
	Provider     string    `db:"provider"`
	Installation string    `db:"installation"`
	ExternalID   string    `db:"external_id"`
	BuildID      string    `db:"build_id"`
	// ConfigJSON is the immutable compiled harness config the bootstrapper
	// writes to the guest's ConfigPath — Revision.ConfigJSON, not the
	// provenance snapshot.
	ConfigJSON []byte `db:"config_json"`
	// LaunchContract is the resolved guest launch contract (supervisor command
	// and file contract paths) the workload mapping selected, frozen on the
	// binding so a later installation-config change cannot re-map an existing
	// revision's bootstrap.
	LaunchContract []byte `db:"launch_contract"`
	// Environment is the revision's resolved non-secret process environment —
	// variable name to literal value — the bootstrap launch exports into the
	// guest. Secret-bearing variables are never stored here; the credential
	// broker injects those outside the guest.
	Environment []byte    `db:"environment"`
	CreatedAt   time.Time `db:"created_at"`
}

// E2BRuntimeRevisionInput carries the immutable agent-side inputs of an E2B
// prepared revision plus its genuine artifact binding. ActorTemplate fields
// are absent: an E2B revision never holds a Substrate artifact identity.
type E2BRuntimeRevisionInput struct {
	Revision       string
	Namespace      string
	AgentName      string
	AgentUID       string
	SourceSnapshot []byte
	// ConfigJSON is the compiled harness configuration the bootstrapper writes
	// to the guest's ConfigPath. It is persisted on the binding (not the
	// provenance SourceSnapshot) and required — a revision may not record an
	// artifact binding without launchable config.
	ConfigJSON []byte
	// LaunchContract is the resolved guest launch contract JSON the bootstrap
	// replays, frozen on the binding. Required.
	LaunchContract []byte
	// Environment is the revision's resolved non-secret process environment as
	// variable name -> literal value. Optional; absent means the guest inherits
	// only the contract-owned callback/broker overrides. Secret-bearing values
	// are rejected by the caller and never recorded.
	Environment        map[string]string
	AgentCard          *a2apb.AgentCard
	EgressDestinations []string
	// Installation is the E2B deployment the build lives in; ExternalID is the
	// external template and BuildID the concrete build artifact.
	Installation string
	ExternalID   string
	BuildID      string
}

// RecordE2BRuntimeRevision atomically records an E2B prepared revision, its
// genuine artifact binding, and the agent extension row, then promotes it
// ready for an active definition that still desires it. The revision and
// binding commit together: the deferred provider FK fails the transaction if
// either row is missing or their providers disagree. ready controls
// promotion only; the binding is always required.
func (c *Client) RecordE2BRuntimeRevision(ctx context.Context, in E2BRuntimeRevisionInput, ready bool) error {
	if in.AgentCard == nil {
		return fmt.Errorf("E2B runtime revision %s has no Agent Card", in.Revision)
	}
	if in.Installation == "" || in.ExternalID == "" || in.BuildID == "" {
		return fmt.Errorf("E2B runtime revision %s requires installation, external id, and build id", in.Revision)
	}
	if len(in.ConfigJSON) == 0 {
		return fmt.Errorf("E2B runtime revision %s requires a compiled harness config", in.Revision)
	}
	if len(in.LaunchContract) == 0 {
		return fmt.Errorf("E2B runtime revision %s requires a resolved launch contract", in.Revision)
	}
	environmentJSON, err := json.Marshal(in.Environment)
	if err != nil {
		return fmt.Errorf("encode E2B revision environment: %w", err)
	}
	card, err := proto.Marshal(in.AgentCard)
	if err != nil {
		return fmt.Errorf("encode runtime revision Agent Card: %w", err)
	}
	return c.withTx(ctx, func(tx pgx.Tx) error {
		if ready {
			// Match the Substrate promotion lock order: definition before revision.
			_, err := queryOne(ctx, tx, `
				SELECT 1 FROM agent_definition p
				LEFT JOIN agent_runtime_revision r ON r.revision = $1
				WHERE p.namespace = COALESCE(r.namespace, $2)
				  AND p.agent_uid = COALESCE(r.agent_uid, $3)
				FOR UPDATE OF p
			`, pgx.RowTo[int], in.Revision, in.Namespace, in.AgentUID)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}
		// The revision carries provider='e2b' and NULL ActorTemplate fields;
		// the deferred provider FK requires the matching binding row to exist
		// at commit.
		if err := recordRuntimeRevision(ctx, tx, runtimeRevisionRecord{
			RuntimeArtifact: RuntimeArtifact{
				Revision: in.Revision, Kind: runtimeKindAgent, Namespace: in.Namespace,
				Provider: ExecutionProviderE2B,
			},
			SourceSnapshot:     in.SourceSnapshot,
			EgressDestinations: in.EgressDestinations,
			Credentials:        nil,
		}); err != nil {
			return err
		}
		// An identical re-record (e.g. unready then ready) reuses the existing
		// binding; a conflicting immutable binding input is rejected.
		existing, err := queryOne[ExecutionRevisionBinding](ctx, tx, `
			SELECT installation, external_id, build_id, config_json, launch_contract, environment FROM execution_revision_binding
			WHERE revision = $1
		`, func(row pgx.CollectableRow) (ExecutionRevisionBinding, error) {
			var b ExecutionRevisionBinding
			return b, row.Scan(&b.Installation, &b.ExternalID, &b.BuildID, &b.ConfigJSON, &b.LaunchContract, &b.Environment)
		}, in.Revision)
		switch {
		case err == nil:
			if existing.Installation != in.Installation || existing.ExternalID != in.ExternalID || existing.BuildID != in.BuildID ||
				!jsonEqual(existing.ConfigJSON, in.ConfigJSON) || !jsonEqual(existing.LaunchContract, in.LaunchContract) ||
				!jsonEqual(existing.Environment, environmentJSON) {
				return fmt.Errorf("E2B revision %s artifact binding is immutable: %w", in.Revision, ErrConflict)
			}
		case errors.Is(err, pgx.ErrNoRows):
			if err := execSQL(ctx, tx, `
				INSERT INTO execution_revision_binding (id, revision, provider, installation, external_id, build_id, config_json, launch_contract, environment)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			`, uuid.New(), in.Revision, ExecutionProviderE2B, in.Installation, in.ExternalID, in.BuildID, in.ConfigJSON, in.LaunchContract, environmentJSON); err != nil {
				return fmt.Errorf("record E2B revision binding %s: %w", in.Revision, err)
			}
		default:
			return fmt.Errorf("record E2B revision binding %s: %w", in.Revision, err)
		}
		if err := execSQL(ctx, tx, `
			INSERT INTO agent_revision (revision, agent_name, agent_uid, agent_card)
			VALUES ($1, $2, $3, $4) ON CONFLICT (revision) DO NOTHING
		`, in.Revision, in.AgentName, in.AgentUID, card); err != nil {
			return err
		}
		if !ready {
			return nil
		}
		return execSQL(ctx, tx, `
			UPDATE agent_definition p
			SET latest_successful_revision = r.revision, updated_at = NOW()
			FROM agent_runtime_revision r
			WHERE r.revision = $1
			  AND p.namespace = r.namespace
			  AND p.agent_uid = r.agent_uid
			  AND p.desired_revision = r.revision
			  AND p.retired_at IS NULL
		`, in.Revision)
	})
}

// CreateExecutionBinding inserts a live binding for a session generation. The
// session must exist and not be deleted; uniqueness across
// (session_id, generation), (provider, installation, external_id) and the live
// -binding partial index is enforced by the schema. The binding must be written
// before a runtime credential is issued for that external compute.
func (c *Client) CreateExecutionBinding(ctx context.Context, sessionID, provider, installation, externalID string, generation int64) (*ExecutionInstanceBinding, error) {
	sid, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, fmt.Errorf("invalid session id %q: %w", sessionID, err)
	}
	var row ExecutionInstanceBinding
	err = c.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := readSession(ctx, tx, sessionID); err != nil {
			return fmt.Errorf("bind execution instance: %w", notFoundOr(err))
		}
		row, err = queryOne(ctx, tx, `
			INSERT INTO execution_instance_binding
			    (id, session_id, provider, installation, external_id, generation)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id, session_id, provider, installation, external_id, generation, created_at, revoked_at
		`, pgx.RowToStructByName[ExecutionInstanceBinding], uuid.New(), sid, provider, installation, externalID, generation)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// GetLiveExecutionBinding returns the live (unrevoked) binding for a session,
// or pgx.ErrNoRows if none exists. The runtime authenticator calls this on the
// verified credential's session to confirm the external compute is still the
// bound one for the claimed generation.
func (c *Client) GetLiveExecutionBinding(ctx context.Context, sessionID string) (*ExecutionInstanceBinding, error) {
	sid, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, fmt.Errorf("invalid session id %q: %w", sessionID, err)
	}
	row, err := queryOne(ctx, c.db, `
		SELECT id, session_id, provider, installation, external_id, generation, created_at, revoked_at
		FROM execution_instance_binding
		WHERE session_id = $1 AND revoked_at IS NULL
	`, pgx.RowToStructByName[ExecutionInstanceBinding], sid)
	if err != nil {
		return nil, notFoundOr(err)
	}
	return &row, nil
}

// ExecutionBindingResolution describes why a session has no live execution
// binding: the session's lifecycle state plus the most recent binding row it
// ever recorded, if any. The gateway reads it when GetLiveExecutionBinding
// reports not-found so it can classify the miss — an in-flight CREATE has no
// binding yet, a deleted session is gone, and a revoked binding on a
// still-live session is the loss reaper's durable record of confirmed runtime
// loss (only RevokeExecutionBinding revokes outside the DELETE operation,
// which revokes inside the same transaction that tombstones the session).
type ExecutionBindingResolution struct {
	// State is the session's current lifecycle state.
	State string `db:"state"`
	// Provider/Installation/ExternalID carry the session's most recent
	// binding — live or revoked — or empty when the session never bound.
	Provider     string     `db:"provider"`
	Installation string     `db:"installation"`
	ExternalID   string     `db:"external_id"`
	RevokedAt    *time.Time `db:"revoked_at"`
}

// GetExecutionBindingResolution returns the classification context for a
// session that has no live binding, or a record-not-found error when the
// session does not exist or is already tombstoned. A deleted session resolves
// to not-found rather than a row so a mid-DELETE revocation reads as "session
// gone", never as runtime loss.
func (c *Client) GetExecutionBindingResolution(ctx context.Context, sessionID string) (*ExecutionBindingResolution, error) {
	sid, err := uuid.Parse(sessionID)
	if err != nil {
		return nil, fmt.Errorf("invalid session id %q: %w", sessionID, err)
	}
	row, err := queryOne(ctx, c.db, `
		SELECT i.state,
		       COALESCE(b.provider, '')    AS provider,
		       COALESCE(b.installation, '') AS installation,
		       COALESCE(b.external_id, '')  AS external_id,
		       b.revoked_at                 AS revoked_at
		FROM session_record i
		LEFT JOIN LATERAL (
		    SELECT provider, installation, external_id, revoked_at
		    FROM execution_instance_binding x
		    WHERE x.session_id = i.id
		    ORDER BY x.generation DESC
		    LIMIT 1
		) b ON true
		WHERE i.id = $1 AND i.state <> 'RUNTIME_STATE_DELETED'
	`, pgx.RowToStructByName[ExecutionBindingResolution], sid)
	if err != nil {
		return nil, notFoundOr(err)
	}
	return &row, nil
}

// RevokeExecutionBinding marks the live binding for a session revoked, e.g. on
// runtime loss or deletion so stale credentials stop authorizing callbacks.
func (c *Client) RevokeExecutionBinding(ctx context.Context, sessionID string) error {
	sid, err := uuid.Parse(sessionID)
	if err != nil {
		return fmt.Errorf("invalid session id %q: %w", sessionID, err)
	}
	return execSQL(ctx, c.db, `
		UPDATE execution_instance_binding SET revoked_at = now()
		WHERE session_id = $1 AND revoked_at IS NULL
	`, sid)
}

// GetLiveExecutionBindingByExternal returns the live (unrevoked) binding that
// claims a given external sandbox within an installation, or a record-not-found
// error when no session currently owns it. The orphan reconciler uses it to
// decide whether a running sandbox the provider reports is still bound: an
// external id with no live binding is an orphan (created but never committed,
// or bound to a since-revoked session) and is safe to delete.
func (c *Client) GetLiveExecutionBindingByExternal(ctx context.Context, installation, externalID string) (*ExecutionInstanceBinding, error) {
	row, err := queryOne(ctx, c.db, `
		SELECT id, session_id, provider, installation, external_id, generation, created_at, revoked_at
		FROM execution_instance_binding
		WHERE provider = $1 AND installation = $2 AND external_id = $3 AND revoked_at IS NULL
	`, pgx.RowToStructByName[ExecutionInstanceBinding], ExecutionProviderE2B, installation, externalID)
	if err != nil {
		return nil, notFoundOr(err)
	}
	return &row, nil
}

// GetE2BRevisionBinding returns the durable artifact binding the E2B session-
// create path needs to place a sandbox: the installation, external template id
// and concrete build id recorded when the revision was prepared. It returns a
// record-not-found error for a revision without an E2B binding so the caller
// fails preparation rather than inventing a template.
func (c *Client) GetE2BRevisionBinding(ctx context.Context, revision string) (*ExecutionRevisionBinding, error) {
	b, err := queryOne(ctx, c.db, `
		SELECT id, revision, provider, installation, external_id, build_id, config_json, launch_contract, environment, created_at
		FROM execution_revision_binding
		WHERE revision = $1 AND provider = $2
	`, pgx.RowToStructByName[ExecutionRevisionBinding], revision, ExecutionProviderE2B)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// requireExecutionBinding asserts inside a callback mutation transaction that
// the session's live binding still matches the credential that authorized the
// call. The SELECT ... FOR UPDATE takes the binding row lock, so a concurrent
// RevokeExecutionBinding either commits first (this query finds no live row
// and the write fails) or waits for this write to commit (the write was
// authorized before revocation). It never runs outside the session's mutation
// transaction: callers hold the session lock first, matching lockSession.
func requireExecutionBinding(ctx context.Context, tx pgx.Tx, sessionID string, expected *ExecutionInstanceBinding) error {
	if expected == nil {
		return nil
	}
	sid, err := uuid.Parse(sessionID)
	if err != nil {
		return fmt.Errorf("invalid session id %q: %w", sessionID, err)
	}
	_, err = queryOne(ctx, tx, `
		SELECT id FROM execution_instance_binding
		WHERE session_id = $1 AND revoked_at IS NULL
		  AND provider = $2 AND installation = $3 AND external_id = $4 AND generation = $5
		FOR UPDATE
	`, pgx.RowTo[uuid.UUID], sid, expected.Provider, expected.Installation, expected.ExternalID, expected.Generation)
	// A revoked or mismatched binding must not authorize the in-flight write.
	return notFoundOr(err)
}

// GetSessionForExecution returns the session bound to an E2B runtime, without
// the Substrate actor-UID check GetSessionForRuntime performs. The caller has
// already verified the execution binding; this only loads the session and
// confirms it is not deleted.
func (c *Client) GetSessionForExecution(ctx context.Context, id string) (*apiv1alpha1.Session, error) {
	row, err := queryOne(ctx, c.db, `
		SELECT id, user_id, prepared_revision, state, data, operation, context_id,
		    source_checkpoint_id, history_id, operation_id, executor_id
		FROM session_record WHERE id = $1 AND state <> 'RUNTIME_STATE_DELETED'
	`, pgx.RowToStructByName[sessionRow], id)
	if err != nil {
		return nil, notFoundOr(err)
	}
	return toSession(row)
}

// E2BCreateBinding carries the concrete external compute an E2B session is
// bound to on creation: the sandbox identity and the generation it begins.
type E2BCreateBinding struct {
	// Installation must match the prepared revision's artifact binding.
	Installation string
	// ExternalID is the E2B sandbox this session runs on.
	ExternalID string
	// Generation is the instance generation the binding opens (starts at 1).
	Generation int64
}

// FinishE2BSessionCreate completes an E2B session's CREATE operation and
// BindE2BSessionCreate records the session's instance binding while CREATE is
// still pending — the first of two create-phase commits. It verifies the
// session is inside a claimed CREATE owned by this executor, confirms the
// prepared revision is an E2B revision and the binding's installation matches
// the revision's artifact binding, then inserts the instance binding. The
// operation is left claimed and pending: a bootstrap failure after this
// commit leaves a bound-but-unactivated session whose still-open CREATE can
// be retried against the same sandbox, never a READY session with no harness.
// Call FinishE2BSessionCreate after the bound guest is activated.
func (c *Client) BindE2BSessionCreate(ctx context.Context, sessionID string, id, executorID uuid.UUID, binding E2BCreateBinding) error {
	if binding.Installation == "" || binding.ExternalID == "" || binding.Generation <= 0 {
		return fmt.Errorf("E2B create requires installation, external id, and a positive generation")
	}
	return c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSession(ctx, tx, sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		// Ownership must match a live claimed CREATE: the same operation and
		// the executor that claimed it. A bind from a stale or foreign attempt
		// must not attach compute to this session.
		if row.Operation != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE.String() ||
			row.OperationID == nil || *row.OperationID != id ||
			row.ExecutorID == nil || *row.ExecutorID != executorID {
			return fmt.Errorf("E2B bind requires the claimed CREATE operation: %w", ErrConflict)
		}
		// The prepared revision must be an E2B revision with a genuine artifact
		// binding, and the instance binding's installation must match it.
		revisionBinding, err := queryOne(ctx, tx, `
			SELECT b.installation FROM runtime_revision r
			JOIN execution_revision_binding b ON b.revision = r.revision
			WHERE r.revision = $1 AND r.provider = $2 AND b.provider = $2
		`, pgx.RowTo[string], derefStr(row.PreparedRevision), ExecutionProviderE2B)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("session prepared revision is not an E2B revision with an artifact binding: %w", ErrFailedPrecondition)
		}
		if err != nil {
			return err
		}
		if revisionBinding != binding.Installation {
			return fmt.Errorf("E2B binding installation %q does not match the prepared revision's %q: %w", binding.Installation, revisionBinding, ErrFailedPrecondition)
		}
		// actor_uid stays NULL for E2B; the binding is the private compute
		// record the runtime credential resolves through. Inserting it now —
		// while CREATE is still open — is what makes a post-bind bootstrap
		// failure recoverable on the same sandbox rather than a READY session
		// with no harness.
		if err := execSQL(ctx, tx, `
			INSERT INTO execution_instance_binding
			    (id, session_id, provider, installation, external_id, generation)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, uuid.New(), row.ID, ExecutionProviderE2B, binding.Installation, binding.ExternalID, binding.Generation); err != nil {
			return fmt.Errorf("bind E2B execution instance: %w", err)
		}
		// The binding transaction also resolves the create attempt's outcome:
		// the bound external id IS the verified identity this create landed,
		// for clean create, reconcile, and late adoption alike. Recording it
		// here keeps the cleanup precondition (a named external_id) satisfied
		// atomically with the binding — a crash between a separate mark call
		// and this commit would leave the attempt unresolved and later refuse
		// delete. An attempt row may not exist (direct binds and paths that
		// never reached RecordE2BCreateAttempt); that is not a conflict. Only a
		// row that recorded a DIFFERENT external id is a defect — a known
		// identity must not be silently absorbed.
		tag, err := tx.Exec(ctx, `
			UPDATE execution_create_attempt
			SET external_id = $2, outcome = 'known'
			WHERE operation_id = $1 AND outcome IN ('issued','known')
			  AND (external_id IS NULL OR external_id = $2)
		`, id, binding.ExternalID)
		if err != nil {
			return fmt.Errorf("record E2B create attempt outcome: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// No row updated: either no attempt exists, or one exists that the
			// UPDATE's predicates excluded — a 'rejected' attempt (the create
			// provably placed nothing, so binding is impossible) or an attempt
			// already bound to DIFFERENT compute. Both are defects, not a bind
			// to silently absorb. Only a genuinely absent attempt may proceed —
			// and that only happens for fixtures that bypass the issued-record
			// step, never a real create.
			var outcome *string
			err := tx.QueryRow(ctx, `
				SELECT outcome FROM execution_create_attempt WHERE operation_id = $1
			`, id).Scan(&outcome)
			switch {
			case err == nil:
				return fmt.Errorf("E2B create attempt for operation %s is %q, cannot bind %q: %w", id, derefStr(outcome), binding.ExternalID, ErrConflict)
			case !errors.Is(err, pgx.ErrNoRows):
				return fmt.Errorf("verify E2B create attempt outcome: %w", err)
			}
			// err == pgx.ErrNoRows: no attempt recorded — proceed (fixtures only).
		}
		return nil
	})
}

// FinishE2BSessionCreate completes an E2B session's CREATE operation after the
// bound guest is activated. It verifies operation and executor ownership
// through completeRuntimeOperation, requires the live instance binding
// BindE2BSessionCreate committed (proof the binding phase ran and the
// activation step had a target), leaves actor_uid NULL, sets the logical
// e2b://session authority, and transitions the session to READY. A session
// reaching here with no live binding failed before bind and must not become
// READY. A Substrate revision returns ErrFailedPrecondition; use
// FinishSessionOperation for those.
func (c *Client) FinishE2BSessionCreate(ctx context.Context, sessionID string, id, executorID uuid.UUID) (*apiv1alpha1.Session, error) {
	var result *apiv1alpha1.Session
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSession(ctx, tx, sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		next, err := completeRuntimeOperation(row.runtimeInstanceRow, id, executorID, "")
		if err != nil {
			return err
		}
		result, err = toSession(row)
		if err != nil {
			return err
		}
		if result.Operation != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE {
			return fmt.Errorf("E2B create completion requires a CREATE operation: %w", ErrConflict)
		}
		// READY requires the live instance binding BindE2BSessionCreate
		// committed while CREATE was still open. A session reaching completion
		// with no live binding never had its bound guest activated — a crash
		// or bootstrap failure — so it must not become READY here.
		_, err = queryOne(ctx, tx, `
			SELECT installation FROM execution_instance_binding
			WHERE session_id = $1 AND revoked_at IS NULL
		`, pgx.RowTo[string], row.ID)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("E2B session has no live execution binding: %w", ErrFailedPrecondition)
		}
		if err != nil {
			return err
		}
		// actor_uid stays NULL for E2B; the session's authority is the logical
		// handle resolved later through the private instance binding.
		result.A2AAuthority = e2bSessionAuthority(sessionID)
		result.State, result.Operation, err = next.lifecycle()
		if err != nil {
			return err
		}
		result.PreparedRevision = derefStr(next.PreparedRevision)
		result.UpdatedAt = timestamppb.Now()
		result.Failure = nil
		row.Data, err = marshalSession(result)
		if err != nil {
			return err
		}
		if err := saveRuntimeLifecycle(ctx, tx, next, runtimeKindAgent); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
            UPDATE session SET data = $2 WHERE id = $1 AND actor_uid IS NULL
        `, row.ID, row.Data)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("E2B session unexpectedly bound to an actor UID: %w", ErrConflict)
		}
		row.runtimeInstanceRow = next
		result, err = toSession(row)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("finish E2B Session create: %w", err)
	}
	return result, nil
}

// FinishE2BSessionDelete completes an E2B session's DELETE operation and
// revokes its live instance binding in the same transaction, so a deleted
// session's sandbox handle can never be resolved again. It verifies operation
// and executor ownership through completeRuntimeOperation, confirms the
// prepared revision was E2B, clears the routing authority, releases the
// session's runtime references, preserves actor_uid NULL, and runs the shared
// deletion cleanup. The caller is responsible for deleting the external
// sandbox first (or tolerating an already-gone sandbox) before committing this
// durable deletion; the binding is revoked regardless so the gateway stops
// resolving it.
func (c *Client) FinishE2BSessionDelete(ctx context.Context, sessionID string, id, executorID uuid.UUID) (*apiv1alpha1.Session, error) {
	var result *apiv1alpha1.Session
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSession(ctx, tx, sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		next, err := completeRuntimeOperation(row.runtimeInstanceRow, id, executorID, "")
		if err != nil {
			return err
		}
		result, err = toSession(row)
		if err != nil {
			return err
		}
		if result.Operation != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE {
			return fmt.Errorf("E2B delete completion requires a DELETE operation: %w", ErrConflict)
		}
		// The deleted revision must have been an E2B revision; a Substrate
		// session is deleted through FinishSessionOperation.
		if row.PreparedRevision != nil {
			provider, err := queryOne(ctx, tx, `
				SELECT provider FROM runtime_revision WHERE revision = $1
			`, pgx.RowTo[string], *row.PreparedRevision)
			if err != nil {
				return notFoundOr(err)
			}
			if provider != ExecutionProviderE2B {
				return fmt.Errorf("runtime revision %s is not an E2B revision: %w", *row.PreparedRevision, ErrFailedPrecondition)
			}
		}
		result.A2AAuthority = ""
		if err := releaseAgentRuntimeReferences(ctx, tx, row.ID); err != nil {
			return err
		}
		result.State, result.Operation, err = next.lifecycle()
		if err != nil {
			return err
		}
		result.PreparedRevision = derefStr(next.PreparedRevision)
		result.UpdatedAt = timestamppb.Now()
		result.Failure = nil
		row.Data, err = marshalSession(result)
		if err != nil {
			return err
		}
		if err := saveRuntimeLifecycle(ctx, tx, next, runtimeKindAgent); err != nil {
			return err
		}
		// Revoke the live binding before the session tombstones, so no runtime
		// credential or gateway dial resolves the deleted sandbox afterwards.
		if err := execSQL(ctx, tx, `
			UPDATE execution_instance_binding SET revoked_at = now()
			WHERE session_id = $1 AND revoked_at IS NULL
		`, row.ID); err != nil {
			return fmt.Errorf("revoke E2B execution binding: %w", err)
		}
		tag, err := tx.Exec(ctx, `
            UPDATE session SET data = $2 WHERE id = $1 AND actor_uid IS NULL
        `, row.ID, row.Data)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("E2B session unexpectedly bound to an actor UID: %w", ErrConflict)
		}
		row.runtimeInstanceRow = next
		result, err = toSession(row)
		if err != nil {
			return err
		}
		if result.State == apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED {
			return finishSessionDeletion(ctx, tx, row.ID)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("finish E2B Session delete: %w", err)
	}
	return result, nil
}

// e2bSessionAuthorityScheme is the non-secret logical routing scheme recorded
// in Session.a2a_authority for an E2B session; the handle is
// "e2b://session/<session-id>". It exposes neither the sandbox ID nor the
// installation; those are resolved through the private instance binding. The
// gateway resolves it through E2BSessionAuthority, not by parsing a host.
const e2bSessionAuthorityScheme = "e2b://session/"

func e2bSessionAuthority(sessionID string) string {
	return e2bSessionAuthorityScheme + sessionID
}

// E2BSessionAuthority returns the logical handle recorded in
// Session.a2a_authority for an E2B session, so callers that only have the
// session id can compare the whole authority rather than re-derive the scheme.
func E2BSessionAuthority(sessionID string) string {
	return e2bSessionAuthority(sessionID)
}

// IsE2BSessionAuthority reports whether an a2a_authority value is the E2B
// logical-session scheme. The trailing id is not validated here; matching the
// concrete session is the caller's consistency check.
func IsE2BSessionAuthority(authority string) bool {
	return strings.HasPrefix(authority, e2bSessionAuthorityScheme) &&
		len(authority) > len(e2bSessionAuthorityScheme)
}

// E2BCreateAttempt is the durable record that a CREATE operation's
// CreateSandbox call was issued. It persists across a lost response or an
// executor crash so a later attempt on the same operation reconciles rather
// than issuing a second create.
type E2BCreateAttempt struct {
	ID           uuid.UUID `db:"id"`
	SessionID    uuid.UUID `db:"session_id"`
	OperationID  uuid.UUID `db:"operation_id"`
	Provider     string    `db:"provider"`
	Installation string    `db:"installation"`
	ExternalID   *string   `db:"external_id"`
	Outcome      string    `db:"outcome"`
}

// RecordE2BCreateAttempt writes the "create was issued" marker for a claimed
// CREATE operation in its own transaction — after the claim, before the
// provider call. It verifies the caller still holds the operation's executor
// claim, so only the claiming executor can mark create issued. If an
// unresolved attempt already exists for this operation the call is a no-op
// returning the existing record: one operation issues at most one in-flight
// create, and a retry reconciles that attempt. A resolved 'rejected' attempt
// is different — the provider provably placed nothing, so the same row is
// re-armed to 'issued' (keeping the single-attempt-per-operation invariant)
// to record that a NEW create is being issued on this retry.
func (c *Client) RecordE2BCreateAttempt(ctx context.Context, sessionID string, operationID, executorID uuid.UUID, installation string) (*E2BCreateAttempt, bool, error) {
	var attempt *E2BCreateAttempt
	var created bool
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSession(ctx, tx, sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		// Only the operation's claiming executor may mark create issued; a stale
		// or foreign attempt must not pin the operation to a create it never ran.
		if row.Operation != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE.String() ||
			row.OperationID == nil || *row.OperationID != operationID ||
			row.ExecutorID == nil || *row.ExecutorID != executorID {
			return fmt.Errorf("E2B create-attempt requires the claimed CREATE operation: %w", ErrConflict)
		}
		existing, err := queryOne[E2BCreateAttempt](ctx, tx, `
			SELECT id, session_id, operation_id, provider, installation, external_id, outcome
			FROM execution_create_attempt WHERE operation_id = $1
		`, func(r pgx.CollectableRow) (E2BCreateAttempt, error) {
			var a E2BCreateAttempt
			return a, r.Scan(&a.ID, &a.SessionID, &a.OperationID, &a.Provider, &a.Installation, &a.ExternalID, &a.Outcome)
		}, operationID)
		if err == nil {
			if existing.Outcome == "rejected" {
				// Resolved rejection: re-arm the same row so this retry's new
				// create is durably marked issued. Reusing it preserves one
				// attempt record per operation; clearing external_id keeps a
				// stale id from steering the new create's reconcile.
				if err := execSQL(ctx, tx, `
					UPDATE execution_create_attempt
					SET outcome = 'issued', external_id = NULL, installation = $2
					WHERE operation_id = $1 AND outcome = 'rejected'
				`, operationID, installation); err != nil {
					return fmt.Errorf("re-arm rejected E2B create attempt: %w", err)
				}
				existing.Outcome = "issued"
				existing.ExternalID = nil
				existing.Installation = installation
				attempt = &existing
				created = true
				return nil
			}
			attempt = &existing
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		a := E2BCreateAttempt{
			ID: uuid.New(), SessionID: row.ID, OperationID: operationID,
			Provider: ExecutionProviderE2B, Installation: installation, Outcome: "issued",
		}
		if err := execSQL(ctx, tx, `
			INSERT INTO execution_create_attempt
			    (id, session_id, operation_id, provider, installation, outcome)
			VALUES ($1, $2, $3, $4, $5, 'issued')
		`, a.ID, a.SessionID, a.OperationID, a.Provider, a.Installation); err != nil {
			return fmt.Errorf("record E2B create attempt: %w", err)
		}
		attempt = &a
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return attempt, created, nil
}

// GetE2BCreateAttempt returns the issued create attempt for an operation, or
// ErrNotFound if no create has been issued for it.
func (c *Client) GetE2BCreateAttempt(ctx context.Context, operationID uuid.UUID) (*E2BCreateAttempt, error) {
	a, err := queryOne[E2BCreateAttempt](ctx, c.db, `
		SELECT id, session_id, operation_id, provider, installation, external_id, outcome
		FROM execution_create_attempt WHERE operation_id = $1
	`, func(r pgx.CollectableRow) (E2BCreateAttempt, error) {
		var a E2BCreateAttempt
		return a, r.Scan(&a.ID, &a.SessionID, &a.OperationID, &a.Provider, &a.Installation, &a.ExternalID, &a.Outcome)
	}, operationID)
	if err != nil {
		return nil, notFoundOr(err)
	}
	return &a, nil
}

// MarkE2BCreateAttemptKnown records the external sandbox id the issued create
// resolved to, so a later reconcile prefers this verified identity over a
// metadata-tag match. No-op if the attempt does not exist.
func (c *Client) MarkE2BCreateAttemptKnown(ctx context.Context, operationID uuid.UUID, externalID string) error {
	return execSQL(ctx, c.db, `
		UPDATE execution_create_attempt SET external_id = $2, outcome = 'known'
		WHERE operation_id = $1
	`, operationID, externalID)
}

// FailE2BSessionCreate resolves a definitively-rejected E2B create and
// releases the executor's claim in ONE transaction. A definitive rejection
// (auth/validation) placed no sandbox, so the issued attempt is resolved
// 'rejected' — never reconciled — and the CREATE is released so a corrected
// request can re-claim and issue a fresh create. It is fenced on the session's
// claimed CREATE owned by this executor; a stale or foreign attempt is a
// conflict, not a silent release. Ambiguous outcomes (timeout, 5xx, dropped
// response) do NOT come here — they keep 'issued' and reconcile instead.
func (c *Client) FailE2BSessionCreate(ctx context.Context, sessionID string, operationID, executorID uuid.UUID) error {
	return c.withTx(ctx, func(tx pgx.Tx) error {
		row, err := lockSession(ctx, tx, sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if row.Operation != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE.String() ||
			row.OperationID == nil || *row.OperationID != operationID ||
			row.ExecutorID == nil || *row.ExecutorID != executorID {
			return fmt.Errorf("E2B create rejection requires the claimed CREATE operation: %w", ErrConflict)
		}
		// Resolve the issued attempt to 'rejected' so no later attempt steers
		// this operation toward reconciling a create that provably placed no
		// sandbox. The attempt MUST exist and be 'issued': a missing row means
		// the create was never durably issued (the rejection is a no-op the
		// caller misattributed), and an already-'known' or 'rejected' row is a
		// conflicting durable state — neither may silently release the claim.
		tag, err := tx.Exec(ctx, `
			UPDATE execution_create_attempt SET outcome = 'rejected'
			WHERE operation_id = $1 AND outcome = 'issued'
		`, operationID)
		if err != nil {
			return fmt.Errorf("mark E2B create attempt rejected: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("E2B create rejection requires the operation's issued attempt: %w", ErrConflict)
		}
		// Release the executor claim so the operation can be re-claimed by a
		// corrected request. Releasing (not completing) keeps the CREATE open:
		// the create itself failed, there is nothing to finalize.
		if _, err := tx.Exec(ctx, `
			UPDATE runtime_instance SET executor_id = NULL, executor_expires_at = clock_timestamp()
			WHERE id = $1 AND operation_id = $2 AND executor_id = $3
		`, row.ID, operationID, executorID); err != nil {
			return fmt.Errorf("release E2B create executor: %w", err)
		}
		return nil
	})
}

// E2BOrphanDeletionGate is the durable fence between the orphan reaper's delete
// and a create operation's adoption of the same sandbox. The reaper's earlier
// checks — the live-binding lookup, the create-attempt correlation, the open-
// operation snapshot — are all separate non-transactional reads, so a
// concurrent adoption or re-claim can commit between the last read and the
// provider DeleteSandbox call and leave a READY session pointing at deleted
// compute. This method serializes the final decision on the owning session's
// runtime_instance row lock, the exact lock BindE2BSessionCreate,
// claimRuntimeOperation, FinishE2BSessionCreate, and beginSessionDeletion all
// contend on, and re-verifies inside that critical section that the sandbox is
// still orphaned.
//
// It returns false — the delete must not happen — when:
//   - the attempt tag resolved to no local create attempt (foreign/co-tenant),
//   - the attempt's session or installation does not match, or a recorded
//     external_id names a different sandbox,
//   - a live execution binding now claims this external id (adoption committed),
//   - the owning session is still inside an open CREATE operation (a binder can
//     still adopt), or
//   - the session row is gone or no longer decodeable (indeterminate).
//
// It returns true only when, under the session lock, the sandbox is provably
// unclaimed and un-adoptable. The row lock is released when this transaction
// commits — it does NOT span the caller's provider DeleteSandbox call, and it
// does not need to: safety rests on the durable state the lock made the
// predicate check against being irreversible. The gate only approves when the
// owning CREATE is no longer the session's open operation (superseded by a
// DELETE, settled, or the session tombstoned). BindE2BSessionCreate requires
// operation == CREATE with that exact operation id and a live executor claim,
// so once the gate observes the operation closed, that operation id can never
// bind the sandbox afterward — there is no lock-free window through which a
// stale adoption can resurrect a reaped sandbox.
func (c *Client) E2BOrphanDeletionGate(ctx context.Context, operationID uuid.UUID, installation, externalID string) (bool, error) {
	var allowed bool
	err := c.withTx(ctx, func(tx pgx.Tx) error {
		// Resolve the owning create attempt to its session and confirm the
		// attempt names this installation and, when recorded, this sandbox.
		// The attempt row is the durable correlation the create path stamped.
		attempt, err := queryOne[E2BCreateAttempt](ctx, tx, `
			SELECT id, session_id, operation_id, provider, installation, external_id, outcome
			FROM execution_create_attempt WHERE operation_id = $1
		`, func(r pgx.CollectableRow) (E2BCreateAttempt, error) {
			var a E2BCreateAttempt
			return a, r.Scan(&a.ID, &a.SessionID, &a.OperationID, &a.Provider, &a.Installation, &a.ExternalID, &a.Outcome)
		}, operationID)
		if errors.Is(err, pgx.ErrNoRows) {
			// No local attempt: the tag is foreign or already cleaned up. Not
			// this deployment's to delete.
			return nil
		}
		if err != nil {
			return err
		}
		if attempt.Installation != installation {
			return nil // tag resolves to a different installation
		}
		if attempt.ExternalID != nil && *attempt.ExternalID != "" && *attempt.ExternalID != externalID {
			return nil // a recorded external id names another sandbox
		}

		// Lock the owning session row. This is the serialization point every
		// adoption path takes, so while we hold it no claim, bind, finish, or
		// delete can commit against this session.
		row, err := lockSession(ctx, tx, attempt.SessionID.String())
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // session gone; indeterminate, do not delete
		}
		if err != nil {
			return err
		}
		state, operation, err := row.lifecycle()
		if err != nil {
			return err
		}
		// An open CREATE is the only operation that can still adopt this
		// sandbox: BindE2BSessionCreate requires operation == CREATE with the
		// matching operation id and a live executor claim. While that operation
		// is open the sandbox is adoptable, so deleting now would race the
		// bind. Any other operation — a DELETE that superseded the create, or a
		// resumed create under a different id — cannot bind this sandbox, so
		// the only remaining question is whether a live binding already claims
		// it (checked below). A session whose owning CREATE is the open
		// operation is never orphaned; a session already DELETED has no path
		// back to binding either.
		if state != apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED &&
			operation == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE &&
			row.OperationID != nil && *row.OperationID == operationID {
			return nil
		}

		// Under the lock, confirm no live binding claims this external id. An
		// adoption that committed before we took the lock is observed here; one
		// that commits after we release still sees the sandbox deleted by our
		// caller — but it cannot have bound a sandbox we already reaped because
		// the gate proved it was unbound at the serialized instant.
		_, err = queryOne[uuid.UUID](ctx, tx, `
			SELECT id FROM execution_instance_binding
			WHERE provider = $1 AND installation = $2 AND external_id = $3 AND revoked_at IS NULL
			FOR UPDATE
		`, pgx.RowTo[uuid.UUID], ExecutionProviderE2B, installation, externalID)
		if err == nil {
			return nil // a live binding now claims it — adopted, do not delete
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		allowed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return allowed, nil
}
