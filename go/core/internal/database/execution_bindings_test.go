package database

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
)

// execSessionFixture creates a session with a runtime revision so an
// execution_instance_binding row can reference a real session.
func execSessionFixture(t *testing.T, client *Client, ctx context.Context) string {
	t.Helper()
	sessionFixture(t, client, ctx, "team-a", "exec-rev-"+uuid.NewString()[:8], "assistant", "kagent")
	s, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.NoError(t, err)
	return s.GetId()
}

func TestExecutionBindingLifecycle(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	sessionID := execSessionFixture(t, client, ctx)

	b, err := client.CreateExecutionBinding(ctx, sessionID, ExecutionProviderE2B, "embed-lab", "sandbox-abc", 1)
	require.NoError(t, err)
	assert.Equal(t, ExecutionProviderE2B, b.Provider)
	assert.Equal(t, "sandbox-abc", b.ExternalID)
	assert.EqualValues(t, 1, b.Generation)
	assert.Nil(t, b.RevokedAt)

	got, err := client.GetLiveExecutionBinding(ctx, sessionID)
	require.NoError(t, err)
	assert.Equal(t, b.ID, got.ID)
	assert.Equal(t, "sandbox-abc", got.ExternalID)

	// A second live binding for the same session is rejected by the partial
	// unique index.
	_, err = client.CreateExecutionBinding(ctx, sessionID, ExecutionProviderE2B, "embed-lab", "sandbox-other", 2)
	require.Error(t, err)

	// The same external id cannot bind a second session in one installation.
	other := execSessionFixture(t, client, ctx)
	_, err = client.CreateExecutionBinding(ctx, other, ExecutionProviderE2B, "embed-lab", "sandbox-abc", 1)
	require.Error(t, err)

	require.NoError(t, client.RevokeExecutionBinding(ctx, sessionID))
	_, err = client.GetLiveExecutionBinding(ctx, sessionID)
	require.Error(t, err) // no live binding remains
}

func TestGetSessionForExecution(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	sessionID := execSessionFixture(t, client, ctx)

	s, err := client.GetSessionForExecution(ctx, sessionID)
	require.NoError(t, err)
	assert.Equal(t, sessionID, s.GetId())

	_, err = client.GetSessionForExecution(ctx, uuid.NewString())
	require.Error(t, err)
}

// A revoked or mismatched binding must fence the runtime write it was carried
// into. The binding is re-verified inside the mutation transaction, so a
// credential authorized before a concurrent revoke cannot commit a write
// afterwards, and a binding for the wrong generation/external compute never
// authorizes a write in the first place.
func TestExecutionBindingFencesRuntimeWrite(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	sessionID := execSessionFixture(t, client, ctx)
	markSessionReady(ctx, client, sessionID, "agent.example")

	live, err := client.CreateExecutionBinding(ctx, sessionID, ExecutionProviderE2B, "embed-lab", "sandbox-abc", 1)
	require.NoError(t, err)

	task := newSessionTask(uuid.NewString(), "initial")
	session, err := client.GetSessionForExecution(ctx, sessionID)
	require.NoError(t, err)
	task.ContextID = session.ContextId

	// The live binding authorizes a runtime write.
	_, err = client.CreateRuntimeTask(ctx, sessionID, taskMutationHash("initial"), task, "", live)
	require.NoError(t, err)

	// A credential for the wrong generation does not authorize a write.
	staleGen := *live
	staleGen.Generation = 99
	other := newSessionTask(uuid.NewString(), "stale-gen")
	other.ContextID = session.ContextId
	_, err = client.CreateRuntimeTask(ctx, sessionID, taskMutationHash("stale-gen"), other, "", &staleGen)
	require.ErrorIs(t, err, ErrNotFound)

	// A credential for different external compute does not authorize a write.
	wrongExternal := *live
	wrongExternal.ExternalID = "sandbox-other"
	other = newSessionTask(uuid.NewString(), "wrong-external")
	other.ContextID = session.ContextId
	_, err = client.CreateRuntimeTask(ctx, sessionID, taskMutationHash("wrong-external"), other, "", &wrongExternal)
	require.ErrorIs(t, err, ErrNotFound)

	// After revocation, even the previously-live binding no longer authorizes.
	require.NoError(t, client.RevokeExecutionBinding(ctx, sessionID))
	other = newSessionTask(uuid.NewString(), "after-revoke")
	other.ContextID = session.ContextId
	_, err = client.CreateRuntimeTask(ctx, sessionID, taskMutationHash("after-revoke"), other, "", live)
	require.ErrorIs(t, err, ErrNotFound)
}

// A revoke committed before a write begins leaves no live binding, so the
// carried credential is stale and the write fails. (Sequential coverage only;
// the FOR UPDATE lock ordering that serializes a truly concurrent revoke is
// exercised at the SQL level, not by this test.)
func TestExecutionBindingRevokedBeforeWrite(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	sessionID := execSessionFixture(t, client, ctx)
	markSessionReady(ctx, client, sessionID, "agent.example")

	live, err := client.CreateExecutionBinding(ctx, sessionID, ExecutionProviderE2B, "embed-lab", "sandbox-abc", 1)
	require.NoError(t, err)
	session, err := client.GetSessionForExecution(ctx, sessionID)
	require.NoError(t, err)

	// Revoke before the write: the carried binding is stale and the write fails.
	require.NoError(t, client.RevokeExecutionBinding(ctx, sessionID))
	task := newSessionTask(uuid.NewString(), "post-revoke")
	task.ContextID = session.ContextId
	_, err = client.CreateRuntimeTask(ctx, sessionID, taskMutationHash("post-revoke"), task, "", live)
	require.ErrorIs(t, err, ErrNotFound)
}

// e2bRevisionFixture registers an E2B prepared revision ready for an active
// definition, returning its revision id. The agent definition must exist.
func e2bRevisionFixture(t *testing.T, client *Client, ctx context.Context, namespace, revisionID, template, installation string) {
	t.Helper()
	require.NoError(t, client.UpsertAgentDefinition(ctx, AgentDefinition{
		Namespace: namespace, AgentName: template, AgentUID: template + "-uid",
		DesiredRevision: revisionID,
	}))
	require.NoError(t, client.RecordE2BRuntimeRevision(ctx, E2BRuntimeRevisionInput{
		Revision: revisionID, Namespace: namespace, AgentName: template, AgentUID: template + "-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), ConfigJSON: []byte(`{"version":5}`), AgentCard: &a2apb.AgentCard{}, EgressDestinations: []string{},
		LaunchContract: []byte(`{"command":"nohup h &"}`), Installation: installation, ExternalID: "template-" + revisionID, BuildID: "build-" + revisionID,
	}, true))
}

// e2bSessionFixture creates a session against a ready E2B revision, returns it
// still in RUNTIME_STATE_CREATING with the CREATE operation pending.
func e2bSessionFixture(t *testing.T, client *Client, ctx context.Context, revisionID string) *apiv1alpha1.Session {
	t.Helper()
	session, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.NoError(t, err)
	require.Equal(t, revisionID, session.GetPreparedRevision())
	return session
}

// An E2B revision records NULL ActorTemplate fields and its genuine artifact
// binding; readers must not see it as a valid Substrate artifact.
func TestE2BRevisionRecordsNullActorTemplateAndBinding(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")

	var provider string
	var atespace, name, uid *string
	require.NoError(t, client.db.QueryRow(ctx, `
		SELECT provider, actor_template_atespace, actor_template_name, actor_template_uid
		FROM runtime_revision WHERE revision = $1
	`, revisionID).Scan(&provider, &atespace, &name, &uid))
	assert.Equal(t, ExecutionProviderE2B, provider)
	assert.Nil(t, atespace)
	assert.Nil(t, name)
	assert.Nil(t, uid)

	var installation, externalID, buildID string
	require.NoError(t, client.db.QueryRow(ctx, `
		SELECT installation, external_id, build_id FROM execution_revision_binding WHERE revision = $1
	`, revisionID).Scan(&installation, &externalID, &buildID))
	assert.Equal(t, "embed-lab", installation)
	assert.Equal(t, "template-"+revisionID, externalID)
	assert.Equal(t, "build-"+revisionID, buildID)

	// The reader surfaces the E2B revision with empty ActorTemplate fields and
	// the e2b provider, never a fabricated Substrate identity.
	stored, err := client.GetRuntimeRevision(ctx, revisionID)
	require.NoError(t, err)
	assert.Equal(t, ExecutionProviderE2B, stored.Provider)
	assert.Empty(t, stored.ActorTemplateAtespace)
	assert.Empty(t, stored.ActorTemplateName)
	assert.Empty(t, stored.ActorTemplateUID)
}

// The deferred provider FK rejects an E2B revision whose artifact binding is
// missing, and rejects a binding whose provider disagrees with its revision.
func TestE2BRevisionRequiresArtifactBinding(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()

	// A revision row committed without its binding violates the deferred
	// foreign key at commit.
	err := client.withTx(ctx, func(tx pgx.Tx) error {
		return recordRuntimeRevision(ctx, tx, runtimeRevisionRecord{
			RuntimeArtifact: RuntimeArtifact{
				Revision: "orphan-" + uuid.NewString()[:8], Kind: runtimeKindAgent, Namespace: "team-a",
				Provider: ExecutionProviderE2B,
			},
			SourceSnapshot: []byte(`{"provenance":true}`),
		})
	})
	require.Error(t, err)

	// A binding cannot attach to a Substrate revision (provider mismatch on the
	// composite foreign key).
	revisionID := "substrate-rev-" + uuid.NewString()[:8]
	sessionFixture(t, client, ctx, "team-a", revisionID, "assistant", "kagent")
	err = client.withTx(ctx, func(tx pgx.Tx) error {
		return execSQL(ctx, tx, `
			INSERT INTO execution_revision_binding (id, revision, provider, installation, external_id, build_id, config_json)
			VALUES ($1, $2, 'e2b', 'embed-lab', 'x', 'b', '{"v":5}')
		`, uuid.New(), revisionID)
	})
	require.Error(t, err)
}

// FinishE2BSessionCreate persists the instance binding and transitions to
// READY atomically with actor_uid NULL and the e2b:// authority handle.
func TestFinishE2BSessionCreate(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
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
	finished, err := client.FinishE2BSessionCreate(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	assert.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, finished.State)
	assert.Equal(t, "e2b://session/"+session.Id, finished.A2AAuthority)

	// actor_uid stays NULL and the live instance binding matches the create.
	var actorUID *string
	require.NoError(t, client.db.QueryRow(ctx, `SELECT actor_uid FROM session WHERE id = $1`, session.Id).Scan(&actorUID))
	assert.Nil(t, actorUID)
	binding, err := client.GetLiveExecutionBinding(ctx, session.Id)
	require.NoError(t, err)
	assert.Equal(t, "sandbox-1", binding.ExternalID)
	assert.Equal(t, "embed-lab", binding.Installation)
	assert.EqualValues(t, 1, binding.Generation)
}

// A freshly READY E2B session must accept its first dispatch reservation: the
// settled-runtime gate requires no unpublished events and no held dispatch, and
// a clean E2B session has neither.
func TestFinishE2BSessionCreateAcceptsFirstDispatch(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)
	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	_, err = client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.NoError(t, client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sandbox-1", Generation: 1,
	}))
	_, err = client.FinishE2BSessionCreate(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.NoError(t, client.ReserveSessionDispatch(ctx, session.Id, uuid.New(), "msg-1"))
}

// The E2B completion validates the binding installation against the prepared
// revision and rejects stale operation ownership.
func TestFinishE2BSessionCreateRejectsMismatchAndStaleOwner(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")

	// Installation must match the revision's artifact binding — rejected at
	// bind, before the session can complete.
	session := e2bSessionFixture(t, client, ctx, revisionID)
	op, _ := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	executor := uuid.New()
	_, _ = client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	err := client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "other-lab", ExternalID: "sandbox-1", Generation: 1,
	})
	require.ErrorIs(t, err, ErrFailedPrecondition)

	// A wrong executor cannot bind the operation.
	session2 := e2bSessionFixture(t, client, ctx, revisionID)
	op2, _ := client.BeginSessionOperation(ctx, session2.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	executor2 := uuid.New()
	_, _ = client.ClaimSessionOperation(ctx, session2.Id, op2.ID, executor2)
	err = client.BindE2BSessionCreate(ctx, session2.Id, op2.ID, uuid.New(), E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sandbox-1", Generation: 1,
	})
	require.ErrorIs(t, err, ErrConflict)
}

// The Substrate completion must refuse an E2B-prepared revision rather than
// bind an actor UID to it.
func TestSubstrateFinishRejectsE2BRevision(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)
	op, _ := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	executor := uuid.New()
	_, _ = client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	_, err := client.FinishSessionOperation(ctx, session.Id, op.ID, executor, "team-a/session-"+session.Id, "actor-uid", "")
	require.ErrorIs(t, err, ErrFailedPrecondition)
}

// A failing E2B create rolls the session and its instance binding back
// together: no READY session and no live binding remain.
func TestFinishE2BSessionCreateRollsBackOnBindingConflict(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")

	// First session takes sandbox-1; a second session cannot bind the same
	// external id in one installation.
	s1 := e2bSessionFixture(t, client, ctx, revisionID)
	op1, _ := client.BeginSessionOperation(ctx, s1.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	ex1 := uuid.New()
	_, _ = client.ClaimSessionOperation(ctx, s1.Id, op1.ID, ex1)
	require.NoError(t, client.BindE2BSessionCreate(ctx, s1.Id, op1.ID, ex1, E2BCreateBinding{Installation: "embed-lab", ExternalID: "sandbox-1", Generation: 1}))
	_, err := client.FinishE2BSessionCreate(ctx, s1.Id, op1.ID, ex1)
	require.NoError(t, err)

	s2 := e2bSessionFixture(t, client, ctx, revisionID)
	op2, _ := client.BeginSessionOperation(ctx, s2.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	ex2 := uuid.New()
	_, _ = client.ClaimSessionOperation(ctx, s2.Id, op2.ID, ex2)
	err = client.BindE2BSessionCreate(ctx, s2.Id, op2.ID, ex2, E2BCreateBinding{Installation: "embed-lab", ExternalID: "sandbox-1", Generation: 1})
	require.Error(t, err) // duplicate external_id in one installation

	// The failed create left no READY session and no live binding for s2.
	got, err := client.GetSessionForExecution(ctx, s2.Id)
	require.NoError(t, err)
	assert.NotEqual(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, got.State)
	_, err = client.GetLiveExecutionBinding(ctx, s2.Id)
	require.Error(t, err)
}

// An identical re-record (e.g. unready then ready promotion) reuses the
// existing binding; a conflicting immutable binding input is rejected.
func TestE2BRevisionRerecordReusesIdenticalBinding(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	input := E2BRuntimeRevisionInput{
		Revision: revisionID, Namespace: "team-a", AgentName: "assistant", AgentUID: "assistant-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), ConfigJSON: []byte(`{"version":5}`), AgentCard: &a2apb.AgentCard{}, EgressDestinations: []string{},
		LaunchContract: []byte(`{"command":"nohup h &"}`), Installation: "embed-lab", ExternalID: "template-" + revisionID, BuildID: "build-" + revisionID,
	}

	require.NoError(t, client.UpsertAgentDefinition(ctx, AgentDefinition{
		Namespace: "team-a", AgentName: "assistant", AgentUID: "assistant-uid", DesiredRevision: revisionID,
	}))
	require.NoError(t, client.RecordE2BRuntimeRevision(ctx, input, false))

	var promoted *string
	require.NoError(t, client.db.QueryRow(ctx, `
		SELECT latest_successful_revision FROM agent_definition
		WHERE namespace = 'team-a' AND agent_uid = 'assistant-uid'
	`).Scan(&promoted))
	assert.Nil(t, promoted) // recorded but not yet promoted

	// Re-recording the identical input and promoting ready must not fail on
	// the existing binding row.
	require.NoError(t, client.RecordE2BRuntimeRevision(ctx, input, true))
	require.NoError(t, client.db.QueryRow(ctx, `
		SELECT latest_successful_revision FROM agent_definition
		WHERE namespace = 'team-a' AND agent_uid = 'assistant-uid'
	`).Scan(&promoted))
	require.NotNil(t, promoted)
	assert.Equal(t, revisionID, *promoted)

	// A conflicting immutable binding for the same revision is rejected.
	conflicting := input
	conflicting.BuildID = "build-other"
	require.Error(t, client.RecordE2BRuntimeRevision(ctx, conflicting, true))

	// A conflicting compiled config for the same revision is likewise
	// rejected: the binding's config is immutable once recorded.
	conflictingConfig := input
	conflictingConfig.ConfigJSON = []byte(`{"version":6}`)
	require.Error(t, client.RecordE2BRuntimeRevision(ctx, conflictingConfig, true))

	// A revision may not record an artifact binding with no harness config.
	missing := input
	missing.Revision = "e2b-rev-nocfg-" + uuid.NewString()[:6]
	missing.ConfigJSON = nil
	require.Error(t, client.RecordE2BRuntimeRevision(ctx, missing, false))
}

// Many E2B revisions may share one external build artifact; only the revision
// itself is unique in execution_revision_binding.
func TestE2BRevisionsMayShareExternalBuild(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revA := "e2b-rev-a-" + uuid.NewString()[:8]
	revB := "e2b-rev-b-" + uuid.NewString()[:8]

	require.NoError(t, client.UpsertAgentDefinition(ctx, AgentDefinition{
		Namespace: "team-a", AgentName: "assistant", AgentUID: "assistant-uid", DesiredRevision: revA,
	}))
	require.NoError(t, client.RecordE2BRuntimeRevision(ctx, E2BRuntimeRevisionInput{
		Revision: revA, Namespace: "team-a", AgentName: "assistant", AgentUID: "assistant-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), ConfigJSON: []byte(`{"version":5}`), AgentCard: &a2apb.AgentCard{}, EgressDestinations: []string{},
		LaunchContract: []byte(`{"command":"nohup h &"}`), Installation: "embed-lab", ExternalID: "shared-template", BuildID: "shared-build",
	}, true))
	require.NoError(t, client.RecordE2BRuntimeRevision(ctx, E2BRuntimeRevisionInput{
		Revision: revB, Namespace: "team-a", AgentName: "assistant", AgentUID: "assistant-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), ConfigJSON: []byte(`{"version":5}`), AgentCard: &a2apb.AgentCard{}, EgressDestinations: []string{},
		LaunchContract: []byte(`{"command":"nohup h &"}`), Installation: "embed-lab", ExternalID: "shared-template", BuildID: "shared-build",
	}, true))
}

// A binding cannot be deleted out from under a retained E2B revision; the
// deferred trigger re-checks at COMMIT. Deleting the revision itself cascades
// the binding without tripping the guard.
func TestE2BRevisionBindingCannotBeRemovedInIsolation(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")

	// Deleting the binding alone commits to a dangling E2B revision: rejected.
	err := client.withTx(ctx, func(tx pgx.Tx) error {
		return execSQL(ctx, tx, `DELETE FROM execution_revision_binding WHERE revision = $1`, revisionID)
	})
	require.Error(t, err)

	// Re-pointing the binding at a different revision is also rejected.
	err = client.withTx(ctx, func(tx pgx.Tx) error {
		return execSQL(ctx, tx, `
			UPDATE execution_revision_binding SET revision = revision || '-moved' WHERE revision = $1
		`, revisionID)
	})
	require.Error(t, err)

	// Deleting the revision cascades the binding cleanly. The promoted
	// definition reference must clear first; clearing it rolls the definition
	// back to an unready desire, which is how retirement works.
	require.NoError(t, client.withTx(ctx, func(tx pgx.Tx) error {
		if err := execSQL(ctx, tx, `
			UPDATE agent_definition SET latest_successful_revision = NULL
			WHERE latest_successful_revision = $1
		`, revisionID); err != nil {
			return err
		}
		return execSQL(ctx, tx, `DELETE FROM runtime_revision WHERE revision = $1`, revisionID)
	}))
	var n int
	require.NoError(t, client.db.QueryRow(ctx, `
		SELECT count(*) FROM execution_revision_binding WHERE revision = $1
	`, revisionID).Scan(&n))
	assert.Equal(t, 0, n)
}

// ListOpenE2BCreateOperations returns only E2B CREATE operations that still
// lack a live binding — the durable intent the late-binding sweep reconciles.
func TestListOpenE2BCreateOperations(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()

	// An E2B-prepared session: provider=e2b revision + artifact binding, then a
	// session whose create prepares that revision.
	require.NoError(t, client.UpsertAgentDefinition(ctx, AgentDefinition{
		Namespace: "team-a", AgentName: "e2b-agent", AgentUID: "e2b-uid", DesiredRevision: "e2b-rev-1"}))
	require.NoError(t, client.RecordE2BRuntimeRevision(ctx, E2BRuntimeRevisionInput{
		Revision: "e2b-rev-1", Namespace: "team-a", AgentName: "e2b-agent", AgentUID: "e2b-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), ConfigJSON: []byte(`{"version":5}`), AgentCard: &a2apb.AgentCard{},
		LaunchContract: []byte(`{"command":"nohup h &"}`), Installation: "embed-lab", ExternalID: "tpl-abc", BuildID: "build-1"}, true))
	e2bSession, _, err := client.CreateSession(ctx, &apiv1alpha1.Session{
		Id: uuid.NewString(), Creator: "alice",
		Agent:            &apiv1alpha1.ResourceReference{Namespace: "team-a", Name: "e2b-agent"},
		PreparedRevision: "e2b-rev-1",
	}, uuid.NewString())
	require.NoError(t, err)

	// Begin the CREATE so operation_id is assigned — CreateSession leaves
	// operation='CREATE' but operation_id NULL until the operation is begun.
	_, err = client.BeginSessionOperation(ctx, e2bSession.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)

	// A Substrate-prepared session for contrast — must not appear.
	sessionFixture(t, client, ctx, "team-a", "sub-rev-1", "sub-agent", "kagent")
	subSession, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "sub-agent", "kagent", ""), uuid.NewString())
	require.NoError(t, err)
	_, err = client.BeginSessionOperation(ctx, subSession.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)

	opens, err := client.ListOpenE2BCreateOperations(ctx)
	require.NoError(t, err)
	var found bool
	for _, o := range opens {
		require.NotEqual(t, subSession.Id, o.SessionID.String(), "Substrate CREATE must not appear")
		if o.SessionID.String() == e2bSession.Id {
			found = true
			require.Equal(t, "e2b-rev-1", o.Revision)
		}
	}
	require.True(t, found, "E2B open CREATE with no binding must appear")

	// Once a live binding exists the operation is reconciled — it drops out.
	_, err = client.CreateExecutionBinding(ctx, e2bSession.Id, ExecutionProviderE2B, "embed-lab", "sb-1", 1)
	require.NoError(t, err)
	opens, err = client.ListOpenE2BCreateOperations(ctx)
	require.NoError(t, err)
	for _, o := range opens {
		require.NotEqual(t, e2bSession.Id, o.SessionID.String(), "bound E2B CREATE must not appear")
	}
}

// The orphan deletion gate serializes on the owning session's row lock — the
// same lock BindE2BSessionCreate takes — so a bound sandbox is never reaped
// and an adoption can never commit behind the gate's approval.
func TestE2BOrphanDeletionGate(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	_, err = client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)

	// While CREATE is still open the sandbox is adoptable — the gate refuses.
	allowed, err := client.E2BOrphanDeletionGate(ctx, op.ID, "embed-lab", "sb-late")
	require.NoError(t, err)
	assert.False(t, allowed, "open CREATE can still adopt; do not delete")

	// A foreign operation id (no local attempt) is never this deployment's.
	allowed, err = client.E2BOrphanDeletionGate(ctx, uuid.New(), "embed-lab", "sb-late")
	require.NoError(t, err)
	assert.False(t, allowed, "foreign tag is never reaped")

	// A correct-installation but different-sandbox recorded external id refuses.
	require.NoError(t, client.MarkE2BCreateAttemptKnown(ctx, op.ID, "sb-other"))
	allowed, err = client.E2BOrphanDeletionGate(ctx, op.ID, "embed-lab", "sb-late")
	require.NoError(t, err)
	assert.False(t, allowed, "attempt names another sandbox")
	require.NoError(t, client.MarkE2BCreateAttemptKnown(ctx, op.ID, "sb-late"))
}

// A completed (bound) session can never be reaped: the live binding claims the
// external id under the gate's lock.
func TestE2BOrphanDeletionGateRejectsBoundSandbox(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, _ := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	executor := uuid.New()
	_, _ = client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	_, _, _ = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab") //nolint:dogsled
	require.NoError(t, client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sb-bound", Generation: 1,
	}))
	_, err := client.FinishE2BSessionCreate(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)

	// CREATE completed and bound; the gate must refuse even though the op is
	// no longer open — the live binding claims the sandbox.
	allowed, err := client.E2BOrphanDeletionGate(ctx, op.ID, "embed-lab", "sb-bound")
	require.NoError(t, err)
	assert.False(t, allowed, "bound sandbox is never an orphan")
}

// The gate approves only when the owning CREATE is durably finished with no
// live binding — the true orphan: a create that placed a sandbox whose binding
// was never committed because the executor died after the op timed out.
func TestE2BOrphanDeletionGateApprovesCommittedOrphan(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, _ := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	executor := uuid.New()
	_, _ = client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	_, _, _ = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab") //nolint:dogsled
	require.NoError(t, client.MarkE2BCreateAttemptKnown(ctx, op.ID, "sb-orphan"))

	// The executor's claim must fully lapse before a DELETE can supersede the
	// open CREATE (a still-tagged executor, even an expired one, blocks the
	// supersede). Clearing executor_id directly reflects the abandoned-claim
	// durable state the sweep produces.
	_, err := client.db.Exec(ctx, `UPDATE runtime_instance SET executor_id = NULL WHERE id = $1`, session.Id)
	require.NoError(t, err)

	// Supersede the still-open CREATE with a DELETE (the operation that ends a
	// create whose binding never committed). This is the durable state in
	// which the placed sandbox is orphaned.
	del, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.NoError(t, err)
	_ = del

	// DELETE supersedes CREATE: operation is now DELETE (or tombstoned). The
	// create attempt still correlates this sandbox to the superseded op; under
	// the lock there is no live binding and no adoptable CREATE, so the gate
	// must approve deleting the sandbox the dead create left behind.
	allowed, err := client.E2BOrphanDeletionGate(ctx, op.ID, "embed-lab", "sb-orphan")
	require.NoError(t, err)
	assert.True(t, allowed, "durably unbound, un-adoptable sandbox is orphaned")
}

// Adoption-committed-before-gate: when a binding for the external id already
// exists by the time the gate evaluates — and the owning CREATE is no longer
// the open operation — the gate refuses because the binding claims the sandbox.
// The session is moved out of CREATE first (a DELETE supersedes it) so the only
// refusal reason left is the live binding, proving the gate observes it rather
// than a still-open operation.
func TestE2BOrphanDeletionGateRefusesCommittedBinding(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	_, err = client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)
	require.NoError(t, client.MarkE2BCreateAttemptKnown(ctx, op.ID, "sb-race"))

	// Supersede the CREATE with a DELETE so the session is no longer inside an
	// adoptable CREATE — an abandoned create whose placed sandbox is orphaned.
	// The executor claim must fully lapse before the supersede is admitted.
	_, err = client.db.Exec(ctx, `UPDATE runtime_instance SET executor_id = NULL WHERE id = $1`, session.Id)
	require.NoError(t, err)
	_, err = client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.NoError(t, err)

	// A concurrent adoption committed the binding before the gate evaluates:
	// the live execution_instance_binding claims sb-race. With no open CREATE
	// remaining, the binding row itself is what must make the gate refuse.
	_, err = client.CreateExecutionBinding(ctx, session.Id, ExecutionProviderE2B, "embed-lab", "sb-race", 1)
	require.NoError(t, err)

	allowed, err := client.E2BOrphanDeletionGate(ctx, op.ID, "embed-lab", "sb-race")
	require.NoError(t, err, "gate must not error on a committed-binding case")
	assert.False(t, allowed, "a committed binding claims the sandbox — refuse to reap")
}

// Supersede-then-approve and bind-rejection: once a DELETE supersedes the
// owning CREATE and no binding claims the sandbox, the gate approves, and the
// old CREATE operation id can no longer bind the sandbox afterward — the
// durable state the gate's approval depended on is irreversible.
func TestE2BOrphanDeletionGateApprovalIsFinal(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	_, err = client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)
	require.NoError(t, client.MarkE2BCreateAttemptKnown(ctx, op.ID, "sb-orphan"))

	// Lapse the executor claim and supersede CREATE with DELETE — the create
	// is now irreversibly closed for this operation id.
	_, err = client.db.Exec(ctx, `UPDATE runtime_instance SET executor_id = NULL WHERE id = $1`, session.Id)
	require.NoError(t, err)
	_, err = client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.NoError(t, err)

	allowed, err := client.E2BOrphanDeletionGate(ctx, op.ID, "embed-lab", "sb-orphan")
	require.NoError(t, err)
	require.True(t, allowed, "superseded CREATE with no binding is orphaned — approve")

	// The approval must be final: the old CREATE operation id can never bind
	// this sandbox afterward. BindE2BSessionCreate requires operation ==
	// CREATE with that exact id, and the session's open operation is now the
	// superseding DELETE — so a stale adoption is rejected.
	err = client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sb-orphan", Generation: 1,
	})
	require.ErrorIs(t, err, ErrConflict, "old CREATE operation must not bind after the gate approved")
}

// GetExecutionBindingResolution classifies a session that has no live binding
// so the gateway can tell reaper-confirmed loss from an in-flight create or a
// deleted session. A session whose live binding was revoked while still
// non-deleted resolves to its lifecycle state plus the revoked binding row; a
// never-bound session resolves to empty binding fields; a tombstoned session
// is not found.
func TestGetExecutionBindingResolution(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()

	// Never-bound CREATING session: a row with empty binding fields and no
	// revoked_at — an in-flight create, not runtime loss.
	creating := execSessionFixture(t, client, ctx)
	res, err := client.GetExecutionBindingResolution(ctx, creating)
	require.NoError(t, err)
	assert.Equal(t, "RUNTIME_STATE_CREATING", res.State)
	assert.Empty(t, res.Provider)
	assert.Empty(t, res.Installation)
	assert.Empty(t, res.ExternalID)
	assert.Nil(t, res.RevokedAt)

	// Live session whose binding was revoked (the loss reaper's record):
	// resolves to READY state plus the revoked binding's external identity.
	lost := execSessionFixture(t, client, ctx)
	_, err = client.db.Exec(ctx,
		`UPDATE runtime_instance SET state = 'RUNTIME_STATE_READY', operation = 'RUNTIME_OPERATION_NONE',
		 operation_id = NULL, executor_id = NULL, executor_expires_at = NULL WHERE id = $1`, lost)
	require.NoError(t, err)
	_, err = client.CreateExecutionBinding(ctx, lost, ExecutionProviderE2B, "embed-lab", "sandbox-gone", 1)
	require.NoError(t, err)
	require.NoError(t, client.RevokeExecutionBinding(ctx, lost))
	res, err = client.GetExecutionBindingResolution(ctx, lost)
	require.NoError(t, err)
	assert.Equal(t, "RUNTIME_STATE_READY", res.State)
	assert.Equal(t, ExecutionProviderE2B, res.Provider)
	assert.Equal(t, "embed-lab", res.Installation)
	assert.Equal(t, "sandbox-gone", res.ExternalID)
	assert.NotNil(t, res.RevokedAt, "revoked binding carries its revocation time")

	// A tombstoned session is not found — a mid-DELETE revocation resolves to
	// "session gone", never to runtime loss.
	deleted := execSessionFixture(t, client, ctx)
	_, err = client.db.Exec(ctx,
		`UPDATE runtime_instance SET state = 'RUNTIME_STATE_DELETED', operation = 'RUNTIME_OPERATION_NONE',
		 operation_id = NULL, executor_id = NULL, executor_expires_at = NULL, prepared_revision = NULL WHERE id = $1`, deleted)
	require.NoError(t, err)
	_, err = client.GetExecutionBindingResolution(ctx, deleted)
	require.ErrorIs(t, err, ErrNotFound)
}

// TestE2BBoundThenRevokedCreateIsDeletable covers the recovery gap the kill
// witness surfaced: a CREATE that committed its binding and then failed
// activation leaves the session CREATING+CREATE with the executor claim
// retained (the issued CreateSandbox outcome was uncertain). If the bound
// sandbox is then definitively lost (provider delete -> reaper revokes the
// binding), the session would otherwise have no exit: CREATE-retry reconciles
// to "uncertain" forever, and DELETE is rejected while executor_id is retained.
//
// With the Shape-1 recovery fix, that bound-and-lost CREATE is now deletable:
// beginSessionDeletion verifies the open operation is the E2B CREATE, its
// executor lease has ended, the session's latest binding is revoked and
// correlates to the create attempt, and no live binding remains — then clears
// the stale claim and lets ordinary DELETE supersede it. This test asserts the
// carve-out unblocks the resolved-lost case; the sibling protections are
// covered by the tests below.
func TestE2BBoundThenRevokedCreateIsDeletable(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	// Begin + claim CREATE, then commit the binding — the sequence the real
	// e2bCreate runs before activation. Activation is not driven here: the
	// stuck state is created by binding + retained claim + revoked binding.
	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	// Record the issued create attempt (real path records it before the
	// provider call), then mark the observed sandbox id, then bind — the
	// durable sequence e2bCreate runs before activation.
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)
	require.NoError(t, client.MarkE2BCreateAttemptKnown(ctx, op.ID, "sandbox-lost"))
	require.NoError(t, client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sandbox-lost", Generation: 1,
	}))

	// Simulate the failed-attempt release: the claim expires but executor_id is
	// retained (the issued create's outcome is uncertain). Then the reaper
	// revokes the binding after the provider-side sandbox is deleted.
	require.NoError(t, client.ReleaseRuntimeOperation(ctx, session.Id, op.ID, executor))
	require.NoError(t, client.RevokeExecutionBinding(ctx, session.Id))

	// The session is stuck: state CREATING, operation CREATE, claim retained.
	row := client.db.QueryRow(ctx,
		`SELECT state, operation, executor_id IS NOT NULL FROM runtime_instance WHERE id = $1`, session.Id)
	var state, operation string
	var executorRetained bool
	require.NoError(t, row.Scan(&state, &operation, &executorRetained))
	assert.Equal(t, "RUNTIME_STATE_CREATING", state)
	assert.Equal(t, "RUNTIME_OPERATION_CREATE", operation)
	assert.True(t, executorRetained, "the issued create's executor claim is retained")

	// The "bound, definitively lost" signal is detectable: the latest binding
	// is revoked (compute is gone), distinct from a live or absent binding.
	res, err := client.GetExecutionBindingResolution(ctx, session.Id)
	require.NoError(t, err)
	assert.NotNil(t, res.RevokedAt, "revoked binding marks definitively-lost compute")
	assert.Equal(t, "sandbox-lost", res.ExternalID)

	// The bound-and-lost CREATE is now deletable: the carve-out clears the
	// stale claim and DELETE supersedes it in the same admission.
	del, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.NoError(t, err, "resolved-lost E2B CREATE must be deletable")
	assert.Equal(t, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE, del.Instance.Operation)

	// The superseded executor is fenced: it can neither bind new compute to the
	// session nor finish the dead CREATE — its operation id and executor are no
	// longer the live claim.
	err = client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sandbox-late", Generation: 1,
	})
	require.ErrorIs(t, err, ErrConflict, "a superseded executor must not bind after DELETE")
	_, err = client.FinishE2BSessionCreate(ctx, session.Id, op.ID, executor)
	require.ErrorIs(t, err, ErrConflict, "a superseded executor must not finish after DELETE")
}

// TestE2BIssuedUnboundCreateIsStuck covers the sibling case the fix must NOT
// unblock blindly: a CREATE that issued CreateSandbox but whose outcome is
// unknown — no binding was ever committed (e.g. the response was lost before
// BindE2BSessionCreate). It shares the CREATING+CREATE+retained-claim shape but
// has NO binding row, so GetExecutionBindingResolution reports no external id
// and no revocation. The no-blind-retry protection must remain: an unknown-
// outcome create may have placed a live sandbox that must be reconciled, not
// tombstoned.
func TestE2BIssuedUnboundCreateIsStuck(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	// Issue CREATE and retain the claim WITHOUT ever binding — the create
	// response was lost, so whether a sandbox was placed is genuinely unknown.
	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)
	require.NoError(t, client.ReleaseRuntimeOperation(ctx, session.Id, op.ID, executor))

	// Resolution reports no binding ever committed — the "issued, unknown
	// outcome" case, NOT the definitively-lost one.
	res, err := client.GetExecutionBindingResolution(ctx, session.Id)
	require.NoError(t, err)
	assert.Equal(t, "", res.ExternalID, "no binding was ever committed")
	assert.Nil(t, res.RevokedAt, "nothing to revoke — the outcome is unknown, not lost")

	// With no binding row there is no revoked latest binding, so the carve-out
	// must NOT apply: DELETE stays blocked on the retained claim — an unknown-
	// outcome create may have placed a live sandbox that must be reconciled.
	_, err = client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.ErrorIs(t, err, ErrConflict, "issued-but-unbound create must not be deleted blind")
}

// TestE2BLiveBoundCreateDeleteStillBlocked proves the carve-out does not apply
// while the binding is still live: a bound CREATE whose compute was NOT lost
// retains the executor claim, and DELETE must keep failing on it — the binding
// being unrevoked means the sandbox may still be running and must not be
// superseded by a delete that abandons the durable binding.
func TestE2BLiveBoundCreateDeleteStillBlocked(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)
	require.NoError(t, client.MarkE2BCreateAttemptKnown(ctx, op.ID, "sandbox-live"))
	require.NoError(t, client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sandbox-live", Generation: 1,
	}))
	// Release the attempt so executor_id is retained but the lease has ended —
	// identical shape to the lost case EXCEPT the binding is still live.
	require.NoError(t, client.ReleaseRuntimeOperation(ctx, session.Id, op.ID, executor))

	// The live binding means compute is not lost: DELETE must stay blocked.
	_, err = client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.ErrorIs(t, err, ErrConflict, "live-bound create must not be deleted while its binding is unrevoked")
}

// TestE2BRevokedCreateInFlightLeaseProtected proves the carve-out requires the
// executor lease to have ENDED, not merely a retained executor id. A bound-
// then-revoked create whose executor still holds an unexpired claim is an
// in-flight attempt: clearing it could race a real bind/finish, so DELETE must
// stay blocked even though the binding is revoked.
func TestE2BRevokedCreateInFlightLeaseProtected(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)
	require.NoError(t, client.MarkE2BCreateAttemptKnown(ctx, op.ID, "sandbox-inflight"))
	require.NoError(t, client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sandbox-inflight", Generation: 1,
	}))
	// Revoke the binding (compute lost) but DO NOT release the executor lease:
	// the claim is still within its 2-minute bound, so an in-flight attempt may
	// legitimately still be writing to the now-revoked sandbox.
	require.NoError(t, client.RevokeExecutionBinding(ctx, session.Id))

	// The lease is unexpired: the carve-out must not fire even though the
	// binding is revoked.
	_, err = client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.ErrorIs(t, err, ErrConflict, "in-flight claimed create stays protected even when its binding was revoked")
}

// TestSubstrateRetainedCreateDeleteUnchanged proves the carve-out is scoped to
// E2B: a Substrate session's pending CREATE with a retained executor claim must
// keep DELETE blocked exactly as before. A Substrate row can never satisfy the
// provider='e2b' predicate (and a substrate binding row can't exist), so the
// no-blind-supersede invariant is untouched.
func TestSubstrateRetainedCreateDeleteUnchanged(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	sessionFixture(t, client, ctx, "team-a", "sub-rev-"+uuid.NewString()[:8], "assistant", "kagent")
	session, _, err := client.CreateSession(ctx, newSessionRequest(uuid.NewString(), "assistant", "kagent", ""), uuid.NewString())
	require.NoError(t, err)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)
	// Retain the claim with the lease ended — the same stuck shape a failed
	// Substrate create would leave. There is no E2B binding, so the carve-out
	// must not fire and DELETE must stay blocked.
	require.NoError(t, client.ReleaseRuntimeOperation(ctx, session.Id, op.ID, executor))

	_, err = client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
	require.ErrorIs(t, err, ErrConflict, "Substrate retained-claim DELETE admission is unchanged")
}

// Lock-order regression: the runtime-write path (writeRuntimeTask /
// SettleSessionTask) must acquire the session row lock BEFORE the
// execution_instance_binding row lock — the same session-first order the
// opposing lifecycle path (FinishE2BSessionDelete, E2BOrphanDeletionGate)
// uses. If the writer ever took the binding lock first, a concurrent delete
// holding the session lock and reaching for the binding would deadlock with a
// writer holding the binding and reaching for the session.
//
// Rather than hoping timing exposes the interleave, this test reproduces the
// exact two-transaction deadlock deterministically: transaction A locks the
// binding row first (the reversed order the fix removed), then a writer
// running the production session→binding path on the same session must block
// on A's binding lock while holding the session lock — and A then reaching
// for the session lock deadlocks. A correct session-first writer can never be
// holding the binding while A holds it, so it either waits on the session
// lock (not A's binding) or completes — it never participates in a cycle.
//
// The inverse assertion is the meaningful one: with the PRODUCTION order in
// place, a delete that holds the session lock then revokes the binding can
// never deadlock against a concurrent task write, because both acquire
// session before binding. We prove that by running the real write and delete
// paths against a held binding-row lock and asserting the write blocks on the
// binding (proving it reached binding AFTER session) rather than deadlocking.
func TestE2BTaskWriteAndDeleteLockOrder(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
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
	binding, err := client.GetLiveExecutionBinding(ctx, session.Id)
	require.NoError(t, err)

	// TX A: hold the binding row FOR UPDATE — this is the lock the write path
	// must acquire SECOND. While held, a session-first writer must reach the
	// binding lock only after taking the session lock, so it blocks on A's
	// binding lock (not the session). If the writer took binding first, this
	// same setup is indistinguishable, so we additionally hold the session
	// lock in a separate tx to prove ordering.
	txA, err := client.db.Begin(ctx)
	require.NoError(t, err)
	defer txA.Rollback(ctx) //nolint:errcheck
	var lockedExt string
	require.NoError(t, txA.QueryRow(ctx, `
		SELECT external_id FROM execution_instance_binding
		WHERE id = $1 FOR UPDATE
	`, binding.ID).Scan(&lockedExt))

	// Writer goroutine runs the production write path. It should block on the
	// binding-row lock A holds — meaning it already passed the session lock.
	writeErr := make(chan error, 1)
	go func() {
		task := &a2a.Task{ID: a2a.TaskID(uuid.NewString()), ContextID: created.ContextId,
			Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
		_, err := client.CreateRuntimeTask(ctx, session.Id, taskMutationHash(string(task.ID)), task, "", binding)
		writeErr <- err
	}()

	// Give the writer a moment to take the session lock and queue on binding.
	select {
	case err := <-writeErr:
		t.Fatalf("write completed while binding row held (impossible unless it skipped the binding lock): %v", err)
	case <-time.After(300 * time.Millisecond):
		// Still blocked on the binding lock — writer is queued behind A.
	}

	// While the writer holds the session lock (queued on the binding lock A
	// holds), probe whether the session row is lock-free. session_record is a
	// view, so NOWAIT must target the base runtime_instance row lockSession
	// actually locks. If the writer already passed the session lock, NOWAIT
	// fails — proving the writer took session BEFORE binding (session-first).
	var sessionLocked atomic.Bool
	go func() {
		var lockedID uuid.UUID
		serr := txA.QueryRow(ctx, `
			SELECT id FROM runtime_instance WHERE id = $1 FOR UPDATE NOWAIT
		`, session.Id).Scan(&lockedID)
		if serr != nil {
			// NOWAIT failed: the writer holds the session lock while queued on
			// the binding => it acquired session FIRST. Confirmed.
			sessionLocked.Store(true)
		}
	}()
	time.Sleep(400 * time.Millisecond)

	require.NoError(t, txA.Rollback(ctx))
	select {
	case err := <-writeErr:
		// After A released the binding lock the write commits (or fails the
		// binding re-verify, not a lock error).
		if err != nil {
			require.True(t, errors.Is(err, ErrConflict) || errors.Is(err, ErrFailedPrecondition) || errors.Is(err, ErrNotFound),
				"write must fail only on the binding check, not a lock error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not complete after binding lock released — possible lock inversion")
	}
	require.True(t, sessionLocked.Load(),
		"writer held the session lock while queued on the binding lock — session-first order confirmed")
}

// FailE2BSessionCreate must only resolve a real issued attempt. A missing
// attempt (the create was never durably issued) or a conflicting one must
// return ErrConflict WITHOUT releasing the executor claim — releasing would
// let a stale path abandon an operation that may have placed compute.
func TestFailE2BSessionCreateRequiresIssuedAttempt(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)

	// No attempt row yet — a reject cannot "resolve" a create that was never
	// issued; releasing the claim here would be a false rejection.
	err = client.FailE2BSessionCreate(ctx, session.Id, op.ID, executor)
	require.ErrorIs(t, err, ErrConflict, "rejecting a create with no issued attempt must not release the claim")

	// The claim must still be held: a foreign executor cannot steal it.
	stolen, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, uuid.New())
	require.NoError(t, err)
	require.False(t, stolen, "a failed rejection must leave the original claim intact")

	// Record a real issued attempt, then reject — now the row resolves and the
	// claim releases for a corrected retry.
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)
	require.NoError(t, client.FailE2BSessionCreate(ctx, session.Id, op.ID, executor))

	attempt, err := client.GetE2BCreateAttempt(ctx, op.ID)
	require.NoError(t, err)
	require.Equal(t, "rejected", attempt.Outcome)

	// A second reject on the now-resolved attempt is a conflict — 'rejected'
	// is not 'issued', so nothing more to flip and no double-release.
	err = client.FailE2BSessionCreate(ctx, session.Id, op.ID, executor)
	require.ErrorIs(t, err, ErrConflict, "a resolved attempt cannot be rejected twice")
}

// A bind must not silently absorb a 'rejected' attempt or a 'known' attempt
// pointing at different compute — both are contradictory durable states.
func TestBindE2BSessionCreateRejectsResolvedOrConflictingAttempt(t *testing.T) {
	client := NewClient(setupTestDB(t), "public")
	ctx := context.Background()
	revisionID := "e2b-rev-" + uuid.NewString()[:8]
	e2bRevisionFixture(t, client, ctx, "team-a", revisionID, "assistant", "embed-lab")
	session := e2bSessionFixture(t, client, ctx, revisionID)

	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executor := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executor)
	require.NoError(t, err)
	require.True(t, claimed)

	// A rejected attempt: the create provably placed nothing — bind must fail.
	_, _, err = client.RecordE2BCreateAttempt(ctx, session.Id, op.ID, executor, "embed-lab")
	require.NoError(t, err)
	require.NoError(t, client.FailE2BSessionCreate(ctx, session.Id, op.ID, executor))
	err = client.BindE2BSessionCreate(ctx, session.Id, op.ID, executor, E2BCreateBinding{
		Installation: "embed-lab", ExternalID: "sb-x", Generation: 1,
	})
	require.ErrorIs(t, err, ErrConflict, "binding a rejected create is a contradiction")
}
