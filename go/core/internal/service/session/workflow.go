package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"google.golang.org/protobuf/encoding/protojson"
)

type workflowStore interface {
	ClaimSessionQuiescence(context.Context) (*database.SessionQuiescence, error)
	FinishSessionQuiescence(context.Context, *database.SessionQuiescence, *database.SessionTaskSnapshot) error
	FinishE2BSessionQuiescence(context.Context, *database.SessionQuiescence) error
	GetSessionForRuntime(context.Context, string, string) (*apiv1alpha1.Session, error)
	GetSessionCheckpointSnapshot(context.Context, string, string) (*database.SessionTaskSnapshot, string, error)
	GetRuntimeRevision(context.Context, string) (*database.RuntimeRevision, error)
	BeginSessionOperation(context.Context, string, apiv1alpha1.RuntimeOperation) (*database.SessionOperation, error)
	ClaimSessionOperation(context.Context, string, uuid.UUID, uuid.UUID) (bool, error)
	ReleaseRuntimeOperation(context.Context, string, uuid.UUID, uuid.UUID) error
	FinishSessionOperation(context.Context, string, uuid.UUID, uuid.UUID, string, string, string) (*apiv1alpha1.Session, error)
	GetSessionOperation(context.Context, string, uuid.UUID) (*database.SessionOperation, error)
	GetE2BRevisionBinding(context.Context, string) (*database.ExecutionRevisionBinding, error)
	BindE2BSessionCreate(context.Context, string, uuid.UUID, uuid.UUID, database.E2BCreateBinding) error
	FinishE2BSessionCreate(context.Context, string, uuid.UUID, uuid.UUID) (*apiv1alpha1.Session, error)
	GetLiveExecutionBinding(context.Context, string) (*database.ExecutionInstanceBinding, error)
	// GetExecutionBindingResolution classifies a session with no live binding:
	// revoked identity (proven loss) vs never-bound/inconsistent (unknown).
	GetExecutionBindingResolution(context.Context, string) (*database.ExecutionBindingResolution, error)
	FinishE2BSessionDelete(context.Context, string, uuid.UUID, uuid.UUID) (*apiv1alpha1.Session, error)
	// Create-attempt records: the durable marker that a CREATE operation's
	// CreateSandbox was issued, so a retry reconciles instead of re-creating.
	RecordE2BCreateAttempt(context.Context, string, uuid.UUID, uuid.UUID, string) (*database.E2BCreateAttempt, bool, error)
	GetE2BCreateAttempt(context.Context, uuid.UUID) (*database.E2BCreateAttempt, error)
	MarkE2BCreateAttemptKnown(context.Context, uuid.UUID, string) error
	// FailE2BSessionCreate resolves a definitively-rejected create and
	// releases the executor claim atomically, so a corrected request can
	// re-claim rather than steering toward reconciling a non-existent sandbox.
	FailE2BSessionCreate(context.Context, string, uuid.UUID, uuid.UUID) error
}

type actorClient interface {
	substrate.LifecycleClient
	PauseActor(context.Context, string, string) (*ateapipb.Actor, error)
}

// E2BSessionCreator is the narrow provider effect the E2B create path needs:
// placing, removing and pausing a sandbox. It is satisfied by *e2b.Client in
// production and a fake in tests.
type E2BSessionCreator interface {
	CreateSandbox(context.Context, e2b.CreateRequest) (*e2b.Sandbox, error)
	DeleteSandbox(context.Context, string) error
	PauseSandbox(context.Context, string) error
	ResumeSandbox(context.Context, string) (*e2b.Sandbox, error)
	// GetSandbox returns the sandbox's current record, including the live envd
	// access token a refresh needs. A 404 means the sandbox is gone.
	GetSandbox(context.Context, string) (*e2b.Sandbox, error)
}

// E2BSessionLister is the read effect the create-reconcile path needs:
// enumerating sandboxes by their correlation metadata. It is satisfied by
// *e2b.Client.ListSandboxes in production and a fake in tests. A nil Lister
// means a lost create cannot be reconciled — the operation stays pending for
// the orphan reaper rather than risking a blind re-create.
type E2BSessionLister interface {
	ListSandboxes(context.Context, map[string]string) ([]e2b.ListedSandbox, error)
}

// E2BBootstrapper performs the post-bind activation probe the plan requires:
// only the bound sandbox receives the runtime credential and revision config,
// written through envd's files API before the supervisor launches the harness.
// It is a separate effect from CreateSandbox so the credential is never issued
// to an unbound guest. Satisfied by a real envd-backed implementation in
// production and a fake in tests.
type E2BBootstrapper interface {
	// Bootstrap writes the revision config and a fresh instance credential
	// into the bound sandbox and signals the supervisor. sessionID and
	// externalID are the durable binding; generation scopes the credential.
	// configPayload is the revision's source snapshot; agentCardPayload is
	// the revision's rendered Agent Card JSON the harness's
	// KAGENT_AGENT_CARD_JSON launch contract reads.
	// environment carries the revision's resolved non-secret process
	// environment the launch exports into the guest; E2B-owned
	// callback/broker overrides are applied by the bootstrapper itself.
	Bootstrap(ctx context.Context, installation, sessionID, externalID string, generation int64, configPayload, agentCardPayload []byte, environment map[string]string) error

	// RefreshCredential re-issues the bound sandbox's runtime credential with
	// a new expiry, preserving the session/installation/externalID/generation
	// identity. It writes atomically (temp + rename) so a concurrent reader
	// never sees a partial token, and leaves the prior credential on failure.
	// Used by the credential renewer and by dispatch/resume before admitting
	// work to a credential that may have lapsed.
	RefreshCredential(ctx context.Context, installation, sessionID, externalID string, generation int64) error
}

// E2BCreateConfig is the slice of installation config the create path reads:
// the broker and callback addresses drive the egress allowlist and the
// harness's model base_url. The API endpoint is implicit in the per-
// installation E2BSessionCreator the lookup returns.
type E2BCreateConfig struct {
	BrokerAddress   string
	CallbackAddress string
	// SandboxTimeoutSeconds is the sandbox lifetime the create sets. With
	// AutoPause it is a timeout-to-pause bound, not a kill: idle compute is
	// preserved, and admitted tasks renew it via SetSandboxTimeout. Zero uses
	// the provider's own default.
	SandboxTimeoutSeconds int32
}

// E2BProvider bundles one installation's create-path dependencies: the sandbox
// creator (its control-plane API client), the egress endpoints the enforced
// allowlist and harness base_url need, and the post-bind bootstrap probe. A
// BootstrapperFactory builds the per-sandbox activation probe after create
// returns the sandbox's envd access token. It is a factory because the probe
// needs the per-sandbox token and proxy endpoint, which only exist once the
// sandbox is bound — the installation-level provider cannot hold a ready-made
// bootstrapper. The launch contract is the revision's frozen binding value
// (artifact.LaunchContract), resolved at preparation, not live install config.
// A nil factory means create binds READY without the activation step — valid
// for a pre-launch milestone where the supervisor contract is not yet wired,
// not for a deployment that must run the harness.
type E2BBootstrapperFactory func(sandbox *e2b.Sandbox, launchContract []byte) (E2BBootstrapper, error)

type E2BProvider struct {
	Creator E2BSessionCreator
	Config  E2BCreateConfig
	// Lister reconciles a lost create response to the sandbox it actually
	// placed. nil means the provider cannot reconcile; an ambiguous create
	// leaves the operation pending rather than re-issuing.
	Lister       E2BSessionLister
	Bootstrapper E2BBootstrapperFactory
}

// E2BProviderLookup resolves one installation's create-path dependencies. It
// is a function so the workflow stays testable without constructing a real
// e2b.Client or installation map; an unknown installation must return an
// error so create fails closed.
type E2BProviderLookup func(installation string) (*E2BProvider, error)

// boolPtr returns a *bool for the optional create flags.
func boolPtr(v bool) *bool { return &v }

// ActorWorkflow runs the imperative Substrate operations behind Session
// lifecycle RPCs. Only the claiming caller issues lifecycle mutations; others
// observe current completion or receive a pending/superseded-operation error.
type ActorWorkflow struct {
	store  workflowStore
	actors actorClient
	// e2b resolves an installation's sandbox creator and egress endpoints. nil
	// means no E2B provider is wired; an E2B revision's create then fails
	// closed rather than falling back to Substrate.
	e2b E2BProviderLookup
	// credentialRefresh renews a bound sandbox's runtime credential on resume
	// and before dispatch. nil disables renewal (pre-refresh deployments).
	credentialRefresh *E2BCredentialRefresher
}

// ActorWorkflowOption configures optional workflow dependencies.
type ActorWorkflowOption func(*ActorWorkflow)

// WithE2BProvider wires the E2B session-create lookup. The resolver maps an
// installation identity to its sandbox creator and egress endpoints.
func WithE2BProvider(lookup E2BProviderLookup) ActorWorkflowOption {
	return func(w *ActorWorkflow) { w.e2b = lookup }
}

// WithE2BCredentialRefresher wires the shared credential refresher so resume
// and dispatch renew a lapsed runtime credential rather than completing on a
// dead token.
func WithE2BCredentialRefresher(r *E2BCredentialRefresher) ActorWorkflowOption {
	return func(w *ActorWorkflow) { w.credentialRefresh = r }
}

func NewActorWorkflow(store workflowStore, actors actorClient, opts ...ActorWorkflowOption) *ActorWorkflow {
	w := &ActorWorkflow{store: store, actors: actors}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Pause checkpoints the runtime on its current worker without changing the
// Session logical state or persisting A2A task state.
func (w *ActorWorkflow) Pause(ctx context.Context, session *apiv1alpha1.Session) error {
	revision, err := w.store.GetRuntimeRevision(ctx, session.GetPreparedRevision())
	if err != nil {
		return fmt.Errorf("load prepared revision: %w", err)
	}
	atespace, name := revision.ActorTemplateAtespace, substrate.ActorName(session.GetId())
	current, err := w.actors.GetActor(ctx, atespace, name)
	if err != nil {
		return err
	}
	if err := w.verifyActor(ctx, session, revision, current); err != nil {
		return err
	}
	actor, err := w.actors.PauseActor(ctx, atespace, name)
	if err != nil {
		return fmt.Errorf("pause Actor %s/%s: %w", atespace, name, err)
	}
	if !validActorIdentity(actor, revision, name) || actor.GetMetadata().GetUid() != current.GetMetadata().GetUid() || actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSED {
		return fmt.Errorf("pause Actor %s/%s returned status %s", atespace, name, actor.GetStatus().GetState())
	}
	return nil
}

// Quiesce durably suspends the runtime without changing the Session's
// logical READY state and records its external snapshot URI. Only a checkpoint
// retains a copy after the Actor advances.
func (w *ActorWorkflow) Quiesce(ctx context.Context, session *apiv1alpha1.Session) (*database.SessionTaskSnapshot, error) {
	revision, err := w.store.GetRuntimeRevision(ctx, session.GetPreparedRevision())
	if err != nil {
		return nil, fmt.Errorf("load prepared revision: %w", err)
	}
	atespace, name := revision.ActorTemplateAtespace, substrate.ActorName(session.GetId())
	current, err := w.actors.GetActor(ctx, atespace, name)
	if err != nil {
		return nil, err
	}
	if err := w.verifyActor(ctx, session, revision, current); err != nil {
		return nil, err
	}
	actor, err := w.actors.SuspendActor(ctx, atespace, name)
	if err != nil {
		return nil, fmt.Errorf("suspend Actor %s/%s: %w", atespace, name, err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		return nil, fmt.Errorf("suspend Actor %s/%s returned status %s", atespace, name, actor.GetStatus().GetState())
	}
	metadata := actor.GetMetadata()
	if !validActorIdentity(actor, revision, name) || metadata.GetUid() != current.GetMetadata().GetUid() {
		return nil, fmt.Errorf("suspend actor %s/%s returned invalid identity", atespace, name)
	}
	snapshot := actor.GetStatus().GetExternalSnapshot()
	if snapshot.GetSnapshotUri() == "" {
		return nil, fmt.Errorf("suspend Actor %s/%s returned no snapshot", atespace, name)
	}
	scope := snapshot.GetContentScope()
	if scope != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL && scope != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		return nil, fmt.Errorf("actor %s/%s returned invalid snapshot content scope %s", atespace, name, scope)
	}
	return &database.SessionTaskSnapshot{
		Atespace: atespace, URI: snapshot.GetSnapshotUri(),
		ContentScope: strings.TrimPrefix(scope.String(), "SNAPSHOT_CONTENT_SCOPE_"),
	}, nil
}

// Create provisions the persisted session once, using its pinned checkpoint for
// forks. Retries return current state; an uncertain prior creation blocks execution.
func (w *ActorWorkflow) Create(ctx context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	return w.run(ctx, session.GetId(), apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
}

// Suspend returns after the Actor and session are suspended. A retry observes
// the same operation rather than issuing a second mutation.
func (w *ActorWorkflow) Suspend(ctx context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	return w.run(ctx, session.GetId(), apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND)
}

// Resume returns after the Actor is running and the session is ready. Missing
// Actors are errors; Resume never creates replacement compute.
func (w *ActorWorkflow) Resume(ctx context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	return w.run(ctx, session.GetId(), apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_RESUME)
}

// Delete closes admission before stopping and deleting compute. It can supersede
// unissued creation, but never deletes a session while a prior call is uncertain.
func (w *ActorWorkflow) Delete(ctx context.Context, session *apiv1alpha1.Session) (*apiv1alpha1.Session, error) {
	return w.run(ctx, session.GetId(), apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE)
}

func (w *ActorWorkflow) run(ctx context.Context, sessionID string, requestedKind apiv1alpha1.RuntimeOperation) (*apiv1alpha1.Session, error) {
	ctx, cancelAttempt := context.WithTimeout(ctx, database.RuntimeOperationTimeout)
	defer cancelAttempt()
	operation, err := w.store.BeginSessionOperation(ctx, sessionID, requestedKind)
	if err != nil {
		return nil, err
	}
	return w.execute(ctx, operation)
}

// execute keeps lifecycle preparation separate from the durable issue boundary.
// Multiple callers may prepare using read-only calls; exactly one can authorize
// runtime mutations for a bounded attempt. Errors retain the operation and its
// resource pins so a client retry can continue it. Session admission still
// serializes lifecycle against runtime writes, idle work, and checkpoints.
func (w *ActorWorkflow) execute(ctx context.Context, operation *database.SessionOperation) (_ *apiv1alpha1.Session, err error) {
	sessionID := operation.Instance.Id
	if operation.Instance.Operation == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE {
		return operationOutcome(operation)
	}
	kind := operation.Instance.Operation
	session := operation.Instance
	revision, err := w.store.GetRuntimeRevision(ctx, session.GetPreparedRevision())
	if err != nil {
		return w.failPreparation(ctx, operation, fmt.Errorf("load prepared revision: %w", err))
	}
	// E2B sessions take the provider path: the durable CREATE completion and
	// its instance binding are committed by FinishE2BSessionCreate, never by
	// the Substrate actor path. Only CREATE is supported in this slice; pause,
	// resume, suspend and delete on an E2B session are rejected as unsupported
	// rather than adopted into a Substrate actor lookup.
	if revision.Provider == database.ExecutionProviderE2B {
		return w.executeE2B(ctx, operation, revision)
	}
	var snapshot *database.SessionTaskSnapshot
	var tagName string
	if kind == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE && operation.SourceCheckpointID != nil {
		snapshot, _, err = w.store.GetSessionCheckpointSnapshot(ctx, operation.SourceCheckpointID.String(), session.Creator)
		if err != nil {
			return w.failPreparation(ctx, operation, fmt.Errorf("load pinned checkpoint: %w", err))
		}
		if snapshot == nil || snapshot.URI == "" || snapshot.Atespace == "" || snapshot.ContentScope != "DATA" {
			return w.failPreparation(ctx, operation, fmt.Errorf("fork requires a retained DATA checkpoint"))
		}
		tagName = "checkpoint-" + operation.SourceCheckpointID.String()
	}

	binding := substrate.ActorBinding{Atespace: revision.ActorTemplateAtespace, Name: substrate.ActorName(session.Id),
		TemplateAtespace: revision.ActorTemplateAtespace, TemplateName: revision.ActorTemplateName}
	var creation *substrate.ActorCreation
	if kind == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE {
		policy, err := substrate.ActorEgressPolicy(binding.Atespace, revision.EgressDestinations, revision.Credentials)
		if err != nil {
			return w.failPreparation(ctx, operation, err)
		}
		creation = &substrate.ActorCreation{EgressPolicy: policy}
		if snapshot != nil {
			creation.Snapshot = &substrate.ActorSnapshot{Tag: &ateapipb.ObjectRef{Atespace: snapshot.Atespace, Name: tagName}, URI: snapshot.URI, ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA}
		}
	}
	prepare := substrate.PrepareActorTransition
	if operation.ExecutorID != uuid.Nil {
		prepare = substrate.PrepareActorRetry
	}
	transition, err := prepare(ctx, w.actors, binding, kind, creation)
	if err != nil {
		return w.failPreparation(ctx, operation, err)
	}

	if uid := transition.ActorUID(); uid != "" && kind != apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE {
		if _, err := w.store.GetSessionForRuntime(ctx, session.Id, uid); err != nil {
			return w.failPreparation(ctx, operation, fmt.Errorf("verify runtime actor UID: %w", err))
		}
	}

	executorID := uuid.New()
	claimed, err := w.store.ClaimSessionOperation(ctx, sessionID, operation.ID, executorID)
	if err != nil {
		return nil, err
	}
	if !claimed {
		// A superseded generation returns a conflict without runtime work.
		current, err := w.store.GetSessionOperation(ctx, sessionID, operation.ID)
		if err != nil {
			return nil, err
		}
		return operationOutcome(current)
	}
	defer func() {
		if err == nil {
			return // Completion already cleared the claim.
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, w.store.ReleaseRuntimeOperation(finishCtx, sessionID, operation.ID, executorID))
	}()
	if err := substrate.ApplyActorTransition(ctx, w.actors, transition); err != nil {
		return nil, fmt.Errorf("lifecycle operation %s remains pending: %w", operation.ID, err)
	}
	var authority string
	if kind == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE {
		authority = substrate.ActorHost(binding.Atespace, binding.Name, "")
	}

	// A disconnected client must not discard an already known runtime outcome.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return w.store.FinishSessionOperation(finishCtx, sessionID, operation.ID, executorID, authority, transition.ActorUID(), "")
}

// executeE2B runs an E2B session's CREATE operation. Unlike Substrate it does
// not prepare an actor transition: the only runtime effect is placing a
// sandbox, whose external id the durable completion binds atomically with
// READY. Preparation resolves the revision's artifact binding and the
// installation's egress endpoints before claiming so a misconfigured revision
// fails without issuing work. CREATE places a sandbox under the enforced
// egress policy; DELETE removes it and revokes the binding; pause, resume and
// suspend have no E2B implementation in this slice and are rejected rather
// than adopted into a Substrate actor lookup.
func (w *ActorWorkflow) executeE2B(ctx context.Context, operation *database.SessionOperation, revision *database.RuntimeRevision) (_ *apiv1alpha1.Session, err error) {
	kind := operation.Instance.Operation
	// Resolve the artifact binding and provider before claiming so a revision
	// without a genuine E2B build or an unconfigured installation fails in
	// preparation rather than leaving an orphaned sandbox.
	artifact, err := w.store.GetE2BRevisionBinding(ctx, revision.Revision)
	if err != nil {
		return w.failPreparation(ctx, operation, fmt.Errorf("load E2B revision binding: %w", err))
	}
	if w.e2b == nil {
		return w.failPreparation(ctx, operation, fmt.Errorf("no E2B provider configured for installation %q", artifact.Installation))
	}
	provider, err := w.e2b(artifact.Installation)
	if err != nil {
		return w.failPreparation(ctx, operation, fmt.Errorf("resolve E2B installation %q: %w", artifact.Installation, err))
	}
	switch kind {
	case apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE:
		return w.e2bCreate(ctx, operation, revision, artifact, provider)
	case apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_DELETE:
		return w.e2bDelete(ctx, operation, artifact, provider.Creator)
	case apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_SUSPEND:
		return w.e2bSuspend(ctx, operation, provider.Creator)
	case apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_RESUME:
		return w.e2bResume(ctx, operation, provider)
	default:
		return nil, fmt.Errorf("E2B sessions do not support lifecycle operation %s: %w", kind, database.ErrFailedPrecondition)
	}
}

// e2bSuspend pauses an E2B session's bound sandbox and completes the durable
// transition to SUSPENDED atomically. Pausing is memory-preserving on the
// pinned pin, so the sandbox keeps its live identity for a later resume. A
// sandbox already gone is not an error — the durable suspend still commits so
// the session's state matches its (absent) compute.
func (w *ActorWorkflow) e2bSuspend(ctx context.Context, operation *database.SessionOperation, creator E2BSessionCreator) (_ *apiv1alpha1.Session, err error) {
	sessionID := operation.Instance.Id
	executorID := uuid.New()
	claimed, err := w.store.ClaimSessionOperation(ctx, sessionID, operation.ID, executorID)
	if err != nil {
		return nil, err
	}
	if !claimed {
		current, err := w.store.GetSessionOperation(ctx, sessionID, operation.ID)
		if err != nil {
			return nil, err
		}
		return operationOutcome(current)
	}
	defer func() {
		if err == nil {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, w.store.ReleaseRuntimeOperation(releaseCtx, sessionID, operation.ID, executorID))
	}()

	if binding, bindErr := w.store.GetLiveExecutionBinding(ctx, sessionID); bindErr == nil && binding.ExternalID != "" {
		if err := creator.PauseSandbox(ctx, binding.ExternalID); err != nil && !e2b.IsNotFound(err) {
			return nil, fmt.Errorf("pause E2B sandbox %s: %w", binding.ExternalID, err)
		}
	} else if bindErr != nil && !errors.Is(bindErr, database.ErrNotFound) {
		return nil, fmt.Errorf("resolve E2B binding for suspend: %w", bindErr)
	}

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	// FinishSessionOperation preserves actor_uid NULL on a non-CREATE op and
	// leaves the live binding in place — suspend does not revoke it, so the
	// resume path and the timeout renewer keep resolving the same sandbox.
	return w.store.FinishSessionOperation(finishCtx, sessionID, operation.ID, executorID, "", "", "")
}

// e2bResume wakes an E2B session's paused sandbox and completes the durable
// transition back to READY atomically. It requires a live binding — a session
// with no bound sandbox has nothing to wake — and treats a 404 from the
// provider as runtime loss rather than a retryable transient. Resume never
// creates replacement compute.
func (w *ActorWorkflow) e2bResume(ctx context.Context, operation *database.SessionOperation, provider *E2BProvider) (_ *apiv1alpha1.Session, err error) {
	creator := provider.Creator
	sessionID := operation.Instance.Id
	executorID := uuid.New()
	claimed, err := w.store.ClaimSessionOperation(ctx, sessionID, operation.ID, executorID)
	if err != nil {
		return nil, err
	}
	if !claimed {
		current, err := w.store.GetSessionOperation(ctx, sessionID, operation.ID)
		if err != nil {
			return nil, err
		}
		return operationOutcome(current)
	}
	defer func() {
		if err == nil {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, w.store.ReleaseRuntimeOperation(releaseCtx, sessionID, operation.ID, executorID))
	}()

	binding, err := w.store.GetLiveExecutionBinding(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("resolve E2B binding for resume: %w", err)
	}
	resumed, err := creator.ResumeSandbox(ctx, binding.ExternalID)
	if err != nil {
		if e2b.IsNotFound(err) {
			return nil, fmt.Errorf("E2B session %s bound sandbox %s is gone: %w", sessionID, binding.ExternalID, database.ErrNotFound)
		}
		return nil, fmt.Errorf("resume E2B sandbox %s: %w", binding.ExternalID, err)
	}
	if resumed.SandboxID != binding.ExternalID {
		return nil, fmt.Errorf("E2B resume returned unexpected sandbox %s for bound %s: %w", resumed.SandboxID, binding.ExternalID, database.ErrConflict)
	}
	// The resumed sandbox carries a fresh envd token; refresh the runtime
	// credential now so a session paused past its credential TTL resumes with
	// signed TaskStore access rather than an expired file. Reusing the resumed
	// record avoids a second Resume call. A refresh failure is transient — the
	// sandbox is up but its credential lapsed, so fail rather than complete
	// READY on a dead token.
	if w.credentialRefresh != nil && provider.Bootstrapper != nil {
		if err := w.credentialRefresh.refreshOnSandbox(ctx, sessionID, resumed); err != nil {
			return nil, fmt.Errorf("refresh credential on resume for session %s: %w", sessionID, err)
		}
	}

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return w.store.FinishSessionOperation(finishCtx, sessionID, operation.ID, executorID, "", "", "")
}

// e2bCreate runs an E2B session's CREATE operation: it places a sandbox under
// the enforced egress policy and commits the external id's instance binding
// with READY atomically. A lost create response is reconciled via the
// operation/generation metadata, never by blindly creating a second sandbox.
func (w *ActorWorkflow) e2bCreate(ctx context.Context, operation *database.SessionOperation, revision *database.RuntimeRevision, artifact *database.ExecutionRevisionBinding, provider *E2BProvider) (_ *apiv1alpha1.Session, err error) {
	sessionID := operation.Instance.Id
	// A CREATE with no bootstrapper can never reach a correct READY: the
	// sandbox would bind and complete with no harness launched. Reject the
	// misconfiguration in preparation, before provisioning any compute, rather
	// than landing a READY session that runs nothing.
	if provider.Bootstrapper == nil {
		return w.failPreparation(ctx, operation, fmt.Errorf("E2B installation %q has no harness bootstrapper configured; create cannot run the activation step", artifact.Installation))
	}
	// The guest's ConfigPath must receive the binding's compiled harness
	// config, not the provenance snapshot. An empty binding config is a
	// preparation defect — reject before provisioning rather than launching a
	// harness that cannot parse its config.
	if len(artifact.ConfigJSON) == 0 {
		return w.failPreparation(ctx, operation, fmt.Errorf("E2B revision %s has no compiled harness config on its artifact binding", revision.Revision))
	}
	// The egress policy enforces the broker baseline: only the credential
	// broker, the callback endpoint and the revision's declared destinations
	// are reachable; everything else is denied by the provider.
	egressNet, err := e2b.BuildEgressNetworkConfig(e2b.EgressInput{
		BrokerAddress:        provider.Config.BrokerAddress,
		CallbackAddress:      provider.Config.CallbackAddress,
		RevisionDestinations: revision.EgressDestinations,
	})
	if err != nil {
		return w.failPreparation(ctx, operation, err)
	}

	executorID := uuid.New()
	claimed, err := w.store.ClaimSessionOperation(ctx, sessionID, operation.ID, executorID)
	if err != nil {
		return nil, err
	}
	if !claimed {
		current, err := w.store.GetSessionOperation(ctx, sessionID, operation.ID)
		if err != nil {
			return nil, err
		}
		return operationOutcome(current)
	}
	defer func() {
		if err == nil {
			return // Completion already cleared the claim.
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, w.store.ReleaseRuntimeOperation(finishCtx, sessionID, operation.ID, executorID))
	}()

	// A live instance binding means this CREATE already placed and bound its
	// sandbox before an earlier attempt's bootstrap failed. Resume from the
	// bound sandbox — activate it — rather than create a second one: the
	// binding is durable intent the retry must honor.
	if live, bindErr := w.store.GetLiveExecutionBinding(ctx, sessionID); bindErr == nil && live.ExternalID != "" {
		bootSB, gerr := provider.Creator.ResumeSandbox(ctx, live.ExternalID)
		if gerr != nil {
			if e2b.IsNotFound(gerr) {
				return nil, fmt.Errorf("E2B create bound sandbox %s is gone before activation: %w", live.ExternalID, database.ErrNotFound)
			}
			return nil, fmt.Errorf("reach bound E2B sandbox %s for activation: %w", live.ExternalID, gerr)
		}
		if err := w.activateBoundSandbox(ctx, provider, artifact, revision, sessionID, bootSB, live.Generation); err != nil {
			return nil, err
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return w.store.FinishE2BSessionCreate(finishCtx, sessionID, operation.ID, executorID)
	} else if bindErr != nil && !errors.Is(bindErr, database.ErrNotFound) {
		return nil, fmt.Errorf("resolve E2B binding for create: %w", bindErr)
	}

	// A durable create-attempt record means this operation already issued a
	// CreateSandbox whose outcome is uncertain (a lost response or a crashed
	// executor). It is not permission to create again — reconcile the in-flight
	// create instead. Without this check a retry on an empty listing re-issues
	// CreateSandbox and places a second sandbox for one session.
	attempt, attErr := w.store.GetE2BCreateAttempt(ctx, operation.ID)
	if attErr == nil {
		if attempt.Outcome == "rejected" {
			// The prior create was definitively refused — no sandbox exists to
			// reconcile. Record a fresh attempt below and issue a new create.
			// A rejected attempt is resolved, so it is safe to create again.
			attempt = nil
		} else {
			return w.reconcileIssuedCreate(ctx, operation, revision, artifact, provider, executorID, attempt, nil)
		}
	} else if !errors.Is(attErr, database.ErrNotFound) {
		return nil, fmt.Errorf("resolve E2B create attempt: %w", attErr)
	}

	// Persist correlation metadata with the create so a lost response can be
	// reconciled to this operation/generation rather than retried blind.
	createReq := e2b.CreateRequest{
		// The template reference is pinned to the recorded build: the resolver
		// splits identifier:tag and a UUID tag matches the build_id column, so
		// ExternalID:BuildID selects the exact immutable artifact the revision
		// was prepared against. Without it a later rebuild of the same template
		// would silently create from a newer build while the binding still
		// records the old one. The sandbox still reports templateID=ExternalID,
		// so the reconcile/late-binding template match is unaffected.
		TemplateID: artifact.ExternalID + ":" + artifact.BuildID,
		Network:    egressNet,
		// timeout-to-pause preserves idle compute instead of killing it, and
		// autoResume lets the proxy wake a paused sandbox on ingress — the wake
		// path the gateway relies on. autoPauseMemory retains the memory image
		// so a resumed sandbox keeps its live identity.
		Timeout:         provider.Config.SandboxTimeoutSeconds,
		AutoPause:       boolPtr(true),
		AutoPauseMemory: boolPtr(true),
		AutoResume:      &e2b.AutoResumeConfig{Enabled: true},
		// Secure makes the provider mint a per-sandbox envdAccessToken the
		// bootstrap probe needs to reach envd through the proxy; without it the
		// create returns no access token and file/process activation is
		// impossible. It is the private-ingress credential, distinct from the
		// traffic token and the runtime credential.
		Secure: boolPtr(true),
		Metadata: map[string]string{
			"kagent.dev/session":   sessionID,
			"kagent.dev/operation": operation.ID.String(),
			"kagent.dev/executor":  executorID.String(),
		},
	}
	// Mark the create issued BEFORE the provider call, while this executor
	// still holds the claim. A crash or a lost response after this commit
	// leaves the attempt row that steers every later attempt to reconcile
	// instead of re-create.
	if _, _, err := w.store.RecordE2BCreateAttempt(ctx, sessionID, operation.ID, executorID, artifact.Installation); err != nil {
		return nil, fmt.Errorf("record E2B create attempt: %w", err)
	}
	sandbox, err := provider.Creator.CreateSandbox(ctx, createReq)
	if err != nil {
		// A definitive rejection (auth/validation) provably placed no sandbox:
		// the request was understood and refused, so reconciliation could never
		// find one. Resolve the issued attempt to 'rejected' and release the
		// claim in one transaction so a corrected request re-claims — it must
		// not be steered to reconcile a create that never landed.
		if e2b.IsDefinitiveCreateRejection(err) {
			failCtx, failCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			failErr := w.store.FailE2BSessionCreate(failCtx, sessionID, operation.ID, executorID)
			failCancel()
			if failErr != nil {
				return nil, fmt.Errorf("resolve rejected E2B create: %w", errors.Join(err, failErr))
			}
			return nil, fmt.Errorf("create E2B sandbox rejected: %w", err)
		}
		// The create outcome is ambiguous: a timeout, dropped connection, or
		// non-definitive response may still have placed a sandbox. Reconcile it
		// against the correlation metadata instead of re-issuing — re-creating
		// would either duplicate an already-placed sandbox or adopt one that
		// does not belong to this operation.
		return w.reconcileIssuedCreate(ctx, operation, revision, artifact, provider, executorID, attempt, err)
	}
	// The sandbox is now known-issued. Commit its binding while CREATE is
	// still pending, THEN activate the bound guest, THEN complete READY.
	// Committing READY before bootstrap would leave a READY session with no
	// harness and no open CREATE to recover on a bootstrap failure; binding
	// first keeps the operation open so a retry re-activates the same sandbox
	// instead of creating a second one.
	bindCtx, bindCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer bindCancel()
	if err := w.store.BindE2BSessionCreate(bindCtx, sessionID, operation.ID, executorID, database.E2BCreateBinding{
		Installation: artifact.Installation,
		ExternalID:   sandbox.SandboxID,
		Generation:   1,
	}); err != nil {
		return nil, err
	}
	// Only the now-bound sandbox receives the activation step.
	if err := w.activateBoundSandbox(ctx, provider, artifact, revision, sessionID, sandbox, 1); err != nil {
		return nil, err
	}
	// READY completes only after the bound guest is activated; the live
	// binding persisted above is what makes this session resolvable.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return w.store.FinishE2BSessionCreate(finishCtx, sessionID, operation.ID, executorID)
}

// activateBoundSandbox runs the post-bind activation probe against one bound
// sandbox at the given instance generation: the instance credential and
// revision config are written into the guest and the supervisor signaled,
// never to an unbound guest. A nil bootstrapper is a configuration defect, not
// a milestone — e2bCreate rejects it before provisioning, and this guard keeps
// the reconcile/resume paths from ever returning READY-without-harness if they
// are reached without one. It is the single activation step every create path —
// clean, lost-create reconcile and resume-from-binding — must run before
// READY, so no path lands a READY session whose bound guest was never
// activated.
func (w *ActorWorkflow) activateBoundSandbox(ctx context.Context, provider *E2BProvider, artifact *database.ExecutionRevisionBinding, revision *database.RuntimeRevision, sessionID string, sandbox *e2b.Sandbox, generation int64) error {
	if provider.Bootstrapper == nil {
		return fmt.Errorf("E2B installation %q has no harness bootstrapper configured; cannot activate bound sandbox %s: %w", artifact.Installation, sandbox.SandboxID, database.ErrFailedPrecondition)
	}
	bootstrapper, err := provider.Bootstrapper(sandbox, artifact.LaunchContract)
	if err != nil {
		return fmt.Errorf("build E2B bootstrapper for sandbox %s: %w", sandbox.SandboxID, err)
	}
	// Write the revision's stored Agent Card verbatim so the harness's
	// KAGENT_AGENT_CARD_JSON contract reads a file the bootstrap writes —
	// the card is revision data, not part of the operator's launch command.
	// protojson (not the public A2A render) keeps the payload faithful to the
	// stored card; the harness fills transport interfaces itself.
	var cardJSON []byte
	if revision.AgentCard != nil {
		card, cerr := protojson.Marshal(revision.AgentCard)
		if cerr != nil {
			return fmt.Errorf("encode E2B agent card for revision %s: %w", revision.Revision, cerr)
		}
		cardJSON = card
	}
	// The revision's resolved non-secret environment rides the binding and is
	// exported into the guest process at launch; decode it once per activation.
	var environment map[string]string
	if len(artifact.Environment) > 0 {
		if err := json.Unmarshal(artifact.Environment, &environment); err != nil {
			return fmt.Errorf("decode E2B revision environment for %s: %w", revision.Revision, err)
		}
	}
	bootCtx, bootCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer bootCancel()
	if err := bootstrapper.Bootstrap(bootCtx, artifact.Installation, sessionID, sandbox.SandboxID, generation, artifact.ConfigJSON, cardJSON, environment); err != nil {
		return fmt.Errorf("bootstrap E2B session %s on sandbox %s: %w", sessionID, sandbox.SandboxID, err)
	}
	return nil
}

// reconcileIssuedCreate resolves an issued-but-unbound CreateSandbox without
// re-issuing it. It runs both when the create call returned an ambiguous error
// and when a later attempt finds the durable create-attempt record (meaning a
// create was already issued for this operation and must be reconciled, never
// recreated). The durable proof is the operation's correlation metadata (and a
// known external_id when the create response was observed), so we list all
// sandboxes carrying this operation's tag and act on the verified count:
//   - exactly one verified match (executor + template) -> bind it through the
//     same FinishE2BSessionCreate transaction a clean response would have used;
//   - more than one verified match -> quarantine: delete every candidate and
//     leave the operation pending so admission stays closed until resolved;
//   - zero matches -> NOT proof of absence. The request may still complete or
//     listing may lag, so the operation stays pending rather than re-created.
//
// The executor claim is retained throughout: a later retry of the same
// generation reconciles to the same outcome instead of creating a second
// sandbox. A nil Lister means the provider cannot reconcile, so the operation
// is left pending for the orphan reaper to clean up. createErr is nil when the
// reconcile runs off the durable attempt record rather than a live error.
func (w *ActorWorkflow) reconcileIssuedCreate(ctx context.Context, operation *database.SessionOperation, revision *database.RuntimeRevision, artifact *database.ExecutionRevisionBinding, provider *E2BProvider, executorID uuid.UUID, attempt *database.E2BCreateAttempt, createErr error) (*apiv1alpha1.Session, error) {
	if provider.Lister == nil {
		return nil, fmt.Errorf("create E2B sandbox outcome uncertain and provider cannot reconcile: %w", createErr)
	}
	// A known external id from an observed create response is the strongest
	// match: prefer it over the metadata tag, which only proves the attempt
	// ran, not which sandbox it placed.
	knownExternal := ""
	if attempt != nil && attempt.ExternalID != nil {
		knownExternal = *attempt.ExternalID
	}
	listCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	listed, err := provider.Lister.ListSandboxes(listCtx, map[string]string{
		"kagent.dev/operation": operation.ID.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile lost E2B create: %w", errors.Join(createErr, err))
	}
	// The list is already scoped to this operation's metadata tag, so every
	// returned sandbox was placed by an attempt on this generation. A verified
	// candidate additionally matches the bound artifact's template; the
	// executor tag is recorded for audit but operation+template is the
	// ownership proof (a superseded executor's create cannot reuse this
	// operation's tag).
	var matches []e2b.ListedSandbox
	for _, sb := range listed {
		if knownExternal != "" && sb.SandboxID != knownExternal {
			continue
		}
		if sb.TemplateID == artifact.ExternalID {
			matches = append(matches, sb)
		}
	}
	switch len(matches) {
	case 0:
		// Zero verified matches is not proof the create did not land: the
		// response may be in flight or listing may lag. Keep the operation
		// pending (claim retained by the caller's deferred release) so a later
		// attempt reconciles rather than re-creates.
		return nil, fmt.Errorf("create E2B sandbox outcome uncertain: no sandbox yet carries operation %s metadata; operation left pending for reconciliation: %w", operation.ID, createErr)
	case 1:
		// One verified match is the sandbox this operation placed. Bind it
		// through the same pending-Bind + FinishE2BSessionCreate a clean
		// response uses; the reconciled create lands READY-bound like a clean
		// one (the bound guest's activation is the create path's next phase).
		bindCtx, bindCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer bindCancel()
		if err := w.store.BindE2BSessionCreate(bindCtx, operation.Instance.Id, operation.ID, executorID, database.E2BCreateBinding{
			Installation: artifact.Installation,
			ExternalID:   matches[0].SandboxID,
			Generation:   1,
		}); err != nil {
			return nil, fmt.Errorf("bind reconciled E2B sandbox %s: %w", matches[0].SandboxID, err)
		}
		// Like a clean create, the bound guest must be activated before READY:
		// resolve the sandbox's live record (with a fresh envdAccessToken) and
		// run the same activation step, so a reconciled create never lands a
		// READY session whose harness was never launched.
		bootSB, gerr := provider.Creator.ResumeSandbox(ctx, matches[0].SandboxID)
		if gerr != nil {
			if e2b.IsNotFound(gerr) {
				return nil, fmt.Errorf("reconciled E2B sandbox %s is gone before activation: %w", matches[0].SandboxID, database.ErrNotFound)
			}
			return nil, fmt.Errorf("reach reconciled E2B sandbox %s for activation: %w", matches[0].SandboxID, gerr)
		}
		if err := w.activateBoundSandbox(ctx, provider, artifact, revision, operation.Instance.Id, bootSB, 1); err != nil {
			return nil, err
		}
		finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer finishCancel()
		finished, err := w.store.FinishE2BSessionCreate(finishCtx, operation.Instance.Id, operation.ID, executorID)
		if err != nil {
			return nil, fmt.Errorf("bind reconciled E2B sandbox %s: %w", matches[0].SandboxID, err)
		}
		return finished, nil
	default:
		// Multiple verified matches is a quarantine case: revoke every
		// candidate and keep admission closed until resolved. Cleanup deletes
		// them; the binding is never written.
		var cleanupErrs []error
		for _, sb := range matches {
			delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			cerr := provider.Creator.DeleteSandbox(delCtx, sb.SandboxID)
			delCancel()
			if cerr != nil && !e2b.IsNotFound(cerr) {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("quarantine %s: %w", sb.SandboxID, cerr))
			}
		}
		return nil, fmt.Errorf("create E2B sandbox produced %d verified matches; quarantined, operation left pending: %w", len(matches), errors.Join(append([]error{createErr}, cleanupErrs...)...))
	}
}

// e2bDelete runs an E2B session's DELETE operation: it resolves the live
// binding, deletes the external sandbox, and commits the durable deletion +
// binding revocation atomically. A sandbox that is already gone (404) is
// treated as deleted; any other provider failure leaves the operation pending
// so a retry can finish it rather than tombstoning a still-running sandbox.
func (w *ActorWorkflow) e2bDelete(ctx context.Context, operation *database.SessionOperation, artifact *database.ExecutionRevisionBinding, creator E2BSessionCreator) (_ *apiv1alpha1.Session, err error) {
	sessionID := operation.Instance.Id
	executorID := uuid.New()
	claimed, err := w.store.ClaimSessionOperation(ctx, sessionID, operation.ID, executorID)
	if err != nil {
		return nil, err
	}
	if !claimed {
		current, err := w.store.GetSessionOperation(ctx, sessionID, operation.ID)
		if err != nil {
			return nil, err
		}
		return operationOutcome(current)
	}
	defer func() {
		if err == nil {
			return // Completion already cleared the claim.
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, w.store.ReleaseRuntimeOperation(finishCtx, sessionID, operation.ID, executorID))
	}()

	// Resolve the live binding to learn the external sandbox id. A session
	// with no live binding still deletes cleanly — its sandbox is already
	// unbound, so there is nothing external to remove.
	binding, bindErr := w.store.GetLiveExecutionBinding(ctx, sessionID)
	switch {
	case bindErr == nil && binding.ExternalID != "":
		if err := creator.DeleteSandbox(ctx, binding.ExternalID); err != nil && !e2b.IsNotFound(err) {
			return nil, fmt.Errorf("delete E2B sandbox %s: %w", binding.ExternalID, err)
		}
	case bindErr != nil && !errors.Is(bindErr, database.ErrNotFound):
		return nil, fmt.Errorf("resolve E2B binding for delete: %w", bindErr)
	}

	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return w.store.FinishE2BSessionDelete(finishCtx, sessionID, operation.ID, executorID)
}

// failPreparation releases only unissued work. If another caller won, observe
// the same generation instead. Completion returns current state; supersession
// returns a conflict. A local preparation error cannot clear a newer operation.
func (w *ActorWorkflow) failPreparation(ctx context.Context, admitted *database.SessionOperation, cause error) (*apiv1alpha1.Session, error) {
	if admitted.ExecutorID != uuid.Nil {
		return nil, cause // An earlier attempt may have issued work; retain its intent.
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := w.store.FinishSessionOperation(finishCtx, admitted.Instance.Id, admitted.ID, uuid.Nil, "", "", "lifecycle preparation failed")
	if errors.Is(err, database.ErrConflict) {
		operation, readErr := w.store.GetSessionOperation(finishCtx, admitted.Instance.Id, admitted.ID)
		if readErr != nil {
			return nil, errors.Join(cause, err, readErr)
		}
		return operationOutcome(operation)
	}
	return nil, errors.Join(cause, err)
}

func operationOutcome(operation *database.SessionOperation) (*apiv1alpha1.Session, error) {
	if operation.Instance.Operation == apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE {
		return operation.Instance, nil
	}
	return nil, fmt.Errorf("lifecycle operation %s is pending; runtime effects may be unresolved: %w", operation.ID, database.ErrConflict)
}

// Verify the recorded actor UID before issuing lifecycle work. Names and
// templates alone also match an externally replaced actor, which we must not
// adopt or checkpoint as this session's runtime.
func (w *ActorWorkflow) verifyActor(ctx context.Context, session *apiv1alpha1.Session, revision *database.RuntimeRevision, actor *ateapipb.Actor) error {
	if !validActorIdentity(actor, revision, substrate.ActorName(session.Id)) {
		return fmt.Errorf("runtime actor identity or template changed")
	}
	if _, err := w.store.GetSessionForRuntime(ctx, session.Id, actor.GetMetadata().GetUid()); err != nil {
		return fmt.Errorf("verify runtime actor UID: %w", err)
	}
	return nil
}

func validActorIdentity(actor *ateapipb.Actor, revision *database.RuntimeRevision, name string) bool {
	metadata, ref := actor.GetMetadata(), actor.GetActorTemplate()
	return metadata.GetName() == name && metadata.GetAtespace() == revision.ActorTemplateAtespace && metadata.GetUid() != "" &&
		ref.GetAtespace() == revision.ActorTemplateAtespace && ref.GetName() == revision.ActorTemplateName
}
