package session

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	v2controller "github.com/kagent-dev/kagent/go/core/internal/controller"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/dbtest"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/stretchr/testify/require"
)

// e2bFixture registers an E2B revision (artifact binding through
// execution_revision_binding, no ActorTemplate) and a session against real
// Postgres, so FinishE2BSessionCreate's binding+READY transaction is exercised
// end to end rather than mocked.
func e2bFixture(t *testing.T) (*database.Client, *apiv1alpha1.Session, *database.RuntimeRevision) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	t.Cleanup(cancel)
	conn, cleanup, err := dbtest.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, dbtest.Migrate(conn, false))
	pool, err := pgxpool.New(t.Context(), conn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	client := database.NewClient(pool)

	revision := &database.RuntimeRevision{
		Revision: "e2b-rev-1", Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), AgentCard: &a2apb.AgentCard{Name: "assistant"},
		EgressDestinations: []string{"https://api.openai.com:443"}, Provider: database.ExecutionProviderE2B,
	}
	require.NoError(t, client.UpsertAgentDefinition(t.Context(), database.AgentDefinition{
		Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid", DesiredRevision: revision.Revision}))
	require.NoError(t, client.RecordE2BRuntimeRevision(t.Context(), database.E2BRuntimeRevisionInput{
		Revision: revision.Revision, Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), ConfigJSON: []byte(`{"version":5}`), AgentCard: &a2apb.AgentCard{Name: "assistant"},
		EgressDestinations: revision.EgressDestinations,
		Installation:       "embed-lab", ExternalID: "tpl-abc", BuildID: "4bb0446c-b1f1-4341-acc9-ab51de691cae",
		LaunchContract: []byte(`{"command":"nohup h &"}`),
	}, true))
	session, _, err := client.CreateSession(t.Context(), &apiv1alpha1.Session{
		Id: uuid.NewString(), Creator: "alice",
		Agent: &apiv1alpha1.ResourceReference{Namespace: "team-a", Name: "assistant"},
	}, uuid.NewString())
	require.NoError(t, err)
	return client, session, revision
}

// fakeE2B captures the create request the workflow issues so the test can
// assert the egress policy and template it sends to the provider.
type fakeE2B struct {
	mu          sync.Mutex
	got         *e2b.CreateRequest
	sandbox     *e2b.Sandbox
	err         error
	createCalls int
	deleted     []string
	paused      []string
	pauseErr    error
	resumed     []string
	resumeErr   error
	listed      []e2b.ListedSandbox
	listErr     error
	gotMeta     map[string]string
	listCalls   int
}

func (f *fakeE2B) CreateSandbox(_ context.Context, req e2b.CreateRequest) (*e2b.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	r := req
	f.got = &r
	if f.err != nil {
		return nil, f.err
	}
	if f.sandbox == nil {
		f.sandbox = &e2b.Sandbox{SandboxID: "sb-live-1"}
	}
	return f.sandbox, nil
}

func (f *fakeE2B) DeleteSandbox(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, id)
	return nil
}

// PauseSandbox records the paused external ids so the idle-quiescence tests can
// assert the bound sandbox is paused, not a Substrate actor.
func (f *fakeE2B) PauseSandbox(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paused = append(f.paused, id)
	return f.pauseErr
}

// ResumeSandbox records the resumed external ids; resumeErr lets a test fail
// the wake (e.g. a gone sandbox) to assert runtime-loss, not silent recreate.
func (f *fakeE2B) ResumeSandbox(_ context.Context, id string) (*e2b.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumed = append(f.resumed, id)
	if f.resumeErr != nil {
		return nil, f.resumeErr
	}
	return &e2b.Sandbox{SandboxID: id}, nil
}

// GetSandbox returns a live record carrying the id's token so the credential
// refresh path can resolve a bound sandbox; a fake treats every requested id
// as present.
func (f *fakeE2B) GetSandbox(_ context.Context, id string) (*e2b.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.resumeErr != nil {
		return nil, f.resumeErr
	}
	return &e2b.Sandbox{SandboxID: id}, nil
}

// ListSandboxes lets a test drive the reconcile path: it records the metadata
// filter the workflow lists by and returns the planted summaries.
func (f *fakeE2B) ListSandboxes(_ context.Context, metadata map[string]string) ([]e2b.ListedSandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	f.gotMeta = metadata
	return f.listed, f.listErr
}

func e2bLookup(provider *E2BProvider) E2BProviderLookup {
	return func(string) (*E2BProvider, error) { return provider, nil }
}

func e2bProvider(creator E2BSessionCreator, cfg E2BCreateConfig) *E2BProvider {
	p := &E2BProvider{Creator: creator, Config: cfg}
	// Wire the lister only when the fake implements it, so tests that do not
	// exercise reconcile keep a nil Lister (the no-reconcile path).
	if l, ok := creator.(E2BSessionLister); ok {
		p.Lister = l
	}
	// Default a no-op bootstrapper: a CREATE without one now fails preparation,
	// so every fixture that reaches READY needs the activation step wired.
	// Tests asserting the missing-bootstrapper rejection build the provider
	// literally instead of through this helper.
	p.Bootstrapper = func(*e2b.Sandbox, []byte) (E2BBootstrapper, error) { return &fakeBootstrapper{}, nil }
	return p
}

func TestE2BCreateIssuesSandboxAndBindsIt(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "198.51.100.1:19001", CallbackAddress: "198.51.100.1:18443", SandboxTimeoutSeconds: 3600}))))

	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, created.GetState())
	require.Equal(t, database.E2BSessionAuthority(session.Id), created.GetA2AAuthority())

	// The provider saw the enforced default-deny egress policy: broker +
	// callback + the revision's declared destination host, deny ALL else.
	require.NotNil(t, fake.got)
	// The create pins the immutable build: identifier:buildUUID resolves to the
	// exact recorded build, not whatever latest tag a later rebuild moved.
	require.Equal(t, "tpl-abc:4bb0446c-b1f1-4341-acc9-ab51de691cae", fake.got.TemplateID)
	require.NotNil(t, fake.got.Network)
	require.Equal(t, []string{e2b.DenyAllTraffic}, fake.got.Network.DenyOut)
	require.ElementsMatch(t, []string{"198.51.100.1", "api.openai.com"}, fake.got.Network.AllowOut)
	require.Equal(t, session.Id, fake.got.Metadata["kagent.dev/session"])

	// The create configures timeout-to-pause + auto-resume: idle compute pauses
	// rather than being killed, and the proxy wakes a paused sandbox on ingress.
	require.Equal(t, int32(3600), fake.got.Timeout)
	require.NotNil(t, fake.got.AutoPause)
	require.True(t, *fake.got.AutoPause)
	require.NotNil(t, fake.got.AutoPauseMemory)
	require.True(t, *fake.got.AutoPauseMemory)
	require.NotNil(t, fake.got.AutoResume)
	require.True(t, fake.got.AutoResume.Enabled)

	// The live binding now resolves the external sandbox for the gateway.
	binding, err := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.NoError(t, err)
	require.Equal(t, "sb-live-1", binding.ExternalID)
	require.Equal(t, "embed-lab", binding.Installation)
}

func TestE2BCreateFailsWithoutProvider(t *testing.T) {
	client, session, _ := e2bFixture(t)
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}})
	// No WithE2BProvider: an E2B revision's create must fail closed, not dial
	// Substrate.
	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err)
}

func TestE2BNonCreateOperationRejected(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))
	// A RESUME on a session that was never created is rejected at admission —
	// its state is CREATING, not SUSPENDED — and never reaches the provider.
	_, err := workflow.Resume(context.Background(), session)
	require.Error(t, err)
	require.Nil(t, fake.got, "non-CREATE must not issue a sandbox create")
}

// Suspend pauses the bound sandbox and commits SUSPENDED, keeping the live
// binding so resume resolves the same external compute.
func TestE2BSuspendPausesBoundSandboxKeepsBinding(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))

	_, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)

	suspended, err := workflow.Suspend(context.Background(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_SUSPENDED, suspended.GetState())
	require.Equal(t, []string{"sb-live-1"}, fake.paused, "suspend must pause the bound external sandbox")

	// Suspend does not revoke the binding — resume resolves the same sandbox.
	binding, err := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.NoError(t, err)
	require.Equal(t, "sb-live-1", binding.ExternalID)
}

// Resume wakes the bound sandbox through the provider and commits READY,
// never creating replacement compute.
func TestE2BResumeWakesBoundSandbox(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))

	_, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	_, err = workflow.Suspend(context.Background(), session)
	require.NoError(t, err)

	resumed, err := workflow.Resume(context.Background(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, resumed.GetState())
	require.Equal(t, []string{"sb-live-1"}, fake.resumed, "resume must wake the bound external sandbox")
	require.Equal(t, 1, fake.createCalls, "resume must never create replacement compute")
}

// A gone bound sandbox is runtime loss, not a retryable transient or a
// re-create.
func TestE2BResumeGoneSandboxIsRuntimeLoss(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}, resumeErr: &e2b.Error{Status: 404, Message: "gone"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))

	_, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	_, err = workflow.Suspend(context.Background(), session)
	require.NoError(t, err)

	_, err = workflow.Resume(context.Background(), session)
	require.Error(t, err)
	require.Equal(t, 1, fake.createCalls, "runtime loss must not create a replacement sandbox")
}

func TestE2BDeleteRemovesSandboxAndRevokesBinding(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "198.51.100.1:19001", CallbackAddress: "198.51.100.1:18443"}))))

	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, created.GetState())

	deleted, err := workflow.Delete(context.Background(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED, deleted.GetState())
	require.Empty(t, deleted.GetA2AAuthority())
	require.Equal(t, []string{"sb-live-1"}, fake.deleted, "delete must remove the bound external sandbox")

	// The binding is revoked: no live binding resolves for the deleted session.
	_, err = client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.Error(t, err)
}

// fakeBootstrapper records the post-bind activation call so the test can
// assert the bound sandbox — and only the bound sandbox — is activated.
type fakeBootstrapper struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeBootstrapper) Bootstrap(_ context.Context, installation, sessionID, externalID string, generation int64, configPayload, agentCardPayload []byte, _ map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, installation+"/"+sessionID+"/"+externalID+"/"+itoa(generation))
	return f.err
}

func (f *fakeBootstrapper) RefreshCredential(_ context.Context, installation, sessionID, externalID string, generation int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "refresh/"+installation+"/"+sessionID+"/"+externalID+"/"+itoa(generation))
	return f.err
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func TestE2BCreateBootstrapsOnlyBoundSandbox(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	boot := &fakeBootstrapper{}
	provider := &E2BProvider{
		Creator:      fake,
		Config:       E2BCreateConfig{BrokerAddress: "198.51.100.1:19001", CallbackAddress: "198.51.100.1:18443"},
		Bootstrapper: func(*e2b.Sandbox, []byte) (E2BBootstrapper, error) { return boot, nil },
	}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	_, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	require.Len(t, boot.calls, 1, "bootstrap must run once, on the bound sandbox")
	require.Contains(t, boot.calls[0], "embed-lab/"+session.Id+"/sb-live-1/1",
		"bootstrap got the installation, session, bound external id and generation")
}

// A bootstrap failure after the binding commits must NOT produce a READY
// session or lose the open CREATE: the live binding + pending operation let a
// retry re-activate the SAME bound sandbox without creating a second one.
func TestE2BCreateBootstrapFailureRecoversSameSandbox(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	boot := &fakeBootstrapper{err: fmt.Errorf("activation failed")}
	provider := &E2BProvider{
		Creator:      fake,
		Config:       E2BCreateConfig{BrokerAddress: "198.51.100.1:19001", CallbackAddress: "198.51.100.1:18443"},
		Bootstrapper: func(*e2b.Sandbox, []byte) (E2BBootstrapper, error) { return boot, nil },
	}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	// First attempt: create lands, binding commits, bootstrap fails -> error,
	// no READY, but a live binding + still-open CREATE remain for recovery.
	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err)
	got, gerr := client.GetSessionForExecution(context.Background(), session.Id)
	require.NoError(t, gerr)
	require.NotEqual(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, got.State,
		"bootstrap failure must not produce a READY session")
	binding, berr := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.NoError(t, berr)
	require.Equal(t, "sb-live-1", binding.ExternalID, "binding must persist the placed sandbox")
	require.Equal(t, 1, fake.createCalls, "one sandbox created")

	// Retry the same CREATE: the live binding makes it resume the SAME
	// sandbox (ResumeSandbox, not CreateSandbox) and re-run bootstrap, which
	// now succeeds -> READY.
	boot.err = nil
	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, created.State)
	require.Equal(t, 1, fake.createCalls, "retry must NOT create a second sandbox")
	require.Equal(t, []string{"sb-live-1"}, fake.resumed, "retry resumes the bound sandbox")
	require.Len(t, boot.calls, 2, "bootstrap re-ran on the same sandbox")
}

// A CREATE on an installation with no harness bootstrapper must fail in
// preparation before any sandbox is provisioned — never bind and reach READY
// with no harness launched. Regression for the nil-bootstrapper defect.
func TestE2BCreateRejectsMissingBootstrapper(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	// No Bootstrapper wired: a misconfigured deployment, not a milestone.
	provider := &E2BProvider{
		Creator: fake,
		Config:  E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"},
	}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err, "missing bootstrapper must fail preparation")
	require.Zero(t, fake.createCalls, "no sandbox is provisioned without a bootstrapper")
	got, gerr := client.GetSessionForExecution(context.Background(), session.Id)
	require.NoError(t, gerr)
	require.NotEqual(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, got.State,
		"a no-bootstrapper create must never reach READY")
}

// A lost create response that actually placed a sandbox must reconcile to it
// via the correlation metadata and bind it — never re-create or leave an
// unbound orphan.
func TestE2BCreateReconcilesSingleLostSandbox(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{err: context.DeadlineExceeded}
	// The "lost" sandbox carries this operation's metadata; the workflow does
	// not know the executor UUID ahead of time, so it lists by operation and
	// the executor check is satisfied because the fake planted it — build the
	// summary with the operation tag the workflow will list for.
	fake.listed = []e2b.ListedSandbox{{
		SandboxID: "sb-lost-1", TemplateID: "tpl-abc",
		Metadata: map[string]string{"kagent.dev/session": session.Id},
	}}
	provider := e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"})
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, created.GetState())
	require.Equal(t, 1, fake.listCalls, "reconcile lists once by operation metadata")
	require.NotEmpty(t, fake.gotMeta["kagent.dev/operation"], "list is scoped to this operation")

	binding, err := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.NoError(t, err)
	require.Equal(t, "sb-lost-1", binding.ExternalID)
}

// Multiple verified matches must be quarantined (deleted) and leave the
// operation pending — never pick one arbitrarily.
func TestE2BCreateQuarantinesMultipleMatches(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{err: context.DeadlineExceeded}
	fake.listed = []e2b.ListedSandbox{
		{SandboxID: "sb-a", TemplateID: "tpl-abc", Metadata: map[string]string{}},
		{SandboxID: "sb-b", TemplateID: "tpl-abc", Metadata: map[string]string{}},
	}
	provider := e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"})
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err, "multiple verified matches leave the operation pending")
	require.ElementsMatch(t, []string{"sb-a", "sb-b"}, fake.deleted, "both candidates are quarantined")
	_, berr := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.Error(t, berr, "no binding is written for a quarantined create")
}

// Zero matches is not proof of absence: the operation must stay pending rather
// than re-create. No binding, no delete, and the claim is released for a later
// reconcile attempt.
func TestE2BCreateZeroMatchesStaysPending(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{err: context.DeadlineExceeded}
	provider := e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"})
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err)
	require.Empty(t, fake.deleted, "zero matches must not delete anything")
	_, berr := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.Error(t, berr, "zero matches writes no binding")
}

// A nil Lister cannot reconcile: the operation stays pending for the orphan
// reaper rather than re-issuing a blind create.
func TestE2BCreateNoListerStaysPending(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{err: context.DeadlineExceeded}
	provider := &E2BProvider{Creator: fake, Config: E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}} // no Lister
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err)
	require.Zero(t, fake.listCalls, "nil lister never lists")
	_, berr := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.Error(t, berr)
}

// A retry after an ambiguous create + empty listing must NOT issue a second
// CreateSandbox. The durable create-attempt record written before the first
// provider call steers the retry to reconcile the in-flight create instead —
// regressions here place two sandboxes for one session.
func TestE2BCreateRetryAfterEmptyListDoesNotRecreate(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{err: context.DeadlineExceeded} // create outcome ambiguous, list empty
	boot := &fakeBootstrapper{}
	provider := e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"})
	provider.Bootstrapper = func(*e2b.Sandbox, []byte) (E2BBootstrapper, error) { return boot, nil }
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	// First attempt: create issued, listing empty -> pending, createCalls=1.
	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err, "ambiguous create with empty listing stays pending")
	require.Equal(t, 1, fake.createCalls, "first attempt issues exactly one create")

	// A persisted attempt row now exists for this operation: the retry must
	// reconcile it, never create a second sandbox.
	_, err = workflow.Create(context.Background(), session)
	require.Error(t, err, "still zero verified matches: stays pending, no recreate")
	require.Equal(t, 1, fake.createCalls, "retry must NOT issue a second CreateSandbox")
	require.GreaterOrEqual(t, fake.listCalls, 1, "retry reconciles via list, not create")
}

// A definitive rejection (auth/validation) provably placed no sandbox, so the
// attempt resolves to 'rejected' and the claim releases. Two properties must
// hold: a corrected retry issues a FRESH create (never reconciled against a
// nonexistent sandbox), and the resolved attempt never fences lifecycle work.
func TestE2BCreateDefinitiveRejectionReleasesAndRetries(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{err: &e2b.Error{Status: 403, Message: "credential not permitted"}}
	boot := &fakeBootstrapper{}
	provider := e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"})
	provider.Bootstrapper = func(*e2b.Sandbox, []byte) (E2BBootstrapper, error) { return boot, nil }
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	// First attempt: definitive rejection. No listing happens — there is no
	// in-flight create to reconcile — and the claim releases for a retry.
	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err, "definitive rejection fails the create")
	require.Equal(t, 1, fake.createCalls)
	require.Zero(t, fake.listCalls, "a definitive rejection is never reconciled")

	// The attempt resolved to 'rejected' — not left 'issued' to strand retry.
	opens, oerr := client.ListOpenE2BCreateOperations(context.Background())
	require.NoError(t, oerr)
	require.Len(t, opens, 1)
	attempt, aerr := client.GetE2BCreateAttempt(context.Background(), opens[0].OperationID)
	require.NoError(t, aerr)
	require.Equal(t, "rejected", attempt.Outcome, "a definitive refusal must not stay reconcile-pending")

	// A corrected retry re-issues a fresh create rather than reconciling the
	// rejected (non-existent) first attempt.
	fake.err = nil
	fake.sandbox = &e2b.Sandbox{SandboxID: "sb-retry"}
	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err, "a retry after definitive rejection issues a fresh create")
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, created.GetState())
	require.Equal(t, 2, fake.createCalls, "a rejected create permits exactly one re-issue")

	binding, berr := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.NoError(t, berr)
	require.Equal(t, "sb-retry", binding.ExternalID, "the retry's sandbox binds, not the rejected attempt")
}

// A session whose create was definitively rejected can still be deleted
// cleanly — the resolved 'rejected' attempt placed no compute, so it does not
// fence the orphan-delete gate the way an ambiguous "may have placed" would.
func TestE2BCreateDefinitiveRejectionStillDeletes(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{err: &e2b.Error{Status: 400, Message: "invalid template"}}
	provider := e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"})
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err)

	deleted, err := workflow.Delete(context.Background(), session)
	require.NoError(t, err, "a definitively-rejected create does not block delete")
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED, deleted.GetState())
}

// An idle E2B session pauses its bound sandbox through the provider and
// records paused-without-checkpoint — snapshot columns stay NULL, the idle
// claim clears, and a later checkpoint/fork gets FailedPrecondition because no
// retained boundary exists.
func TestE2BIdleSessionPausesWithoutCheckpoint(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))

	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_READY, created.GetState())

	// Settle a terminal task so an idle boundary exists.
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello"))
	message.ContextID = created.ContextId
	task := a2a.NewSubmittedTask(message, message)
	task.Status.State = a2a.TaskStateCompleted
	hash := sha256.Sum256([]byte("done"))
	version, err := client.CreateRuntimeTask(context.Background(), created.Id, hash[:], task, "", nil)
	require.NoError(t, err)
	require.NoError(t, client.SettleSessionTask(context.Background(), created.Id, string(task.ID), version, nil))

	// Claim the idle boundary the sweep would discover.
	work, err := client.ClaimSessionQuiescence(context.Background())
	require.NoError(t, err)

	workflow.quiesceIdleSession(context.Background(), work)

	require.Equal(t, []string{"sb-live-1"}, fake.paused, "E2B idle pauses the bound sandbox, not a Substrate actor")

	// The claim cleared with no retained snapshot: a second claim finds nothing.
	_, err = client.ClaimSessionQuiescence(context.Background())
	require.ErrorIs(t, err, database.ErrNotFound)

	// The gate: a SECOND turn after the non-checkpoint quiescence admits work
	// directly — the session stays READY (the sandbox is paused beneath it and
	// the proxy auto-resumes on ingress), so no Resume op is needed and the
	// binding still resolves the same external compute.
	message2 := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("second"))
	message2.ContextID = created.ContextId
	task2 := a2a.NewSubmittedTask(message2, message2)
	task2.ID = a2a.TaskID(uuid.NewString())
	task2.Status.State = a2a.TaskStateWorking
	hash2 := sha256.Sum256([]byte("second"))
	// A Working task is admitted and stored; no settle needed (settlement is
	// for terminal/idle boundaries, which a running task has not reached).
	_, err = client.CreateRuntimeTask(context.Background(), created.Id, hash2[:], task2, "", nil)
	require.NoError(t, err, "a second turn must admit on the still-READY session")

	// Confirmed paused-without-checkpoint means there is no retained boundary
	// to reserve or fork from — checkpoint must fail, not fabricate a
	// Substrate-style snapshot. Asserted after the second turn so a leaked
	// CREATING checkpoint cannot block task admission first.
	checkpoint := &apiv1alpha1.Checkpoint{Id: uuid.NewString(), SessionId: created.Id, HeadTaskId: string(task.ID)}
	_, _, err = client.ReserveSessionCheckpoint(context.Background(), checkpoint, "alice", "checkpoint")
	require.Error(t, err, "paused-without-checkpoint session has no boundary to checkpoint")

	// Controller restart: a fresh workflow over the same store still resolves
	// the live binding — durable state, not process memory, owns the session.
	restarted := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))
	binding, err := client.GetLiveExecutionBinding(context.Background(), created.Id)
	require.NoError(t, err, "binding survives controller restart")
	require.Equal(t, "sb-live-1", binding.ExternalID)
	_ = restarted
}

// claimSettledE2BIdle settles one terminal task on the created E2B session and
// claims its idle boundary, so each loss-ordering test starts at the same
// durable point the sweep would discover.
func claimSettledE2BIdle(t *testing.T, client *database.Client, session *apiv1alpha1.Session) *database.SessionQuiescence {
	t.Helper()
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("idle"))
	message.ContextID = session.ContextId
	task := a2a.NewSubmittedTask(message, message)
	task.Status.State = a2a.TaskStateCompleted
	hash := sha256.Sum256([]byte("idle-boundary"))
	version, err := client.CreateRuntimeTask(context.Background(), session.Id, hash[:], task, "", nil)
	require.NoError(t, err)
	require.NoError(t, client.SettleSessionTask(context.Background(), session.Id, string(task.ID), version, nil))
	work, err := client.ClaimSessionQuiescence(context.Background())
	require.NoError(t, err)
	return work
}

// A pause that returns the provider's definitive 404 is confirmed loss, not an
// unknown outcome: the boundary resolves without a fabricated checkpoint, the
// claim clears, and an ordinary Delete is not blocked by retained idle work.
func TestE2BIdlePauseNotFoundResolvesAsLoss(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))
	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)

	work := claimSettledE2BIdle(t, client, created)

	// The provider proves the bound sandbox is gone at pause time.
	fake.pauseErr = &e2b.Error{Status: 404, Message: "sandbox not found"}
	workflow.quiesceIdleSession(context.Background(), work)

	// The claim resolved: nothing remains to re-claim, and the loss was never
	// recorded as a retained checkpoint.
	_, err = client.ClaimSessionQuiescence(context.Background())
	require.ErrorIs(t, err, database.ErrNotFound, "a 404 pause must resolve the boundary, not strand the claim")

	// Ordinary lifecycle work is not blocked by the resolved boundary.
	deleted, err := workflow.Delete(context.Background(), created)
	require.NoError(t, err, "delete must not be blocked by a lost idle boundary")
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED, deleted.GetState())
}

// The other ordering: the loss reaper revokes the binding before quiescence
// runs. The session's E2B identity comes from its durable revision — with no
// live binding the boundary is still confirmed loss, never a fall-through to
// the Substrate actor path.
func TestE2BIdleRevokedBindingResolvesAsLoss(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))
	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)

	work := claimSettledE2BIdle(t, client, created)

	// The reaper proves loss and revokes first; no live binding remains.
	require.NoError(t, client.RevokeExecutionBinding(context.Background(), created.Id))

	workflow.quiesceIdleSession(context.Background(), work)

	// The claim resolved through the E2B finish (no Substrate actor was ever
	// involved: the fixture's actor map stays empty).
	_, err = client.ClaimSessionQuiescence(context.Background())
	require.ErrorIs(t, err, database.ErrNotFound, "a revoked binding must resolve as loss, not strand the claim")

	deleted, err := workflow.Delete(context.Background(), created)
	require.NoError(t, err, "delete must not be blocked after a loss-resolved boundary")
	require.Equal(t, apiv1alpha1.RuntimeState_RUNTIME_STATE_DELETED, deleted.GetState())
}

// A transient pause failure is not loss: the boundary stays claimed and
// unknown so the outcome cannot be silently recorded as resolved.
func TestE2BIdlePauseTransientErrorRetainsUncertainty(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))
	created, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)

	work := claimSettledE2BIdle(t, client, created)

	fake.pauseErr = &e2b.Error{Status: 503, Message: "provider unavailable"}
	workflow.quiesceIdleSession(context.Background(), work)

	// Uncertainty is retained: the claim is still owned by this worker's
	// executor, so a fresh claim finds nothing and Delete is still fenced.
	_, err = client.ClaimSessionQuiescence(context.Background())
	require.ErrorIs(t, err, database.ErrNotFound, "the existing claim is still held, not reissued")
	_, err = workflow.Delete(context.Background(), created)
	require.Error(t, err, "an unresolved boundary still fences lifecycle operations")
}

// Fault-injection per the plan's "delayed-create-after-empty-list" gate: a
// create whose response is lost AND whose list lags stays pending, then the
// sandbox lands and the late-binding sweep adopts it on the next reconcile —
// no re-create, no orphan reap.
func TestE2BDelayedCreateAfterEmptyListBindsOnSweep(t *testing.T) {
	client, session, _ := e2bFixture(t)
	fake := &fakeE2B{err: context.DeadlineExceeded}
	provider := e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"})
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(provider)))

	// First attempt: create "fails" and the list is still empty -> pending.
	_, err := workflow.Create(context.Background(), session)
	require.Error(t, err, "empty list keeps the create pending")
	_, berr := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.Error(t, berr)

	// The delayed create lands: a sandbox now carries this operation's tag.
	// Resolve the open operation so its operation_id drives the sweep's list.
	opens, err := client.ListOpenE2BCreateOperations(context.Background())
	require.NoError(t, err)
	require.Len(t, opens, 1)
	fake.listed = []e2b.ListedSandbox{{
		SandboxID: "sb-late", TemplateID: "tpl-abc",
		Metadata: map[string]string{"kagent.dev/operation": opens[0].OperationID.String()},
	}}

	// The late-binding sweep claims the open op and binds the verified match.
	reconciler := v2controller.NewE2BLateBindingReconciler(client, []string{"embed-lab"},
		func(string) (v2controller.E2BOrphanLister, error) { return fake, nil })
	reconciler.ReconcileOnce(context.Background())

	binding, err := client.GetLiveExecutionBinding(context.Background(), session.Id)
	require.NoError(t, err)
	require.Equal(t, "sb-late", binding.ExternalID, "late sandbox binds to the open operation")
	require.Equal(t, 1, fake.createCalls, "no second create was ever issued")
}

// Fault-injection per the plan's "controller crash with issued intent": an
// executor that claimed and issued a create but crashed before binding leaves
// the operation pending under its (now-abandoned) claim. Once the claim
// expires a later executor's sweep reconciles the SAME operation — it binds
// the sandbox the crashed attempt placed; it does not re-create.
func TestE2BCrashedExecutorIntentReconciledBySweep(t *testing.T) {
	ctx := context.Background()
	conn, cleanup, err := dbtest.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, dbtest.Migrate(conn, false))
	pool, err := pgxpool.New(ctx, conn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	client := database.NewClient(pool)

	revision := &database.RuntimeRevision{
		Revision: "e2b-rev-1", Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), AgentCard: &a2apb.AgentCard{},
		EgressDestinations: []string{"h:1"}, Provider: database.ExecutionProviderE2B,
	}
	require.NoError(t, client.UpsertAgentDefinition(ctx, database.AgentDefinition{
		Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid", DesiredRevision: revision.Revision}))
	require.NoError(t, client.RecordE2BRuntimeRevision(ctx, database.E2BRuntimeRevisionInput{
		Revision: revision.Revision, Namespace: "team-a", AgentName: "assistant", AgentUID: "agent-uid",
		SourceSnapshot: []byte(`{"provenance":true}`), ConfigJSON: []byte(`{"version":5}`), AgentCard: &a2apb.AgentCard{}, EgressDestinations: revision.EgressDestinations,
		Installation: "embed-lab", ExternalID: "tpl-abc", BuildID: "b1",
		LaunchContract: []byte(`{"command":"nohup h &"}`)}, true))
	session, _, err := client.CreateSession(ctx, &apiv1alpha1.Session{
		Id: uuid.NewString(), Creator: "alice",
		Agent: &apiv1alpha1.ResourceReference{Namespace: "team-a", Name: "assistant"},
	}, uuid.NewString())
	require.NoError(t, err)

	// Executor A begins+claims the create, issues it (sandbox lands), then
	// "crashes": it never finishes and never releases.
	op, err := client.BeginSessionOperation(ctx, session.Id, apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_CREATE)
	require.NoError(t, err)
	executorA := uuid.New()
	claimed, err := client.ClaimSessionOperation(ctx, session.Id, op.ID, executorA)
	require.NoError(t, err)
	require.True(t, claimed)
	fake := &fakeE2B{listed: []e2b.ListedSandbox{{
		SandboxID: "sb-issued", TemplateID: "tpl-abc",
		Metadata: map[string]string{"kagent.dev/operation": op.ID.String(), "kagent.dev/executor": executorA.String()},
	}}}

	// While executor A's claim is live the sweep cannot take it over.
	reconciler := v2controller.NewE2BLateBindingReconciler(client, []string{"embed-lab"},
		func(string) (v2controller.E2BOrphanLister, error) { return fake, nil })
	reconciler.ReconcileOnce(ctx)
	_, berr := client.GetLiveExecutionBinding(ctx, session.Id)
	require.Error(t, berr, "a live abandoned claim is not taken over early")

	// Executor A crashes: its claim expires. A later sweep reconciles the SAME
	// operation and binds the issued sandbox.
	_, err = pool.Exec(ctx, `UPDATE runtime_instance SET executor_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, session.Id)
	require.NoError(t, err)
	reconciler.ReconcileOnce(ctx)
	binding, err := client.GetLiveExecutionBinding(ctx, session.Id)
	require.NoError(t, err)
	require.Equal(t, "sb-issued", binding.ExternalID, "crashed executor's issued sandbox is bound by a later reconcile")
}

// --- E2BCredentialRefresher ---

// boundE2B creates a live E2B binding for the fixture session by running the
// create path once, so the refresher has a real binding+revision to act on.
func boundE2B(t *testing.T, fake *fakeE2B, boot *fakeBootstrapper) (*database.Client, *apiv1alpha1.Session) {
	client, session, _ := e2bFixture(t)
	workflow := NewActorWorkflow(client, &lifecycleTestActors{actors: map[string]*ateapipb.Actor{}},
		WithE2BProvider(e2bLookup(e2bProvider(fake, E2BCreateConfig{BrokerAddress: "b:1", CallbackAddress: "c:2"}))))
	_, err := workflow.Create(context.Background(), session)
	require.NoError(t, err)
	return client, session
}

func TestCredentialRefresherDispatchWakesPausedAndRefreshes(t *testing.T) {
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"}}
	boot := &fakeBootstrapper{}
	client, session := boundE2B(t, fake, boot)

	provider := e2bProvider(fake, E2BCreateConfig{})
	provider.Bootstrapper = func(*e2b.Sandbox, []byte) (E2BBootstrapper, error) { return boot, nil }
	refresh := NewE2BCredentialRefresher(client, e2bLookup(provider))

	got, err := refresh.EnsureForDispatch(context.Background(), session.Id)
	require.NoError(t, err)
	require.Equal(t, "sb-live-1", got.ExternalID)
	require.Contains(t, fake.resumed, "sb-live-1", "dispatch refresh wakes the paused sandbox")
	require.Len(t, boot.calls, 1, "the refresh writes once; the fixture create used a no-op bootstrapper")
	require.Contains(t, boot.calls[0], "refresh/", "dispatch path installs a fresh credential")
}

func TestCredentialRefresherBackgroundSkipsPaused(t *testing.T) {
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"},
		listed: []e2b.ListedSandbox{{SandboxID: "sb-live-1", State: e2b.SandboxStatePaused}}}
	boot := &fakeBootstrapper{}
	client, session := boundE2B(t, fake, boot)

	provider := e2bProvider(fake, E2BCreateConfig{})
	provider.Lister = fake
	provider.Bootstrapper = func(*e2b.Sandbox, []byte) (E2BBootstrapper, error) { return boot, nil }
	refresh := NewE2BCredentialRefresher(client, e2bLookup(provider))

	refreshed, err := refresh.RefreshRunning(context.Background(), session.Id)
	require.NoError(t, err)
	require.False(t, refreshed, "a paused sandbox is skipped, never woken")
	require.Empty(t, fake.resumed, "background refresh must not resume a paused sandbox")
	require.Empty(t, boot.calls, "no credential write for a paused sandbox")
}

func TestCredentialRefresherBackgroundRefreshesRunning(t *testing.T) {
	fake := &fakeE2B{sandbox: &e2b.Sandbox{SandboxID: "sb-live-1"},
		listed: []e2b.ListedSandbox{{SandboxID: "sb-live-1", State: e2b.SandboxStateRunning}}}
	boot := &fakeBootstrapper{}
	client, session := boundE2B(t, fake, boot)

	provider := e2bProvider(fake, E2BCreateConfig{})
	provider.Lister = fake
	provider.Bootstrapper = func(*e2b.Sandbox, []byte) (E2BBootstrapper, error) { return boot, nil }
	refresh := NewE2BCredentialRefresher(client, e2bLookup(provider))

	refreshed, err := refresh.RefreshRunning(context.Background(), session.Id)
	require.NoError(t, err)
	require.True(t, refreshed)
	require.Empty(t, fake.resumed, "a running sandbox is refreshed without a resume")
	require.Len(t, boot.calls, 1)
	require.Contains(t, boot.calls[0], "refresh/")
}
