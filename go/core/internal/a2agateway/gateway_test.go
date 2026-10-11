package a2agateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	"github.com/google/uuid"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

const (
	gatewayTestID        = "8bd650a8-9775-488f-8bc1-0d52bf7bdcab"
	gatewayTestContextID = gatewayTestID
	gatewayTestURL       = "https://gateway.example"
)

type gatewayTestAuthSession struct{}

func (gatewayTestAuthSession) Principal() auth.Principal {
	return auth.Principal{User: auth.User{ID: "alice"}}
}

type gatewayTestStore struct {
	*database.Client
	reserveCalls     int
	revokeCalls      int
	revokeContextErr error
	reserveErr       error
	initialID        string
	listedIDs        []string
	revoked          bool
	session          *apiv1alpha1.Session
	revision         *database.RuntimeRevision
	err              error
	task             *a2atype.Task
	tasks            []*a2atype.Task
	total            int
	historyLength    *int
	taskErr          error
	replay           *a2atype.Task
	stored           []a2atype.Event
	id, userID       string
	unscoped         bool
	settledRead      func() error
	created          map[string]*apiv1alpha1.Session
	admissionsClosed bool
	admissionsErr    error
}

// CheckAdmissionsOpen lets a test drive the maintenance barrier without a real
// database; the embedded *database.Client would otherwise panic on a nil pool.
// It returns ErrAdmissionClosed when the test has closed admissions, matching
// the real transactional check's contract.
func (s *gatewayTestStore) CheckAdmissionsOpen(context.Context) error {
	if s.admissionsErr != nil {
		return s.admissionsErr
	}
	if s.admissionsClosed {
		return database.ErrAdmissionClosed
	}
	return nil
}

func (s *gatewayTestStore) ReserveSessionDispatch(_ context.Context, _ string, _ uuid.UUID, initialID string) error {
	s.reserveCalls++
	s.initialID = initialID
	return s.reserveErr
}

func (s *gatewayTestStore) RevokeSessionDispatch(ctx context.Context, _ string, _ uuid.UUID, _ string) (bool, error) {
	s.revokeCalls++
	s.revokeContextErr = ctx.Err()
	return s.revoked, nil
}

func (s *gatewayTestStore) GetSessionByID(_ context.Context, id string) (*apiv1alpha1.Session, error) {
	s.id, s.userID = id, ""
	s.unscoped = true
	return s.session, s.err
}

func (s *gatewayTestStore) GetSession(_ context.Context, id, userID string) (*apiv1alpha1.Session, error) {
	s.id, s.userID = id, userID
	if s.err == nil && (s.session == nil || id != s.session.Id) {
		return nil, database.ErrNotFound
	}
	return s.session, s.err
}

func (s *gatewayTestStore) CreateSession(_ context.Context, request *apiv1alpha1.Session, requestID string) (*apiv1alpha1.Session, bool, error) {
	if s.created == nil {
		return s.session, true, s.err
	}
	key := request.Creator + "/" + requestID
	if existing := s.created[key]; existing != nil {
		return existing, false, nil
	}
	session := gatewayTestSession()
	session.Id, session.ContextId, session.Agent, session.Creator = request.Id, request.Id, request.Agent, request.Creator
	s.created[key], s.session = session, session
	return session, true, nil
}

func (s *gatewayTestStore) ListSessions(_ context.Context, query database.SessionQuery) ([]*apiv1alpha1.Session, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.session == nil || s.session.Id <= query.AfterID {
		return nil, nil
	}
	if query.Agent != nil && (query.Agent.Namespace != s.session.GetAgent().GetNamespace() || query.Agent.Name != s.session.GetAgent().GetName()) {
		return nil, nil
	}
	return []*apiv1alpha1.Session{s.session}, nil
}

func (s *gatewayTestStore) GetRuntimeRevision(context.Context, string) (*database.RuntimeRevision, error) {
	return s.revision, nil
}

func (s *gatewayTestStore) GetSessionTask(_ context.Context, _ string, taskID string, historyLength *int) (*a2atype.Task, error) {
	s.historyLength = historyLength
	if s.taskErr != nil {
		return nil, s.taskErr
	}
	if s.task == nil || string(s.task.ID) != taskID {
		return nil, database.ErrNotFound
	}
	return s.task, nil
}

func (s *gatewayTestStore) ListAgentTasks(_ context.Context, ids []string, _ string, _ a2atype.TaskState, _ *time.Time, _ int, historyLength *int) ([]*a2atype.Task, int, error) {
	s.listedIDs = ids
	s.historyLength = historyLength
	return s.tasks, s.total, s.taskErr
}

func (s *gatewayTestStore) GetSettledSessionTask(ctx context.Context, sessionID, taskID string, historyLength *int) (*a2atype.Task, error) {
	if s.settledRead != nil {
		if err := s.settledRead(); err != nil {
			return nil, err
		}
	}
	return s.GetSessionTask(ctx, sessionID, taskID, historyLength)
}

type gatewayTestAuthorizer struct {
	principal auth.Principal
	err       error
	verb      auth.Verb
	resource  auth.Resource
}

func (a *gatewayTestAuthorizer) Check(_ context.Context, principal auth.Principal, verb auth.Verb, resource auth.Resource) error {
	a.principal, a.verb, a.resource = principal, verb, resource
	return a.err
}

type gatewayTestDialer struct {
	client  *a2aclient.Client
	session *apiv1alpha1.Session
	err     error
}

func (d *gatewayTestDialer) Dial(_ context.Context, session *apiv1alpha1.Session) (*a2aclient.Client, error) {
	d.session = session
	return d.client, d.err
}

type gatewayTestRuntime struct {
	a2aclient.Transport
	sent           bool
	destroyed      bool
	task           *a2atype.Task
	taskErr        error
	taskResults    []*a2atype.Task
	getTaskCalls   int
	subscribeEvent a2atype.Event
	streamState    a2atype.TaskState
	subscribeErr   error
	subscribeCalls int
	// subscribeSeq, when set, drives the stream body so a test can interleave
	// control (e.g. closing admissions) between emitted events.
	subscribeSeq func(yield func(a2atype.Event, error) bool)
	cancelErr    error
	sendCalls    int
	sentTaskID   a2atype.TaskID
	onSend       func() error
}

func (r *gatewayTestRuntime) CancelTask(context.Context, a2aclient.ServiceParams, *a2atype.CancelTaskRequest) (*a2atype.Task, error) {
	if r.cancelErr != nil {
		return nil, r.cancelErr
	}
	return r.task, nil
}

func (r *gatewayTestRuntime) GetTask(context.Context, a2aclient.ServiceParams, *a2atype.GetTaskRequest) (*a2atype.Task, error) {
	call := r.getTaskCalls
	r.getTaskCalls++
	if call < len(r.taskResults) {
		return r.taskResults[call], nil
	}
	return r.task, r.taskErr
}

func (r *gatewayTestRuntime) SubscribeToTask(context.Context, a2aclient.ServiceParams, *a2atype.SubscribeToTaskRequest) iter.Seq2[a2atype.Event, error] {
	r.subscribeCalls++
	if r.subscribeSeq != nil {
		return r.subscribeSeq
	}
	return func(yield func(a2atype.Event, error) bool) {
		if r.subscribeEvent != nil || r.subscribeErr != nil {
			yield(r.subscribeEvent, r.subscribeErr)
		}
	}
}

func (r *gatewayTestRuntime) SendMessage(_ context.Context, _ a2aclient.ServiceParams, req *a2atype.SendMessageRequest) (a2atype.SendMessageResult, error) {
	r.sent = true
	r.sendCalls++
	r.sentTaskID = req.Message.TaskID
	if r.onSend != nil {
		if err := r.onSend(); err != nil {
			return nil, err
		}
	}
	if r.task != nil {
		return r.task, nil
	}
	return &a2atype.Task{ID: "runtime-task", ContextID: req.Message.ContextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}, nil
}

func (r *gatewayTestRuntime) SendStreamingMessage(_ context.Context, _ a2aclient.ServiceParams, req *a2atype.SendMessageRequest) iter.Seq2[a2atype.Event, error] {
	return func(yield func(a2atype.Event, error) bool) {
		if r.task != nil {
			yield(r.task, nil)
			return
		}
		if r.streamState != a2atype.TaskStateUnspecified {
			task := &a2atype.Task{ID: req.Message.TaskID, ContextID: req.Message.ContextID}
			yield(a2atype.NewStatusUpdateEvent(task, r.streamState, a2atype.NewMessage(a2atype.MessageRoleAgent, a2atype.NewTextPart("approve?"))), nil)
			return
		}
		yield(&a2atype.Task{ID: req.Message.TaskID, ContextID: req.Message.ContextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}, nil)
	}
}

func (r *gatewayTestRuntime) Destroy() error {
	r.destroyed = true
	return nil
}

func gatewayTestClient(t *testing.T, runtime a2aclient.Transport) *a2aclient.Client {
	t.Helper()
	client, err := a2aclient.NewFromEndpoints(t.Context(), []*a2atype.AgentInterface{{
		URL:             "runtime.test",
		ProtocolBinding: a2atype.TransportProtocolGRPC,
		ProtocolVersion: a2atype.Version,
	}},
		a2aclient.WithDefaultsDisabled(),
		a2aclient.WithTransport(a2atype.TransportProtocolGRPC, a2aclient.TransportFactoryFn(func(context.Context, *a2atype.AgentCard, *a2atype.AgentInterface) (a2aclient.Transport, error) {
			return runtime, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func gatewayTestContext() context.Context {
	return gatewayTestContextWithRoute("team-a", gatewayTestID)
}

func gatewayTestContextWithRoute(namespace, id string) context.Context {
	ctx := auth.AuthSessionTo(context.Background(), gatewayTestAuthSession{})
	return a2atype.AttachTenant(ctx, namespace+"/assistant")
}

func gatewayTestSession() *apiv1alpha1.Session {
	return &apiv1alpha1.Session{
		Id: gatewayTestID, ContextId: gatewayTestContextID, Creator: "alice",
		PreparedRevision: "revision-1",
		Agent:            &apiv1alpha1.ResourceReference{Namespace: "team-a", Name: "assistant"},
		A2AAuthority:     "private-runtime-authority",
		State:            apiv1alpha1.RuntimeState_RUNTIME_STATE_READY,
		Operation:        apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE,
	}
}

func gatewayTestRequest() *a2atype.SendMessageRequest {
	message := a2atype.NewMessage(a2atype.MessageRoleUser, a2atype.NewTextPart("hello"))
	message.ContextID = gatewayTestID
	return &a2atype.SendMessageRequest{Tenant: gatewayTestAgent, Message: message}
}

// endingRuntime reports whether its stream ran to its end before the gateway
// destroyed the client. With hold set it keeps the stream open until destroyed.
type endingRuntime struct {
	gatewayTestRuntime
	hold   bool
	closed chan struct{}

	mu             sync.Mutex
	endedBeforeEnd bool
}

func (r *endingRuntime) SendStreamingMessage(_ context.Context, _ a2aclient.ServiceParams, req *a2atype.SendMessageRequest) iter.Seq2[a2atype.Event, error] {
	return func(yield func(a2atype.Event, error) bool) {
		if !yield(&a2atype.Task{ID: "runtime-task", ContextID: req.Message.ContextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}, nil) {
			return
		}
		if r.hold {
			<-r.closed
			return
		}
		r.mu.Lock()
		r.endedBeforeEnd = !r.destroyed
		r.mu.Unlock()
	}
}

func (r *endingRuntime) Destroy() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.destroyed {
		r.destroyed = true
		close(r.closed)
	}
	return nil
}

func TestGatewayDrainsTheRuntimeStreamBeforeClosingATerminalTurn(t *testing.T) {
	tests := []struct {
		name      string
		hold      bool
		wantEnded bool
	}{
		{name: "runtime ends its stream", wantEnded: true},
		{name: "runtime keeps its stream open", hold: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := gatewayTestSession()
			runtime := &endingRuntime{hold: tt.hold, closed: make(chan struct{})}
			gateway := newTestGateway(&gatewayTestStore{session: session, task: &a2atype.Task{ID: "runtime-task", ContextID: gatewayTestContextID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}}, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)
			for _, err := range gateway.SendStreamingMessage(gatewayTestContext(), gatewayTestRequest()) {
				if err != nil {
					t.Fatal(err)
				}
			}
			runtime.mu.Lock()
			defer runtime.mu.Unlock()
			if runtime.endedBeforeEnd != tt.wantEnded || !runtime.destroyed {
				t.Fatalf("stream ended before destroy = %v, destroyed = %v", runtime.endedBeforeEnd, runtime.destroyed)
			}
		})
	}
}

func TestGatewayRequiresValidAgentTenant(t *testing.T) {
	gateway := newTestGateway(&gatewayTestStore{session: gatewayTestSession()}, &gatewayTestAuthorizer{}, &gatewayTestDialer{}, gatewayTestURL)
	for _, tenant := range []string{"", "assistant", "INVALID/assistant", "team-a/INVALID", "team-a/assistant/extra"} {
		t.Run(tenant, func(t *testing.T) {
			ctx := auth.AuthSessionTo(t.Context(), gatewayTestAuthSession{})
			request := gatewayTestRequest()
			request.Tenant = tenant
			_, err := gateway.SendMessage(ctx, request)
			require.ErrorIs(t, err, a2atype.ErrInvalidRequest)
		})
	}
}

func TestGatewayHidesInternalErrors(t *testing.T) {
	session := gatewayTestSession()
	for _, test := range []struct {
		name    string
		store   *gatewayTestStore
		dialer  *gatewayTestDialer
		message string
	}{
		{name: "store", store: &gatewayTestStore{err: errors.New("password=secret")}, dialer: &gatewayTestDialer{}, message: a2atype.ErrInternalError.Error()},
		{name: "dialer", store: &gatewayTestStore{session: session}, dialer: &gatewayTestDialer{err: errors.New("internal.host:1234")}, message: "failed to connect to Session runtime"},
		// Runtime loss is a typed lifecycle outcome, not a generic connect
		// failure: it must surface the public ErrRuntimeLost message so the
		// caller does not retry a dead sandbox, while still hiding the wrapped
		// internal detail (binding/sandbox identifiers) that the dialer logged.
		{name: "runtime lost", store: &gatewayTestStore{session: session}, dialer: &gatewayTestDialer{err: fmt.Errorf("session x bound sandbox secret-sandbox-id is gone: %w", ErrRuntimeLost)}, message: "session runtime lost: " + ErrRuntimeLost.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway := newTestGateway(test.store, &gatewayTestAuthorizer{}, test.dialer, gatewayTestURL)
			_, err := gateway.SendMessage(gatewayTestContext(), gatewayTestRequest())
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("SendMessage() error = %v, want %q", err, test.message)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "internal.host") || strings.Contains(err.Error(), "secret-sandbox-id") {
				t.Fatalf("SendMessage() leaked internal error: %v", err)
			}
		})
	}
}

func TestGatewayRoutesByGRPCTenant(t *testing.T) {
	session := gatewayTestSession()
	runtime := &gatewayTestRuntime{}
	gateway := newTestGateway(&gatewayTestStore{session: session}, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(auth.AuthSessionTo(ctx, gatewayTestAuthSession{}), req)
	}))
	a2agrpc.NewHandler(gateway).RegisterWith(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	connection, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	request, err := pbconv.ToProtoSendMessageRequest(gatewayTestRequest())
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs())
	if _, err := a2apb.NewA2AServiceClient(connection).SendMessage(ctx, request); err != nil {
		t.Fatal(err)
	}
	if !runtime.sent {
		t.Fatal("gRPC request did not reach the Session runtime")
	}
}

func TestRuntimeDialerRequiresAuthority(t *testing.T) {
	if _, err := (&RuntimeDialer{}).Dial(t.Context(), &apiv1alpha1.Session{}); err == nil {
		t.Fatal("Dial() accepted an empty runtime authority")
	}
}

func TestGatewayReadsTasksWithoutDialingRuntime(t *testing.T) {
	for _, state := range []a2atype.TaskState{
		a2atype.TaskStateSubmitted, a2atype.TaskStateWorking,
		a2atype.TaskStateInputRequired, a2atype.TaskStateAuthRequired,
		a2atype.TaskStateCompleted, a2atype.TaskStateFailed,
		a2atype.TaskStateCanceled, a2atype.TaskStateRejected,
	} {
		t.Run(string(state), func(t *testing.T) {
			task := &a2atype.Task{
				ID: gatewayTestID, ContextID: gatewayTestContextID,
				Status:    a2atype.TaskStatus{State: state},
				History:   []*a2atype.Message{{ID: "one"}, {ID: "two"}},
				Artifacts: []*a2atype.Artifact{{Name: "result"}},
			}
			store := &gatewayTestStore{session: gatewayTestSession(), task: task, tasks: []*a2atype.Task{task}, total: 1}
			dialer := &gatewayTestDialer{}
			gateway := newTestGateway(store, &gatewayTestAuthorizer{}, dialer, gatewayTestURL)
			historyLength := 1

			got, err := gateway.GetTask(gatewayTestContext(), &a2atype.GetTaskRequest{ID: task.ID, HistoryLength: &historyLength})
			if err != nil || len(got.History) != 1 || len(got.Artifacts) != 1 {
				t.Fatalf("GetTask() = %#v, %v", got, err)
			}
			if store.historyLength == nil || *store.historyLength != historyLength {
				t.Fatal("GetTask did not pass the history limit to persistence")
			}
			store.historyLength = nil
			listed, err := gateway.ListTasks(gatewayTestContext(), &a2atype.ListTasksRequest{HistoryLength: &historyLength})
			if err != nil || len(listed.Tasks) != 1 || len(listed.Tasks[0].History) != 1 || listed.Tasks[0].Artifacts != nil {
				t.Fatalf("ListTasks() = %#v, %v", listed, err)
			}
			if store.historyLength == nil || *store.historyLength != historyLength {
				t.Fatal("ListTasks did not pass the history limit to persistence")
			}
			if dialer.session != nil {
				t.Fatal("task reads dialed the private runtime")
			}
			if len(store.stored) != 0 {
				t.Fatal("task reads persisted runtime state")
			}
		})
	}
}

/*
 * A suspended conversation is still readable, and sending to it is still refused.
 *
 * Both halves matter and they used to be one rule. Every RPC resolved the session
 * through a helper that insisted on READY, which is right for anything needing the
 * worker and wrong for a task list — that comes out of the store, which does not care
 * whether a worker is attached.
 *
 * It became a real fault once conversations started giving their workers back at the
 * end of every turn: opening one to re-read what was said reported "Session is
 * SUSPENDED" as though the transcript had been lost. Resuming on
 * open would have claimed a worker every time somebody glanced at one, which is the
 * thing suspending them exists to avoid.
 */
func TestGatewayReadsTasksWhileSuspended(t *testing.T) {
	session := gatewayTestSession()
	session.State = apiv1alpha1.RuntimeState_RUNTIME_STATE_SUSPENDED
	task := &a2atype.Task{ID: gatewayTestID, ContextID: gatewayTestContextID}
	store := &gatewayTestStore{session: session, task: task, tasks: []*a2atype.Task{task}, total: 1}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{}, gatewayTestURL)

	if _, err := gateway.ListTasks(gatewayTestContext(), &a2atype.ListTasksRequest{Tenant: gatewayTestAgent, ContextID: gatewayTestID}); err != nil {
		t.Fatalf("ListTasks() on a suspended session = %v, want the stored transcript", err)
	}
	if _, err := gateway.GetTask(gatewayTestContext(), &a2atype.GetTaskRequest{ID: task.ID}); err != nil {
		t.Fatalf("GetTask() on a suspended session = %v, want the stored task", err)
	}

	// The other half: what needs the worker is still refused, naming the state. Without
	// this the change would read as "suspended no longer means anything".
	if _, err := gateway.SendMessage(gatewayTestContext(), gatewayTestRequest()); err == nil {
		t.Fatal("SendMessage() to a suspended session succeeded, want a refusal naming the state")
	}
}

func TestGatewayBuildsAgentCardFromAgentRevision(t *testing.T) {
	store := &gatewayTestStore{
		revision: &database.RuntimeRevision{
			Revision: "revision-1",
			AgentCard: &a2apb.AgentCard{
				Name: "assistant", Description: "pinned description", Version: "v1",
				SupportedInterfaces: []*a2apb.AgentInterface{{Url: "http://127.0.0.1:80", ProtocolBinding: "GRPC", ProtocolVersion: "1.0"}},
				Capabilities:        &a2apb.AgentCapabilities{PushNotifications: new(true), Extensions: []*a2apb.AgentExtension{{Uri: "https://kagent.dev/extensions/hitl/v1"}}},
				DefaultInputModes:   []string{"text"}, DefaultOutputModes: []string{"text"},
			},
		},
	}
	authorizer := &gatewayTestAuthorizer{}
	dialer := &gatewayTestDialer{}
	gateway := newTestGateway(store, authorizer, dialer, gatewayTestURL)

	card, err := gateway.GetExtendedAgentCard(gatewayTestContext(), &a2atype.GetExtendedAgentCardRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if card.Name != "assistant" || card.Description != "pinned description" || card.Version != "v1" {
		t.Fatalf("template metadata = %#v", card)
	}
	if len(card.SupportedInterfaces) != 2 ||
		card.SupportedInterfaces[0].URL != gatewayTestURL+HTTPPathPrefix+gatewayTestAgent ||
		card.SupportedInterfaces[0].ProtocolBinding != a2atype.TransportProtocolJSONRPC ||
		card.SupportedInterfaces[1].URL != gatewayTestURL ||
		card.SupportedInterfaces[1].ProtocolBinding != a2atype.TransportProtocolGRPC {
		t.Fatalf("public interfaces = %#v", card.SupportedInterfaces)
	}
	if !card.Capabilities.Streaming || !card.Capabilities.ExtendedAgentCard || card.Capabilities.PushNotifications {
		t.Fatalf("gateway capabilities = %#v", card.Capabilities)
	}
	// Transport and streaming are the gateway's to state, but extensions describe
	// what the runtime can negotiate. Replacing the whole struct used to drop them,
	// which left a client no way to discover that an agent's question is answerable
	// while the card still looked complete.
	if len(card.Capabilities.Extensions) != 1 || card.Capabilities.Extensions[0].URI != "https://kagent.dev/extensions/hitl/v1" {
		t.Fatalf("runtime extensions = %#v, want the runtime's own preserved", card.Capabilities.Extensions)
	}
	if authorizer.verb != auth.VerbGet || dialer.session != nil {
		t.Fatalf("authorization verb = %q, runtime dialed = %v", authorizer.verb, dialer.session != nil)
	}
}

type gatewayDenyAuthorizer struct{ called bool }

func (a *gatewayDenyAuthorizer) Check(context.Context, auth.Principal, auth.Verb, auth.Resource) error {
	a.called = true
	return errors.New("denied")
}

/*
 * Share links over a Session.
 *
 * The session *is* the conversation, so sharing one is sharing what was said. Two
 * things have to hold, and neither is implied by the other:
 *
 *   - the share is authority over its own session, and the record is read as the
 *     *owner* — a session is scoped to its creator, so reading it as the visitor
 *     finds nothing and the link would 404;
 *   - the share is authority over nothing else, so a token for one session cannot
 *     open another.
 */
func TestGatewayHonoursSessionShare(t *testing.T) {
	session := gatewayTestSession()
	store := &gatewayTestStore{session: session}
	authorizer := &gatewayDenyAuthorizer{}
	runtime := &gatewayTestRuntime{}
	gateway := newTestGateway(store, authorizer, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)

	ctx := auth.AuthSessionTo(t.Context(), gatewayTestAuthSession{})
	ctx = auth.ShareContextTo(ctx, &auth.ShareContext{
		Token:     "share",
		UserID:    "the-owner",
		SessionID: session.GetId(),
		ReadOnly:  true,
	})
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs())

	if _, err := gateway.ListTasks(ctx, &a2atype.ListTasksRequest{Tenant: gatewayTestAgent, ContextID: gatewayTestID}); err != nil {
		t.Fatalf("ListTasks() with a share for this session = %v", err)
	}
	// Read as the owner: the visitor is somebody else, and a session is scoped to
	// its creator.
	if store.userID != "the-owner" {
		t.Fatalf("session read as %q, want the share's owner", store.userID)
	}
	if authorizer.called {
		t.Fatal("the ordinary authorization check should be skipped for a matching share")
	}
}

func TestGatewayRefusesAShareForADifferentSession(t *testing.T) {
	session := gatewayTestSession()
	store := &gatewayTestStore{session: session}
	authorizer := &gatewayDenyAuthorizer{}
	gateway := newTestGateway(store, authorizer, &gatewayTestDialer{}, gatewayTestURL)

	ctx := auth.AuthSessionTo(t.Context(), gatewayTestAuthSession{})
	// A perfectly valid share — for something else.
	ctx = auth.ShareContextTo(ctx, &auth.ShareContext{
		Token:     "share",
		UserID:    "the-owner",
		SessionID: "00000000-0000-0000-0000-000000000000",
	})
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs())

	if _, err := gateway.ListTasks(ctx, &a2atype.ListTasksRequest{Tenant: gatewayTestAgent, ContextID: gatewayTestID}); err == nil {
		t.Fatal("a share for another session opened this one")
	}
	if !authorizer.called {
		t.Fatal("a non-matching share must fall through to the ordinary check")
	}
}

// TestGatewayRefusesButPreservesAParkedTurn is the reproduced defect and the
// decision about it. A turn that ends INPUT_REQUIRED holds the session's single
// active-task slot, so every later send was refused as "already has an active
// task" — which reads as a broken agent. But that turn is a *valid pending
// question* (`ask_user` is a long-running call), so the send must be refused with
// a reason the reader can act on, and the question must survive: only the reader
// may give it up.

func TestRuntimeAgentCardAfterBinaryRoundTrip(t *testing.T) {
	for _, description := range []string{"", "description"} {
		t.Run(description, func(t *testing.T) {
			source := &a2apb.AgentCard{Name: "assistant", Description: description, Version: "v1",
				SupportedInterfaces: []*a2apb.AgentInterface{{Url: "http://runtime", ProtocolBinding: "GRPC", ProtocolVersion: "1.0"}},
				Capabilities:        &a2apb.AgentCapabilities{}, DefaultInputModes: []string{"text"}, DefaultOutputModes: []string{"text"}, Skills: []*a2apb.AgentSkill{},
			}
			data, err := proto.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			stored := &a2apb.AgentCard{}
			if err := proto.Unmarshal(data, stored); err != nil {
				t.Fatal(err)
			}
			card, err := apia2a.FromProtoAgentCard(stored)
			if err != nil {
				t.Fatal(err)
			}
			if card.Description != description || card.Skills == nil || len(card.Skills) != 0 {
				t.Fatalf("card = %#v", card)
			}
			encoded, err := json.Marshal(card)
			if err != nil {
				t.Fatal(err)
			}
			var response a2atype.AgentCard
			if err := json.Unmarshal(encoded, &response); err != nil {
				t.Fatal(err)
			}
			if response.Name != source.Name || response.Description != description {
				t.Fatalf("protocol response = %s", encoded)
			}
			if stored.Skills != nil || stored.Description != description {
				t.Fatal("conversion changed persisted card")
			}
		})
	}
}

func TestGatewayContextMatchesSessionAndTask(t *testing.T) {
	session := gatewayTestSession()
	store := &gatewayTestStore{session: session, tasks: []*a2atype.Task{{ID: "task", ContextID: session.Id}}}
	runtime := &gatewayTestRuntime{}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, "")
	for _, contextID := range []string{"", session.Id} {
		listed, err := gateway.ListTasks(gatewayTestContext(), &a2atype.ListTasksRequest{ContextID: contextID})
		require.NoError(t, err)
		require.Len(t, listed.Tasks, 1)
	}
	request := gatewayTestRequest()
	request.Message.TaskID = "task"
	request.Message.ContextID = uuid.NewString()
	_, err := gateway.SendMessage(gatewayTestContext(), request)
	require.ErrorIs(t, err, a2atype.ErrInvalidRequest)
	request.Message.ContextID = ""
	prepared, err := gateway.SendMessage(gatewayTestContext(), request)
	require.NoError(t, err)
	require.Equal(t, session.Id, prepared.(*a2atype.Task).ContextID)
	require.Equal(t, session.Id, request.Message.ContextID)
}

func TestGatewayObservesPublicationAfterAnotherContinuation(t *testing.T) {
	session := gatewayTestSession()
	current := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
	runtime := &gatewayTestRuntime{streamState: a2atype.TaskStateInputRequired}
	reads := 0
	store := &gatewayTestStore{session: session, task: current, settledRead: func() error {
		if !runtime.destroyed {
			t.Fatal("publication polling retained the runtime observer connection")
		}
		reads++
		if reads == 1 {
			return database.ErrConflict
		}
		// The waiting boundary was published and another client continued it.
		// The original observer must not wait for INPUT_REQUIRED to recur.
		return nil
	}}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)
	request := gatewayTestRequest()
	request.Message.TaskID = current.ID
	request.Config = &a2atype.SendMessageConfig{HistoryLength: new(0)}
	ctx, cancel := context.WithTimeout(gatewayTestContext(), time.Second)
	defer cancel()
	var events int
	for event, err := range gateway.SendStreamingMessage(ctx, request) {
		if err != nil || event != current {
			t.Fatalf("published task = %#v, error = %v", event, err)
		}
		events++
	}
	if events != 1 || reads != 2 || store.historyLength == nil || *store.historyLength != 0 {
		t.Fatalf("events = %d, publication reads = %d, history length = %v", events, reads, store.historyLength)
	}
}

func TestGatewayRecoversUnaryResponseLostDuringFinalization(t *testing.T) {
	for _, failure := range []struct {
		name    string
		err     error
		recover bool
	}{
		{"connection lost", errors.New("runtime connection closed during snapshot"), true},
		// The A2A gRPC transport converts Substrate proxy failures to InternalError.
		{"proxy error", a2atype.NewError(a2atype.ErrInternalError, "upstream call failed: broken pipe"), true},
		{"invalid input", a2atype.ErrInvalidParams, false},
		{"permission denied", a2atype.ErrUnauthorized, false},
	} {
		t.Run(failure.name, func(t *testing.T) {
			for _, state := range []a2atype.TaskState{a2atype.TaskStateCompleted, a2atype.TaskStateInputRequired, a2atype.TaskStateWorking} {
				t.Run(string(state), func(t *testing.T) {
					session := gatewayTestSession()
					current := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: state}}
					lost := failure.err
					store := &gatewayTestStore{session: session, task: current}
					runtime := &gatewayTestRuntime{onSend: func() error {
						store.replay = current
						return lost
					}}
					reads := 0
					store.settledRead = func() error {
						if !runtime.destroyed {
							t.Fatal("recovery retained the runtime connection")
						}
						reads++
						if state != a2atype.TaskStateWorking && reads == 1 {
							return database.ErrConflict
						}
						return nil
					}
					gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)
					result, err := gateway.SendMessage(gatewayTestContext(), gatewayTestRequest())
					if failure.recover && state != a2atype.TaskStateWorking {
						if err != nil || result != current || reads != 2 {
							t.Fatalf("recovery = %v, %v; reads = %d", result, err, reads)
						}
					} else if !errors.Is(err, lost) || result != nil {
						t.Fatalf("incomplete task became successful response: %v, %v", result, err)
					}
				})
			}
		})
	}
}

func TestGatewayReportsOnlyRevokedAttemptsAsNotAccepted(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		store := &gatewayTestStore{session: gatewayTestSession(), revoked: revoked, taskErr: database.ErrNotFound}
		failure := a2atype.NewError(a2atype.ErrInternalError, "connection lost before response")
		runtime := &gatewayTestRuntime{onSend: func() error { return failure }}
		gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)
		_, err := gateway.SendMessage(gatewayTestContext(), gatewayTestRequest())
		var protocolError *a2atype.Error
		if !errors.As(err, &protocolError) {
			t.Fatalf("unexpected error: %v", err)
		}
		if revoked {
			meta := protocolError.ErrorInfo().Value["metadata"].(map[string]string)
			if meta["reason"] != "KAGENT_SEND_NOT_ACCEPTED" {
				t.Fatalf("missing retry contract: %v", protocolError)
			}
		} else if !errors.Is(err, a2atype.ErrInternalError) {
			t.Fatalf("ambiguous send became retryable: %v", err)
		}
		if runtime.sendCalls != 1 {
			t.Fatalf("retried a transport failure: %d sends", runtime.sendCalls)
		}
	}
}

func TestGatewayDoesNotRecoverAnUnacceptedInput(t *testing.T) {
	session := gatewayTestSession()
	// An earlier completed task cannot stand in for an input the runtime never saved.
	current := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}
	lost := a2atype.NewError(a2atype.ErrInternalError, "upstream call failed: broken pipe")
	store := &gatewayTestStore{session: session, task: current}
	runtime := &gatewayTestRuntime{onSend: func() error { return lost }}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)
	result, err := gateway.SendMessage(gatewayTestContext(), gatewayTestRequest())
	if result != nil || !errors.Is(err, lost) {
		t.Fatalf("unaccepted input recovered an unrelated task: %v, %v", result, err)
	}
}

func TestGatewayRecoversCompletionDuringSubscriptionAttach(t *testing.T) {
	session := gatewayTestSession()
	current := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
	runtime := &gatewayTestRuntime{subscribeErr: a2atype.ErrTaskNotFound}
	store := &gatewayTestStore{session: session, task: current, settledRead: func() error {
		current.Status.State = a2atype.TaskStateCompleted
		return nil
	}}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)
	var events int
	for event, err := range gateway.SubscribeToTask(gatewayTestContext(), &a2atype.SubscribeToTaskRequest{ID: current.ID}) {
		if err != nil || event != current || current.Status.State != a2atype.TaskStateCompleted {
			t.Fatalf("recovered task = %#v, error = %v", event, err)
		}
		events++
	}
	if events != 1 || runtime.subscribeCalls != 1 || !runtime.destroyed {
		t.Fatalf("events = %d, runtime = %+v", events, runtime)
	}
}

func TestGatewayRecoversLostCancelOnlyAfterTerminalState(t *testing.T) {
	for _, state := range []a2atype.TaskState{a2atype.TaskStateCanceled, a2atype.TaskStateInputRequired} {
		t.Run(string(state), func(t *testing.T) {
			session := gatewayTestSession()
			current := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
			lost := errors.New("runtime connection closed during snapshot")
			runtime := &gatewayTestRuntime{cancelErr: lost}
			store := &gatewayTestStore{session: session, task: current, settledRead: func() error {
				current.Status.State = state
				return nil
			}}
			gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, gatewayTestURL)
			result, err := gateway.CancelTask(gatewayTestContext(), &a2atype.CancelTaskRequest{ID: current.ID})
			if state.Terminal() {
				if err != nil || result != current {
					t.Fatalf("cancel recovery = %v, %v", result, err)
				}
			} else if !errors.Is(err, lost) || result != nil {
				t.Fatalf("uncanceled task became successful cancel response: %v, %v", result, err)
			}
		})
	}
}

func (s *gatewayTestStore) GetSessionTaskByMessage(context.Context, string, string, string) (*a2atype.Task, error) {
	if s.replay != nil {
		return s.replay, nil
	}
	return nil, database.ErrNotFound
}

/*
 * A maintenance closure refuses only the subscription that would open a new
 * runtime connection. Reads and quiescent-task subscriptions keep serving the
 * stored snapshot; a cancel that must reach the runtime to settle accepted work
 * still does. The barrier is a durable admission read, never a dial-time gate.
 */

// TestSubscribeRefusedWhileAdmissionsClosed proves a live (non-quiescent) task's
// subscription is refused with the maintenance signal and never dials.
func TestSubscribeRefusedWhileAdmissionsClosed(t *testing.T) {
	session := gatewayTestSession()
	live := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
	runtime := &gatewayTestRuntime{}
	dialer := &gatewayTestDialer{client: gatewayTestClient(t, runtime)}
	store := &gatewayTestStore{session: session, task: live, admissionsClosed: true}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, dialer, gatewayTestURL)

	var sawErr error
	for _, err := range gateway.SubscribeToTask(gatewayTestContext(), &a2atype.SubscribeToTaskRequest{ID: live.ID}) {
		sawErr = err
	}
	if sawErr == nil || a2atype.ErrorReason(sawErr) != a2atype.ErrorReason(a2atype.ErrUnsupportedOperation) {
		t.Fatalf("closed-admission subscribe error = %v, want UnsupportedOperation maintenance signal", sawErr)
	}
	if dialer.session != nil || runtime.subscribeCalls != 0 {
		t.Fatal("closed admission dialed the runtime or opened a stream")
	}
}

// TestQuiescentSubscribeReadsStoreWhileClosed proves a quiescent task serves its
// stored snapshot during closure with no barrier dependence and no dial.
func TestQuiescentSubscribeReadsStoreWhileClosed(t *testing.T) {
	session := gatewayTestSession()
	done := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}
	dialer := &gatewayTestDialer{}
	store := &gatewayTestStore{session: session, task: done, admissionsClosed: true}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, dialer, gatewayTestURL)

	var got a2atype.Event
	for event, err := range gateway.SubscribeToTask(gatewayTestContext(), &a2atype.SubscribeToTaskRequest{ID: done.ID}) {
		if err != nil {
			t.Fatalf("quiescent subscribe during closure = %v", err)
		}
		got = event
	}
	if got != done {
		t.Fatalf("quiescent subscribe returned %#v, want the stored snapshot", got)
	}
	if dialer.session != nil {
		t.Fatal("a stored snapshot subscription dialed the runtime")
	}
}

// TestCancelStillSettlesWhileAdmissionsClosed proves closure does not block the
// authorized cancel control path: it may dial to settle accepted work.
func TestCancelStillSettlesWhileAdmissionsClosed(t *testing.T) {
	session := gatewayTestSession()
	working := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
	canceled := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateCanceled}}
	runtime := &gatewayTestRuntime{task: canceled}
	dialer := &gatewayTestDialer{client: gatewayTestClient(t, runtime)}
	// The runtime reports canceled; the durable store settles to the same state
	// before the gateway re-reads it as the cancel result.
	store := &gatewayTestStore{session: session, task: working, admissionsClosed: true, settledRead: func() error {
		working.Status.State = a2atype.TaskStateCanceled
		return nil
	}}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, dialer, gatewayTestURL)

	result, err := gateway.CancelTask(gatewayTestContext(), &a2atype.CancelTaskRequest{ID: working.ID})
	if err != nil || result == nil || result.Status.State != a2atype.TaskStateCanceled {
		t.Fatalf("cancel during closure = %v, %v; the settle path must stay open", result, err)
	}
	if dialer.session == nil {
		t.Fatal("cancel needed the runtime to settle but never dialed")
	}
}

// TestEstablishedStreamSurvivesClosure proves closing admissions does not cut a
// stream that was already admitted: the gateway keeps pulling runtime events.
func TestEstablishedStreamSurvivesClosure(t *testing.T) {
	session := gatewayTestSession()
	live := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
	// working -> (barrier closes) -> completed. The stream was admitted while
	// open, so it must deliver the terminal event without a fresh admission
	// check or a second dial even though admissions are now closed.
	firstEventSeen := make(chan struct{})
	resume := make(chan struct{})
	runtime := &gatewayTestRuntime{subscribeSeq: func(yield func(a2atype.Event, error) bool) {
		// First event is non-terminal: establishes the live stream.
		if !yield(&a2atype.Task{ID: "task", ContextID: session.ContextId,
			Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}, nil) {
			return
		}
		<-resume // released only after the test has set admissionsClosed = true
		yield(&a2atype.Task{ID: "task", ContextID: session.ContextId,
			Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}, nil)
	}}
	dialer := &gatewayTestDialer{client: gatewayTestClient(t, runtime)}
	store := &gatewayTestStore{session: session, task: live, settledRead: func() error {
		live.Status.State = a2atype.TaskStateCompleted // terminal for the re-read
		return nil
	}}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, dialer, gatewayTestURL)

	// Consume the iterator on a goroutine so the test can close admissions
	// between the first (working) and second (terminal) runtime events. The
	// consumer signals firstEventSeen (which the main test waits on) but must
	// NOT release the stream's resume gate — that stays closed until the main
	// test has flipped the barrier, so the terminal event cannot race ahead.
	type result struct {
		event a2atype.Event
		err   error
	}
	var events []result
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		for event, err := range gateway.SubscribeToTask(gatewayTestContext(), &a2atype.SubscribeToTaskRequest{ID: live.ID}) {
			events = append(events, result{event, err})
			if len(events) == 1 {
				close(firstEventSeen)
			}
		}
	}()
	// Wait for the first event to be observed, then close admissions while the
	// stream is still open, then let it emit its terminal event.
	select {
	case <-firstEventSeen:
	case <-time.After(5 * time.Second):
		close(resume) // unblock the stream so cleanup can finish
		t.Fatal("stream did not emit its first event")
	}
	store.admissionsClosed = true
	close(resume)
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not finish after the barrier closed")
	}

	if len(events) != 2 {
		t.Fatalf("established stream delivered %d events across closure, want 2", len(events))
	}
	for i, ev := range events {
		if ev.err != nil {
			t.Fatalf("event %d errored after closure: %v", i, ev.err)
		}
	}
	if runtime.subscribeCalls != 1 {
		t.Fatalf("subscribe re-dialed/re-admitted after closure: calls = %d", runtime.subscribeCalls)
	}
}

// TestSubscribeAdmissionFailureDoesNotDial proves a barrier-read failure is
// propagated rather than treated as "open" — it must not permit a new runtime
// connection.
func TestSubscribeAdmissionFailureDoesNotDial(t *testing.T) {
	session := gatewayTestSession()
	live := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
	dialer := &gatewayTestDialer{}
	store := &gatewayTestStore{session: session, task: live, admissionsErr: errors.New("barrier read failed")}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, dialer, gatewayTestURL)

	var sawErr error
	for _, err := range gateway.SubscribeToTask(gatewayTestContext(), &a2atype.SubscribeToTaskRequest{ID: live.ID}) {
		sawErr = err
	}
	if sawErr == nil {
		t.Fatal("a barrier-read failure was treated as open admission")
	}
	if dialer.session != nil {
		t.Fatal("a barrier-read failure dialed the runtime")
	}
}

// TestSubscribeReopenRestoresLiveSubscription proves a subscribe is refused while
// closed and then admitted once the barrier reopens on the same store.
func TestSubscribeReopenRestoresLiveSubscription(t *testing.T) {
	session := gatewayTestSession()
	live := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}
	done := &a2atype.Task{ID: "task", ContextID: session.ContextId, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}
	runtime := &gatewayTestRuntime{subscribeEvent: done}
	dialer := &gatewayTestDialer{client: gatewayTestClient(t, runtime)}
	store := &gatewayTestStore{session: session, task: live, admissionsClosed: true, settledRead: func() error {
		live.Status.State = a2atype.TaskStateCompleted
		return nil
	}}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, dialer, gatewayTestURL)

	// Closed: refused before any dial.
	var sawErr error
	for _, err := range gateway.SubscribeToTask(gatewayTestContext(), &a2atype.SubscribeToTaskRequest{ID: live.ID}) {
		sawErr = err
	}
	if sawErr == nil || dialer.session != nil {
		t.Fatalf("closed subscribe = %v dialed=%v, want refusal with no dial", sawErr, dialer.session != nil)
	}

	// Reopen the same store: a new subscription now dials and delivers.
	store.admissionsClosed = false
	dialer.session = nil
	var got a2atype.Event
	for event, err := range gateway.SubscribeToTask(gatewayTestContext(), &a2atype.SubscribeToTaskRequest{ID: live.ID}) {
		if err != nil {
			t.Fatalf("reopened subscribe = %v", err)
		}
		got = event
	}
	if got == nil || dialer.session == nil {
		t.Fatalf("reopened subscribe = %#v dialed=%v", got, dialer.session != nil)
	}
}

func TestQuiescentTaskStates(t *testing.T) {
	for _, state := range []a2atype.TaskState{
		a2atype.TaskStateCompleted,
		a2atype.TaskStateCanceled,
		a2atype.TaskStateFailed,
		a2atype.TaskStateRejected,
		a2atype.TaskStateInputRequired,
		a2atype.TaskStateAuthRequired,
	} {
		if !isQuiescent(state) {
			t.Errorf("isQuiescent(%s) = false", state)
		}
	}
	if isQuiescent(a2atype.TaskStateWorking) {
		t.Error("working task is quiescent")
	}
}

// A denying authorizer, so "the share is what let this through" is provable rather
// than merely consistent with the result.

// A successful send/cancel may need persisted results after its actor connection
// closes. That recovery must not silently add Get or Update permission to a send.
type gatewayOperationAuthorizer struct{ verb auth.Verb }

func (a gatewayOperationAuthorizer) Check(_ context.Context, _ auth.Principal, verb auth.Verb, _ auth.Resource) error {
	if verb != a.verb {
		return errors.New("operation not permitted")
	}
	return nil
}

func TestGatewayRecoveryUsesOriginalOperationPermission(t *testing.T) {
	for _, operation := range []string{"send", "stream", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			task := &a2atype.Task{ID: "task", ContextID: gatewayTestID, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}
			store := &gatewayTestStore{session: gatewayTestSession(), task: task, replay: task}
			runtime := &gatewayTestRuntime{task: task}
			verb := auth.VerbCreate
			if operation == "cancel" {
				verb = auth.VerbUpdate
				task.Status.State = a2atype.TaskStateWorking
				runtime.cancelErr = a2atype.ErrInternalError
				store.settledRead = func() error { task.Status.State = a2atype.TaskStateCanceled; return nil }
			} else {
				runtime.onSend = func() error { return a2atype.ErrInternalError }
			}
			gateway := newTestGateway(store, gatewayOperationAuthorizer{verb}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, "")
			ctx := gatewayTestContext()
			switch operation {
			case "send":
				result, err := gateway.SendMessage(ctx, gatewayTestRequest())
				require.NoError(t, err)
				require.Equal(t, task, result)
			case "stream":
				count := 0
				for event, err := range gateway.SendStreamingMessage(ctx, gatewayTestRequest()) {
					require.NoError(t, err)
					require.Equal(t, task, event)
					count++
				}
				require.Equal(t, 1, count)
			case "cancel":
				result, err := gateway.CancelTask(ctx, &a2atype.CancelTaskRequest{ID: task.ID})
				require.NoError(t, err)
				require.Equal(t, a2atype.TaskStateCanceled, result.Status.State)
			}
			require.True(t, runtime.destroyed)
		})
	}
}

func TestGatewayReleasesDispatchAfterCallerDisconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(gatewayTestContext())
	defer cancel()
	store := &gatewayTestStore{session: gatewayTestSession()}
	runtime := &gatewayTestRuntime{onSend: func() error { cancel(); return context.Canceled }}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, &gatewayTestDialer{client: gatewayTestClient(t, runtime)}, "")
	_, err := gateway.SendMessage(ctx, gatewayTestRequest())
	require.Error(t, err)
	require.Positive(t, store.revokeCalls)
	require.NoError(t, store.revokeContextErr)
}

func TestGatewayUnusedStreamDoesNotReserveDispatch(t *testing.T) {
	store := &gatewayTestStore{session: gatewayTestSession()}
	dialer := &gatewayTestDialer{}
	gateway := newTestGateway(store, &gatewayTestAuthorizer{}, dialer, "")
	_ = gateway.SendStreamingMessage(gatewayTestContext(), gatewayTestRequest())
	require.Zero(t, store.reserveCalls)
	require.Nil(t, dialer.session)
}
