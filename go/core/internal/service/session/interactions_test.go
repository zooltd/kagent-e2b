package session

import (
	"context"
	"errors"
	"testing"
	"time"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/google/uuid"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Only task reads are implemented. Any dispatch or runtime access in these
// permission tests fails immediately through the nil dependencies.
type interactionTestStore struct {
	*database.Client
	task             *a2atype.Task
	taskLookups      int
	taskReads        int
	reserveErr       error
	admissionsClosed bool
	admissionsErr    error
	admissionReads   int
}

// CheckAdmissionsOpen reports the test's barrier state; the embedded nil
// *database.Client would panic if reached.
func (s *interactionTestStore) CheckAdmissionsOpen(context.Context) error {
	s.admissionReads++
	if s.admissionsErr != nil {
		return s.admissionsErr
	}
	if s.admissionsClosed {
		return database.ErrAdmissionClosed
	}
	return nil
}

func (s *interactionTestStore) SessionForTask(context.Context, string) (string, error) {
	s.taskLookups++
	return s.task.ContextID, nil
}

func (s *interactionTestStore) ReserveSessionDispatch(context.Context, string, uuid.UUID, string) error {
	return s.reserveErr
}

func (s *interactionTestStore) GetSessionTask(context.Context, string, string, *int) (*a2atype.Task, error) {
	s.taskReads++
	return s.task, nil
}

func (s *interactionTestStore) ListAgentTasks(context.Context, []string, string, a2atype.TaskState, *time.Time, int, *int) ([]*a2atype.Task, int, error) {
	s.taskReads++
	return []*a2atype.Task{s.task}, 1, nil
}

func (s *interactionTestStore) GetSettledSessionTask(context.Context, string, string, *int) (*a2atype.Task, error) {
	s.taskReads++
	return s.task, nil
}

func (s *interactionTestStore) GetSessionTaskByMessage(context.Context, string, string, string) (*a2atype.Task, error) {
	s.taskReads++
	return s.task, nil
}

func (s *interactionTestStore) GetTaskPushConfig(context.Context, string, string, string) (*database.TaskPushConfig, error) {
	s.taskReads++
	return &database.TaskPushConfig{ID: "callback", URL: "https://receiver.example/callback"}, nil
}

func (s *interactionTestStore) ListTaskPushConfigs(context.Context, string, string, string, int) ([]database.TaskPushConfig, error) {
	s.taskReads++
	return []database.TaskPushConfig{{ID: "callback", URL: "https://receiver.example/callback"}}, nil
}

func TestTaskPageTokenRejectsInvalidUTF8(t *testing.T) {
	_, err := decodeTaskPageToken("invalid")
	if err == nil {
		t.Fatal("invalid UTF-8 page token was accepted")
	}
}

func TestInteractionsEnforcePermissionsWithoutGateway(t *testing.T) {
	id := uuid.NewString()
	agent := types.NamespacedName{Namespace: "team-a", Name: "assistant"}
	for _, operation := range []struct {
		name string
		verb auth.Verb
		call func(context.Context, *InteractionService, types.NamespacedName) error
	}{
		{name: "settled task", verb: auth.VerbGet, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.GetSettledTask(ctx, agent, id, "task", nil)
			return err
		}},
		{name: "send result", verb: auth.VerbCreate, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.GetSendResult(ctx, agent, &a2atype.Message{ID: "input", ContextID: id}, "task", nil)
			return err
		}},
		{name: "cancel result", verb: auth.VerbUpdate, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.GetCancelResult(ctx, agent, "task")
			return err
		}},
		{name: "accepted message", verb: auth.VerbCreate, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.GetTaskByMessage(ctx, agent, &a2atype.Message{ID: "input", ContextID: id})
			return err
		}},
		{name: "revoke send", verb: auth.VerbCreate, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.RevokeSend(ctx, agent, &a2atype.Message{ID: "input", ContextID: id}, uuid.New())
			return err
		}},
		{name: "get", verb: auth.VerbGet, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.GetTask(ctx, agent, &a2atype.GetTaskRequest{ID: "task"})
			return err
		}},
		{name: "list", verb: auth.VerbGet, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.ListTasks(ctx, agent, &a2atype.ListTasksRequest{ContextID: id})
			return err
		}},
		{name: "push create", verb: auth.VerbCreate, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.CreateTaskPushConfig(ctx, agent, &a2atype.PushConfig{TaskID: "task", ID: "callback", URL: "https://receiver.example/callback", Auth: &a2atype.PushAuthInfo{Scheme: "Bearer"}})
			return err
		}},
		{name: "push get", verb: auth.VerbGet, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.GetTaskPushConfig(ctx, agent, &a2atype.GetTaskPushConfigRequest{TaskID: "task", ID: "callback"})
			return err
		}},
		{name: "push list", verb: auth.VerbGet, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.ListTaskPushConfigs(ctx, agent, &a2atype.ListTaskPushConfigRequest{TaskID: "task"})
			return err
		}},
		{name: "push delete", verb: auth.VerbDelete, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			return s.DeleteTaskPushConfig(ctx, agent, &a2atype.DeleteTaskPushConfigRequest{TaskID: "task", ID: "callback"})
		}},
		{name: "subscribe", verb: auth.VerbGet, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, _, err := s.PrepareTaskSubscription(ctx, agent, &a2atype.SubscribeToTaskRequest{ID: "task"})
			return err
		}},
		{name: "cancel", verb: auth.VerbUpdate, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, _, err := s.PrepareCancelTask(ctx, agent, &a2atype.CancelTaskRequest{ID: "task"})
			return err
		}},
		{name: "send", verb: auth.VerbCreate, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.PrepareSend(ctx, agent, &a2atype.SendMessageRequest{Message: &a2atype.Message{ID: "input", ContextID: id}})
			return err
		}},
		{name: "reply", verb: auth.VerbUpdate, call: func(ctx context.Context, s *InteractionService, agent types.NamespacedName) error {
			_, err := s.PrepareSend(ctx, agent, &a2atype.SendMessageRequest{Message: &a2atype.Message{ID: "input", TaskID: "task"}})
			return err
		}},
	} {
		for _, test := range []struct {
			name  string
			ctx   context.Context
			agent types.NamespacedName
			want  error
		}{
			{name: "unauthenticated", ctx: context.Background(), agent: agent, want: a2atype.ErrUnauthenticated},
			{name: "missing Agent", ctx: serviceTestContext("visitor"), want: a2atype.ErrInvalidRequest},
			{name: "denied caller", ctx: serviceTestContext("visitor"), agent: agent, want: a2atype.ErrUnauthorized},
			{name: "read-only share", ctx: auth.ShareContextTo(serviceTestContext("visitor"), &auth.ShareContext{SessionID: id, UserID: "owner", ReadOnly: true}), agent: agent},
			{name: "other Agent", ctx: auth.ShareContextTo(serviceTestContext("visitor"), &auth.ShareContext{SessionID: id, UserID: "owner"}), agent: types.NamespacedName{Namespace: "team-a", Name: "other"}, want: a2atype.ErrUnauthorized},
		} {
			t.Run(operation.name+"/"+test.name, func(t *testing.T) {
				stored := &apiv1alpha1.Session{Id: id, ContextId: id, State: apiv1alpha1.RuntimeState_RUNTIME_STATE_SUSPENDED,
					Agent: &apiv1alpha1.ResourceReference{Namespace: agent.Namespace, Name: agent.Name}}
				sessionStore := &serviceTestStore{getResult: stored}
				authorizer := &recordingAuthorizer{denied: map[string]bool{id: true}}
				tasks := &interactionTestStore{task: &a2atype.Task{ID: "task", ContextID: id, Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}}
				service := NewInteractionService(tasks, nil, NewService(sessionStore, authorizer, nil))
				want := test.want
				if test.name == "read-only share" && operation.verb != auth.VerbGet {
					want = a2atype.ErrUnauthorized
				}
				// No A2A SDK tenant or interceptor is installed for this call.
				err := operation.call(test.ctx, service, test.agent)
				if !errors.Is(err, want) {
					t.Fatalf("operation error = %v, want %v", err, want)
				}
				if want != nil && tasks.taskReads != 0 {
					t.Fatal("unauthorized operation read task content")
				}
				if want == nil && (tasks.taskReads != 1 || sessionStore.getCreator != "owner") {
					t.Fatal("shared history was not read through its owner")
				}
				if test.name == "unauthenticated" && tasks.taskLookups != 0 {
					t.Fatal("unauthenticated operation looked up a task")
				}
				if test.name == "denied caller" && (authorizer.verb != operation.verb || authorizer.resource.Type != "Session" || authorizer.resource.Name != id) {
					t.Fatalf("operation used the wrong permission: %+v", authorizer)
				}
			})
		}
	}
}

func TestInteractionsRequireAgentForUnfilteredTaskList(t *testing.T) {
	service := NewInteractionService(nil, nil, nil)
	for _, agent := range []types.NamespacedName{
		{},
		{Namespace: "team-a"},
		{Name: "assistant"},
		{Namespace: "team-a", Name: "INVALID"},
	} {
		_, err := service.ListTasks(serviceTestContext("alice"), agent, nil)
		if !errors.Is(err, a2atype.ErrInvalidRequest) {
			t.Fatalf("ListTasks(%v) = %v, want invalid Agent before storage", agent, err)
		}
	}
}

// admissionClosedAgentService resolves any Agent reference so a send reaches
// session creation rather than failing at Agent lookup.
type admissionClosedAgentService struct{ err error }

func (s admissionClosedAgentService) Get(_ context.Context, ref types.NamespacedName) (*v1alpha3.Agent, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &v1alpha3.Agent{ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name}}, nil
}

// TestNewConversationSendPreservesAdmissionClosed proves a send with no
// TaskID/ContextID — which resolves its session through sessions.Create — keeps
// the specific maintenance rejection instead of collapsing into ErrInternalError.
// A closed barrier returns ErrUnavailable-wrapped ErrAdmissionClosed from
// Create; interactionError must surface it as retryable UnsupportedOperation.
// Ordinary and streaming sends share resolveSend/PrepareSend, so one call covers
// the new-conversation branch both transports take.
func TestNewConversationSendPreservesAdmissionClosed(t *testing.T) {
	agent := types.NamespacedName{Namespace: "team-a", Name: "assistant"}
	sessionStore := &serviceTestStore{createErr: database.ErrAdmissionClosed}
	sessions := NewService(sessionStore, serviceTestAuthorizer{}, serviceTestWorkflow{})
	service := NewInteractionService(&interactionTestStore{}, admissionClosedAgentService{}, sessions)
	// A request without a task or context ID is the new-conversation branch that
	// calls sessions.Create.
	_, err := service.PrepareSend(serviceTestContext("alice"), agent,
		&a2atype.SendMessageRequest{Message: &a2atype.Message{ID: "first", Role: a2atype.MessageRoleUser}})
	require.Error(t, err)
	assert.ErrorIs(t, err, a2atype.ErrUnsupportedOperation)
	assert.ErrorContains(t, err, "admission is closed for maintenance",
		"new-conversation send must surface the maintenance signal, not internal error")
}

// TestExistingConversationSendPreservesAdmissionClosed is the complement: an
// existing-context send already reached the maintenance message through
// interactionStoreError; confirm both paths report the same rejection.
func TestExistingConversationSendPreservesAdmissionClosed(t *testing.T) {
	agent := types.NamespacedName{Namespace: "team-a", Name: "assistant"}
	ctxID := uuid.NewString()
	tasks := &interactionTestStore{task: &a2atype.Task{ID: "task", ContextID: ctxID}}
	tasks.reserveErr = database.ErrAdmissionClosed
	// The existing-context branch resolves the session via storedSession, which
	// requires a READY session whose ContextId and Agent match the request.
	ready := &apiv1alpha1.Session{
		Id: ctxID, ContextId: ctxID,
		State:     apiv1alpha1.RuntimeState_RUNTIME_STATE_READY,
		Operation: apiv1alpha1.RuntimeOperation_RUNTIME_OPERATION_NONE,
		Agent:     &apiv1alpha1.ResourceReference{Namespace: agent.Namespace, Name: agent.Name},
	}
	sessions := NewService(&serviceTestStore{getResult: ready}, serviceTestAuthorizer{}, serviceTestWorkflow{})
	service := NewInteractionService(tasks, admissionClosedAgentService{}, sessions)
	_, err := service.PrepareSend(serviceTestContext("alice"), agent,
		&a2atype.SendMessageRequest{Message: &a2atype.Message{ID: "input", ContextID: ctxID}})
	require.Error(t, err)
	assert.ErrorIs(t, err, a2atype.ErrUnsupportedOperation)
}
