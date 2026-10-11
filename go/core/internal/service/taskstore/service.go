// Package taskstore owns the private persistence API used by runtime SDKs.
// Execution stays in the runtime; public access checks stay in the public API.
package taskstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Store interface {
	SettleSessionTask(context.Context, string, string, int64, *database.ExecutionInstanceBinding) error
	GetSessionForRuntime(context.Context, string, string) (*apiv1alpha1.Session, error)
	GetSessionForExecution(context.Context, string) (*apiv1alpha1.Session, error)
	GetLiveExecutionBinding(context.Context, string) (*database.ExecutionInstanceBinding, error)
	CreateRuntimeTask(context.Context, string, []byte, *a2a.Task, string, *database.ExecutionInstanceBinding) (int64, error)
	GetVersionedSessionTask(context.Context, string, string) (*a2a.Task, int64, error)
	UpdateSessionTask(context.Context, string, int64, []byte, *a2a.Task, a2a.Event, string, *database.ExecutionInstanceBinding) (int64, error)
	ListSessionTasks(context.Context, string, string, a2a.TaskState, *time.Time, int, *int) ([]*a2a.Task, int, error)
}

var _ Store = (*database.Client)(nil)

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

// session verifies the caller's runtime authority for sessionID and returns
// the session plus the live execution binding the caller was authorized under.
// The binding is nil for the Substrate path; runtime write methods pass it back
// to the store so revocation is rechecked inside the mutation transaction.
func (s *Service) session(ctx context.Context, sessionID string) (*apiv1alpha1.Session, *database.ExecutionInstanceBinding, error) {
	authSession, _ := auth.AuthSessionFrom(ctx)
	// Dispatch on the verified identity type. Substrate's runtimeSession keeps
	// its actor UID + authority checks; a signed E2B e2bSession checks the
	// durable execution binding instead.
	if e2b, ok := authSession.(e2bSession); ok {
		return s.e2bSession(ctx, e2b, sessionID)
	}
	identity, ok := authSession.(runtimeSession)
	if !ok || identity.sessionID != sessionID {
		return nil, nil, status.Error(codes.PermissionDenied, "actor does not belong to this session")
	}
	session, err := s.store.GetSessionForRuntime(ctx, sessionID, identity.actorUID)
	if err != nil {
		return nil, nil, storageError(err)
	}
	if session.A2AAuthority != substrate.ActorHost(identity.atespace, substrate.ActorName(sessionID), "") {
		return nil, nil, status.Error(codes.PermissionDenied, "actor does not belong to this session")
	}
	return session, nil, nil
}

// e2bSession authorizes a signed E2B runtime credential against the durable
// execution binding. The credential's session, external compute and generation
// must all match the live binding; there is no Substrate actor fallback.
func (s *Service) e2bSession(ctx context.Context, identity e2bSession, sessionID string) (*apiv1alpha1.Session, *database.ExecutionInstanceBinding, error) {
	if identity.sessionID != sessionID {
		return nil, nil, status.Error(codes.PermissionDenied, "runtime does not belong to this session")
	}
	binding, err := s.store.GetLiveExecutionBinding(ctx, sessionID)
	if err != nil {
		return nil, nil, storageError(err)
	}
	if binding.Provider != database.ExecutionProviderE2B ||
		binding.Installation != identity.installation ||
		binding.ExternalID != identity.externalID ||
		binding.Generation != identity.generation {
		return nil, nil, status.Error(codes.PermissionDenied, "runtime credential does not match the bound execution instance")
	}
	session, err := s.store.GetSessionForExecution(ctx, sessionID)
	if err != nil {
		return nil, nil, storageError(err)
	}
	return session, binding, nil
}

// CreateTask persists the SDK's first task snapshot. Runtime execution and
// request serialization remain owned by the agent's SDK.
func (s *Service) CreateTask(ctx context.Context, input *apiv1alpha1.TaskStoreServiceCreateTaskRequest) (*apiv1alpha1.TaskStoreServiceCreateTaskResponse, error) {
	_, binding, err := s.session(ctx, input.SessionId)
	if err != nil {
		return nil, err
	}
	task, err := pbconv.FromProtoTask(input.Task)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(input)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	version, err := s.store.CreateRuntimeTask(ctx, input.SessionId, hash[:], task, input.GetDispatchId(), binding)
	return &apiv1alpha1.TaskStoreServiceCreateTaskResponse{Version: version}, storageError(err)
}

func (s *Service) GetTask(ctx context.Context, input *apiv1alpha1.TaskStoreServiceGetTaskRequest) (*apiv1alpha1.TaskStoreServiceGetTaskResponse, error) {
	if _, _, err := s.session(ctx, input.SessionId); err != nil {
		return nil, err
	}
	stored, err := s.getTask(ctx, input.SessionId, input.TaskId)
	return &apiv1alpha1.TaskStoreServiceGetTaskResponse{Stored: stored}, err
}

func (s *Service) getTask(ctx context.Context, sessionID, taskID string) (*apiv1alpha1.StoredTask, error) {
	task, version, err := s.store.GetVersionedSessionTask(ctx, sessionID, taskID)
	if err != nil {
		return nil, storageError(err)
	}
	wire, err := pbconv.ToProtoTask(task)
	return &apiv1alpha1.StoredTask{Task: wire, Version: version}, err
}

func (s *Service) UpdateTask(ctx context.Context, input *apiv1alpha1.TaskStoreServiceUpdateTaskRequest) (*apiv1alpha1.TaskStoreServiceUpdateTaskResponse, error) {
	_, binding, err := s.session(ctx, input.SessionId)
	if err != nil {
		return nil, err
	}
	task, err := pbconv.FromProtoTask(input.Task)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var event a2a.Event = task
	if input.Event != nil {
		event, err = pbconv.FromProtoStreamResponse(input.Event)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(input)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	version, err := s.store.UpdateSessionTask(ctx, input.SessionId, input.ExpectedVersion, hash[:], task, event, input.GetDispatchId(), binding)
	return &apiv1alpha1.TaskStoreServiceUpdateTaskResponse{Version: version}, storageError(err)
}

func (s *Service) ListTasks(ctx context.Context, input *apiv1alpha1.TaskStoreServiceListTasksRequest) (*apiv1alpha1.TaskStoreServiceListTasksResponse, error) {
	session, _, err := s.session(ctx, input.SessionId)
	if err != nil {
		return nil, err
	}
	request, err := pbconv.FromProtoListTasksRequest(input.Request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if request.ContextID != "" && request.ContextID != session.ContextId {
		return nil, status.Error(codes.InvalidArgument, "context does not match Session")
	}
	after, err := base64.RawURLEncoding.DecodeString(request.PageToken)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid page token")
	}
	limit := 50
	if request.PageSize != 0 {
		limit = request.PageSize
	}
	if limit < 1 || limit > 100 {
		return nil, status.Error(codes.InvalidArgument, "page size must be between 1 and 100")
	}
	tasks, total, err := s.store.ListSessionTasks(ctx, session.Id, string(after), request.Status, request.StatusTimestampAfter, limit+1, request.HistoryLength)
	if err != nil {
		return nil, storageError(err)
	}
	result := &a2a.ListTasksResponse{Tasks: tasks, PageSize: limit, TotalSize: total}
	if len(tasks) > limit {
		result.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(tasks[limit-1].ID))
		result.Tasks = tasks[:limit]
	}
	if !request.IncludeArtifacts {
		for _, task := range result.Tasks {
			task.Artifacts = nil
		}
	}
	wire, err := pbconv.ToProtoListTasksResponse(result)
	if err != nil {
		return nil, fmt.Errorf("encode task list: %w", err)
	}
	return &apiv1alpha1.TaskStoreServiceListTasksResponse{Result: wire}, nil
}

func (s *Service) SettleTask(ctx context.Context, input *apiv1alpha1.TaskStoreServiceSettleTaskRequest) (*apiv1alpha1.TaskStoreServiceSettleTaskResponse, error) {
	_, binding, err := s.session(ctx, input.SessionId)
	if err != nil {
		return nil, err
	}
	if err := s.store.SettleSessionTask(ctx, input.SessionId, input.TaskId, input.Version, binding); err != nil {
		return nil, storageError(err)
	}
	return &apiv1alpha1.TaskStoreServiceSettleTaskResponse{}, nil
}

func storageError(err error) error {
	switch {
	case errors.Is(err, database.ErrNotFound):
		return status.Error(codes.NotFound, "session or task does not exist")
	case errors.Is(err, database.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "task ID already exists with another creation")
	case errors.Is(err, database.ErrConflict):
		return status.Error(codes.Aborted, "task changed or the session cannot accept this update")
	case errors.Is(err, database.ErrFailedPrecondition):
		return status.Error(codes.FailedPrecondition, "task state does not allow this operation")
	default:
		return err
	}
}
