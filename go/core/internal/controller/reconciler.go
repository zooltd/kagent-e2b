package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	kagentclient "github.com/kagent-dev/kagent/go/api/clientset/versioned/typed/api/v1alpha3"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/internal/translator"
	byotranslator "github.com/kagent-dev/kagent/go/core/internal/translator/byo"
	claudetranslator "github.com/kagent-dev/kagent/go/core/internal/translator/claude"
	codextranslator "github.com/kagent-dev/kagent/go/core/internal/translator/codex"
	kagenttranslator "github.com/kagent-dev/kagent/go/core/internal/translator/kagent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
)

// AgentReconciliation is the complete desired and observed state for one
// Agent. Compilation failures are data so invalid Agents still produce
// status instead of disappearing from the graph.
type AgentReconciliation struct {
	Agent                 *kagentv1alpha3.Agent
	Target                *compiledTarget
	Warnings              []string
	CompilationFailure    *ReconciliationFailure
	ObservedActorTemplate *ateapipb.ActorTemplate
	PreparationFailure    *ReconciliationFailure
	// E2BObserved carries the E2B artifact-readiness observation once the
	// reconcile publishes it; nil until then so status stays pending.
	E2BObserved *bool
}

// compiledTarget is published only after compilation, hashing, and ActorTemplate
// construction all succeed. An absent target means there is no desired runtime.
type compiledTarget struct {
	Revision      translator.Revision
	RevisionID    translator.RevisionID
	ActorTemplate *ateapipb.ActorTemplate
	// E2B carries the resolved artifact binding and launch contract when the
	// provider is E2B; ActorTemplate is nil in that mode.
	E2B *resolvedE2BArtifact
}

func (c *compiledTarget) equals(other *compiledTarget) bool {
	if c == nil || other == nil {
		return c == other
	}
	return c.RevisionID == other.RevisionID && c.Revision.Equals(other.Revision) &&
		proto.Equal(c.ActorTemplate, other.ActorTemplate)
}

// resolvedE2BArtifact is the workload-mapping outcome a compiled E2B revision
// binds to: the installation, the external template and build, and the frozen
// launch contract the bootstrapper replays.
type resolvedE2BArtifact struct {
	Installation   string
	TemplateID     string
	BuildID        string
	LaunchContract []byte
}

func (r AgentReconciliation) ResourceName() string { return r.Agent.Namespace + "/" + r.Agent.Name }

var _ krt.Equaler[AgentReconciliation] = AgentReconciliation{}

// Equals keeps KRT from reflecting over protobuf caches that mutate during reads.
func (r AgentReconciliation) Equals(other AgentReconciliation) bool {
	if !r.Target.equals(other.Target) ||
		!proto.Equal(r.ObservedActorTemplate, other.ObservedActorTemplate) {
		return false
	}
	r.Target, other.Target = nil, nil
	r.ObservedActorTemplate, other.ObservedActorTemplate = nil, nil
	return reflect.DeepEqual(r, other)
}

func (r AgentReconciliation) desiredRevision() string {
	if r.Target == nil {
		return ""
	}
	return r.Target.RevisionID.String()
}

// ReconciliationFailure identifies the condition stage blocked by an Agent.
type ReconciliationFailure struct {
	Condition string
	Reason    string
	Message   string
	Retryable bool
}

func newAgentReconciliations(
	agents krt.Collection[*kagentv1alpha3.Agent],
	collections translator.Collections,
	agentRuntimeObservations krt.Collection[AgentRuntimeObservation],
	opts krt.OptionsBuilder,
	provider string,
	e2bInstallations map[string]e2b.E2BInstallation,
	e2bInstallation string,
) krt.Collection[AgentReconciliation] {
	return krt.NewCollection(agents, func(ctx krt.HandlerContext, agent *kagentv1alpha3.Agent) *AgentReconciliation {
		state := &AgentReconciliation{Agent: agent}
		compilation, err := translator.NewCompiler(ctx, collections, map[translator.HarnessType]translator.HarnessCompiler{
			translator.HarnessTypeKagent: kagenttranslator.NewCompiler(ctx, collections),
			translator.HarnessTypeCodex:  codextranslator.NewCompiler(ctx, collections),
			translator.HarnessTypeClaude: claudetranslator.NewCompiler(ctx, collections),
			translator.HarnessTypeBYO:    byotranslator.NewCompiler(ctx, collections),
		}, provider).CompileAgent(context.Background(), agent)
		if err != nil {
			condition, reason := kagentv1alpha3.AgentConditionResolvedRefs, "ReferenceResolutionFailed"
			var validation *translator.ValidationError
			var missingPool *translator.WorkerPoolNotFoundError
			switch {
			case errors.As(err, &validation):
				condition, reason = kagentv1alpha3.AgentConditionCompatible, "UnsupportedConfiguration"
			case errors.As(err, &missingPool):
				reason = "WorkerPoolNotFound"
			}
			state.CompilationFailure = &ReconciliationFailure{Condition: condition, Reason: reason, Message: err.Error()}
			return state
		}
		revision := &compilation.Revision
		state.Warnings = append([]string(nil), compilation.Warnings...)

		if provider == "e2b" {
			// E2B resolves the compiled workload to a prepared artifact through
			// the selected installation's workload mapping, then digests the
			// execution identity. Missing or ambiguous mappings fail here; no
			// ActorTemplate is ever constructed.
			inst, ok := e2bInstallations[e2bInstallation]
			if !ok || e2bInstallation == "" {
				state.CompilationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "E2BInstallationMissing", Message: fmt.Sprintf("E2B installation %q is not configured", e2bInstallation)}
				return state
			}
			workload, err := inst.ResolveWorkload(revision.Image, revision.Command, revision.Args)
			if err != nil {
				state.CompilationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "E2BArtifactUnmapped", Message: err.Error()}
				return state
			}
			contract, err := json.Marshal(workload.Harness)
			if err != nil {
				state.CompilationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "E2BArtifactInvalid", Message: err.Error()}
				return state
			}
			revisionID, err := revision.E2BDigest(translator.E2BArtifact{
				Installation: e2bInstallation, TemplateID: workload.TemplateID, BuildID: workload.BuildID,
				LaunchContract: contract,
			})
			if err != nil {
				state.CompilationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "RevisionInvalid", Message: err.Error()}
				return state
			}
			state.Target = &compiledTarget{Revision: *revision, RevisionID: revisionID, E2B: &resolvedE2BArtifact{
				Installation: e2bInstallation, TemplateID: workload.TemplateID, BuildID: workload.BuildID,
				LaunchContract: contract,
			}}

			// Consume the artifact-readiness observation the reconcile published.
			// Until one lands for this revision the E2B preparation is pending;
			// a failure becomes the Agent's Ready=False reason.
			observed := krt.FetchOne(ctx, agentRuntimeObservations, krt.FilterKey(state.ResourceName()))
			if observed != nil && observed.RevisionID == state.Target.RevisionID {
				if observed.Failure != nil {
					state.PreparationFailure = observed.Failure
				} else {
					ready := observed.E2BReady
					state.E2BObserved = &ready
				}
			}
			return state
		}

		revisionID, err := revision.Digest()
		if err != nil {
			state.CompilationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "RevisionInvalid", Message: err.Error()}
			return state
		}

		actorTemplate, err := substrate.ActorTemplateForRevision(revision, revisionID)
		if err != nil {
			state.CompilationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "ActorTemplateInvalid", Message: err.Error()}
			return state
		}
		state.Target = &compiledTarget{Revision: *revision, RevisionID: revisionID, ActorTemplate: actorTemplate}

		observed := krt.FetchOne(ctx, agentRuntimeObservations, krt.FilterKey(state.ResourceName()))
		if observed == nil || observed.RevisionID != state.Target.RevisionID {
			return state
		}
		if observed.Failure != nil {
			state.PreparationFailure = observed.Failure
			return state
		}
		state.ObservedActorTemplate = (*observed).Template
		if !substrate.ActorTemplateSpecEqual(state.ObservedActorTemplate, state.Target.ActorTemplate) {
			state.PreparationFailure = &ReconciliationFailure{
				Condition: kagentv1alpha3.AgentConditionReady,
				Reason:    "ActorTemplateConflict",
				Message:   "existing immutable ActorTemplate differs from the compiled revision",
			}
		} else if message := state.ObservedActorTemplate.GetStatus().GetGoldenSnapshotStatus().GetErrorMessage(); message != "" {
			state.PreparationFailure = &ReconciliationFailure{Condition: kagentv1alpha3.AgentConditionReady, Reason: "ActorTemplateFailed", Message: message}
		}
		return state
	}, opts.WithName("AgentReconciliations")...)
}

// runtimeRevisionStore is the controller's narrow view of the shared database.
// Substrate owns ActorTemplates; the database retains revisions while an Agent
// or a Session or checkpoint references them.
type runtimeRevisionStore interface {
	UpsertAgentDefinition(context.Context, database.AgentDefinition) error
	RecordRuntimeRevision(context.Context, database.RuntimeRevision, bool) error
	RecordE2BRuntimeRevision(context.Context, database.E2BRuntimeRevisionInput, bool) error
	RetireAgentIdentities(ctx context.Context, namespace, name string, except *database.AgentDefinition) error
}

type actorTemplateClient interface {
	EnsureAtespace(context.Context, string) error
	GetActorTemplate(context.Context, string, string) (*ateapipb.ActorTemplate, error)
	CreateActorTemplate(context.Context, *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error)
}

// Reconciler is the side-effect boundary for the pure KRT graph. Collection
// handlers enqueue stable keys; retries always read the latest derived state.
type Reconciler struct {
	collections Collections
	templates   actorTemplateClient
	store       runtimeRevisionStore
	status      kagentclient.ApiV1alpha3Interface

	// e2bArtifactReady resolves an E2B installation's control-plane client and
	// reports whether the mapped template/build is present and ready. nil means
	// no artifact verification is possible; an unverifiable artifact is never
	// promoted ready.
	e2bArtifactReady func(ctx context.Context, installation, templateID, buildID string) (bool, error)

	agents                   controllers.Queue
	agentStatuses            controllers.Queue
	modelConfigStatuses      controllers.Queue
	agentHandler             krt.HandlerRegistration
	agentStatusHandler       krt.HandlerRegistration
	modelConfigStatusHandler krt.HandlerRegistration
}

// NewReconciler creates the Kubernetes and database write boundary. Run starts
// its queues after the registered KRT handlers have received initial state.
func NewReconciler(config *rest.Config, collections Collections, store runtimeRevisionStore, templates actorTemplateClient, e2bArtifactReady func(ctx context.Context, installation, templateID, buildID string) (bool, error)) (*Reconciler, error) {
	statusClient, err := kagentclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create kagent status client: %w", err)
	}
	return newReconciler(collections, templates, store, statusClient, e2bArtifactReady), nil
}

func newReconciler(
	collections Collections,
	templates actorTemplateClient,
	store runtimeRevisionStore,
	status kagentclient.ApiV1alpha3Interface,
	e2bArtifactReady func(ctx context.Context, installation, templateID, buildID string) (bool, error),
) *Reconciler {
	r := &Reconciler{
		collections:      collections,
		templates:        templates,
		store:            store,
		status:           status,
		e2bArtifactReady: e2bArtifactReady,
	}
	r.agents = newReconciliationQueue("agents", func(item any) error {
		return r.reconcileAgent(context.Background(), item.(string))
	})
	r.agentStatuses = newReconciliationQueue("agent-status", func(item any) error {
		return r.reconcileAgentStatus(context.Background(), item.(string))
	})
	r.modelConfigStatuses = newReconciliationQueue("model-config-status", func(item any) error {
		return r.reconcileModelConfigStatus(context.Background(), item.(string))
	})

	r.agentHandler = collections.Reconciliations.Register(func(event krt.Event[AgentReconciliation]) {
		// Observations and status writes must not bypass failure backoff. Only
		// a changed desired runtime or Agent identity needs immediate work.
		if event.Old != nil && event.New != nil && event.Old.Agent.UID == event.New.Agent.UID &&
			event.Old.desiredRevision() == event.New.desiredRevision() {
			return
		}
		r.agents.Add(krt.GetKey(event.Latest()))
	})
	r.agentStatusHandler = collections.AgentStatuses.Register(func(event krt.Event[krt.ObjectWithStatus[*kagentv1alpha3.Agent, kagentv1alpha3.AgentStatus]]) {
		status := event.Latest()
		if apiequality.Semantic.DeepEqual(statusWithTransitionTimes(status.Status, status.Obj.Status), status.Obj.Status) {
			return
		}
		r.agentStatuses.Add(status.ResourceName())
	})
	r.modelConfigStatusHandler = collections.ModelConfigStatuses.Register(func(event krt.Event[krt.ObjectWithStatus[*kagentv1alpha3.ModelConfig, kagentv1alpha3.ModelConfigStatus]]) {
		status := event.Latest()
		if apiequality.Semantic.DeepEqual(modelConfigStatusWithTransitionTimes(status.Status, status.Obj.Status), status.Obj.Status) {
			return
		}
		r.modelConfigStatuses.Add(status.ResourceName())
	})
	return r
}

// Keep retrying at a capped interval so prerequisites can recover without a
// Kubernetes event. Istio's queue requires a positive attempt limit; MaxInt
// makes that limit unreachable during a controller's lifetime.
func newReconciliationQueue(name string, reconcile func(any) error) controllers.Queue {
	return controllers.NewQueue(name, controllers.WithGenericReconciler(reconcile),
		controllers.WithMaxAttempts(math.MaxInt),
		controllers.WithRateLimiter(workqueue.NewTypedItemExponentialFailureRateLimiter[any](time.Second, 30*time.Second)),
	)
}

// Run waits for the graph boundary to observe initial state, then processes
// Agent and status writes until stop closes.
func (r *Reconciler) Run(stop <-chan struct{}) {
	if !r.agentHandler.WaitUntilSynced(stop) || !r.agentStatusHandler.WaitUntilSynced(stop) || !r.modelConfigStatusHandler.WaitUntilSynced(stop) {
		r.agents.ShutDownEarly()
		r.agentStatuses.ShutDownEarly()
		r.modelConfigStatuses.ShutDownEarly()
		return
	}
	go r.pollPendingTemplates(stop)
	go r.agentStatuses.Run(stop)
	go r.modelConfigStatuses.Run(stop)
	r.agents.Run(stop)
}

func (r *Reconciler) pollPendingTemplates(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			for _, state := range r.collections.Reconciliations.List() {
				golden := state.ObservedActorTemplate.GetStatus().GetGoldenSnapshotStatus()
				if state.Target != nil && state.PreparationFailure == nil && state.ObservedActorTemplate != nil && golden.GetGoldenTag() == nil {
					r.agents.Add(state.ResourceName())
				}
			}
		}
	}
}

func (r *Reconciler) Start(ctx context.Context) error {
	r.Run(ctx.Done())
	return nil
}

func (r *Reconciler) NeedLeaderElection() bool { return true }

func (r *Reconciler) reconcileAgent(ctx context.Context, key string) error {
	state := r.collections.Reconciliations.GetKey(key)
	if observation := r.collections.AgentRuntimeObservations.GetKey(key); observation != nil &&
		(state == nil || state.Target == nil || observation.RevisionID != state.Target.RevisionID) {
		r.collections.AgentRuntimeObservations.DeleteObject(key)
	}
	if state == nil {
		parts := strings.Split(key, "/")
		if len(parts) != 2 {
			return fmt.Errorf("invalid Agent key %q", key)
		}
		if err := r.store.RetireAgentIdentities(ctx, parts[0], parts[1], nil); err != nil {
			return fmt.Errorf("retire Agent %s: %w", key, err)
		}
		return nil
	}
	definition := database.AgentDefinition{
		Namespace: state.Agent.Namespace, AgentName: state.Agent.Name,
		AgentUID: string(state.Agent.UID), DesiredRevision: state.desiredRevision(),
	}

	// Store the desired edge before creating compute so a concurrent collector
	// cannot mistake the revision for abandoned state. Unresolved inputs clear
	// the old desired edge without inventing a runtime revision;
	// the upsert preserves the current UID's last-good runtime in either case.
	if err := r.store.UpsertAgentDefinition(ctx, definition); err != nil {
		return r.observePreparationError(*state, fmt.Errorf("store Agent %s: %w", key, err))
	}
	if state.Target == nil {
		return nil
	}
	if state.PreparationFailure != nil && !state.PreparationFailure.Retryable {
		return nil
	}
	target := state.Target
	if target.E2B != nil {
		// E2B revisions record their resolved artifact binding directly; there
		// is no Substrate ActorTemplate to reconcile. The revision is promoted
		// only when the mapped external artifact verifies ready; an absent,
		// not-ready or unverifiable artifact stays unpromoted and reports a
		// preparation observation the status path reads.
		ready := false
		var prepFailure *ReconciliationFailure
		if r.e2bArtifactReady == nil {
			prepFailure = &ReconciliationFailure{
				Condition: kagentv1alpha3.AgentConditionReady, Reason: "E2BArtifactUnchecked",
				Message:   "no E2B artifact readiness check is configured; the revision cannot be promoted",
				Retryable: true,
			}
		} else {
			ok, err := r.e2bArtifactReady(ctx, target.E2B.Installation, target.E2B.TemplateID, target.E2B.BuildID)
			switch {
			case err != nil:
				prepFailure = &ReconciliationFailure{
					Condition: kagentv1alpha3.AgentConditionReady, Reason: "E2BArtifactUnchecked",
					Message:   fmt.Sprintf("could not verify E2B artifact %s/%s in %s: %s", target.E2B.TemplateID, target.E2B.BuildID, target.E2B.Installation, status.Code(err)),
					Retryable: true,
				}
			case !ok:
				prepFailure = &ReconciliationFailure{
					Condition: kagentv1alpha3.AgentConditionReady, Reason: "E2BArtifactNotReady",
					Message:   fmt.Sprintf("E2B artifact %s build %s in installation %s is not ready", target.E2B.TemplateID, target.E2B.BuildID, target.E2B.Installation),
					Retryable: true,
				}
			default:
				ready = true
			}
		}
		err := r.store.RecordE2BRuntimeRevision(ctx, database.E2BRuntimeRevisionInput{
			Revision: target.RevisionID.String(), Namespace: definition.Namespace,
			AgentName: definition.AgentName, AgentUID: definition.AgentUID,
			SourceSnapshot: target.Revision.Provenance, AgentCard: target.Revision.AgentCard,
			EgressDestinations: target.Revision.EgressDestinations,
			ConfigJSON:         target.Revision.ConfigJSON,
			Environment:        e2bRevisionEnvironment(target.Revision.Environment),
			Installation:       target.E2B.Installation, ExternalID: target.E2B.TemplateID, BuildID: target.E2B.BuildID,
			LaunchContract: target.E2B.LaunchContract,
		}, ready)
		if err != nil {
			return r.observePreparationError(*state, fmt.Errorf("store E2B runtime revision %s: %w", target.RevisionID, err))
		}
		// Publish the E2B preparation outcome through the same observation
		// channel Substrate uses so status derives Ready from it. Template is
		// nil; E2BReady carries the signal.
		r.collections.AgentRuntimeObservations.ConditionalUpdateObject(AgentRuntimeObservation{
			Namespace: state.Agent.Namespace, AgentName: state.Agent.Name,
			RevisionID: target.RevisionID, E2BReady: ready, Failure: prepFailure,
		})
		if !ready {
			// Return a retryable error so the queue backs off and re-checks —
			// the observation alone does not re-enqueue, and only the Substrate
			// path polls pending work. An artifact that later becomes ready is
			// picked up on the retry.
			return fmt.Errorf("E2B artifact %s/%s in %s not ready", target.E2B.TemplateID, target.E2B.BuildID, target.E2B.Installation)
		}
		return nil
	}
	desiredRef := target.ActorTemplate.GetMetadata()
	observed, err := r.templates.GetActorTemplate(ctx, desiredRef.GetAtespace(), desiredRef.GetName())
	if status.Code(err) == codes.NotFound {
		if err := r.templates.EnsureAtespace(ctx, desiredRef.GetAtespace()); err != nil {
			return r.observePreparationError(*state, fmt.Errorf("ensure Atespace %s: %w", desiredRef.GetAtespace(), err))
		}
		observed, err = r.templates.CreateActorTemplate(ctx, target.ActorTemplate)
		if status.Code(err) == codes.InvalidArgument {
			// Validation rejects the immutable request, so retries cannot repair
			// it. Backend messages may quote configuration; keep status safe.
			r.observePreparation(*state, nil, &ReconciliationFailure{
				Condition: kagentv1alpha3.AgentConditionReady,
				Reason:    "ActorTemplateRejected",
				Message:   "Substrate rejected the compiled ActorTemplate (InvalidArgument); verify the Agent configuration against Substrate's validation requirements",
			})
			return nil
		}
		if status.Code(err) == codes.AlreadyExists {
			observed, err = r.templates.GetActorTemplate(ctx, desiredRef.GetAtespace(), desiredRef.GetName())
		}
	}
	if err != nil {
		return r.observePreparationError(*state, fmt.Errorf("reconcile ActorTemplate %s/%s: %w", desiredRef.GetAtespace(), desiredRef.GetName(), err))
	}
	if !substrate.ActorTemplateSpecEqual(observed, target.ActorTemplate) {
		r.observePreparation(*state, observed, nil)
		return nil
	}

	revision := database.RuntimeRevision{
		Revision: target.RevisionID.String(), Namespace: definition.Namespace,
		AgentName: definition.AgentName, AgentUID: definition.AgentUID,
		SourceSnapshot: target.Revision.Provenance, AgentCard: target.Revision.AgentCard,
		EgressDestinations:    target.Revision.EgressDestinations,
		Credentials:           target.Revision.Credentials,
		ActorTemplateAtespace: observed.GetMetadata().GetAtespace(), ActorTemplateName: observed.GetMetadata().GetName(), ActorTemplateUID: observed.GetMetadata().GetUid(),
	}
	ready := observed.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil
	if err := r.store.RecordRuntimeRevision(ctx, revision, ready); err != nil {
		return r.observePreparationError(*state, fmt.Errorf("store runtime revision %s: %w", target.RevisionID, err))
	}
	// This observation drives Kubernetes Ready status on a separate queue.
	// Publish it only after session creation can select the persisted revision.
	r.observePreparation(*state, observed, nil)
	return nil
}

func (r *Reconciler) observePreparationError(state AgentReconciliation, err error) error {
	if state.Target == nil {
		return err
	}
	// Preserve terminal diagnostics if persisting the desired edge fails.
	if state.PreparationFailure != nil && !state.PreparationFailure.Retryable {
		return err
	}
	// E2B targets have no ActorTemplate; the failure path must not deref it.
	var configName string
	if state.Target.ActorTemplate != nil {
		configName = state.Target.ActorTemplate.GetSandboxConfig().GetConfigName()
	}
	r.observePreparation(state, nil, runtimePreparationFailure(err, configName, state.Target.Revision.SandboxClass))
	return err
}

// e2bRevisionEnvironment flattens the revision's resolved environment to a
// literal name->value map for the artifact binding. Secret-bearing entries
// (ValueFrom) are dropped — credentials are injected by the broker outside
// the guest and must never be persisted on the binding.
func e2bRevisionEnvironment(env []corev1.EnvVar) map[string]string {
	if len(env) == 0 {
		return nil
	}
	out := make(map[string]string, len(env))
	for _, v := range env {
		if v.Name == "" || v.ValueFrom != nil {
			continue
		}
		if _, dup := out[v.Name]; dup {
			continue
		}
		out[v.Name] = v.Value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func runtimePreparationFailure(err error, configName string, class atev1alpha1.SandboxClass) *ReconciliationFailure {
	// Backend errors can contain credentials or infrastructure details. Publish
	// only the code and expected configuration, never the raw error or details.
	message := fmt.Sprintf("Runtime preparation failed (%s); check controller logs for details", status.Code(err))
	if status.Code(err) == codes.FailedPrecondition {
		if class == "" {
			class = atev1alpha1.SandboxClassGvisor
		}
		message = fmt.Sprintf("Substrate rejected runtime preparation (FailedPrecondition); verify SandboxConfig %q exists with spec.sandboxClass=%q and the required runtime assets; check controller logs for details",
			configName, class)
	}
	return &ReconciliationFailure{
		Condition: kagentv1alpha3.AgentConditionReady,
		Reason:    "RuntimePreparationFailed",
		Message:   message,
		Retryable: true,
	}
}

// Observations belong to the Agent's current preparation, independently of how
// long sessions or checkpoints keep its old runtime alive in the database.
func (r *Reconciler) observePreparation(state AgentReconciliation, template *ateapipb.ActorTemplate, failure *ReconciliationFailure) {
	r.collections.AgentRuntimeObservations.ConditionalUpdateObject(AgentRuntimeObservation{
		Namespace: state.Agent.Namespace, AgentName: state.Agent.Name,
		RevisionID: state.Target.RevisionID,
		Template:   template,
		Failure:    failure,
	})
}

func (r *Reconciler) reconcileAgentStatus(ctx context.Context, key string) error {
	desired := r.collections.AgentStatuses.GetKey(key)
	template := r.collections.Agents.GetKey(key)
	if desired == nil || template == nil {
		return nil
	}
	updated := (*template).DeepCopy()
	updated.Status = statusWithTransitionTimes(desired.Status, updated.Status)
	if apiequality.Semantic.DeepEqual(updated.Status, (*template).Status) {
		return nil
	}
	if _, err := r.status.Agents(updated.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update Agent %s status: %w", key, err)
	}
	return nil
}

func (r *Reconciler) reconcileModelConfigStatus(ctx context.Context, key string) error {
	desired := r.collections.ModelConfigStatuses.GetKey(key)
	modelConfig := r.collections.ModelConfigs.GetKey(key)
	if desired == nil || modelConfig == nil {
		return nil
	}
	updated := (*modelConfig).DeepCopy()
	updated.Status = modelConfigStatusWithTransitionTimes(desired.Status, updated.Status)
	if apiequality.Semantic.DeepEqual(updated.Status, (*modelConfig).Status) {
		return nil
	}
	if _, err := r.status.ModelConfigs(updated.Namespace).UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update ModelConfig %s status: %w", key, err)
	}
	return nil
}

func statusWithTransitionTimes(desired, current kagentv1alpha3.AgentStatus) kagentv1alpha3.AgentStatus {
	desired.Conditions = append([]metav1.Condition(nil), desired.Conditions...)
	for i := range desired.Conditions {
		condition := &desired.Conditions[i]
		previous := apimeta.FindStatusCondition(current.Conditions, condition.Type)
		if previous != nil && previous.Status == condition.Status {
			condition.LastTransitionTime = previous.LastTransitionTime
		} else {
			condition.LastTransitionTime = metav1.Now()
		}
	}
	return desired
}

func modelConfigStatusWithTransitionTimes(desired, current kagentv1alpha3.ModelConfigStatus) kagentv1alpha3.ModelConfigStatus {
	desired.Conditions = append([]metav1.Condition(nil), desired.Conditions...)
	for conditionIndex := range desired.Conditions {
		condition := &desired.Conditions[conditionIndex]
		if previous := apimeta.FindStatusCondition(current.Conditions, condition.Type); previous != nil &&
			previous.Status == condition.Status && previous.Reason == condition.Reason &&
			previous.Message == condition.Message && previous.ObservedGeneration == condition.ObservedGeneration {
			condition.LastTransitionTime = previous.LastTransitionTime
			continue
		}
		condition.LastTransitionTime = metav1.Now()
	}
	return desired
}
