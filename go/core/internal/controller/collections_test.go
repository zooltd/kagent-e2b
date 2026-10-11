package controller

import (
	"testing"
	"time"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	ateclient "github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	atefake "github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	kagentclient "github.com/kagent-dev/kagent/go/api/clientset/versioned"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/controller/apiclient"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	v2translator "github.com/kagent-dev/kagent/go/core/internal/translator"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/kube/krt/krttest"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var e2bTestInstallations = map[string]e2b.E2BInstallation{
	"embed-lab": {
		ClientProxy: "http://proxy:1",
		Workloads: []e2b.E2BWorkload{{
			Image:      "example.com/kagent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			TemplateID: "tmpl-kagent",
			BuildID:    "build-kagent",
			Harness: &e2b.E2BHarnessSpec{
				Command: "nohup h &", ConfigPath: "/c", CredentialPath: "/cr", ActivatePath: "/a",
				User: "kagent", Cwd: "/data/workspace",
			},
		}},
	},
}

// TestE2BReconciliationResolvesArtifactBinding asserts the E2B branch compiles
// the revision, resolves the workload to a prepared artifact via the
// installation mapping, and produces an E2B-bound target with no Substrate
// ActorTemplate — even though the authored Harness still carries an (inactive)
// Substrate policy.
func TestE2BReconciliationResolvesArtifactBinding(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test-e2b", nil)

	template := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid", Labels: map[string]string{"runtime": "python"}},
		Spec:       kagentv1alpha3.AgentTemplateSpec{SystemPrompt: "help"},
	}
	h := harness("team-a", "kagent", map[string]string{"runtime": "python"})
	h.UID = "harness-uid"
	h.Spec.Kagent = &kagentv1alpha3.KagentHarness{}
	h.Spec.Workload.Image = "example.com/kagent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	// An authored-but-inactive Substrate policy must not be required to resolve
	// nor leak into the E2B target.
	h.Spec.Substrate = &kagentv1alpha3.RuntimeSubstratePolicy{
		WorkerPoolRef: corev1.LocalObjectReference{Name: "default"}, SnapshotPolicy: kagentv1alpha3.RuntimeSnapshotPolicy{Location: "snapshots"},
	}
	template.Spec.ModelConfig = &corev1.LocalObjectReference{Name: "model"}
	model := &kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "model-uid"}, Spec: kagentv1alpha3.ModelConfigSpec{
		Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5", APIKeySecret: "model-auth", APIKeySecretKey: "api-key",
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-auth", UID: "secret-uid"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	mock := krttest.NewMock(t, []any{h, model, secret})
	agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, h)}, opts.WithName("Agents")...)
	templates := krt.NewStaticCollection(nil, []*kagentv1alpha3.AgentTemplate{template}, opts.WithName("AgentTemplates")...)
	configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
	secrets := krttest.GetMockCollection[*corev1.Secret](mock)
	_, resolvedModelConfigs := newModelConfigReconciliations(krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock), configMaps, secrets, opts)
	reconciliations := newAgentReconciliations(agents, v2translator.Collections{
		Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModelConfigs,
		RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
		ConfigMaps:       configMaps, Secrets: secrets,
	}, krttest.GetMockCollection[AgentRuntimeObservation](mock), opts, "e2b", e2bTestInstallations, "embed-lab")
	waitFor(t, func() bool {
		states := reconciliations.List()
		return len(states) == 1 && (states[0].CompilationFailure != nil || states[0].Target != nil)
	})
	state := reconciliations.List()[0]
	require.Nil(t, state.CompilationFailure, "E2B reconciliation must compile: %v", state.CompilationFailure)
	require.NotNil(t, state.Target)
	require.NotNil(t, state.Target.E2B, "E2B target must carry the resolved artifact binding")
	require.Nil(t, state.Target.ActorTemplate, "E2B target must not build a Substrate ActorTemplate")
	require.Equal(t, "embed-lab", state.Target.E2B.Installation)
	require.Equal(t, "tmpl-kagent", state.Target.E2B.TemplateID)
	require.Equal(t, "build-kagent", state.Target.E2B.BuildID)
	require.NotEmpty(t, state.Target.E2B.LaunchContract)
}

// TestE2BReconciliationUnmappedWorkloadFails asserts a compiled revision whose
// image/command/args matches no installation workload entry fails preparation
// closed rather than inventing an artifact.
func TestE2BReconciliationUnmappedWorkloadFails(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test-e2b-unmapped", nil)

	template := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid", Labels: map[string]string{"runtime": "python"}},
		Spec:       kagentv1alpha3.AgentTemplateSpec{SystemPrompt: "help"},
	}
	h := harness("team-a", "kagent", map[string]string{"runtime": "python"})
	h.UID = "harness-uid"
	h.Spec.Kagent = &kagentv1alpha3.KagentHarness{}
	// A different image than the mapping selects -> no workload matches.
	h.Spec.Workload.Image = "example.com/other@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	template.Spec.ModelConfig = &corev1.LocalObjectReference{Name: "model"}
	model := &kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "model-uid"}, Spec: kagentv1alpha3.ModelConfigSpec{
		Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5", APIKeySecret: "model-auth", APIKeySecretKey: "api-key",
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-auth", UID: "secret-uid"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	mock := krttest.NewMock(t, []any{h, model, secret})
	agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, h)}, opts.WithName("Agents")...)
	templates := krt.NewStaticCollection(nil, []*kagentv1alpha3.AgentTemplate{template}, opts.WithName("AgentTemplates")...)
	configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
	secrets := krttest.GetMockCollection[*corev1.Secret](mock)
	_, resolvedModelConfigs := newModelConfigReconciliations(krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock), configMaps, secrets, opts)
	reconciliations := newAgentReconciliations(agents, v2translator.Collections{
		Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModelConfigs,
		RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
		ConfigMaps:       configMaps, Secrets: secrets,
	}, krttest.GetMockCollection[AgentRuntimeObservation](mock), opts, "e2b", e2bTestInstallations, "embed-lab")
	waitFor(t, func() bool {
		states := reconciliations.List()
		return len(states) == 1 && states[0].CompilationFailure != nil
	})
	state := reconciliations.List()[0]
	require.Equal(t, "E2BArtifactUnmapped", state.CompilationFailure.Reason)
	require.Nil(t, state.Target)
}

func TestReconciliationCollectionsCompileAndObserveRevision(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test", nil)

	template := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid", Labels: map[string]string{"runtime": "python"}},
		Spec: kagentv1alpha3.AgentTemplateSpec{
			ModelConfig:  &corev1.LocalObjectReference{Name: "model"},
			SystemPrompt: "help",
		},
	}
	matchingHarness := harness("team-a", "kagent", map[string]string{"runtime": "python"})
	matchingHarness.UID = "harness-uid"
	matchingHarness.Spec.Kagent = &kagentv1alpha3.KagentHarness{}
	matchingHarness.Spec.Workload.Image = "example.com/kagent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	matchingHarness.Spec.Substrate = &kagentv1alpha3.RuntimeSubstratePolicy{
		WorkerPoolRef:  corev1.LocalObjectReference{Name: "default"},
		SnapshotPolicy: kagentv1alpha3.RuntimeSnapshotPolicy{Location: "snapshots"},
	}
	modelConfigs := krt.NewStaticCollection(nil, []*kagentv1alpha3.ModelConfig{{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"}, Spec: kagentv1alpha3.ModelConfigSpec{Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5"}}}, opts.WithName("ModelConfigs")...)
	templates := krt.NewStaticCollection[*kagentv1alpha3.AgentTemplate](nil, nil, opts.WithName("AgentTemplates")...)
	mock := krttest.NewMock(t, []any{
		matchingHarness,
		&atev1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "default"}},
	})

	collections := Collections{
		AgentTemplates:           templates,
		Harnesses:                krttest.GetMockCollection[*kagentv1alpha3.Harness](mock),
		ModelConfigs:             modelConfigs,
		RemoteMCPServers:         krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
		ConfigMaps:               krttest.GetMockCollection[*corev1.ConfigMap](mock),
		Secrets:                  krttest.GetMockCollection[*corev1.Secret](mock),
		WorkerPools:              krttest.GetMockCollection[*atev1alpha1.WorkerPool](mock),
		AgentRuntimeObservations: krt.NewStaticCollection[AgentRuntimeObservation](nil, nil, opts.WithName("AgentRuntimeObservations")...),
	}
	collections.ModelConfigStatuses, collections.ResolvedModelConfigs = newModelConfigReconciliations(collections.ModelConfigs, collections.ConfigMaps, collections.Secrets, opts)
	collections.Agents = krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, matchingHarness)}, opts.WithName("Agents")...)
	collections.Reconciliations = newAgentReconciliations(
		collections.Agents, v2translator.Collections{
			Harnesses: collections.Harnesses, AgentTemplates: collections.AgentTemplates, ResolvedModelConfigs: collections.ResolvedModelConfigs,
			RemoteMCPServers: collections.RemoteMCPServers, ConfigMaps: collections.ConfigMaps,
			Secrets: collections.Secrets, WorkerPools: collections.WorkerPools,
		}, collections.AgentRuntimeObservations, opts, "", nil, "")
	collections.AgentStatuses = newAgentStatuses(collections.Agents, collections.Reconciliations, opts)

	// The Agent informer can observe a new Agent before the template informer
	// observes its reference. Adding the template must clear the failure without
	// changing or re-enqueuing the Agent explicitly.
	waitFor(t, func() bool {
		updates := collections.AgentStatuses.List()
		if len(updates) != 1 {
			return false
		}
		condition := apimeta.FindStatusCondition(updates[0].Status.Conditions, kagentv1alpha3.AgentConditionResolvedRefs)
		return condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "ReferenceResolutionFailed"
	})
	templates.UpdateObject(template)

	waitFor(t, func() bool {
		states := collections.Reconciliations.List()
		return len(states) == 1 && states[0].CompilationFailure == nil && states[0].Target != nil
	})
	state := collections.Reconciliations.List()[0]
	if state.ObservedActorTemplate != nil {
		t.Fatal("ActorTemplate was observed before it existed")
	}
	waitFor(t, func() bool {
		updates := collections.AgentStatuses.List()
		if len(updates) != 1 {
			return false
		}
		ready := apimeta.FindStatusCondition(updates[0].Status.Conditions, kagentv1alpha3.AgentConditionReady)
		return ready != nil && ready.Status == metav1.ConditionFalse
	})

	observed := proto.CloneOf(state.Target.ActorTemplate)
	observed.Metadata.Uid = "actor-template-uid"
	observed.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{GoldenTag: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "golden"}}}
	store := &fakeRuntimeRevisionStore{}
	reconciler := &Reconciler{
		collections: collections, templates: &fakeActorTemplates{template: observed}, store: store,
	}
	require.NoError(t, reconciler.reconcileAgent(t.Context(), state.ResourceName()))
	waitFor(t, func() bool {
		states := collections.Reconciliations.List()
		updates := collections.AgentStatuses.List()
		if len(states) != 1 || states[0].ObservedActorTemplate == nil || len(updates) != 1 {
			return false
		}
		ready := apimeta.FindStatusCondition(updates[0].Status.Conditions, kagentv1alpha3.AgentConditionReady)
		return ready != nil && ready.Status == metav1.ConditionTrue && updates[0].Status.LatestSuccessfulRevision == state.Target.RevisionID.String()
	})

	modelConfigs.UpdateObject(&kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"}, Spec: kagentv1alpha3.ModelConfigSpec{Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5.1"}})
	waitFor(t, func() bool {
		states := collections.Reconciliations.List()
		return len(states) == 1 && states[0].Target.RevisionID != state.Target.RevisionID && states[0].ObservedActorTemplate == nil
	})
}

func TestReconciliationWorkerPoolSandboxClass(t *testing.T) {
	for _, harnessType := range []v2translator.HarnessType{
		v2translator.HarnessTypeKagent, v2translator.HarnessTypeCodex, v2translator.HarnessTypeClaude, v2translator.HarnessTypeBYO,
	} {
		t.Run(string(harnessType), func(t *testing.T) {
			stop := make(chan struct{})
			t.Cleanup(func() { close(stop) })
			opts := krt.NewOptionsBuilder(stop, "test-sandbox", nil)
			template := &kagentv1alpha3.AgentTemplate{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid"},
				Spec:       kagentv1alpha3.AgentTemplateSpec{ModelConfig: &corev1.LocalObjectReference{Name: "model"}, SystemPrompt: "help"},
			}
			runtimeHarness := harness("team-a", string(harnessType), nil)
			runtimeHarness.UID = "harness-uid"
			runtimeHarness.Spec.Workload.Image = "example.com/agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			runtimeHarness.Spec.Substrate = &kagentv1alpha3.RuntimeSubstratePolicy{
				WorkerPoolRef: corev1.LocalObjectReference{Name: "selected"}, SnapshotPolicy: kagentv1alpha3.RuntimeSnapshotPolicy{Location: "snapshots"},
			}
			responses := kagentv1alpha3.OpenAIAPIFormatResponses
			model := &kagentv1alpha3.ModelConfig{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "model-uid"},
				Spec: kagentv1alpha3.ModelConfigSpec{
					Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5", APIKeySecret: "model-auth", APIKeySecretKey: "api-key",
					OpenAI: &kagentv1alpha3.OpenAIConfig{APIFormat: &responses},
				},
			}
			switch harnessType {
			case v2translator.HarnessTypeKagent:
				runtimeHarness.Spec.Kagent = &kagentv1alpha3.KagentHarness{}
			case v2translator.HarnessTypeCodex:
				runtimeHarness.Spec.Codex = &kagentv1alpha3.CodexHarness{}
			case v2translator.HarnessTypeClaude:
				runtimeHarness.Spec.Claude = &kagentv1alpha3.ClaudeHarness{}
				model.Spec.Provider, model.Spec.Model, model.Spec.OpenAI = kagentv1alpha3.ModelProviderAnthropic, "claude-sonnet-4-5", nil
			case v2translator.HarnessTypeBYO:
				runtimeHarness.Spec.BYO = &kagentv1alpha3.BYOHarness{}
				runtimeHarness.Spec.Workload.Command = []string{"/agent"}
				template.Spec.ModelConfig = nil
			}
			mock := krttest.NewMock(t, []any{
				template, runtimeHarness, model,
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-auth"}, Data: map[string][]byte{"api-key": []byte("secret")}},
			})
			templates := krttest.GetMockCollection[*kagentv1alpha3.AgentTemplate](mock)
			agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, runtimeHarness)}, opts.WithName("Agents")...)
			configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
			secrets := krttest.GetMockCollection[*corev1.Secret](mock)
			_, resolvedModels := newModelConfigReconciliations(krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock), configMaps, secrets, opts)
			workerPools := krt.NewStaticCollection(nil, []*atev1alpha1.WorkerPool{
				{ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "selected"}, Spec: atev1alpha1.WorkerPoolSpec{SandboxClass: atev1alpha1.SandboxClassMicroVM}},
				{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "unselected"}, Spec: atev1alpha1.WorkerPoolSpec{SandboxClass: atev1alpha1.SandboxClassMicroVM}},
			}, opts.WithName("WorkerPools")...)
			observations := krt.NewStaticCollection[AgentRuntimeObservation](nil, nil, opts.WithName("AgentRuntimeObservations")...)
			reconciliations := newAgentReconciliations(agents, v2translator.Collections{
				Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModels,
				RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
				ConfigMaps:       configMaps, Secrets: secrets, WorkerPools: workerPools,
			}, observations, opts, "", nil, "")
			key := "team-a/assistant"
			waitFor(t, func() bool {
				state := reconciliations.GetKey(key)
				return state != nil && state.CompilationFailure != nil
			})
			missingPool := &ReconciliationFailure{
				Condition: kagentv1alpha3.AgentConditionResolvedRefs, Reason: "WorkerPoolNotFound", Message: `WorkerPool "team-a/selected" not found`,
			}
			require.Equal(t, missingPool, reconciliations.GetKey(key).CompilationFailure)
			require.Nil(t, reconciliations.GetKey(key).Target, "missing capacity must fail compilation")

			workerPools.UpdateObject(&atev1alpha1.WorkerPool{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "selected"},
			})
			waitFor(t, func() bool {
				state := reconciliations.GetKey(key)
				return state != nil && state.CompilationFailure == nil && state.Target != nil && state.Target.Revision.SandboxClass == ""
			})
			baseline := reconciliations.GetKey(key)
			gvisorRevision := baseline.Target.RevisionID
			require.False(t, gvisorRevision.IsZero())
			gvisorTemplate := proto.CloneOf(baseline.Target.ActorTemplate)
			observed := proto.CloneOf(gvisorTemplate)
			observed.Metadata.Uid = "actor-template-uid"
			observations.UpdateObject(AgentRuntimeObservation{
				Namespace: template.Namespace, AgentName: template.Name, RevisionID: gvisorRevision, Template: observed,
			})
			waitFor(t, func() bool { return reconciliations.GetKey(key).ObservedActorTemplate != nil })

			for _, step := range []struct {
				name    string
				class   atev1alpha1.SandboxClass
				remove  bool
				failure *ReconciliationFailure
			}{
				{name: "default"},
				{name: "explicit gvisor", class: atev1alpha1.SandboxClassGvisor},
				{name: "microvm", class: atev1alpha1.SandboxClassMicroVM},
				{name: "back to gvisor", class: atev1alpha1.SandboxClassGvisor},
				{name: "unsupported", class: "unsupported", failure: &ReconciliationFailure{
					Condition: kagentv1alpha3.AgentConditionCompatible, Reason: "RevisionInvalid", Message: `unsupported sandbox class "unsupported"`,
				}},
				{name: "deleted", remove: true, failure: missingPool},
				{name: "recreated"},
			} {
				t.Run(step.name, func(t *testing.T) {
					if step.remove {
						workerPools.DeleteObject("team-a/selected")
					} else {
						workerPools.UpdateObject(&atev1alpha1.WorkerPool{
							ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "selected"},
							Spec:       atev1alpha1.WorkerPoolSpec{SandboxClass: step.class},
						})
					}
					waitFor(t, func() bool {
						state := reconciliations.GetKey(key)
						if step.failure != nil {
							return state != nil && state.CompilationFailure != nil && *state.CompilationFailure == *step.failure
						}
						return state != nil && state.CompilationFailure == nil && state.Target != nil && state.Target.Revision.SandboxClass == step.class
					})
					state := reconciliations.GetKey(key)
					if step.failure != nil {
						require.Nil(t, state.Target)
						require.Empty(t, state.desiredRevision())
						require.Nil(t, state.ObservedActorTemplate)
						pairStatus := statusForAgent(*state, 1, gvisorRevision.String())
						require.Equal(t, gvisorRevision.String(), pairStatus.LatestSuccessfulRevision)
						condition := apimeta.FindStatusCondition(pairStatus.Conditions, step.failure.Condition)
						require.NotNil(t, condition)
						require.Equal(t, metav1.ConditionFalse, condition.Status)
						require.Equal(t, step.failure.Reason, condition.Reason)
						return
					}
					expected := proto.CloneOf(gvisorTemplate)
					expected.Metadata.Name = state.Target.ActorTemplate.GetMetadata().GetName()
					if step.class == atev1alpha1.SandboxClassMicroVM {
						require.NotEqual(t, gvisorRevision, state.Target.RevisionID)
						require.NotEqual(t, gvisorTemplate.GetMetadata().GetName(), expected.Metadata.Name)
						require.Nil(t, state.ObservedActorTemplate, "a gVisor observation must not satisfy a MicroVM revision")
						expected.SandboxConfig = &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM, ConfigName: "microvm"}
					} else {
						require.Equal(t, gvisorRevision, state.Target.RevisionID)
						require.Equal(t, gvisorTemplate.GetMetadata().GetName(), expected.Metadata.Name)
						expected.SandboxConfig = &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor-default"}
					}
					require.True(t, proto.Equal(expected, state.Target.ActorTemplate), "sandbox selection must preserve the rest of the ActorTemplate")
					require.Equal(t, map[string]string{"kagent.dev/worker-pool": "selected"}, state.Target.ActorTemplate.GetWorkerSelector().GetMatchLabels())
				})
			}
		})
	}
}

func TestClaudeReconciliationCompilesActorTemplate(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test-claude", nil)

	template := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid", Labels: map[string]string{"runtime": "claude"}},
		Spec:       kagentv1alpha3.AgentTemplateSpec{ModelConfig: &corev1.LocalObjectReference{Name: "model"}, SystemPrompt: "help"},
	}
	claudeHarness := harness("team-a", "claude", map[string]string{"runtime": "claude"})
	claudeHarness.UID = "harness-uid"
	claudeHarness.Spec.Claude = &kagentv1alpha3.ClaudeHarness{}
	claudeHarness.Spec.Workload.Image = "example.com/claude@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	claudeHarness.Spec.Substrate = &kagentv1alpha3.RuntimeSubstratePolicy{
		WorkerPoolRef: corev1.LocalObjectReference{Name: "default"}, SnapshotPolicy: kagentv1alpha3.RuntimeSnapshotPolicy{Location: "snapshots"},
	}
	model := &kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "model-uid"}, Spec: kagentv1alpha3.ModelConfigSpec{
		Provider: kagentv1alpha3.ModelProviderAnthropic, Model: "claude-sonnet-4-5", APIKeySecret: "model-auth", APIKeySecretKey: "api-key",
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-auth", UID: "secret-uid"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	mock := krttest.NewMock(t, []any{
		template,
		claudeHarness,
		model,
		secret,
		&atev1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "default"}},
	})
	templates := krttest.GetMockCollection[*kagentv1alpha3.AgentTemplate](mock)
	agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, claudeHarness)}, opts.WithName("Agents")...)
	configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
	secrets := krttest.GetMockCollection[*corev1.Secret](mock)
	_, resolvedModelConfigs := newModelConfigReconciliations(
		krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock), configMaps, secrets, opts,
	)
	reconciliations := newAgentReconciliations(
		agents, v2translator.Collections{
			Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModelConfigs,
			RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
			ConfigMaps:       configMaps, Secrets: secrets,
			WorkerPools: krttest.GetMockCollection[*atev1alpha1.WorkerPool](mock),
		}, krttest.GetMockCollection[AgentRuntimeObservation](mock), opts, "", nil, "")
	waitFor(t, func() bool {
		states := reconciliations.List()
		return len(states) == 1 && states[0].CompilationFailure == nil && states[0].Target != nil
	})
	state := reconciliations.List()[0]
	if state.Target == nil || state.Target.Revision.Environment[0].Name != "ANTHROPIC_API_KEY" || state.Target.Revision.Environment[0].Value != v2translator.CredentialPlaceholder {
		t.Fatalf("Claude revision environment = %#v", state.Target.Revision)
	}
	if state.Target.ActorTemplate.GetContainers()[0].GetWakeupProbe().GetHttpGet().GetPort() != 8081 {
		t.Fatalf("Claude ActorTemplate readiness = %#v", state.Target.ActorTemplate.GetContainers()[0].GetWakeupProbe())
	}
}

func TestCodexReconciliationCompilesActorTemplate(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test-codex", nil)
	responses := kagentv1alpha3.OpenAIAPIFormatResponses
	template := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "assistant", UID: "template-uid", Labels: map[string]string{"runtime": "codex"}},
		Spec:       kagentv1alpha3.AgentTemplateSpec{ModelConfig: &corev1.LocalObjectReference{Name: "model"}, SystemPrompt: "help"},
	}
	codexHarness := harness("team-a", "codex", map[string]string{"runtime": "codex"})
	codexHarness.UID = "harness-uid"
	codexHarness.Spec.Codex = &kagentv1alpha3.CodexHarness{}
	codexHarness.Spec.Workload.Image = "example.com/codex@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	codexHarness.Spec.Substrate = &kagentv1alpha3.RuntimeSubstratePolicy{
		WorkerPoolRef: corev1.LocalObjectReference{Name: "default"}, SnapshotPolicy: kagentv1alpha3.RuntimeSnapshotPolicy{Location: "snapshots"},
	}
	model := &kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "model-uid"}, Spec: kagentv1alpha3.ModelConfigSpec{
		Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5.2-codex", APIKeySecret: "model-auth", APIKeySecretKey: "api-key",
		OpenAI: &kagentv1alpha3.OpenAIConfig{APIFormat: &responses},
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-auth", UID: "secret-uid"}, Data: map[string][]byte{"api-key": []byte("secret")}}
	mock := krttest.NewMock(t, []any{
		template,
		codexHarness,
		model,
		secret,
		&atev1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "default"}},
	})
	templates := krttest.GetMockCollection[*kagentv1alpha3.AgentTemplate](mock)
	agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(template, codexHarness)}, opts.WithName("Agents")...)
	configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
	secrets := krttest.GetMockCollection[*corev1.Secret](mock)
	_, resolvedModelConfigs := newModelConfigReconciliations(
		krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock), configMaps, secrets, opts,
	)
	reconciliations := newAgentReconciliations(
		agents, v2translator.Collections{
			Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModelConfigs,
			RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
			ConfigMaps:       configMaps, Secrets: secrets,
			WorkerPools: krttest.GetMockCollection[*atev1alpha1.WorkerPool](mock),
		}, krttest.GetMockCollection[AgentRuntimeObservation](mock), opts, "", nil, "")
	waitFor(t, func() bool {
		states := reconciliations.List()
		return len(states) == 1 && states[0].CompilationFailure == nil && states[0].Target != nil
	})
	state := reconciliations.List()[0]
	if state.Target == nil || state.Target.Revision.Environment[0].Name != "OPENAI_API_KEY" || state.Target.Revision.Environment[0].Value != v2translator.CredentialPlaceholder {
		t.Fatalf("Codex revision environment = %#v", state.Target.Revision)
	}
	if state.Target.ActorTemplate.GetContainers()[0].GetWakeupProbe().GetHttpGet().GetPort() != 8081 {
		t.Fatalf("Codex ActorTemplate readiness = %#v", state.Target.ActorTemplate.GetContainers()[0].GetWakeupProbe())
	}
}

func TestReconciliationTracksSharedAgentTemplate(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test", nil)
	child := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "child", Labels: map[string]string{"runtime": "python"}},
		Spec:       kagentv1alpha3.AgentTemplateSpec{ModelConfig: &corev1.LocalObjectReference{Name: "model"}, SystemPrompt: "before"},
	}
	root := &kagentv1alpha3.AgentTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "root", Labels: map[string]string{"runtime": "python"}},
		Spec: kagentv1alpha3.AgentTemplateSpec{
			ModelConfig: &corev1.LocalObjectReference{Name: "model"},
			Tools: []kagentv1alpha3.ToolBinding{{SubAgent: &kagentv1alpha3.SubAgentToolBinding{
				Name: "child", Description: "delegate", TemplateRef: &corev1.LocalObjectReference{Name: child.Name},
			}}},
		},
	}
	harness := harness("team-a", "kagent", map[string]string{"runtime": "python"})
	harness.Spec.Kagent = &kagentv1alpha3.KagentHarness{}
	harness.Spec.Workload.Image = "example.com/kagent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	harness.Spec.Substrate = &kagentv1alpha3.RuntimeSubstratePolicy{WorkerPoolRef: corev1.LocalObjectReference{Name: "default"}, SnapshotPolicy: kagentv1alpha3.RuntimeSnapshotPolicy{Location: "snapshots"}}
	templates := krt.NewStaticCollection(nil, []*kagentv1alpha3.AgentTemplate{root, child}, opts.WithName("AgentTemplates")...)
	mock := krttest.NewMock(t, []any{
		harness,
		&kagentv1alpha3.ModelConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"}, Spec: kagentv1alpha3.ModelConfigSpec{Provider: kagentv1alpha3.ModelProviderOpenAI, Model: "gpt-5"}},
		&atev1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "default"}},
	})
	agents := krt.NewStaticCollection(nil, []*kagentv1alpha3.Agent{testAgent(root, harness)}, opts.WithName("Agents")...)
	modelConfigs := krttest.GetMockCollection[*kagentv1alpha3.ModelConfig](mock)
	configMaps := krttest.GetMockCollection[*corev1.ConfigMap](mock)
	secrets := krttest.GetMockCollection[*corev1.Secret](mock)
	_, resolvedModelConfigs := newModelConfigReconciliations(modelConfigs, configMaps, secrets, opts)
	reconciliations := newAgentReconciliations(
		agents, v2translator.Collections{
			Harnesses: krttest.GetMockCollection[*kagentv1alpha3.Harness](mock), AgentTemplates: templates, ResolvedModelConfigs: resolvedModelConfigs,
			RemoteMCPServers: krttest.GetMockCollection[*kagentv1alpha3.RemoteMCPServer](mock),
			ConfigMaps:       configMaps, Secrets: secrets,
			WorkerPools: krttest.GetMockCollection[*atev1alpha1.WorkerPool](mock),
		}, krttest.GetMockCollection[AgentRuntimeObservation](mock), opts, "", nil, "")
	var initial string
	waitFor(t, func() bool {
		for _, state := range reconciliations.List() {
			if state.Agent.Name == root.Name && state.CompilationFailure == nil {
				initial = state.Target.RevisionID.String()
				return true
			}
		}
		return false
	})
	updated := child.DeepCopy()
	updated.Spec.SystemPrompt = "after"
	templates.UpdateObject(updated)
	waitFor(t, func() bool {
		for _, state := range reconciliations.List() {
			if state.Agent.Name == root.Name {
				return state.CompilationFailure == nil && state.Target.RevisionID.String() != initial
			}
		}
		return false
	})
}

func harness(namespace, name string, matchLabels map[string]string) *kagentv1alpha3.Harness {
	return &kagentv1alpha3.Harness{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       kagentv1alpha3.HarnessSpec{},
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func testAgent(template *kagentv1alpha3.AgentTemplate, harness *kagentv1alpha3.Harness) *kagentv1alpha3.Agent {
	return &kagentv1alpha3.Agent{ObjectMeta: metav1.ObjectMeta{Namespace: template.Namespace, Name: template.Name, UID: template.UID}, Spec: kagentv1alpha3.AgentSpec{
		TemplateRef: &corev1.LocalObjectReference{Name: template.Name}, HarnessRef: &corev1.LocalObjectReference{Name: harness.Name},
	}}
}

// TestE2BWorkerPoolCollectionIsStaticAndEmpty asserts the E2B-only WorkerPool
// input is an already-synced empty static collection, not an informer that
// would block sync on a Substrate CRD set that does not exist under E2B. The
// WorkerPool is Substrate placement input and E2B compilation never resolves
// one, so the empty collection is the honest value.
func TestE2BWorkerPoolCollectionIsStaticAndEmpty(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	opts := krt.NewOptionsBuilder(stop, "test-e2b-noworkerpool", nil)

	pools := workerPoolCollection(nil, nil, opts, "e2b")
	require.True(t, pools.HasSynced(), "E2B WorkerPool collection must already be synced")
	require.Empty(t, pools.List(), "E2B WorkerPool collection must be empty")
}

// fakeAPIClient implements apiclient.Client so the registered WorkerPool
// ListWatch — which asserts c.(apiclient.Client).Substrate() — resolves against
// a substrate fake clientset. The embedded kube fake supplies the informer
// factory and RunAndWait; only the substrate accessor carries seeded state.
type fakeAPIClient struct {
	kube.Client
	kagent    kagentclient.Interface
	substrate ateclient.Interface
}

func newFakeAPIClient(substrateObjects ...runtime.Object) *fakeAPIClient {
	return &fakeAPIClient{
		Client:    kube.NewFakeClient(),
		substrate: atefake.NewSimpleClientset(substrateObjects...),
	}
}

func (c *fakeAPIClient) Kagent() kagentclient.Interface { return c.kagent }
func (c *fakeAPIClient) Substrate() ateclient.Interface { return c.substrate }

// TestSubstrateWorkerPoolCollectionIsAnInformer guards the complementary
// branch: under Substrate the WorkerPool input is a real informer reading live
// WorkerPool objects through the generated substrate client — not yet synced
// until the client runs — so the E2B-only static substitution did not drop the
// Substrate input.
func TestSubstrateWorkerPoolCollectionIsAnInformer(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	apiclient.RegisterTypes()
	client := newFakeAPIClient(&atev1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "default"}})
	opts := krt.NewOptionsBuilder(stop, "test-substrate-workerpool", nil)

	pools := workerPoolCollection(client, nil, opts, "substrate")
	require.True(t, client.RunAndWait(stop), "WorkerPool informer must sync")
	require.True(t, pools.HasSynced())
	got := pools.List()
	require.Len(t, got, 1)
	require.Equal(t, "default", got[0].Name)
}
