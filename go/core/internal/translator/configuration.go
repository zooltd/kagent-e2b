package translator

import (
	"github.com/kagent-dev/kagent/go/api/v1alpha3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TemplateConfiguration is reusable behavior resolved for compilation.
// Name identifies this node for diagnostics and prompt rendering. For inline
// behavior it is the owning Agent's name. Source is nil for inline configuration.
type TemplateConfiguration struct {
	Name      string
	Namespace string
	Spec      v1alpha3.AgentTemplateSpec
	Source    *metav1.ObjectMeta
}

// HarnessConfiguration is execution configuration resolved for compilation.
// Source records the referenced Harness, or is nil for an inline spec.
type HarnessConfiguration struct {
	Name      string
	Namespace string
	Spec      v1alpha3.HarnessSpec
	Source    *metav1.ObjectMeta
}

func templateConfiguration(resource *v1alpha3.AgentTemplate) *TemplateConfiguration {
	return &TemplateConfiguration{Name: resource.Name, Namespace: resource.Namespace,
		Spec: *resource.Spec.DeepCopy(), Source: resource.ObjectMeta.DeepCopy()}
}

func harnessConfiguration(resource *v1alpha3.Harness) *HarnessConfiguration {
	return &HarnessConfiguration{Name: resource.Name, Namespace: resource.Namespace,
		Spec: *resource.Spec.DeepCopy(), Source: resource.ObjectMeta.DeepCopy()}
}

// SubstratePolicy returns the authored Substrate placement policy, or nil when
// the Harness leaves it unset (valid under the E2B provider). Harness
// compilers read it through this accessor so an absent policy is a nil value
// rather than a nil-pointer dereference.
func (h *HarnessConfiguration) SubstratePolicy() *v1alpha3.RuntimeSubstratePolicy {
	if h == nil {
		return nil
	}
	return h.Spec.Substrate
}

// SubstratePlacement returns the WorkerPool name and snapshot location the
// authored Substrate policy selects, or empty strings when the policy is
// absent (the E2B provider ignores placement). Harness compilers use it rather
// than dereferencing Spec.Substrate so an absent policy cannot panic.
func (h *HarnessConfiguration) SubstratePlacement() (workerPool, snapshotLocation string) {
	p := h.SubstratePolicy()
	if p == nil {
		return "", ""
	}
	return p.WorkerPoolRef.Name, p.SnapshotPolicy.Location
}
