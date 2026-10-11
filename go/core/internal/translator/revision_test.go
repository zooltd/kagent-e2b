package translator

import (
	"encoding/json"
	"strings"
	"testing"

	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestRevisionDigestIncludesSandboxClass(t *testing.T) {
	revision := &Revision{Namespace: "agents", AgentName: "helper"}
	original, err := revision.Digest()
	require.NoError(t, err)
	require.NotEqual(t, "563beefdd1b191baae375aa92e70bcab812a3c4fd2a189cdf74668f1ad45456d", original.String(),
		"the default class must participate in the digest instead of preserving the legacy digest")

	revision.SandboxClass = atev1alpha1.SandboxClassGvisor
	gvisor, err := revision.Digest()
	require.NoError(t, err)
	require.Equal(t, original, gvisor, "empty and explicit gVisor select the same runtime")
	require.Equal(t, atev1alpha1.SandboxClassGvisor, revision.SandboxClass, "hashing must not mutate the revision")

	revision.SandboxClass = atev1alpha1.SandboxClassMicroVM
	microvm, err := revision.Digest()
	require.NoError(t, err)
	require.NotEqual(t, gvisor, microvm, "changing sandbox class must create a new immutable revision")
	repeated, err := revision.Digest()
	require.NoError(t, err)
	require.Equal(t, microvm, repeated)

	revision.SandboxClass = "unsupported"
	invalid, err := revision.Digest()
	require.EqualError(t, err, `unsupported sandbox class "unsupported"`)
	require.True(t, invalid.IsZero())
}

func TestRevisionDigestIncludesProvenance(t *testing.T) {
	revision := &Revision{Namespace: "agents", AgentName: "helper", Provenance: []byte(`[{"kind":"ConfigMap","hash":"first"}]`)}
	first, err := revision.Digest()
	if err != nil {
		t.Fatal(err)
	}
	revision.Provenance = []byte(`[{"kind":"ConfigMap","hash":"second"}]`)
	second, err := revision.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("configuration change did not change runtime revision")
	}
	if len(first.Short()) != 12 || !strings.HasPrefix(first.String(), first.Short()) {
		t.Fatalf("short revision %q is not a prefix of %q", first.Short(), first.String())
	}
}

func TestRevisionDigestIncludesConfig(t *testing.T) {
	revision := &Revision{Namespace: "agents", AgentName: "helper", ConfigJSON: []byte(`{"version":5,"runtime_telemetry":{"capture_content":false}}`)}
	first, err := revision.Digest()
	if err != nil {
		t.Fatal(err)
	}
	revision.ConfigJSON = []byte(`{"version":5,"runtime_telemetry":{"capture_content":true}}`)
	second, err := revision.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("compiled configuration change did not change runtime revision")
	}
}

func TestCompilationWarningsDoNotAffectRevisionDigest(t *testing.T) {
	compilation := &CompileResult{Revision: Revision{Namespace: "agents", AgentName: "helper"}}
	first, err := compilation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	compilation.Warnings = []string{"partial MCP selection is not enforced"}
	if len(compilation.Warnings) != 1 {
		t.Fatalf("warnings = %v, want one warning", compilation.Warnings)
	}
	second, err := compilation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("non-behavioral warning changed runtime revision")
	}
}

func TestRevisionDigestIncludesCommand(t *testing.T) {
	revision := &Revision{Namespace: "agents", AgentName: "helper", Command: []string{"/agent"}}
	first, err := revision.Digest()
	if err != nil {
		t.Fatal(err)
	}
	revision.Command = []string{"/other-agent"}
	second, err := revision.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("command change did not change runtime revision")
	}
}

func TestRevisionDigestIncludesBinaryAgentCard(t *testing.T) {
	card := &a2apb.AgentCard{Name: "assistant"}
	revision := &Revision{AgentCard: card}
	first, err := revision.Digest()
	require.NoError(t, err)
	card.Name = "changed"
	second, err := revision.Digest()
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	card.ProtoReflect().SetUnknown(protowire.AppendString(protowire.AppendTag(nil, 1000, protowire.BytesType), "future"))
	third, err := revision.Digest()
	require.NoError(t, err)
	require.NotEqual(t, second, third)
	data, err := proto.Marshal(card)
	require.NoError(t, err)
	revision.AgentCard = &a2apb.AgentCard{}
	require.NoError(t, proto.Unmarshal(data, revision.AgentCard))
	roundTrip, err := revision.Digest()
	require.NoError(t, err)
	require.Equal(t, third, roundTrip)
	revision.AgentCard.Name = string([]byte{0xff})
	_, err = revision.Digest()
	require.Error(t, err)
}

// e2bTestArtifact is a fixed resolved artifact the E2B digest tests bind to.
func e2bTestArtifact() E2BArtifact {
	return E2BArtifact{
		Installation:   "embed-lab",
		TemplateID:     "tmpl-1",
		BuildID:        "build-1",
		LaunchContract: json.RawMessage(`{"command":"nohup h &","configPath":"/c"}`),
	}
}

// TestE2BDigestIgnoresInactiveSubstratePlacement is the core oracle
// assertion: an authored-but-inactive Substrate placement policy and the raw
// provenance snapshot must not enter the E2B execution digest, so flipping
// only those fields leaves the revision identical.
func TestE2BDigestIgnoresInactiveSubstratePlacement(t *testing.T) {
	revision := &Revision{
		Namespace: "agents", AgentName: "helper", Image: "img@sha256:aaa",
		WorkerPoolName: "pool-a", SnapshotLocation: "s3://a", Provenance: []byte(`[{"hash":"one"}]`),
	}
	first, err := revision.E2BDigest(e2bTestArtifact())
	require.NoError(t, err)

	// Change only the inactive placement + provenance; identity must not move.
	revision.WorkerPoolName = "pool-b"
	revision.SnapshotLocation = "s3://b"
	revision.SandboxClass = atev1alpha1.SandboxClassMicroVM
	revision.Provenance = []byte(`[{"hash":"two"}]`)
	second, err := revision.E2BDigest(e2bTestArtifact())
	require.NoError(t, err)
	require.Equal(t, first, second, "inactive Substrate placement and provenance must not change the E2B digest")

	// The same placement change must move the Substrate digest — it is live there.
	subFirst, err := (&Revision{Namespace: "agents", AgentName: "helper", Image: "img@sha256:aaa", WorkerPoolName: "pool-a"}).Digest()
	require.NoError(t, err)
	subSecond, err := (&Revision{Namespace: "agents", AgentName: "helper", Image: "img@sha256:aaa", WorkerPoolName: "pool-b"}).Digest()
	require.NoError(t, err)
	require.NotEqual(t, subFirst, subSecond, "Substrate digest must still react to placement changes")
}

// TestE2BDigestChangesOnExecutionInputs asserts the resolved execution inputs
// that DO matter — compiled config, the artifact identity, and the selected
// launch contract — each move the E2B digest.
func TestE2BDigestChangesOnExecutionInputs(t *testing.T) {
	base := func() *Revision {
		return &Revision{Namespace: "agents", AgentName: "helper", Image: "img@sha256:aaa", ConfigJSON: []byte(`{"v":1}`)}
	}
	first, err := base().E2BDigest(e2bTestArtifact())
	require.NoError(t, err)

	mut := base()
	mut.ConfigJSON = []byte(`{"v":2}`)
	d, err := mut.E2BDigest(e2bTestArtifact())
	require.NoError(t, err)
	require.NotEqual(t, first, d, "config change must change E2B digest")

	for _, art := range []E2BArtifact{
		{Installation: "embed-lab", TemplateID: "tmpl-2", BuildID: "build-1", LaunchContract: e2bTestArtifact().LaunchContract},
		{Installation: "embed-lab", TemplateID: "tmpl-1", BuildID: "build-2", LaunchContract: e2bTestArtifact().LaunchContract},
		{Installation: "other", TemplateID: "tmpl-1", BuildID: "build-1", LaunchContract: e2bTestArtifact().LaunchContract},
	} {
		d, err := base().E2BDigest(art)
		require.NoError(t, err)
		require.NotEqual(t, first, d, "artifact change must change E2B digest")
	}

	d, err = base().E2BDigest(E2BArtifact{Installation: "embed-lab", TemplateID: "tmpl-1", BuildID: "build-1",
		LaunchContract: json.RawMessage(`{"command":"other","configPath":"/c"}`)})
	require.NoError(t, err)
	require.NotEqual(t, first, d, "launch contract change must change E2B digest")
}

// TestE2BDigestDiffersFromSubstrateDigest confirms the same revision produces
// distinct identities under the two providers — an E2B revision can never
// collide with a Substrate one.
func TestE2BDigestDiffersFromSubstrateDigest(t *testing.T) {
	revision := &Revision{Namespace: "agents", AgentName: "helper", Image: "img@sha256:aaa"}
	sub, err := revision.Digest()
	require.NoError(t, err)
	e2b, err := revision.E2BDigest(e2bTestArtifact())
	require.NoError(t, err)
	require.NotEqual(t, sub, e2b)
}
