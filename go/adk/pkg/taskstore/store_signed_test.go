package taskstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/kagent-dev/kagent/go/adk/pkg/controllerclient"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
)

func TestSignedCallContextSendsBearerNotInsecure(t *testing.T) {
	dir := t.TempDir()
	credFile := filepath.Join(dir, "credential")
	if err := os.WriteFile(credFile, []byte("signed-token-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := NewSigned(&controllerclient.Client{}, filepath.Join(dir, "name"), credFile)
	ctx, cancel, err := s.callContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("no outgoing metadata")
	}
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer signed-token-1" {
		t.Fatalf("authorization = %v", got)
	}
	if got := md.Get("x-kagent-insecure-runtime-identity"); len(got) != 0 {
		t.Fatalf("insecure header sent in signed mode: %v", got)
	}
}

// An outgoing context already carrying the insecure runtime identity header
// must not leak it into a signed call: the runtime presents exactly one
// credential and never mixes the two authentication modes.
func TestSignedCallContextClearsInheritedInsecureHeader(t *testing.T) {
	dir := t.TempDir()
	credFile := filepath.Join(dir, "credential")
	if err := os.WriteFile(credFile, []byte("signed-token-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := NewSigned(&controllerclient.Client{}, filepath.Join(dir, "name"), credFile)
	parent := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		apia2a.InsecureRuntimeIdentityHeader, "team-a/session-x/uid",
		"x-share-token", "share", "x-user-id", "alice",
	))
	ctx, cancel, err := s.callContext(parent)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	md, _ := metadata.FromOutgoingContext(ctx)
	if got := md.Get(apia2a.InsecureRuntimeIdentityHeader); len(got) != 0 {
		t.Fatalf("insecure header leaked into signed call: %v", got)
	}
	if got := md.Get("x-share-token"); len(got) != 0 {
		t.Fatalf("share token leaked into signed call: %v", got)
	}
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer signed-token-1" {
		t.Fatalf("authorization = %v", got)
	}
}

func TestSignedCallContextRereadsCredential(t *testing.T) {
	dir := t.TempDir()
	credFile := filepath.Join(dir, "credential")
	s := NewSigned(&controllerclient.Client{}, filepath.Join(dir, "name"), credFile)
	for _, token := range []string{"token-a", "token-b"} {
		if err := os.WriteFile(credFile, []byte(token), 0o600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel, err := s.callContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		md, _ := metadata.FromOutgoingContext(ctx)
		if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer "+token {
			cancel()
			t.Fatalf("authorization = %v, want Bearer %s", got, token)
		}
		cancel()
	}
}

func TestSignedCallContextFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s := NewSigned(&controllerclient.Client{}, filepath.Join(dir, "name"), filepath.Join(dir, "credential"))
	if _, _, err := s.callContext(context.Background()); err == nil {
		t.Fatal("expected failure on missing credential")
	}
	if err := os.WriteFile(filepath.Join(dir, "credential"), []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.callContext(context.Background()); err == nil {
		t.Fatal("expected failure on empty credential")
	}
}
