package grpcserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	systemservice "github.com/kagent-dev/kagent/go/core/internal/service/system"
	"github.com/kagent-dev/kagent/go/core/internal/service/taskstore"
	"github.com/kagent-dev/kagent/go/core/internal/version"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func testSystemService() *systemservice.Service {
	return systemservice.NewService(nil, nil, nil, nil)
}

func TestServerServesGRPCAndHTTP(t *testing.T) {
	oldVersion, oldCommit, oldDate := version.Version, version.GitCommit, version.BuildDate
	version.Version, version.GitCommit, version.BuildDate = "v1.2.3", "abc123", "2026-07-28"
	t.Cleanup(func() {
		version.Version, version.GitCommit, version.BuildDate = oldVersion, oldCommit, oldDate
	})

	listener := bufconn.Listen(1024 * 1024)
	server, err := New(Config{
		Listener:      listener,
		SystemService: testSystemService(),
		HTTPHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()

	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		cancel()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	response, err := apiv1alpha1.NewSystemServiceClient(connection).GetVersion(t.Context(), &apiv1alpha1.GetVersionRequest{})
	if err != nil {
		cancel()
		t.Fatalf("GetVersion() error = %v", err)
	}
	if response.GetKagentVersion() != "v1.2.3" || response.GetGitCommit() != "abc123" || response.GetBuildDate() != "2026-07-28" {
		t.Fatalf("GetVersion() = %+v", response)
	}

	healthResponse, err := grpc_health_v1.NewHealthClient(connection).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		cancel()
		t.Fatalf("Health.Check() error = %v", err)
	}
	if healthResponse.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("Health.Check() status = %v", healthResponse.GetStatus())
	}

	httpClient := &http.Client{Transport: &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) { return listener.Dial() },
	}}
	httpResponse, err := httpClient.Get("http://bufnet/healthz")
	if err != nil {
		cancel()
		t.Fatalf("HTTP GET error = %v", err)
	}
	_ = httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusNoContent {
		cancel()
		t.Fatalf("HTTP GET status = %v", httpResponse.Status)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gRPC server did not stop after context cancellation")
	}
}

func TestNewRejectsPartialTLSConfiguration(t *testing.T) {
	_, err := New(Config{TLSCertFile: "cert.pem", SystemService: testSystemService()})
	if err == nil {
		t.Fatal("New() error = nil, want partial TLS configuration error")
	}
}

func TestNewRejectsMissingSystemService(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("New() error = nil, want missing system service error")
	}
}

// The TLS listener completes the handshake before http.Server sees the
// connection, so http.Server never fills ALPN itself. gRPC clients require
// the h2 token; loadTLSConfig must advertise it or those clients fail the
// handshake.
func TestLoadTLSConfigAdvertisesH2(t *testing.T) {
	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))

	cfg, err := loadTLSConfig(certFile, keyFile)
	require.NoError(t, err)
	require.Contains(t, cfg.NextProtos, "h2")

	none, err := loadTLSConfig("", "")
	require.NoError(t, err)
	require.Nil(t, none)
}

// A real TLS gRPC round trip: the server must advertise h2 via ALPN or the
// client fails the handshake. This exercises the full listener->TLS->gRPC path
// the E2B callback uses, not just the tls.Config contents.
func TestTLSGRPCRoundTrip(t *testing.T) {
	dir := t.TempDir()
	caPool := x509.NewCertPool()
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ca"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, BasicConstraintsValid: true,
	}
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, _ := x509.ParseCertificate(caDER)
	caPool.AddCert(caCert)

	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "srv"},
		NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	require.NoError(t, err)
	certFile := filepath.Join(dir, "srv.pem")
	keyFile := filepath.Join(dir, "srv.key")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}), 0o600))
	srvKeyDER, _ := x509.MarshalECPrivateKey(srvKey)
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER}), 0o600))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server, err := New(Config{
		Listener: listener, TLSCertFile: certFile, TLSKeyFile: keyFile,
		SystemService: testSystemService(),
		Authenticator: &authimpl.InsecureAuthenticator{}, RuntimeAuthenticator: &taskstore.Authenticator{},
	})
	require.NoError(t, err)
	serverCtx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.Start(serverCtx) }()
	defer func() { stop(); require.NoError(t, <-done) }()

	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: caPool, ServerName: "localhost", MinVersion: tls.VersionTLS12})))
	require.NoError(t, err)
	defer conn.Close()
	health := grpc_health_v1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := health.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
}
