package env

const (
	AuthModeInsecure     = "insecure"
	AuthModeTrustedProxy = "trusted-proxy"
)

var (
	AuthMode = RegisterStringVar(
		"KAGENT_AUTH_MODE", AuthModeInsecure,
		"Controller authentication mode: insecure or trusted-proxy. trusted-proxy requires an upstream credential-validating proxy and network isolation preventing bypass.", ComponentController,
	)
	AuthUserIDClaim = RegisterStringVar(
		"KAGENT_AUTH_USER_ID_CLAIM", "",
		"JWT claim used for the caller identity in trusted-proxy mode. Empty uses sub; a missing or empty custom claim falls back to sub.", ComponentController,
	)
	HTTPBindAddress = RegisterStringVar(
		"KAGENT_HTTP_BIND_ADDRESS", ":8083",
		"Listen address for the controller HTTP, gRPC, A2A, and MCP server.", ComponentController,
	)
	GRPCReflection = RegisterBoolVar(
		"KAGENT_GRPC_REFLECTION", false,
		"Enable gRPC server reflection on the controller.", ComponentController,
	)
	WatchNamespaces = RegisterStringVar(
		"KAGENT_WATCH_NAMESPACES", "",
		"Comma-separated namespaces to watch. Empty watches all namespaces.", ComponentController,
	)
)

// Shared settings read by logging and Kubernetes libraries.
var (
	LogLevel                = RegisterStringVar("KAGENT_LOG_LEVEL", "info", "Logging level for the controller, CLI, and Go/Python runtimes, including the Python ADK HTTP server: debug, info, warn, or error. Python also accepts standard Python logging levels.", ComponentController, ComponentCLI, ComponentAgentRuntime)
	Kubeconfig              = RegisterStringVar("KUBECONFIG", "", "Kubernetes client configuration file list for the controller, CLI Kubernetes operations, and tests. When unset, client-go uses its normal in-cluster or user kubeconfig discovery.", ComponentController, ComponentCLI, ComponentTesting)
	ExecutionSigningKeyFile = RegisterStringVar("KAGENT_EXECUTION_SIGNING_KEY_FILE", "", "File holding the HMAC key the controller uses to sign and verify E2B execution-provider runtime credentials. When set, the private runtime callback also accepts signed E2B credentials; when unset, only the Substrate identity path is served.", ComponentController)

	// GRPCTLSCertFile and GRPCTLSKeyFile terminate TLS on the combined
	// controller server (public HTTP/gRPC plus the private runtime callback).
	// They are optional: an E2B deployment sets both so the sandbox callback's
	// signed credential travels over an authenticated channel; an unset pair
	// keeps the plaintext default used by Substrate and local development.
	GRPCTLSCertFile = RegisterStringVar("KAGENT_GRPC_TLS_CERT_FILE", "", "PEM certificate file the controller server serves for TLS. Requires KAGENT_GRPC_TLS_KEY_FILE; unset keeps the plaintext listener.", ComponentController)
	GRPCTLSKeyFile  = RegisterStringVar("KAGENT_GRPC_TLS_KEY_FILE", "", "PEM private-key file matching KAGENT_GRPC_TLS_CERT_FILE. Requires KAGENT_GRPC_TLS_CERT_FILE; unset keeps the plaintext listener.", ComponentController)

	// E2BInstallations maps an E2B installation identity to its endpoint config
	// ({"<installation>": {"clientProxy": "http://host:port"}}). Installation
	// identities are immutable and recorded in execution bindings; the endpoint
	// is operator config, not durable state. The client-proxy endpoint must be
	// private to trusted control-plane clients.
	E2BInstallations = RegisterStringVar(
		"KAGENT_E2B_INSTALLATIONS",
		"",
		"JSON map of E2B installation identity to endpoint config, e.g. "+
			"{\"embed-lab\": {\"clientProxy\": \"http://127.0.0.1:3002\"}}. When empty, "+
			"E2B sessions cannot be dialed; Substrate stays the default provider.",
		ComponentController,
	)

	// E2BInstallation is the single installation identity the E2B provider
	// prepares revisions against — an explicit selection, never a first-key
	// guess from the installations map. Required when KAGENT_EXECUTION_PROVIDER
	// is 'e2b'; ignored otherwise.
	E2BInstallation = RegisterStringVar(
		"KAGENT_E2B_INSTALLATION",
		"",
		"The E2B installation identity (a key of KAGENT_E2B_INSTALLATIONS) the "+
			"provider prepares new revisions against. Required in E2B-only mode; "+
			"revisions pin it into their artifact binding.",
		ComponentController,
	)

	// ExecutionProvider selects the default runtime backend. The empty/substrate
	// default keeps every existing Substrate behavior. 'e2b' means E2B-only:
	// the controller does not dial ate-api or construct Substrate clients,
	// reconcilers or the standalone Sandbox/guest services — those operations
	// return FailedPrecondition. This is a deployment-mode decision, not a
	// per-session one; mixed deployments keep Substrate and route E2B sessions
	// through the installation map.
	ExecutionProvider = RegisterStringVar(
		"KAGENT_EXECUTION_PROVIDER",
		"",
		"Runtime provider mode: empty or 'substrate' keeps Substrate as the "+
			"default; 'e2b' runs E2B-only — no ate-api dial or Substrate clients, "+
			"standalone Sandbox/guest ops return FailedPrecondition.",
		ComponentController,
	)

	// E2BSandboxTimeout is the sandbox lifetime the create path sets. With the
	// provider's autoPause it is a timeout-to-pause bound: idle compute pauses
	// instead of being killed, and admitted tasks renew it via the timeout
	// endpoint. Zero leaves the provider's own default.
	E2BSandboxTimeout = RegisterDurationVar(
		"KAGENT_E2B_SANDBOX_TIMEOUT",
		0,
		"Lifetime an E2B sandbox is created with (e.g. 1h). With autoPause it "+
			"is a timeout-to-pause bound; admitted tasks renew it. Zero uses the "+
			"provider's own default.",
		ComponentController,
	)
)
