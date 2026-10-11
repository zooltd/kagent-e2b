// Package app boots the kagent controller.
//
// It exists so that the controller can be started by something other than this
// repository's own main package. The only parts of it a library consumer may
// replace are authentication and authorization; everything else — the database,
// the controller manager, the Substrate connection, the A2A gateway and the gRPC
// server — is core's to build. Those are not policy decisions, and a component
// that reaches agent runtimes should not take whatever handler a library
// consumer happens to assemble for it.
package app

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	apia2a "github.com/kagent-dev/kagent/go/api/a2a"
	kagentv1alpha3 "github.com/kagent-dev/kagent/go/api/v1alpha3"
	"github.com/kagent-dev/kagent/go/core/internal/a2agateway"
	"github.com/kagent-dev/kagent/go/core/internal/controller"
	mcpservercontroller "github.com/kagent-dev/kagent/go/core/internal/controller/mcpserver"
	remotemcpcontroller "github.com/kagent-dev/kagent/go/core/internal/controller/remotemcpserver"
	scheduledruncontroller "github.com/kagent-dev/kagent/go/core/internal/controller/scheduledrun"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/e2b"
	"github.com/kagent-dev/kagent/go/core/internal/grpcserver"
	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/mcp"
	"github.com/kagent-dev/kagent/go/core/internal/service/checkpoint"
	"github.com/kagent-dev/kagent/go/core/internal/service/kubecrud"
	memoryservice "github.com/kagent-dev/kagent/go/core/internal/service/memory"
	modelservice "github.com/kagent-dev/kagent/go/core/internal/service/model"
	prompttemplateservice "github.com/kagent-dev/kagent/go/core/internal/service/prompttemplate"
	sandboxservice "github.com/kagent-dev/kagent/go/core/internal/service/sandbox"
	"github.com/kagent-dev/kagent/go/core/internal/service/scheduledrun"
	sessionsvc "github.com/kagent-dev/kagent/go/core/internal/service/session"
	systemservice "github.com/kagent-dev/kagent/go/core/internal/service/system"
	"github.com/kagent-dev/kagent/go/core/internal/service/taskstore"
	toolservice "github.com/kagent-dev/kagent/go/core/internal/service/tool"
	"github.com/kagent-dev/kagent/go/core/internal/substrate"
	"github.com/kagent-dev/kagent/go/core/internal/translator"
	"github.com/kagent-dev/kagent/go/core/internal/version"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	kagentenv "github.com/kagent-dev/kagent/go/core/pkg/env"
	"github.com/kagent-dev/kagent/go/core/pkg/migrations"
	"github.com/kagent-dev/kagent/go/pkg/logging"
	"github.com/kagent-dev/kagent/go/pkg/telemetry"
	kmcp "github.com/kagent-dev/kmcp/api/v1alpha1"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Options are the components a library consumer may supply in place of core's own.
//
// A nil field is not an error: it selects the library default. The shipped
// controller selects its authenticator from environment settings. Supplying
// one does not change how core uses it — the authenticator still guards the
// gRPC server and the /mcp endpoint, and the authorizer is still consulted by
// every service that takes one — so a library consumer cannot narrow where
// its own policy applies.
type Options struct {
	// Authenticator identifies the caller. Nil selects InsecureAuthenticator,
	// which admits every request.
	Authenticator auth.AuthProvider
	// Authorizer decides what an identified caller may do and which collection
	// entries it may see. Nil selects NoopAuthorizer, which permits every action.
	Authorizer auth.CollectionAuthorizer
	// SetupWithManager registers additional controllers and scheme types on
	// core's manager. It runs after the manager exists and before it starts, so
	// a scheme added here is in place before any cache is built. Returning an
	// error aborts startup.
	//
	// This exists so that a library consumer does not have to run a second manager
	// alongside core's: two managers means two caches of the same objects and a
	// second leader election to keep consistent with the first.
	SetupWithManager func(manager.Manager) error
	// ExtraMigrations are applied after the built-in tracks, in the order
	// given. A library consumer that owns tables uses this rather than migrating
	// separately, so that one run leaves the database wholly at one version.
	// Each source must set Schema.
	ExtraMigrations []migrations.Source
	// GRPCServices registers additional services on core's gRPC server, so a
	// consumer's API shares core's transport, authenticator and interceptors
	// instead of standing up a second server on another port.
	//
	// Every method it registers needs an entry in MethodPolicies: the
	// authentication interceptor refuses a method it has no policy for, so a
	// service registered without one is closed rather than open.
	GRPCServices func(grpc.ServiceRegistrar)
	// MethodPolicies declares access for the methods GRPCServices registers,
	// keyed by full method name.
	//
	// Run fails if a key names one of core's own methods. A consumer describes
	// its own surface here; letting it reclassify core's would turn a config
	// field into a way to make an authenticated method public.
	MethodPolicies map[string]auth.AccessMode
}

// resolve substitutes core's defaults for whichever components the caller left
// nil. It never returns a nil component, so callers do not have to check.
func (o Options) resolve() (auth.AuthProvider, auth.CollectionAuthorizer) {
	authenticator := o.Authenticator
	if authenticator == nil {
		authenticator = &authimpl.InsecureAuthenticator{}
	}
	authorizer := o.Authorizer
	if authorizer == nil {
		authorizer = &auth.NoopAuthorizer{}
	}
	return authenticator, authorizer
}

// SetupLogger installs the controller-runtime logger, at the level named by
// KAGENT_LOG_LEVEL.
//
// Run calls this itself, so a library consumer needs it only when it logs
// before Run — and it must then call it first, because controller-runtime
// discards everything written through log.Log until the first SetLogger, and
// Run's call comes after the consumer has already built its Options. A startup
// error reported in that window is otherwise lost, which reads as a process
// that died silently.
//
// Calling it twice is harmless. SetLogger fulfils a promise that can only be
// fulfilled once, so the first caller wins and Run's own call does nothing.
func SetupLogger() error {
	logger, err := logging.NewFromEnv(os.Stderr)
	if err != nil {
		return fmt.Errorf("parse KAGENT_LOG_LEVEL: %w", err)
	}
	slog.SetDefault(logger)
	ctrl.SetLogger(logging.AsLogr(logger))
	return nil
}

// Run boots the controller and blocks until ctx is cancelled or a component
// fails. It returns the first error rather than exiting, so a library consumer
// keeps control of how the process ends.
func Run(ctx context.Context, opts Options) error {
	if err := SetupLogger(); err != nil {
		return err
	}
	quiescenceInterval := kagentenv.SessionQuiescencePollInterval.Get()
	if quiescenceInterval <= 0 {
		return fmt.Errorf("%s must be positive", kagentenv.SessionQuiescencePollInterval.Name())
	}
	dbURL := kagentenv.PostgresDatabaseURL.Get()
	if dbURL == "" {
		return fmt.Errorf("%s is required", kagentenv.PostgresDatabaseURL.Name())
	}
	if err := database.ValidateURL(dbURL); err != nil {
		return fmt.Errorf("validate database connection: %w", err)
	}
	logger := slog.Default()
	ctx = logging.IntoContext(ctx, logger)
	_, telemetryWarnings := translator.TelemetryConfigFromProcess()
	for _, warning := range telemetryWarnings {
		logger.WarnContext(ctx, "invalid agent telemetry configuration; disabling signal", "error", warning)
	}
	telemetryOptions := telemetry.Options{Defaults: []attribute.KeyValue{
		semconv.ServiceName("kagent-controller"), semconv.ServiceNamespace("kagent"), semconv.ServiceVersion(version.Version),
	}}
	if metricsBindAddress() != "0" {
		reader, err := otelprometheus.New(otelprometheus.WithRegisterer(crmetrics.Registry))
		if err != nil {
			return fmt.Errorf("create Prometheus metric reader: %w", err)
		}
		telemetryOptions.MetricReaders = []sdkmetric.Reader{reader}
	}
	// otelgrpc snapshots the global providers and propagator when its handler is
	// constructed, so telemetry has to be registered before any server is built.
	providers, err := telemetry.Init(ctx, telemetryOptions)
	if err != nil {
		logger.ErrorContext(ctx, "failed to initialize telemetry", "error", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			logger.ErrorContext(shutdownCtx, "failed to shut down telemetry", "error", err)
		}
	}()

	// Resolve the execution provider and reject incompatible auth/provider
	// combinations before any side-effects (migrations, manager, Substrate
	// dial). A signing-key + Substrate deployment would silently break every
	// guest callback, so it fails fast here.
	executionProvider := strings.ToLower(strings.TrimSpace(kagentenv.ExecutionProvider.Get()))
	e2bOnly := executionProvider == "e2b"
	if err := validateExecutionProviderConfig(executionProvider, kagentenv.ExecutionSigningKeyFile.Get()); err != nil {
		return err
	}

	vectorEnabled := kagentenv.DatabaseVectorEnabled.Get()
	dbRole := kagentenv.DatabaseRole.Get()
	sources, err := migrationSources(vectorEnabled, opts.ExtraMigrations)
	if err != nil {
		return err
	}
	if kagentenv.SkipMigrations.Get() {
		if err := migrations.VerifyMigratedAsRole(ctx, dbURL, dbRole, sources); err != nil {
			return fmt.Errorf("verify database migrations: %w", err)
		}
	} else if err := migrations.RunUpAsRole(ctx, dbURL, dbRole, sources); err != nil {
		return fmt.Errorf("run database migrations: %w", err)
	}
	db, err := database.Connect(ctx, postgresConfigFromEnv(dbURL, vectorEnabled))
	if err != nil {
		return err
	}
	defer db.Close()
	store := database.NewClient(db, env(kagentenv.DatabaseVectorSchema))

	kubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes config: %w", err)
	}
	// The manager's client is what serves the Harness and AgentTemplate RPCs, so
	// it needs v1alpha3 in its scheme; the controller-runtime default carries
	// only the built-in kinds and would fail every one of those calls at runtime
	// rather than at startup.
	managerScheme := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(managerScheme))
	utilruntime.Must(kagentv1alpha3.AddToScheme(managerScheme))
	utilruntime.Must(atev1alpha1.AddToScheme(managerScheme))
	utilruntime.Must(kmcp.AddToScheme(managerScheme))
	watchNamespaces := namespaces(kagentenv.WatchNamespaces.Get())
	managerClientOptions := client.Options{}
	managerCacheOptions := cache.Options{DefaultNamespaces: namespaceCache(watchNamespaces)}
	if len(watchNamespaces) > 0 {
		// A namespaced Role cannot list cluster-scoped Namespace objects. Read them
		// directly so SystemService can fall back to the configured names on a
		// Forbidden response without a failing Namespace informer blocking startup.
		managerClientOptions.Cache = &client.CacheOptions{DisableFor: []client.Object{&corev1.Namespace{}}}
	}
	metricsOptions := metricsserver.Options{
		BindAddress:   metricsBindAddress(),
		SecureServing: kagentenv.MetricsSecure.Get(),
	}
	if metricsOptions.SecureServing {
		// SecureServing alone only encrypts. The filter authenticates the scraper
		// with a TokenReview and authorizes it with a SubjectAccessReview on the
		// /metrics nonResourceURL.
		metricsOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	manager, err := ctrl.NewManager(kubeConfig, ctrl.Options{
		Scheme:                  managerScheme,
		Cache:                   managerCacheOptions,
		Client:                  managerClientOptions,
		Metrics:                 metricsOptions,
		LeaderElection:          kagentenv.LeaderElect.Get(),
		LeaderElectionID:        "0e9f6799.kagent.dev",
		LeaderElectionNamespace: env(kagentenv.KagentNamespace),
	})
	if err != nil {
		return fmt.Errorf("create controller manager: %w", err)
	}
	// Parse the E2B installation map before the collection graph so the
	// reconciler's E2B preparation can resolve workload->artifact mappings.
	// The provider is a deployment mode; the selected installation is explicit.
	// executionProvider and e2bOnly were resolved and validated above.
	var e2bInstallations map[string]a2agateway.E2BInstallation
	var e2bInstallation string
	if raw := kagentenv.E2BInstallations.Get(); raw != "" {
		e2bInstallations, err = a2agateway.E2BInstallations(raw)
		if err != nil {
			return err
		}
	}
	if e2bOnly {
		e2bInstallation = kagentenv.E2BInstallation.Get()
		if e2bInstallation == "" {
			return fmt.Errorf("KAGENT_E2B_INSTALLATION must select the installation KAGENT_EXECUTION_PROVIDER=e2b prepares revisions against")
		}
		if _, ok := e2bInstallations[e2bInstallation]; !ok {
			return fmt.Errorf("KAGENT_E2B_INSTALLATION %q is not a key of KAGENT_E2B_INSTALLATIONS", e2bInstallation)
		}
	}
	runtime, err := controller.NewRuntime(kubeConfig, watchNamespaces, ctx.Done(), executionProvider, e2bInstallations, e2bInstallation)
	if err != nil {
		return err
	}
	// In E2B-only mode there is no ate-api to dial: Substrate actor calls are
	// served by a disabled stub that returns FailedPrecondition, so services
	// constructed with it fail closed rather than panic on nil. The standalone
	// Sandbox/guest services and Substrate reconcilers are skipped below.
	var actors substrate.Actors
	if e2bOnly {
		actors = substrate.DisabledActorClient()
	} else {
		dialed, err := substrate.Dial(ctx, substrate.Config{
			AteAPIEndpoint: env(kagentenv.SubstrateATEAPIEndpoint),
			CAFile:         kagentenv.SubstrateATEAPICAFile.Get(),
			ClientCertFile: kagentenv.SubstrateATEAPIClientCertFile.Get(),
			CallTimeout:    30 * time.Second,
		})
		if err != nil {
			return err
		}
		defer dialed.Close()
		actors = dialed
	}
	// The reconciler runs in both modes: under Substrate it applies
	// ActorTemplates; under E2B it records the resolved artifact binding.
	// templates is nil under E2B — the reconciler branches to the E2B path
	// before ever touching it.
	{
		reconciler, err := controller.NewReconciler(kubeConfig, runtime.Collections, store, actors, e2bArtifactReadiness(e2bInstallations))
		if err != nil {
			return err
		}
		if err := manager.Add(reconciler); err != nil {
			return fmt.Errorf("add reconciler to controller manager: %w", err)
		}
		if !e2bOnly {
			runtimeGC, err := controller.NewRuntimeRevisionGC(store, actors, kagentenv.RuntimeRevisionGCInterval.Get(), otel.GetMeterProvider())
			if err != nil {
				return fmt.Errorf("create runtime revision GC: %w", err)
			}
			if err := manager.Add(runtimeGC); err != nil {
				return fmt.Errorf("add runtime revision GC to controller manager: %w", err)
			}
		}
	}
	if opts.SetupWithManager != nil {
		if err := opts.SetupWithManager(manager); err != nil {
			return fmt.Errorf("set up library consumer controllers: %w", err)
		}
	}
	mcpClient := toolservice.NewRuntimeMCPClient(manager.GetClient())
	remoteMCPDiscovery := remotemcpcontroller.New(manager.GetClient(), mcpClient, store)
	if err := remoteMCPDiscovery.SetupWithManager(manager); err != nil {
		return fmt.Errorf("set up RemoteMCPServer discovery: %w", err)
	}
	mcpServerDiscovery := mcpservercontroller.New(manager.GetClient(), mcpClient, store)
	if err := mcpServerDiscovery.SetupWithManager(manager); err != nil {
		return fmt.Errorf("set up MCPServer discovery: %w", err)
	}

	authenticator, authorizer := opts.resolve()
	resourceNamespace := env(kagentenv.KagentNamespace)
	models := modelservice.NewService(manager.GetClient(), authorizer, resourceNamespace)
	tools := toolservice.NewService(manager.GetClient(), store, authorizer, resourceNamespace, mcpClient)
	prompts := prompttemplateservice.NewService(manager.GetClient(), authorizer)
	system := systemservice.NewService(manager.GetClient(), watchNamespaces, authorizer, actors)
	memory := memoryservice.NewService(store)
	// The E2B installation map was parsed before the collection graph above
	// and is shared by the gateway dialer and the session create path.
	// The create-path provider lookup resolves one installation to a sandbox
	// creator (its control-plane API client) plus the egress endpoints the
	// enforced allowlist and harness base_url need.
	var workflowOpts []sessionsvc.ActorWorkflowOption
	// e2bCredRefresh is the shared credential refresher the gateway dispatch
	// hook also uses; built only when E2B is configured with a signing key.
	var e2bCredRefresh *sessionsvc.E2BCredentialRefresher
	// The execution credential signer is shared by the E2B bootstrap probe and
	// the runtime authenticator; it is only present when a signing key is
	// configured. It is read once here and reused by both.
	var executionAuth *taskstore.ExecutionAuthenticator
	if keyFile := kagentenv.ExecutionSigningKeyFile.Get(); keyFile != "" {
		// A signing key switches the single runtime-callback endpoint to
		// signed-only auth; validateExecutionProviderConfig already rejected
		// the key + non-E2B combination before any startup side-effects.
		key, err := os.ReadFile(keyFile)
		if err != nil {
			return fmt.Errorf("read execution signing key: %w", err)
		}
		executionAuth, err = taskstore.NewExecutionAuthenticator(key)
		if err != nil {
			return fmt.Errorf("configure execution authenticator: %w", err)
		}
	}
	if len(e2bInstallations) > 0 {
		// The credential refresher is shared by the workflow resume path, the
		// background renewer, and the gateway dispatch hook — one refresher
		// serializes per binding so concurrent triggers cannot regress the
		// installed credential. It is wired only when a signing key exists so a
		// sign-less install never pretends to refresh.
		if executionAuth != nil {
			e2bCredRefresh = sessionsvc.NewE2BCredentialRefresher(store,
				func(installation string) (*sessionsvc.E2BProvider, error) {
					inst, ok := e2bInstallations[installation]
					if !ok || inst.API == "" {
						return nil, fmt.Errorf("E2B installation %q is not configured for credential refresh", installation)
					}
					client, err := e2b.New(e2b.Config{APIURL: inst.API, APIKey: inst.APIKey})
					if err != nil {
						return nil, err
					}
					return &sessionsvc.E2BProvider{
						Creator:      client,
						Lister:       client,
						Bootstrapper: E2BBootstrapperFactory(inst, executionAuth),
					}, nil
				})
		}
		// The orphan reaper sweeps provider sandboxes no live binding claims;
		// it shares the installation map so an unconfigured installation is
		// never listed. Only installations with a control-plane api endpoint
		// can be swept.
		var reaperInstallations []string
		for id, inst := range e2bInstallations {
			if inst.API != "" {
				reaperInstallations = append(reaperInstallations, id)
			}
		}
		if len(reaperInstallations) > 0 {
			orphanLookup := func(installation string) (controller.E2BOrphanLister, error) {
				inst, ok := e2bInstallations[installation]
				if !ok {
					return nil, fmt.Errorf("E2B installation %q is not configured", installation)
				}
				return e2b.New(e2b.Config{APIURL: inst.API, APIKey: inst.APIKey})
			}
			// The late-binding reconciler runs ahead of the orphan reaper's
			// delete so a sandbox whose create response was lost is adopted by
			// its open operation before the reaper could reap it as unbound.
			if err := manager.Add(controller.NewE2BLateBindingReconciler(store, reaperInstallations, orphanLookup)); err != nil {
				return fmt.Errorf("add E2B late-binding reconciler to controller manager: %w", err)
			}
			reaper := controller.NewE2BOrphanReaper(store, reaperInstallations,
				orphanLookup)
			if err := manager.Add(reaper); err != nil {
				return fmt.Errorf("add E2B orphan reaper to controller manager: %w", err)
			}
			// The timeout renewer keeps a bound sandbox's lifetime ahead of its
			// running tasks so a mid-turn compute is never killed or paused. It
			// shares the installation map; a configured-but-lifetime-less
			// installation yields no renewal (the sandbox ages to its
			// timeout-to-pause bound).
			if err := manager.Add(controller.NewE2BTimeoutRenewer(store,
				func(installation string) (controller.E2BTimeoutSetter, int32, error) {
					inst, ok := e2bInstallations[installation]
					if !ok || inst.API == "" {
						return nil, 0, fmt.Errorf("E2B installation %q is not configured for timeout renewal", installation)
					}
					// Zero uses the provider default at create, but an admitted
					// task must never renew under the resume floor — resolve it
					// to the shared resume lifetime rather than skipping renewal.
					configured := int32(kagentenv.E2BSandboxTimeout.Get() / time.Second)
					var lifetime int32
					switch {
					case configured < 0:
						return nil, 0, fmt.Errorf("E2B sandbox timeout %ds is invalid: negative lifetime cannot renew an admitted task", configured)
					case configured == 0:
						// Omitted: resolve to the resume floor.
						lifetime = e2b.ResumeLifetimeSeconds
					case configured < controller.MinRenewableLifetimeSeconds:
						// A positive lifetime shorter than the renewal cadence
						// lets an admitted task pause between sweeps. Reject it
						// rather than silently upgrading the operator's intent.
						return nil, 0, fmt.Errorf("E2B sandbox timeout %ds is unsafe: below the %ds renewal floor, an admitted task could pause mid-turn", configured, controller.MinRenewableLifetimeSeconds)
					default:
						lifetime = configured
					}
					client, err := e2b.New(e2b.Config{APIURL: inst.API, APIKey: inst.APIKey})
					if err != nil {
						return nil, 0, err
					}
					return client, lifetime, nil
				})); err != nil {
				return fmt.Errorf("add E2B timeout renewer to controller manager: %w", err)
			}
			// The loss reaper revokes a live binding whose bound sandbox is
			// gone, killing the credential minted against that compute so a
			// stale one stops authorizing callbacks. Only a definitive 404
			// revokes; any uncertainty leaves the binding live and the dialer
			// reports runtime loss at use time instead.
			if err := manager.Add(controller.NewE2BLossReaper(store,
				func(installation string) (controller.E2BLossInspector, error) {
					inst, ok := e2bInstallations[installation]
					if !ok || inst.API == "" {
						return nil, fmt.Errorf("E2B installation %q is not configured for loss inspection", installation)
					}
					return e2b.New(e2b.Config{APIURL: inst.API, APIKey: inst.APIKey})
				})); err != nil {
				return fmt.Errorf("add E2B loss reaper to controller manager: %w", err)
			}
			// The credential renewer keeps each live binding's runtime
			// credential ahead of expiry for running sandboxes; a paused one is
			// refreshed when work wakes it, never by the sweep.
			// Register only when a refresher was actually built: e2bCredRefresh
			// is a concrete pointer that is nil without a signing key, and a
			// typed-nil into the interface defeats the renewer's nil guard.
			if e2bCredRefresh != nil {
				if err := manager.Add(controller.NewE2BCredentialRenewer(store, e2bCredRefresh)); err != nil {
					return fmt.Errorf("add E2B credential renewer to controller manager: %w", err)
				}
			}
		}
		if e2bCredRefresh != nil {
			workflowOpts = append(workflowOpts, sessionsvc.WithE2BCredentialRefresher(e2bCredRefresh))
		}
		workflowOpts = append(workflowOpts, sessionsvc.WithE2BProvider(
			func(installation string) (*sessionsvc.E2BProvider, error) {
				inst, ok := e2bInstallations[installation]
				if !ok {
					return nil, fmt.Errorf("E2B installation %q is not configured", installation)
				}
				if inst.API == "" {
					return nil, fmt.Errorf("E2B installation %q has no control-plane api endpoint", installation)
				}
				client, err := e2b.New(e2b.Config{APIURL: inst.API, APIKey: inst.APIKey})
				if err != nil {
					return nil, err
				}
				return &sessionsvc.E2BProvider{
					Creator: client,
					Config: sessionsvc.E2BCreateConfig{
						BrokerAddress:         inst.BrokerAddress,
						CallbackAddress:       inst.CallbackAddress,
						SandboxTimeoutSeconds: int32(kagentenv.E2BSandboxTimeout.Get() / time.Second),
					},
					// The same control-plane client lists sandboxes by
					// correlation metadata, so a lost create reconciles to the
					// sandbox it placed instead of leaving an orphan.
					Lister: client,
					// The bootstrap probe activates the bound sandbox: only set
					// when the install declares its machine template's harness
					// contract AND a signing key is configured — without a
					// signer there is no credential to write, so activation is
					// impossible and create stays at the pre-launch milestone.
					Bootstrapper: E2BBootstrapperFactory(inst, executionAuth),
				}, nil
			}))
	}
	quiescenceWake := make(chan struct{}, 1)
	sessionWorkflow := sessionsvc.NewActorWorkflow(store, actors, quiescenceWake, quiescenceInterval, workflowOpts...)
	runtimeTasks := taskstore.NewService(store, quiescenceWake)
	if err := manager.Add(sessionWorkflow); err != nil {
		return fmt.Errorf("register idle session worker: %w", err)
	}
	expiration, err := sessionsvc.NewExpirationWorker(store, sessionWorkflow, kagentenv.SessionIdleTTL.Get(), kagentenv.SessionExpirationPollInterval.Get())
	if err != nil {
		return err
	}
	if err := manager.Add(expiration); err != nil {
		return fmt.Errorf("register session expiration worker: %w", err)
	}
	shareMaxTTL := kagentenv.SessionShareMaxTTL.Get()
	if shareMaxTTL < 0 {
		return fmt.Errorf("%s must not be negative", kagentenv.SessionShareMaxTTL.Name())
	}
	sessions := sessionsvc.NewService(store, authorizer, sessionWorkflow, sessionsvc.WithShareMaxTTL(shareMaxTTL))
	checkpoints := checkpoint.NewService(store, authorizer, actors, sessionWorkflow)
	var substrateDialer *a2agateway.RuntimeDialer
	if !e2bOnly {
		substrateDialer, err = a2agateway.NewRuntimeDialer(
			kagentenv.SubstrateAtenetRouterURL.Get(),
			authenticator,
		)
		if err != nil {
			return err
		}
	}
	// E2B sessions route through the same runtimeDialer boundary as Substrate.
	// The installation map was parsed above and is optional; an unconfigured
	// E2B session fails closed rather than falling back to Substrate.
	var e2bDialer *a2agateway.E2BDialer
	if len(e2bInstallations) > 0 {
		// e2bCredRefresh is a concrete pointer that is nil without a signing
		// key. Passing it to the interface parameter directly would produce a
		// non-nil interface wrapping a nil refresher, defeating the dialer's
		// nil check — assign it only inside the guard so a genuine nil
		// interface is passed when refresh is disabled.
		var dispatchEnsure a2agateway.E2BCredentialEnsure
		if e2bCredRefresh != nil {
			dispatchEnsure = e2bCredRefresh
		}
		e2bDialer, err = a2agateway.NewE2BDialer(store, e2bInstallations, authenticator,
			dispatchEnsure,
			func(installation string) (a2agateway.E2BSandboxInspector, error) {
				inst, ok := e2bInstallations[installation]
				if !ok {
					return nil, fmt.Errorf("E2B installation %q is not configured", installation)
				}
				if inst.API == "" {
					return nil, fmt.Errorf("E2B installation %q has no api endpoint for liveness", installation)
				}
				return e2b.New(e2b.Config{APIURL: inst.API, APIKey: inst.APIKey})
			})
		if err != nil {
			return err
		}
	}
	gatewayDialer := a2agateway.NewProviderDialer(substrateDialer, e2bDialer)
	agents := kubecrud.NewService(manager.GetClient(), authorizer, &kagentv1alpha3.Agent{}, &kagentv1alpha3.AgentList{}, "Agent")
	interactions := sessionsvc.NewInteractionService(store, agents, sessions)
	pushSender := sessionsvc.NewHTTPPushSender(5*time.Second,
		kagentenv.A2APushAllowHTTP.Get(), kagentenv.A2APushAllowPrivateNetworks.Get())
	pushIssuer := cmp.Or(kagentenv.A2APushIssuer.Get(), kagentenv.KagentGatewayURL.Get(), "http://127.0.0.1:8083")
	pushSigner, err := sessionsvc.NewPushJWTSigner(kagentenv.A2APushSigningPrivateKey.Get(), strings.TrimRight(pushIssuer, "/"))
	if err != nil {
		return fmt.Errorf("configure push JWT signing: %w", err)
	}
	if err := manager.Add(sessionsvc.NewPushWorker(store, pushSender, pushSigner)); err != nil {
		return fmt.Errorf("failed to add push worker: %w", err)
	}
	gateway := a2agateway.New(interactions, gatewayDialer, cmp.Or(kagentenv.KagentGatewayURL.Get(), "http://127.0.0.1:8083"))
	schedules := scheduledrun.NewService(store, manager.GetClient(), authorizer)
	if err := manager.Add(scheduledruncontroller.NewScheduler(store, kagentenv.ScheduledRunPollInterval.Get())); err != nil {
		return fmt.Errorf("add scheduled run scheduler: %w", err)
	}
	if err := manager.Add(scheduledruncontroller.NewController(store, sessionWorkflow,
		gateway, kagentenv.ScheduledRunExecutionPollInterval.Get())); err != nil {
		return fmt.Errorf("add scheduled run controller: %w", err)
	}
	sandboxTemplates := kubecrud.NewService(manager.GetClient(), authorizer, &kagentv1alpha3.SandboxTemplate{}, &kagentv1alpha3.SandboxTemplateList{}, kagentv1alpha3.SandboxTemplateKind)
	var guests *sandboxservice.GuestDialer
	if !e2bOnly {
		var gerr error
		guests, gerr = sandboxservice.NewGuestDialer(kagentenv.SubstrateAtenetRouterURL.Get(), authenticator)
		if gerr != nil {
			return gerr
		}
		defer guests.Close()
		policy := substrate.SandboxPolicy{
			GuestImage: env(kagentenv.SandboxGuestImage),
			CPU:        kagentenv.SandboxCPU.Get(),
			Memory:     kagentenv.SandboxMemory.Get(),
		}
		preparation, err := controller.NewSandboxReconciler(kubeConfig, runtime, store, actors, policy)
		if err != nil {
			return err
		}
		if err := manager.Add(preparation); err != nil {
			return err
		}
	}
	sandboxes, err := sandboxservice.NewService(sandboxservice.Config{Store: store, Kube: manager.GetClient(), Authorizer: authorizer, Actors: actors, Guests: guests,
		DefaultTTL: kagentenv.SandboxDefaultTTL.Get(), MaxTTL: kagentenv.SandboxMaxTTL.Get(), ExpirationPollInterval: kagentenv.SandboxExpirationPollInterval.Get(),
		// E2B-only: standalone sandbox + guest/PTY/file ops return
		// FailedPrecondition rather than driving a Substrate runtime that does
		// not exist. Reads still work so the API stays honest.
		Disabled: e2bOnly})
	if err != nil {
		return err
	}
	if err := manager.Add(sandboxes); err != nil {
		return err
	}
	mcpHandler, err := mcp.New(sessions, checkpoints, gateway, sandboxes, sandboxTemplates)
	if err != nil {
		return err
	}
	policies, err := mergePolicies(grpcserver.DefaultMethodPolicies(), opts.MethodPolicies)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", rootHealthHandler)
	if pushSigner != nil {
		mux.Handle("GET /.well-known/jwks.json", pushSigner)
	} else {
		mux.HandleFunc("GET /.well-known/jwks.json", http.NotFound)
	}
	mux.Handle("/mcp", otelhttp.NewHandler(auth.AuthnMiddleware(authenticator)(mcpHandler), "/mcp"))
	mux.Handle(a2agateway.HTTPPathPrefix, otelhttp.NewHandler(a2agateway.NewHTTPHandler(gateway, authenticator, store), a2agateway.HTTPPathPrefix))
	// The private runtime callback's trust model is fixed by configuration.
	// When an E2B signing key is set the endpoint is signed-only: every runtime
	// call must carry a valid signed credential and the insecure Substrate
	// identity header is rejected, never a fallback. Without a key the
	// endpoint serves the legacy Substrate identity path only.
	runtimeAuthenticator := taskstore.NewRuntimeAuthenticator()
	if executionAuth != nil {
		runtimeAuthenticator = taskstore.NewSignedRuntimeAuthenticator(executionAuth)
	}
	server, err := grpcserver.New(grpcserver.Config{
		MethodPolicies:        policies,
		RegisterServices:      opts.GRPCServices,
		BindAddress:           env(kagentenv.HTTPBindAddress),
		TLSCertFile:           kagentenv.GRPCTLSCertFile.Get(),
		TLSKeyFile:            kagentenv.GRPCTLSKeyFile.Get(),
		Reflection:            kagentenv.GRPCReflection.Get(),
		Authenticator:         authenticator,
		RuntimeAuthenticator:  runtimeAuthenticator,
		ShareStore:            store,
		ModelService:          models,
		ToolService:           tools,
		PromptTemplateService: prompts,
		SystemService:         system,
		MemoryService:         memory,
		TaskStoreService:      runtimeTasks,
		SessionService:        sessions,
		ScheduledRunService:   schedules,
		// Author Agents and their reusable configuration through the API.
		AgentService:           agents,
		AgentTemplateService:   kubecrud.NewService(manager.GetClient(), authorizer, &kagentv1alpha3.AgentTemplate{}, &kagentv1alpha3.AgentTemplateList{}, "AgentTemplate"),
		HarnessService:         kubecrud.NewService(manager.GetClient(), authorizer, &kagentv1alpha3.Harness{}, &kagentv1alpha3.HarnessList{}, "Harness"),
		SandboxTemplateService: sandboxTemplates,
		SandboxService:         sandboxes,
		CheckpointService:      checkpoints,
		A2AHandler:             gateway,
		HTTPHandler:            mux,
	})
	if err != nil {
		return err
	}

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return runtime.Start(ctx) })
	group.Go(func() error { return manager.Start(ctx) })
	group.Go(func() error { return server.Start(ctx) })
	return group.Wait()
}

// mergePolicies overlays a consumer's method policies onto core's defaults.
//
// A collision is an error rather than an override: core's methods keep the
// access core assigned them, so this cannot be used to make an authenticated
// method public.
func mergePolicies(defaults grpcserver.MethodPolicies, extra map[string]auth.AccessMode) (grpcserver.MethodPolicies, error) {
	merged := make(grpcserver.MethodPolicies, len(defaults)+len(extra))
	maps.Copy(merged, defaults)
	for method, access := range extra {
		if _, taken := defaults[method]; taken {
			return nil, fmt.Errorf("method policy for %s is core's to set", method)
		}
		merged[method] = access
	}
	return merged, nil
}

// env preserves the controller's default-on-empty behavior for string settings.
func env(variable kagentenv.StringVar) string {
	if value := variable.Get(); value != "" {
		return value
	}
	return variable.DefaultValue()
}

// rootHealthHandler answers the bare-root health probe. The mux pattern "/"
// matches every path, so without the explicit path check this handler would
// return 200 for any unmatched request — including RPC-shaped paths that no
// service owns. Restrict the success response to the root and let everything
// else fall through to a 404.
func rootHealthHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// migrationSources appends extra after the built-in tracks: the built-ins must reach
// their final version before a library consumer's tables, which may reference them.
// An extra source without a schema would resolve to the connection's default schema
// rather than Kagent's, so it is rejected.
func migrationSources(vectorEnabled bool, extra []migrations.Source) ([]migrations.Source, error) {
	for _, src := range extra {
		if src.Schema == "" {
			return nil, fmt.Errorf("extra migration source %q must set Schema", src.Name)
		}
	}
	builtins := migrations.BuiltinSourcesInSchema(vectorEnabled, kagentenv.DatabaseSchema.Get(), kagentenv.DatabaseVectorSchema.Get())
	return append(builtins, extra...), nil
}

func postgresConfigFromEnv(source string, vectorEnabled bool) *database.PostgresConfig {
	return &database.PostgresConfig{
		URL:             source,
		Role:            kagentenv.DatabaseRole.Get(),
		Schema:          kagentenv.DatabaseSchema.Get(),
		VectorSchema:    kagentenv.DatabaseVectorSchema.Get(),
		VectorEnabled:   vectorEnabled,
		MaxConns:        new(int32(kagentenv.PostgresDatabaseMaxConns.Get())),
		MinConns:        new(int32(kagentenv.PostgresDatabaseMinConns.Get())),
		MaxConnIdleTime: new(kagentenv.PostgresDatabaseMaxConnIdleTime.Get()),
		MaxConnLifetime: new(kagentenv.PostgresDatabaseMaxConnLifetime.Get()),
	}
}

// metricsBindAddress resolves KAGENT_METRICS_BIND_ADDRESS. controller-runtime reads an
// empty address as "unset" and falls back to :8080, so an empty value would
// serve metrics on a port nobody asked for. "0" disables the metrics server.
func metricsBindAddress() string {
	if address := kagentenv.MetricsBindAddress.Get(); address != "" {
		return address
	}
	return "0"
}

func namespaces(value string) []string {
	var result []string
	for namespace := range strings.SplitSeq(value, ",") {
		if namespace = strings.TrimSpace(namespace); namespace != "" {
			result = append(result, namespace)
		}
	}
	return result
}

func namespaceCache(names []string) map[string]cache.Config {
	if len(names) == 0 {
		return nil
	}
	result := make(map[string]cache.Config, len(names))
	for _, name := range names {
		result[name] = cache.Config{}
	}
	return result
}

// E2BBootstrapperFactory builds the per-sandbox activation probe for one
// installation. It returns nil — leaving create at the pre-launch milestone —
// when no execution signing key is configured or the install has no proxy:
// without a signer there is no credential to write, so activation is
// impossible rather than merely absent. The launch contract arrives with each
// bound sandbox from the revision's persisted binding — a frozen value, not
// live install config — so a later workload-mapping change cannot re-map an
// existing revision's bootstrap.
func E2BBootstrapperFactory(inst a2agateway.E2BInstallation, auth *taskstore.ExecutionAuthenticator) sessionsvc.E2BBootstrapperFactory {
	if auth == nil || inst.ClientProxy == "" {
		return nil
	}
	proxy := strings.TrimSuffix(inst.ClientProxy, "/")
	return func(sandbox *e2b.Sandbox, launchContract []byte) (sessionsvc.E2BBootstrapper, error) {
		var h a2agateway.E2BHarnessSpec
		if err := json.Unmarshal(launchContract, &h); err != nil {
			return nil, fmt.Errorf("decode persisted launch contract: %w", err)
		}
		// The contract files are written as the harness user so they are owned
		// by it (envd applies the username param as the file owner); ownership
		// alone is not isolation — the prepare step also creates the contract
		// directory mode 0700 owned by that user, so no other guest account can
		// read the credential within.
		envd, err := e2b.NewEnvdClient(proxy, sandbox.SandboxID, sandbox.EnvdAccessToken, h.User, nil)
		if err != nil {
			return nil, err
		}
		lifecycle, err := e2b.New(e2b.Config{APIURL: inst.API, APIKey: inst.APIKey})
		if err != nil {
			return nil, err
		}
		return e2b.NewBootstrapper(e2b.BootstrapperConfig{
			Envd:      envd,
			Lifecycle: lifecycle,
			Signer:    auth.SignExecutionCredential,
			Files: e2b.BootstrapFiles{
				ConfigPath:     h.ConfigPath,
				CredentialPath: h.CredentialPath,
				AgentCardPath:  h.AgentCardPath,
				// The signed TaskStore reads its session identity from the ADK's
				// fixed runtime-identity path, not an install-config path.
				IdentityPath: apia2a.RuntimeIdentityPath,
				CAPath:       h.CAPath,
				CAPEM:        []byte(inst.CallbackCABundle),
				ActivatePath: h.ActivatePath,
			},
			Prepare: bootstrapPrepare(h.PrepareCommand, h.ConfigPath, h.AgentCardPath, h.CredentialPath, h.CAPath, apia2a.RuntimeIdentityPath, h.ActivatePath),
			// Command carries the E2B-owned process environment — the
			// control-plane callback endpoint, the runtime credential and CA
			// paths, and the config/card file locations — so the launch
			// contract's wiring always wins over revision-supplied env and the
			// static command line never has to embed per-revision config.
			Command: &e2b.BootstrapCommand{
				Cmd:  h.Command,
				User: h.User, Cwd: h.Cwd,
				Envs: e2bContractEnvs(h, inst),
			},
			ReadyProbe: e2bReadyProbe(proxy, sandbox.SandboxID),
		})
	}
}

// e2bContractEnvs is the E2B-owned slice of the guest process environment —
// the wiring the revision environment may never override: the control-plane
// callback the harness calls back on (KAGENT_API_URL -> the installation's
// callback address, not the in-cluster DNS name the Substrate compiler bakes
// in), the credential and CA file locations the contract provisions, and the
// egress broker the model base_url resolves to. These are trusted installation
// values; a revision-supplied entry of the same name is dropped at merge so
// authored config cannot redirect the callback or claim a different credential
// path.
func e2bContractEnvs(h a2agateway.E2BHarnessSpec, inst a2agateway.E2BInstallation) map[string]string {
	envs := map[string]string{}
	// Contract-declared wiring first — the harness's own overrides (e.g. its
	// model base-url pointed at the credential broker). These are operator
	// values that always win over a same-named revision env entry.
	maps.Copy(envs, h.Env)
	// Point the harness's declared model base-url at the installation's
	// credential broker so model traffic and its credentials stay outside the
	// guest; an authored revision env of the same name never overrides it.
	if h.ModelURLEnv != "" && inst.BrokerAddress != "" {
		envs[h.ModelURLEnv] = inst.BrokerAddress
	}
	if inst.CallbackAddress != "" {
		envs[kagentenv.KagentAPIURL.Name()] = inst.CallbackAddress
	}
	if h.CredentialPath != "" {
		envs["KAGENT_RUNTIME_CREDENTIAL_PATH"] = h.CredentialPath
	}
	if h.CAPath != "" {
		envs["SSL_CERT_FILE"] = h.CAPath
	}
	if len(envs) == 0 {
		return nil
	}
	return envs
}

// e2bArtifactReadiness resolves an installation's control-plane client and
// reports whether the mapped template's build is present and ready. It returns
// nil when no E2B installations are configured so the reconciler's readiness
// check fails closed rather than promoting an unverifiable artifact.
func e2bArtifactReadiness(installations map[string]a2agateway.E2BInstallation) func(ctx context.Context, installation, templateID, buildID string) (bool, error) {
	if len(installations) == 0 {
		return nil
	}
	return func(ctx context.Context, installation, templateID, buildID string) (bool, error) {
		inst, ok := installations[installation]
		if !ok || inst.API == "" {
			return false, fmt.Errorf("E2B installation %q has no control-plane api endpoint", installation)
		}
		client, err := e2b.New(e2b.Config{APIURL: inst.API, APIKey: inst.APIKey})
		if err != nil {
			return false, err
		}
		bs, err := client.GetTemplateBuildStatus(ctx, templateID, buildID)
		if err != nil {
			if e2b.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		// Verify the returned record is the requested artifact, not a build the
		// template happened to surface; a mismatched identity is not ready.
		return bs.TemplateID == templateID && bs.BuildID == buildID && bs.Status == "ready", nil
	}
}

// validateExecutionProviderConfig rejects an execution configuration that
// would silently break the enabled provider's runtime callbacks. It is a
// pure check — Run calls it before creating the manager or dialing Substrate
// so an invalid deployment fails at startup, not after side-effects.
//
// A signing key switches the single runtime-callback endpoint to signed-only
// auth, which the insecure Substrate identity header can never satisfy — so a
// signing key while Substrate is enabled is rejected. E2B-only mode may run
// with or without a key (keyless only for evaluation; a real E2B deployment
// needs one for the activation probe, and a create then fails in preparation,
// which is a separate, already-enforced precondition).
func validateExecutionProviderConfig(provider, signingKeyFile string) error {
	p := strings.ToLower(strings.TrimSpace(provider))
	hasKey := signingKeyFile != ""
	switch p {
	case "e2b":
		// Supported: signing key + e2b is the real E2B deployment; keyless is
		// evaluation-only (a create fails in preparation without a credential
		// to write, an already-enforced precondition).
	case "", "substrate":
		// Signing key + Substrate breaks its insecure-header callbacks.
		if hasKey {
			return fmt.Errorf("KAGENT_EXECUTION_SIGNING_KEY_FILE requires KAGENT_EXECUTION_PROVIDER=e2b: a signing key makes the runtime-callback endpoint signed-only and Substrate guests send the insecure identity header")
		}
	default:
		return fmt.Errorf("unknown KAGENT_EXECUTION_PROVIDER %q: supported providers are \"substrate\" (default) and \"e2b\"", provider)
	}
	return nil
}

// bootstrapPrepare wraps the privileged pre-launch step: the trusted prepare
// command plus the restrictive contract directory (0700, harness-user owned)
// the credential lands in. An empty command with no contract paths means no
// privileged preparation.
func bootstrapPrepare(cmd string, paths ...string) *e2b.BootstrapPrepare {
	var dirs []string
	seen := map[string]bool{}
	for _, p := range paths {
		d := path.Dir(p)
		if d != "" && d != "/" && d != "." && !seen[d] {
			seen[d] = true
			dirs = append(dirs, d)
		}
	}
	if strings.TrimSpace(cmd) == "" && len(dirs) == 0 {
		return nil
	}
	return &e2b.BootstrapPrepare{Cmd: cmd, SecureDirs: dirs}
}

// e2bReadyProbe polls the sandbox's /readyz through the client proxy — the
// same ingress the gateway dials — until it serves 200 or the caller's context
// ends. Port 80 is the fixed harness ingress port.
func e2bReadyProbe(proxy, sandboxID string) func(ctx context.Context) error {
	url := proxy + "/readyz"
	client := &http.Client{Timeout: 5 * time.Second}
	return func(ctx context.Context) error {
		for {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return err
			}
			req.Header.Set("E2b-Sandbox-Id", sandboxID)
			req.Header.Set("E2b-Sandbox-Port", a2agateway.E2BIngressPort)
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return nil
				}
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("sandbox %s not ready: %w", sandboxID, ctx.Err())
			case <-time.After(time.Second):
			}
		}
	}
}
