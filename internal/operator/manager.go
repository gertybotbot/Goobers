package operator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/goobers/goobers/internal/kuberunner"
)

// Options configures the operator manager.
type Options struct {
	// MetricsAddr is the bind address for the metrics endpoint ("0" disables it).
	MetricsAddr string
	// HealthAddr is the bind address for health/readiness probes ("0" disables it).
	HealthAddr string
	// LeaderElection enables leader election for HA operator deployments.
	LeaderElection bool
	// RunsDir is the durable directory holding canonical run journals. Setting
	// it activates the Kubernetes-native runner (internal/kuberunner); leaving
	// it empty leaves the operator reconciling Gaggles only.
	//
	// The native runner is opt-in precisely because the journal is authoritative:
	// a controller pointed at the wrong (or an empty) journal root would project
	// nonsense over real runs, so it must be named explicitly rather than
	// defaulted into existence.
	RunsDir string
	// ResultsDir is where attempt Jobs publish result receipts. Defaults to a
	// "results" sibling of RunsDir.
	ResultsDir string
	// WorkerImage overrides the attempt container image.
	WorkerImage string
	// ClaimNamespace is the single namespace containing retained business-claim
	// records. It must be shared by every watched GooberRun namespace.
	ClaimNamespace string
	// CredentialBindings maps stage capability grants to scoped Secret keys for
	// source-backed attempt Jobs. Only a plan's declared grants are projected.
	CredentialBindings map[string]kuberunner.CredentialSecretBinding
	// GooberCapabilities supplies agentic reviewer gates with the referenced
	// goober definition's declared grants.
	GooberCapabilities map[string][]string
	// NotificationSinks enables journal-derived durable notification requests
	// for the named registered sink kinds. Empty disables projection.
	NotificationSinks []string
	// NotificationTTL bounds delivery eligibility from the source event time.
	NotificationTTL time.Duration
	// WakeGossip receives advisory post-publication hints. Receivers must reread
	// the journal; the hint is never workflow authority.
	WakeGossip kuberunner.WakeGossip
}

// DefaultOptions returns sane defaults for running in-cluster.
func DefaultOptions() Options {
	return Options{MetricsAddr: ":8080", HealthAddr: ":8081", LeaderElection: false, ClaimNamespace: kuberunner.DefaultClaimNamespace}
}

// Run builds the manager, wires the Gaggle reconciler, and blocks until ctx is
// cancelled. It is the body invoked from cmd/operator via internal/app.Main.
func Run(ctx context.Context, logger *slog.Logger, opts Options) error {
	ctrl.SetLogger(logr.FromSlogHandler(logger.Handler()))

	scheme, err := NewScheme()
	if err != nil {
		return fmt.Errorf("build scheme: %w", err)
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}

	mgr, err := ctrl.NewManager(cfg, manager.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: opts.MetricsAddr},
		HealthProbeBindAddress: opts.HealthAddr,
		LeaderElection:         opts.LeaderElection,
		LeaderElectionID:       "goobers-operator.goobers.dev",
	})
	if err != nil {
		return fmt.Errorf("create manager: %w", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add healthz: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add readyz: %w", err)
	}

	reconciler := &GaggleReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Registrar: NoopRegistrar{Log: logger},
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup gaggle reconciler: %w", err)
	}

	if err := setupKubeRunner(mgr, logger, opts); err != nil {
		return err
	}

	logger.Info("operator manager starting")
	return mgr.Start(ctx)
}

// setupKubeRunner wires the Kubernetes-native runner when a journal root is
// configured.
//
// This deliberately does NOT touch internal/engine: the Temporal runner stays
// quarantined, and nothing here starts a Temporal client, a task-queue worker,
// or a Postgres connection. Workflow authority remains the same plain-file
// journal the local runner already owns; retained ConfigMaps provide only the
// atomic business-claim ledger and monotonic fencing epochs.
func setupKubeRunner(mgr manager.Manager, logger *slog.Logger, opts Options) error {
	if opts.RunsDir == "" {
		logger.Info("kubernetes-native runner disabled (no runs directory configured)")
		return nil
	}

	resultsDir := opts.ResultsDir
	if resultsDir == "" {
		resultsDir = filepath.Join(filepath.Dir(opts.RunsDir), "results")
	}

	store := kuberunner.NewFSJournalStore(opts.RunsDir)
	claimNamespace := opts.ClaimNamespace
	if claimNamespace == "" {
		claimNamespace = kuberunner.DefaultClaimNamespace
	}

	runReconciler := &kuberunner.RunReconciler{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		Journal:            store,
		Results:            kuberunner.NewFSResultReader(resultsDir),
		Machine:            kuberunner.StaticMachineResolver{},
		Claims:             &kuberunner.KubeClaimStore{Client: mgr.GetClient(), Namespace: claimNamespace},
		ClaimNamespace:     claimNamespace,
		CredentialBindings: opts.CredentialBindings,
		GooberCapabilities: opts.GooberCapabilities,
		WorkerImage:        opts.WorkerImage,
	}
	if len(opts.NotificationSinks) > 0 {
		runReconciler.Notifications = &kuberunner.JournalNotificationProjector{
			Journal: store,
			Policy: kuberunner.NotificationProjectionPolicy{
				Sinks: append([]string(nil), opts.NotificationSinks...),
				TTL:   opts.NotificationTTL,
			},
			Gossip: opts.WakeGossip,
		}
	}
	if err := runReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup gooberrun reconciler: %w", err)
	}

	actionReconciler := &kuberunner.ActionReconciler{
		Client:  mgr.GetClient(),
		Scheme:  mgr.GetScheme(),
		Journal: store,
	}
	if err := actionReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup gooberrunaction reconciler: %w", err)
	}

	logger.Info("kubernetes-native runner enabled",
		"runsDir", opts.RunsDir, "resultsDir", resultsDir, "claimNamespace", claimNamespace)
	return nil
}
