// Command goober-attempt executes the one immutable attempt plan carried by a
// Kubernetes Job. It publishes evidence only; the controller remains the sole
// owner of workflow transitions and journal completion events.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/goobers/goobers/internal/app"
	"github.com/goobers/goobers/internal/gooberruntime"
	"github.com/goobers/goobers/internal/invoke"
	"github.com/goobers/goobers/internal/journal"
	"github.com/goobers/goobers/internal/kuberunner"
)

func main() {
	registry, scrubber := journal.DefaultScrubber()
	app.MainWithScrubber("goober-attempt", scrubber, func(ctx context.Context, log *slog.Logger) error {
		return run(ctx, log, registry, scrubber)
	})
}

type attemptConfig struct {
	planEncoded    string
	journalRoot    string
	resultsRoot    string
	claimNamespace string
	harnessCommand []string
	workspaceRoot  string
}

func configFromEnv() attemptConfig {
	return attemptConfig{
		planEncoded:    os.Getenv(kuberunner.AttemptPlanEnv),
		journalRoot:    os.Getenv("GOOBERS_JOURNAL_ROOT"),
		resultsRoot:    envDefault("GOOBERS_RESULTS_ROOT", kuberunner.ResultMountPath),
		claimNamespace: envDefault("GOOBERS_CLAIM_NAMESPACE", kuberunner.DefaultClaimNamespace),
		harnessCommand: strings.Fields(os.Getenv("GOOBERS_AGENT_HARNESS_COMMAND")),
		workspaceRoot:  os.Getenv("GOOBERS_WORKSPACE_ROOT"),
	}
}

func (c attemptConfig) validate() error {
	if c.planEncoded == "" {
		return fmt.Errorf("%s is required", kuberunner.AttemptPlanEnv)
	}
	if c.journalRoot == "" {
		return fmt.Errorf("GOOBERS_JOURNAL_ROOT is required")
	}
	if c.resultsRoot == "" {
		return fmt.Errorf("GOOBERS_RESULTS_ROOT is required")
	}
	return nil
}

func run(ctx context.Context, log *slog.Logger, registry *journal.RegistryScrubber, scrubber journal.Scrubber) error {
	cfg := configFromEnv()
	if err := cfg.validate(); err != nil {
		return err
	}
	plan, err := kuberunner.DecodeAttemptPlan(cfg.planEncoded)
	if err != nil {
		return err
	}
	claims, err := claimStoreForPlan(plan, cfg.claimNamespace)
	if err != nil {
		return err
	}
	results := kuberunner.NewFSResultReader(cfg.resultsRoot)
	worker := &kuberunner.Worker{
		Source:    kuberunner.StaticAttemptPlanSource{Plan: plan},
		Agents:    runtimeAgentResolver{cfg: cfg, registry: registry, scrubber: scrubber},
		Providers: unsupportedProviderResolver{},
		Authority: kuberunner.JournalAttemptAuthority{
			Journal: kuberunner.NewFSJournalStore(cfg.journalRoot), Claims: claims, Namespace: cfg.claimNamespace,
		},
		Results: results, Publisher: results,
	}
	log.Info("executing kubernetes attempt", "attempt", plan.Attempt.String(), "kind", plan.Kind)
	return worker.Execute(ctx)
}

func claimStoreForPlan(plan kuberunner.AttemptPlan, namespace string) (kuberunner.ClaimStore, error) {
	if plan.Attempt.FenceEpoch == 0 {
		return nil, nil
	}
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster claim client: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("register claim scheme: %w", err)
	}
	kubeClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("create claim client: %w", err)
	}
	return &kuberunner.KubeClaimStore{Client: kubeClient, Namespace: namespace}, nil
}

type runtimeAgentResolver struct {
	cfg      attemptConfig
	registry *journal.RegistryScrubber
	scrubber journal.Scrubber
}

func (r runtimeAgentResolver) Agent(context.Context, string) (invoke.Goober, error) {
	if len(r.cfg.harnessCommand) == 0 {
		return nil, fmt.Errorf("GOOBERS_AGENT_HARNESS_COMMAND is required for an agentic attempt")
	}
	return gooberruntime.New(gooberruntime.Options{
		Preparer: gooberruntime.InProcessPreparer{
			WorkspaceRoot: r.cfg.workspaceRoot,
			Providers:     gooberruntime.EnvProviderResolver{SecretRegistrar: r.registry},
		},
		Harness:        gooberruntime.NewCopilotHarness(r.cfg.harnessCommand),
		OutputScrubber: r.scrubber,
	}), nil
}

type unsupportedProviderResolver struct{}

func (unsupportedProviderResolver) ProviderExecutor(context.Context, kuberunner.AttemptPlan) (invoke.Deterministic, error) {
	return nil, fmt.Errorf("provider executor is not configured in the stock worker; inject a fenced in-process provider executor")
}

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
